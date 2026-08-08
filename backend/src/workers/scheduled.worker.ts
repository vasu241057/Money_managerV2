import type { ScheduledJobName } from '../../../shared/types';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { isMerchantMappingKillSwitched } from '../lib/feature-flags';
import { EMAIL_SYNC_CRON, SENDER_CLASSIFICATION_CRON } from '../lib/infra';
import {
	claimDueScheduledJobs,
	releaseScheduledJobClaim,
	type ClaimedJob,
} from '../services/job-schedules.service';
import {
	buildEmailSyncDispatchJob,
	buildMerchantMappingJob,
	buildSenderClassificationJob,
} from './queue.messages';

function buildJobFor(job: ScheduledJobName) {
	return job === 'merchant_mapping'
		? buildMerchantMappingJob()
		: buildSenderClassificationJob();
}

/**
 * Enqueues every nightly job whose scheduled local time has arrived.
 *
 * Cron triggers are deploy-time configuration, so the schedule itself lives in
 * the database and every tick asks "is anything due?". The claim is atomic, so
 * it is safe for both crons to call this — including the second where they
 * overlap. A failed queue send hands the day back rather than silently
 * swallowing the run.
 */
async function dispatchDueScheduledJobs(env: Env): Promise<string[]> {
	const eligible: ScheduledJobName[] = ['sender_classification'];
	if (!isMerchantMappingKillSwitched(env)) eligible.push('merchant_mapping');

	const sql = getSqlClient(getAppConfig(env));
	const claimed = await claimDueScheduledJobs(sql, eligible);
	if (claimed.length === 0) return [];

	const dispatched: string[] = [];
	const released: ClaimedJob[] = [];
	await Promise.all(
		claimed.map(async claim => {
			const job = buildJobFor(claim.job);
			try {
				await env.EMAIL_SYNC_QUEUE.send(job, { contentType: 'json' });
				dispatched.push(job.job_type);
			} catch (error) {
				released.push(claim);
				console.error('SCHEDULED_JOB_ENQUEUE_FAILED', {
					job: claim.job,
					message: error instanceof Error ? error.message : 'unknown',
				});
			}
		}),
	);

	for (const claim of released) {
		try {
			await releaseScheduledJobClaim(sql, claim);
		} catch (error) {
			// The day stays consumed; the next day's tick recovers on its own.
			console.error('SCHEDULED_JOB_RELEASE_FAILED', {
				job: claim.job,
				message: error instanceof Error ? error.message : 'unknown',
			});
		}
	}

	return dispatched;
}

export async function handleScheduled(
	controller: ScheduledController,
	env: Env,
	ctx: ExecutionContext,
): Promise<void> {
	const isEmailSyncTick = controller.cron === EMAIL_SYNC_CRON;
	if (!isEmailSyncTick && controller.cron !== SENDER_CLASSIFICATION_CRON) {
		console.warn('Cron expression drift detected between runtime trigger and code constant', {
			receivedCron: controller.cron,
			expectedCron: EMAIL_SYNC_CRON,
		});
	}

	const enqueuedJobTypes: string[] = [];

	// The 10-minute tick keeps its own job: email sync must not wait on the
	// schedule lookup, and a database failure below must not stop it.
	if (isEmailSyncTick || controller.cron !== SENDER_CLASSIFICATION_CRON) {
		const dispatchJob = buildEmailSyncDispatchJob(controller);
		const enqueuePromise = env.EMAIL_SYNC_QUEUE.send(dispatchJob, {
			contentType: 'json',
		});
		ctx.waitUntil(enqueuePromise);
		await enqueuePromise;
		enqueuedJobTypes.push(dispatchJob.job_type);
	}

	// Every tick re-evaluates the schedule, so one failed lookup costs at most
	// one tick — and must never take the email sync dispatch down with it.
	const dueJobs = dispatchDueScheduledJobs(env).catch((error: unknown) => {
		console.error('SCHEDULED_JOB_CLAIM_FAILED', {
			cron: controller.cron,
			message: error instanceof Error ? error.message : 'unknown',
		});
		return [] as string[];
	});
	ctx.waitUntil(dueJobs);
	enqueuedJobTypes.push(...(await dueJobs));

	console.info('Scheduled trigger received', {
		cron: controller.cron,
		scheduledTime: controller.scheduledTime,
		enqueuedJobTypes,
	});
}
