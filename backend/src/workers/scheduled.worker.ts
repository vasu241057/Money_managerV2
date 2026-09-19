import type { ScheduledJobName } from '../../../shared/types';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { reportErrorInBackground } from '../lib/error-reporting';
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
	buildRawEmailBodyPurgeJob,
	buildSenderClassificationJob,
} from './queue.messages';
import { resolveNightlyQueue } from './queue.producers';

interface StalePendingRow {
	user_count: unknown;
	stale_count: unknown;
	oldest_created_at: unknown;
	snapshot_at: unknown;
	snapshot_age_seconds: unknown;
}

/**
 * Past this, the view is not being refreshed and its numbers describe some
 * earlier moment — worth its own line, because alerting on frozen counts is
 * worse than not alerting at all. The refresh runs every 10 minutes.
 */
const STALE_SNAPSHOT_MAX_AGE_SECONDS = 3600;

/**
 * Google's ceiling for an unverified app using a restricted scope
 * (`gmail.readonly`): 100 test users on the OAuth consent screen. User 101
 * signs up fine and then fails at the Google consent step, because signup is
 * not gated — see A.1 §6 and decision D10. Task 8's first bullet is the fix;
 * this is the tripwire until verification lands.
 */
const GOOGLE_UNVERIFIED_TEST_USER_CAP = 100;
/** Warn from 80 of 100, which is enough notice to act on a months-long process. */
const GOOGLE_TEST_USER_WARN_THRESHOLD = 80;
/** Quiet hour; the check runs on the first tick of this UTC hour. */
const CAPACITY_CHECK_UTC_HOUR = 2;

/** True on the first tick of each hour, given the 10-minute trigger. */
function isHourlyNotifyTick(scheduledTime: number): boolean {
	return new Date(scheduledTime).getUTCMinutes() < 10;
}

/**
 * True on exactly one tick per day, given the 10-minute trigger the infra
 * contract test pins. A capacity warning is a "start the paperwork" signal,
 * not an incident: logging it 144 times a day would make it wallpaper.
 */
function isDailyCapacityTick(scheduledTime: number): boolean {
	const tickAt = new Date(scheduledTime);
	return (
		tickAt.getUTCHours() === CAPACITY_CHECK_UTC_HOUR && tickAt.getUTCMinutes() < 10
	);
}

/**
 * Counts users who have connected Gmail and warns as that approaches
 * Google's unverified-app cap.
 *
 * The number is a lower bound, deliberately. Google counts entries on the
 * consent screen's test-user list, and a deleted account's connection row
 * cascades away here while its entry on that list stays until the operator
 * removes it. So this can only under-report; the authoritative count is in
 * the Google Cloud console (OPERATIONS.md).
 */
export async function reportGoogleTestUserCapacity(
	env: Env,
	ctx?: Pick<ExecutionContext, 'waitUntil'>,
): Promise<void> {
	const sql = getSqlClient(getAppConfig(env));
	const rows = await sql<{ connected_user_count: unknown }[]>`
		select count(distinct oc.user_id)::int as connected_user_count
		from public.oauth_connections as oc
		where oc.provider = 'google'
	`;

	const connectedUserCount = Number(rows[0]?.connected_user_count ?? 0);
	if (!Number.isFinite(connectedUserCount)) return;

	if (connectedUserCount < GOOGLE_TEST_USER_WARN_THRESHOLD) {
		console.info('GOOGLE_TEST_USER_CAPACITY', {
			connectedUserCount,
			cap: GOOGLE_UNVERIFIED_TEST_USER_CAP,
		});
		return;
	}

	console.error('GOOGLE_TEST_USER_CAP_WARNING', {
		connectedUserCount,
		cap: GOOGLE_UNVERIFIED_TEST_USER_CAP,
		warnThreshold: GOOGLE_TEST_USER_WARN_THRESHOLD,
		// The Google-side list is the real ceiling; this count cannot see it.
		countIsLowerBound: true,
	});

	reportErrorInBackground(
		env,
		{
			event: 'GOOGLE_TEST_USER_CAP_WARNING',
			severity: 'warning',
			message: `${connectedUserCount} of ${GOOGLE_UNVERIFIED_TEST_USER_CAP} Google test-user slots used (lower bound)`,
			context: {
				connectedUserCount,
				cap: GOOGLE_UNVERIFIED_TEST_USER_CAP,
			},
		},
		ctx,
	);
}

function buildJobFor(job: ScheduledJobName) {
	switch (job) {
		case 'merchant_mapping':
			return buildMerchantMappingJob();
		case 'raw_email_body_purge':
			return buildRawEmailBodyPurgeJob();
		default:
			return buildSenderClassificationJob();
	}
}

/**
 * Reads the stale-extraction materialized view and logs one line when it has
 * rows (Task 7). pg_cron refreshes that view on the same 10-minute cadence as
 * this tick, so the snapshot is at most one cycle old. This is the alert
 * source for mail that arrived but never became a transaction — the failure
 * no queue metric shows, because nothing retried and nothing errored.
 */
export async function reportStalePendingExtraction(
	env: Env,
	options: { ctx?: Pick<ExecutionContext, 'waitUntil'>; notify?: boolean } = {},
): Promise<void> {
	const sql = getSqlClient(getAppConfig(env));
	const rows = await sql<StalePendingRow[]>`
		select
			count(*) filter (where mv.stale_count > 0)::int as user_count,
			coalesce(sum(mv.stale_count) filter (where mv.stale_count > 0), 0)::bigint as stale_count,
			min(mv.oldest_created_at) filter (where mv.stale_count > 0) as oldest_created_at,
			max(mv.snapshot_at) as snapshot_at,
			extract(epoch from (now() - max(mv.snapshot_at)))::bigint as snapshot_age_seconds
		from public.mv_stale_pending_extraction as mv
	`;
	const row = rows[0];
	if (!row) return;

	// An empty view has no snapshot timestamp at all, which is the healthy
	// "nothing is stale" case, not a broken refresh.
	const snapshotAgeSeconds =
		row.snapshot_age_seconds === null || row.snapshot_age_seconds === undefined
			? null
			: Number(row.snapshot_age_seconds);
	if (snapshotAgeSeconds !== null && snapshotAgeSeconds > STALE_SNAPSHOT_MAX_AGE_SECONDS) {
		// pg_cron is not running refresh_mv_stale_pending_extraction; the
		// counts below would describe whenever it last did.
		console.error('STALE_PENDING_EXTRACTION_SNAPSHOT_STALE', {
			snapshotAt: row.snapshot_at,
			snapshotAgeSeconds,
		});
		if (options.notify) {
			reportErrorInBackground(
				env,
				{
					event: 'STALE_PENDING_EXTRACTION_SNAPSHOT_STALE',
					message: 'pg_cron is not refreshing mv_stale_pending_extraction',
					context: { snapshotAgeSeconds },
				},
				options.ctx,
			);
		}
		return;
	}

	const userCount = Number(row.user_count);
	if (userCount === 0) return;

	const staleCount = Number(row.stale_count);
	console.error('STALE_PENDING_EXTRACTION', {
		userCount,
		staleCount,
		oldestCreatedAt: row.oldest_created_at,
		snapshotAt: row.snapshot_at,
		snapshotAgeSeconds,
	});

	if (options.notify) {
		reportErrorInBackground(
			env,
			{
				event: 'STALE_PENDING_EXTRACTION',
				message: `${staleCount} email(s) across ${userCount} user(s) stuck before extraction`,
				context: { userCount, staleCount },
			},
			options.ctx,
		);
	}
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
async function dispatchDueScheduledJobs(
	env: Env,
	ctx?: Pick<ExecutionContext, 'waitUntil'>,
): Promise<string[]> {
	const eligible: ScheduledJobName[] = ['sender_classification', 'raw_email_body_purge'];
	if (!isMerchantMappingKillSwitched(env)) eligible.push('merchant_mapping');

	const sql = getSqlClient(getAppConfig(env));
	const claimed = await claimDueScheduledJobs(sql, eligible);
	if (claimed.length === 0) return [];

	// Task 8: nightly work goes to its own queue, so a batch job that runs
	// for minutes cannot sit in front of a user's email sync.
	const nightlyQueue = resolveNightlyQueue(env);
	const dispatched: string[] = [];
	const released: ClaimedJob[] = [];
	await Promise.all(
		claimed.map(async claim => {
			const job = buildJobFor(claim.job);
			try {
				await nightlyQueue.send(job, { contentType: 'json' });
				dispatched.push(job.job_type);
			} catch (error) {
				released.push(claim);
				const message = error instanceof Error ? error.message : 'unknown';
				console.error('SCHEDULED_JOB_ENQUEUE_FAILED', {
					job: claim.job,
					message,
				});
				reportErrorInBackground(
					env,
					{
						event: 'SCHEDULED_JOB_ENQUEUE_FAILED',
						message,
						context: { job: claim.job },
					},
					ctx,
				);
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
	const dueJobs = dispatchDueScheduledJobs(env, ctx).catch((error: unknown) => {
		const message = error instanceof Error ? error.message : 'unknown';
		console.error('SCHEDULED_JOB_CLAIM_FAILED', {
			cron: controller.cron,
			message,
		});
		reportErrorInBackground(
			env,
			{
				event: 'SCHEDULED_JOB_CLAIM_FAILED',
				message,
				context: { cron: controller.cron },
			},
			ctx,
		);
		return [] as string[];
	});
	ctx.waitUntil(dueJobs);
	enqueuedJobTypes.push(...(await dueJobs));

	// Observability only: a failed read must never cost a tick. The console
	// line goes out every tick; the webhook fires at most hourly, because a
	// stuck backlog stays stuck for hours and an alert channel that repeats
	// every ten minutes stops being read.
	ctx.waitUntil(
		reportStalePendingExtraction(env, {
			ctx,
			notify: isHourlyNotifyTick(controller.scheduledTime),
		}).catch((error: unknown) => {
			console.error('STALE_PENDING_EXTRACTION_CHECK_FAILED', {
				cron: controller.cron,
				message: error instanceof Error ? error.message : 'unknown',
			});
		}),
	);

	// Once a day: how close the Google test-user list is to its cap (Task 8).
	if (isDailyCapacityTick(controller.scheduledTime)) {
		ctx.waitUntil(
			reportGoogleTestUserCapacity(env, ctx).catch((error: unknown) => {
				console.error('GOOGLE_TEST_USER_CAPACITY_CHECK_FAILED', {
					cron: controller.cron,
					message: error instanceof Error ? error.message : 'unknown',
				});
			}),
		);
	}

	console.info('Scheduled trigger received', {
		cron: controller.cron,
		scheduledTime: controller.scheduledTime,
		enqueuedJobTypes,
	});
}
