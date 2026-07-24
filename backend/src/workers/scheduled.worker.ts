import { EMAIL_SYNC_CRON, SENDER_CLASSIFICATION_CRON } from '../lib/infra';
import { isMerchantMappingEnabled } from '../lib/feature-flags';
import {
	buildEmailSyncDispatchJob,
	buildMerchantMappingJob,
	buildSenderClassificationJob,
} from './queue.messages';

export async function handleScheduled(
	controller: ScheduledController,
	env: Env,
	ctx: ExecutionContext,
): Promise<void> {
	if (controller.cron === SENDER_CLASSIFICATION_CRON) {
		const classificationJob = buildSenderClassificationJob();
		const dailyJobs = [
			env.EMAIL_SYNC_QUEUE.send(classificationJob, { contentType: 'json' }),
		];
		const enqueuedJobTypes: string[] = [classificationJob.job_type];
		const merchantMappingEnabled = isMerchantMappingEnabled(env);

		if (merchantMappingEnabled) {
			const mappingJob = buildMerchantMappingJob();
			dailyJobs.push(
				env.EMAIL_SYNC_QUEUE.send(mappingJob, { contentType: 'json' }),
			);
			enqueuedJobTypes.push(mappingJob.job_type);
		}

		const enqueueDailyJobs = Promise.all(dailyJobs);
		ctx.waitUntil(enqueueDailyJobs);
		await enqueueDailyJobs;

		console.info('Scheduled trigger received', {
			cron: controller.cron,
			scheduledTime: controller.scheduledTime,
			enqueuedJobTypes,
			merchantMappingEnabled,
		});
		return;
	}

	if (controller.cron !== EMAIL_SYNC_CRON) {
		console.warn('Cron expression drift detected between runtime trigger and code constant', {
			receivedCron: controller.cron,
			expectedCron: EMAIL_SYNC_CRON,
		});
	}

	const dispatchJob = buildEmailSyncDispatchJob(controller);
	const enqueuePromise = env.EMAIL_SYNC_QUEUE.send(dispatchJob, {
		contentType: 'json',
	});

	ctx.waitUntil(enqueuePromise);
	await enqueuePromise;

	console.info('Scheduled trigger received', {
		cron: controller.cron,
		scheduledTime: controller.scheduledTime,
		enqueuedJobType: dispatchJob.job_type,
	});
}
