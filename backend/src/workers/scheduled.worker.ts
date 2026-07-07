import { EMAIL_SYNC_CRON, SENDER_CLASSIFICATION_CRON } from '../lib/infra';
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
		const mappingJob = buildMerchantMappingJob();
		const enqueueDailyJobs = Promise.all([
			env.EMAIL_SYNC_QUEUE.send(classificationJob, { contentType: 'json' }),
			env.EMAIL_SYNC_QUEUE.send(mappingJob, { contentType: 'json' }),
		]);
		ctx.waitUntil(enqueueDailyJobs);
		await enqueueDailyJobs;

		console.info('Scheduled trigger received', {
			cron: controller.cron,
			scheduledTime: controller.scheduledTime,
			enqueuedJobTypes: [classificationJob.job_type, mappingJob.job_type],
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
