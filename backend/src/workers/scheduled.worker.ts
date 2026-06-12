import { EMAIL_SYNC_CRON } from '../lib/infra';
import { buildEmailSyncDispatchJob } from './queue.messages';

export async function handleScheduled(
	controller: ScheduledController,
	env: Env,
	ctx: ExecutionContext,
): Promise<void> {
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
