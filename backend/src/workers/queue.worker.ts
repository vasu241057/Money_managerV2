import { QUEUE_NAMES } from '../lib/infra';
import { PoisonMessageError, TransientMessageError } from './queue.errors';
import { runEmailSyncDispatchJob } from './email-sync-dispatcher';
import { runEmailSyncUserJob } from './email-sync-fetcher';
import { runNormalizeRawEmailsJob } from './email-normalizer';
import {
	parseAiClassificationJob,
	parseEmailSyncDispatchJob,
	parseEmailSyncUserJob,
	parseNormalizeRawEmailsJob,
} from './queue.messages';

const MAX_PROCESSING_ATTEMPTS = 5;

function retryDelaySeconds(attempts: number): number {
	return Math.min(60, 2 ** Math.max(0, attempts));
}

async function handleEmailSyncDispatch(body: unknown, env: Env): Promise<void> {
	const dispatchJob = parseEmailSyncDispatchJob(body);
	const result = await runEmailSyncDispatchJob(dispatchJob, env);

	console.info('Processing EMAIL_SYNC_DISPATCH queue job', {
		cron: dispatchJob.cron,
		scheduledTime: dispatchJob.scheduled_time,
		triggeredAt: dispatchJob.triggered_at,
		pageCount: result.page_count,
		scannedUsers: result.scanned_user_count,
		enqueuedUserJobs: result.enqueued_user_job_count,
		failedUserJobs: result.failed_user_job_count,
		queueBatches: result.queue_batch_count,
		markedDormantUsers: result.marked_dormant_user_count,
		reactivatedUsers: result.reactivated_user_count,
		continuationOffset: result.continuation_offset,
		scanUpperUserId: result.scan_upper_user_id,
	});
}

async function handleEmailSyncUser(body: unknown, env: Env): Promise<void> {
	const syncJob = parseEmailSyncUserJob(body);
	const result = await runEmailSyncUserJob(syncJob, env);

	console.info('Processing EMAIL_SYNC_USER queue job', {
		userId: syncJob.user_id,
		lastSyncTimestamp: syncJob.last_sync_timestamp,
		connectionCount: result.connection_count,
		processedConnectionCount: result.processed_connection_count,
		revokedConnectionCount: result.revoked_connection_count,
		fetchedMessageCount: result.fetched_message_count,
		insertedOrRetriedRawEmailCount: result.inserted_or_retried_raw_email_count,
		skippedExistingRawEmailCount: result.skipped_existing_raw_email_count,
	});
}

async function handleNormalizeRawEmails(body: unknown, env: Env): Promise<void> {
	const normalizeJob = parseNormalizeRawEmailsJob(body);
	const result = await runNormalizeRawEmailsJob(normalizeJob, env);

	console.info('Processing NORMALIZE_RAW_EMAILS queue job', {
		requestedRawEmailCount: result.requested_raw_email_count,
		processedRawEmailCount: result.processed_raw_email_count,
		ignoredRawEmailCount: result.ignored_raw_email_count,
		unrecognizedRawEmailCount: result.unrecognized_raw_email_count,
		skippedRawEmailCount: result.skipped_raw_email_count,
		createdTransactionCount: result.created_transaction_count,
		needsReviewTransactionCount: result.needs_review_transaction_count,
		aiEnqueuedCount: result.ai_enqueued_count,
	});
}

function handleAiClassification(body: unknown): void {
	const aiJob = parseAiClassificationJob(body);

	console.info('Processing AI_CLASSIFICATION queue job', {
		transactionId: aiJob.transaction_id,
		requestedAt: aiJob.requested_at,
	});
}

async function processMessage(queueName: string, body: unknown, env: Env): Promise<void> {
	switch (queueName) {
		case QUEUE_NAMES.EMAIL_SYNC: {
			const jobType = (body as { job_type?: unknown } | null | undefined)?.job_type;
			if (jobType === 'EMAIL_SYNC_DISPATCH') {
				await handleEmailSyncDispatch(body, env);
				return;
			}

			if (jobType === 'EMAIL_SYNC_USER') {
				await handleEmailSyncUser(body, env);
				return;
			}

			if (jobType === 'NORMALIZE_RAW_EMAILS') {
				await handleNormalizeRawEmails(body, env);
				return;
			}

			throw new PoisonMessageError('Invalid EMAIL_SYNC payload');
		}
		case QUEUE_NAMES.AI_CLASSIFICATION:
			handleAiClassification(body);
			return;
		default:
			throw new TransientMessageError(`Unsupported queue name: ${queueName}`);
	}
}

export async function handleQueue(
	batch: MessageBatch<unknown>,
	env: Env,
	_ctx: ExecutionContext,
): Promise<void> {
	for (const message of batch.messages) {
		try {
			await processMessage(batch.queue, message.body, env);
			message.ack();
		} catch (error) {
			const errorMessage = error instanceof Error ? error.message : 'Unknown queue processing error';
			const isPoisonPayload = error instanceof PoisonMessageError;

			if (isPoisonPayload) {
				console.error('Acknowledging poison queue message', {
					queue: batch.queue,
					messageId: message.id,
					attempts: message.attempts,
					error: errorMessage,
				});
				message.ack();
				continue;
			}

			const exhaustedRetries = message.attempts >= MAX_PROCESSING_ATTEMPTS;
			if (exhaustedRetries) {
				console.error('Final retry attempt failed; delegating dead-letter routing to Cloudflare', {
					queue: batch.queue,
					messageId: message.id,
					attempts: message.attempts,
					error: errorMessage,
				});
				message.retry();
				continue;
			}

			const delaySeconds = retryDelaySeconds(message.attempts);
			console.warn('Retrying queue message after transient handler error', {
				queue: batch.queue,
				messageId: message.id,
				attempts: message.attempts,
				delaySeconds,
				error: errorMessage,
			});
			message.retry({ delaySeconds });
		}
	}
}
