import { QUEUE_NAMES } from '../lib/infra';
import { PoisonMessageError, TransientMessageError } from './queue.errors';
import { runAiClassificationJob } from './ai-classifier';
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
const DEFAULT_QUEUE_ALERT_RETRY_RATE_PERCENT = 15;
const DEFAULT_QUEUE_ALERT_POISON_ACK_COUNT = 3;
const DEFAULT_QUEUE_ALERT_FINAL_RETRY_COUNT = 1;

interface QueueAlertThresholds {
	retry_rate_percent: number;
	poison_ack_count: number;
	final_retry_count: number;
}

interface QueueBatchMetrics {
	total_messages: number;
	acked_messages: number;
	poison_acked_messages: number;
	retried_messages: number;
	final_retry_messages: number;
	max_attempts_seen: number;
}

function retryDelaySeconds(attempts: number): number {
	return Math.min(60, 2 ** Math.max(0, attempts));
}

function parseNonNegativeNumber(value: unknown, fallback: number): number {
	if (typeof value !== 'string') {
		return fallback;
	}

	const normalized = value.trim();
	if (normalized.length === 0) {
		return fallback;
	}

	const parsed = Number(normalized);
	if (!Number.isFinite(parsed) || parsed < 0) {
		return fallback;
	}

	return parsed;
}

function parseNonNegativeInteger(value: unknown, fallback: number): number {
	const parsed = parseNonNegativeNumber(value, fallback);
	return Number.isSafeInteger(parsed) ? parsed : fallback;
}

function resolveQueueAlertThresholds(env: Env): QueueAlertThresholds {
	return {
		retry_rate_percent: parseNonNegativeNumber(
			env.QUEUE_ALERT_RETRY_RATE_PERCENT,
			DEFAULT_QUEUE_ALERT_RETRY_RATE_PERCENT,
		),
		poison_ack_count: parseNonNegativeInteger(
			env.QUEUE_ALERT_POISON_ACK_COUNT,
			DEFAULT_QUEUE_ALERT_POISON_ACK_COUNT,
		),
		final_retry_count: parseNonNegativeInteger(
			env.QUEUE_ALERT_FINAL_RETRY_COUNT,
			DEFAULT_QUEUE_ALERT_FINAL_RETRY_COUNT,
		),
	};
}

function createQueueBatchMetrics(messageCount: number): QueueBatchMetrics {
	return {
		total_messages: messageCount,
		acked_messages: 0,
		poison_acked_messages: 0,
		retried_messages: 0,
		final_retry_messages: 0,
		max_attempts_seen: 0,
	};
}

function computeRetryRatePercent(metrics: QueueBatchMetrics): number {
	if (metrics.total_messages === 0) {
		return 0;
	}

	return Number(
		((metrics.retried_messages / metrics.total_messages) * 100).toFixed(2),
	);
}

function isQueueAlertThresholdBreached(
	metrics: QueueBatchMetrics,
	thresholds: QueueAlertThresholds,
	retryRatePercent: number,
): boolean {
	return (
		retryRatePercent >= thresholds.retry_rate_percent ||
		metrics.poison_acked_messages >= thresholds.poison_ack_count ||
		metrics.final_retry_messages >= thresholds.final_retry_count
	);
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

async function handleAiClassification(body: unknown, env: Env): Promise<void> {
	const aiJob = parseAiClassificationJob(body);
	const result = await runAiClassificationJob(aiJob, env);

	console.info('Processing AI_CLASSIFICATION queue job', {
		transactionId: aiJob.transaction_id,
		requestedAt: aiJob.requested_at,
		outcome: result.outcome,
		transactionStatus: result.transaction_status,
		confidenceScore: result.confidence_score,
		action: result.action,
		reason: result.reason,
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
			await handleAiClassification(body, env);
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
	const metrics = createQueueBatchMetrics(batch.messages.length);

	for (const message of batch.messages) {
		metrics.max_attempts_seen = Math.max(metrics.max_attempts_seen, message.attempts);
		try {
			await processMessage(batch.queue, message.body, env);
			message.ack();
			metrics.acked_messages += 1;
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
				metrics.acked_messages += 1;
				metrics.poison_acked_messages += 1;
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
				metrics.retried_messages += 1;
				metrics.final_retry_messages += 1;
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
			metrics.retried_messages += 1;
		}
	}

	const retryRatePercent = computeRetryRatePercent(metrics);
	console.info('QUEUE_BATCH_SUMMARY', {
		queue: batch.queue,
		...metrics,
		retry_rate_percent: retryRatePercent,
	});

	const alertThresholds = resolveQueueAlertThresholds(env);
	if (isQueueAlertThresholdBreached(metrics, alertThresholds, retryRatePercent)) {
		console.error('QUEUE_ALERT_THRESHOLD_BREACH', {
			queue: batch.queue,
			...metrics,
			retry_rate_percent: retryRatePercent,
			alert_thresholds: alertThresholds,
		});
	}
}
