import { getAppConfig } from '../lib/config';
import { runWithSqlClientLifecycle } from '../lib/db/client';
import { reportErrorInBackground } from '../lib/error-reporting';
import { isMerchantMappingKillSwitched } from '../lib/feature-flags';
import { isNightlyJobType, QUEUE_NAMES } from '../lib/infra';
import { PoisonMessageError, TransientMessageError } from './queue.errors';
import { runEmailSyncDispatchJob } from './email-sync-dispatcher';
import {
	releaseEmailSyncJobLeases,
	runEmailSyncUserJob,
	runSenderBackfillJob,
} from './email-sync-fetcher';
import { runNormalizeRawEmailsJob } from './email-ai-normalizer';
import { runMerchantMappingJob } from './merchant-mapper';
import { runRawEmailBodyPurgeJob } from './raw-email-body-purge';
import { runSenderClassificationJob } from './sender-classifier';
import {
	isEmailSyncUserJob,
	parseEmailSyncDispatchJob,
	parseEmailSyncUserJob,
	parseMerchantMappingJob,
	parseNormalizeRawEmailsJob,
	parseRawEmailBodyPurgeJob,
	parseSenderBackfillJob,
	parseSenderClassificationJob,
} from './queue.messages';

// Cloudflare's max_retries=5 means one initial delivery plus five retries.
const MAX_PROCESSING_ATTEMPTS = 6;
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
	/** Dead letters logged and acked; a subset of acked_messages. */
	dead_letter_messages: number;
	max_attempts_seen: number;
}

function retryDelaySeconds(attempts: number): number {
	return Math.min(60, 2 ** Math.max(0, attempts));
}

function resolveRetryDelaySeconds(error: unknown, attempts: number): number {
	if (
		error instanceof TransientMessageError &&
		error.retryAfterSeconds !== null
	) {
		const jitter =
			error.retryJitterSeconds > 0
				? Math.floor(Math.random() * (error.retryJitterSeconds + 1))
				: 0;
		return error.retryAfterSeconds + jitter;
	}

	return retryDelaySeconds(attempts);
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
		dead_letter_messages: 0,
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
		quotaSkippedUsers: result.quota_skipped_user_count,
		continuationOffset: result.continuation_offset,
		scanUpperUserId: result.scan_upper_user_id,
	});
}

async function handleEmailSyncUser(body: unknown, env: Env): Promise<void> {
	const syncJob = parseEmailSyncUserJob(body);
	const result = await runEmailSyncUserJob(syncJob, env);

	console.info('Processing EMAIL_SYNC_USER queue job', {
		userId: syncJob.user_id,
		syncRunId: syncJob.sync_run_id ?? null,
		lastSyncTimestamp: syncJob.last_sync_timestamp,
		continuationConnectionId: syncJob.continuation_connection_id ?? null,
		continuationPageTokenPresent: Boolean(syncJob.continuation_page_token),
		connectionCount: result.connection_count,
		processedConnectionCount: result.processed_connection_count,
		revokedConnectionCount: result.revoked_connection_count,
		fetchedMessageCount: result.fetched_message_count,
		insertedOrRetriedRawEmailCount: result.inserted_or_retried_raw_email_count,
		skippedExistingRawEmailCount: result.skipped_existing_raw_email_count,
		quotaDeferredConnectionCount: result.quota_deferred_connection_count,
		pendingRequeuedCount: result.pending_requeued_count,
	});

	const hasEmailActivity =
		result.fetched_message_count > 0 ||
		result.inserted_or_retried_raw_email_count > 0 ||
		result.skipped_existing_raw_email_count > 0;
	if (hasEmailActivity) {
		console.info('EMAIL_SYNC_ACTIVITY_HIGHLIGHT', {
			userId: syncJob.user_id,
			fetchedMessageCount: result.fetched_message_count,
			insertedOrRetriedRawEmailCount: result.inserted_or_retried_raw_email_count,
			skippedExistingRawEmailCount: result.skipped_existing_raw_email_count,
		});
	}
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
		templateMatchedCount: result.template_matched_count,
		templateLearnedCount: result.template_learned_count,
		templatePromotedCount: result.template_promoted_count,
		quotaDeferredRawEmailCount: result.quota_deferred_raw_email_count,
	});
}

async function handleSenderClassification(body: unknown, env: Env): Promise<void> {
	const job = parseSenderClassificationJob(body);
	const result = await runSenderClassificationJob(job, env);

	console.info('Processing SENDER_CLASSIFICATION queue job', {
		triggeredAt: job.triggered_at,
		candidateCount: result.candidate_count,
		candidateBacklogCount: result.candidate_backlog_count,
		candidateCap: result.candidate_cap,
		classifiedCount: result.classified_count,
		allowRuleCount: result.allow_rule_count,
		denyRuleCount: result.deny_rule_count,
		backfillRawEmailCount: result.backfill_raw_email_count,
		backfillJobCount: result.backfill_job_count,
		skippedReason: result.skipped_reason,
	});
}

async function handleSenderBackfill(body: unknown, env: Env): Promise<void> {
	const job = parseSenderBackfillJob(body);
	const result = await runSenderBackfillJob(job, env);

	console.info('Processing SENDER_BACKFILL queue job', {
		userId: job.user_id,
		requestedCount: result.requested_count,
		refetchedCount: result.refetched_count,
		skippedCount: result.skipped_count,
		normalizeEnqueued: result.normalize_enqueued,
	});
}

async function handleMerchantMapping(body: unknown, env: Env): Promise<void> {
	const job = parseMerchantMappingJob(body);
	const result = await runMerchantMappingJob(job, env);

	console.info('Processing MERCHANT_MAPPING queue job', {
		triggeredAt: job.triggered_at,
		candidateCount: result.candidate_count,
		candidateBacklogCount: result.candidate_backlog_count,
		candidateCap: result.candidate_cap,
		mappedCount: result.mapped_count,
		aliasCreatedCount: result.alias_created_count,
		ownedAliasCreatedCount: result.owned_alias_created_count,
		personFanoutCount: result.person_fanout_count,
		relinkedTransactionCount: result.relinked_transaction_count,
		skippedReason: result.skipped_reason,
	});
}

async function handleRawEmailBodyPurge(body: unknown, env: Env): Promise<void> {
	const job = parseRawEmailBodyPurgeJob(body);
	const result = await runRawEmailBodyPurgeJob(job, env);

	console.info('Processing RAW_EMAIL_BODY_PURGE queue job', {
		triggeredAt: job.triggered_at,
		retentionDays: result.retention_days,
		purgedCount: result.purge.purged_count,
		batchCount: result.purge.batch_count,
		backlogRemaining: result.purge.exhausted,
		clerkRetryPending: result.clerk_retry.pending_count,
		clerkRetryDeleted: result.clerk_retry.deleted_count,
		clerkRetryUserReturned: result.clerk_retry.user_returned_count,
		clerkRetryFailed: result.clerk_retry.failed_count,
		clerkRetrySkippedReason: result.clerk_retry.skipped_reason,
		idempotencyPurgedCount: result.idempotency.deleted_count,
	});
}

/**
 * Dead-letter consumer (Task 7). Cloudflare keeps the message; this exists so
 * a dead job is visible in the logs instead of only in a queue depth metric.
 * Identifiers only — never payload content — and it never throws: a DLQ has
 * no dead-letter queue of its own, so a failure here would lose the message.
 */
function handleDeadLetterMessage(
	queueName: string,
	body: unknown,
	messageId: string,
	attempts: number,
): void {
	const payload = (body ?? {}) as Record<string, unknown>;
	const idOf = (value: unknown): string | null => (typeof value === 'string' ? value : null);

	console.error('DLQ_MESSAGE', {
		queue: queueName,
		messageId,
		// Deliveries of this message on the DLQ, which starts its own count.
		// The job already spent all six attempts on the main queue to get
		// here, so a 1 here does not mean it was only tried once.
		dlqDeliveryAttempts: attempts,
		jobType: idOf(payload.job_type),
		userId: idOf(payload.user_id),
		syncRunId: idOf(payload.sync_run_id),
		connectionId: idOf(payload.continuation_connection_id),
		rawEmailCount: Array.isArray(payload.raw_email_ids) ? payload.raw_email_ids.length : null,
	});
}

function isWorkQueue(queueName: string): boolean {
	return queueName === QUEUE_NAMES.EMAIL_SYNC || queueName === QUEUE_NAMES.NIGHTLY;
}

async function processMessage(queueName: string, body: unknown, env: Env): Promise<void> {
	switch (true) {
		// Both work queues run the same dispatch table (Task 8). The main
		// queue still accepts nightly types so messages enqueued before the
		// split — and anything the producer fallback routed there — drain
		// normally; the nightly queue accepts the rest for the same reason.
		case isWorkQueue(queueName): {
			const jobType = (body as { job_type?: unknown } | null | undefined)?.job_type;

			if (queueName === QUEUE_NAMES.NIGHTLY && !isNightlyJobType(jobType)) {
				// Handled anyway — routing is not correctness — but a
				// standing stream of these means a producer is misrouting.
				console.warn('QUEUE_ROUTING_UNEXPECTED', {
					queue: queueName,
					jobType: typeof jobType === 'string' ? jobType : null,
				});
			}

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

			if (jobType === 'SENDER_CLASSIFICATION') {
				await handleSenderClassification(body, env);
				return;
			}

			if (jobType === 'SENDER_BACKFILL') {
				await handleSenderBackfill(body, env);
				return;
			}

			if (jobType === 'RAW_EMAIL_BODY_PURGE') {
				await handleRawEmailBodyPurge(body, env);
				return;
			}

			if (jobType === 'MERCHANT_MAPPING') {
				if (isMerchantMappingKillSwitched(env)) {
					console.warn('Skipping MERCHANT_MAPPING queue job: the env kill switch is set');
					return;
				}

				await handleMerchantMapping(body, env);
				return;
			}

			throw new PoisonMessageError(`Invalid ${queueName} payload`);
		}
		default:
			throw new TransientMessageError(`Unsupported queue name: ${queueName}`);
	}
}

function isDeadLetterQueue(queueName: string): boolean {
	return queueName === QUEUE_NAMES.EMAIL_SYNC_DLQ || queueName === QUEUE_NAMES.NIGHTLY_DLQ;
}

export async function handleQueue(
	batch: MessageBatch<unknown>,
	env: Env,
	ctx: ExecutionContext,
): Promise<void> {
	const config = getAppConfig(env);
	await runWithSqlClientLifecycle(config, async () => {
		const metrics = createQueueBatchMetrics(batch.messages.length);

		for (const message of batch.messages) {
			metrics.max_attempts_seen = Math.max(metrics.max_attempts_seen, message.attempts);

			// Dead letters are logged and acked; there is nothing left to run
			// and no queue behind this one to catch a failure.
			if (isDeadLetterQueue(batch.queue)) {
				handleDeadLetterMessage(batch.queue, message.body, message.id, message.attempts);
				message.ack();
				metrics.acked_messages += 1;
				metrics.dead_letter_messages += 1;
				continue;
			}

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
					// Whatever queue carried it: a sync job about to dead-letter
					// still holds connection leases nothing else will release.
					if (isEmailSyncUserJob(message.body)) {
						try {
							await releaseEmailSyncJobLeases(message.body, env);
						} catch (releaseError) {
							console.error('Failed to release EMAIL_SYNC_USER leases before DLQ handoff', {
								queue: batch.queue,
								messageId: message.id,
								userId: message.body.user_id,
								syncRunId: message.body.sync_run_id ?? null,
								error:
									releaseError instanceof Error
										? releaseError.message
										: 'Unknown lease release error',
							});
						}
					}
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

				const delaySeconds = resolveRetryDelaySeconds(error, message.attempts);
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

		// One report per batch, not per message: a DLQ batch of ten is one
		// incident, and the reporting budget is small on purpose.
		if (metrics.dead_letter_messages > 0) {
			reportErrorInBackground(
				env,
				{
					event: 'DLQ_MESSAGE',
					message: `${metrics.dead_letter_messages} dead-lettered message(s) on ${batch.queue}`,
					context: {
						queue: batch.queue,
						deadLetterCount: metrics.dead_letter_messages,
					},
				},
				ctx,
			);
		}

		const alertThresholds = resolveQueueAlertThresholds(env);
		if (isQueueAlertThresholdBreached(metrics, alertThresholds, retryRatePercent)) {
			console.error('QUEUE_ALERT_THRESHOLD_BREACH', {
				queue: batch.queue,
				...metrics,
				retry_rate_percent: retryRatePercent,
				alert_thresholds: alertThresholds,
				evaluation_scope: 'batch',
			});

			reportErrorInBackground(
				env,
				{
					event: 'QUEUE_ALERT_THRESHOLD_BREACH',
					message: `Queue ${batch.queue} breached an alert threshold`,
					context: {
						queue: batch.queue,
						totalMessages: metrics.total_messages,
						retriedMessages: metrics.retried_messages,
						finalRetryMessages: metrics.final_retry_messages,
						poisonAckedMessages: metrics.poison_acked_messages,
						retryRatePercent,
					},
				},
				ctx,
			);
		}
	});
}
