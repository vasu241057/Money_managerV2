import type {
	EmailSyncDispatchJobPayload,
	EmailSyncJobPayload,
	EmailSyncUserJobPayload,
	MerchantMappingJobPayload,
	NormalizeRawEmailsJobPayload,
	RawEmailBodyPurgeJobPayload,
	SenderBackfillJobPayload,
	SenderClassificationJobPayload,
	UUID,
} from '../../../shared/types';
import { NORMALIZE_RAW_EMAILS_MAX_IDS, SENDER_BACKFILL_MAX_IDS } from '../../../shared/types';
import { EMAIL_SYNC_CRON } from '../lib/infra';
import { PoisonMessageError } from './queue.errors';

export type EmailSyncDispatchJob = EmailSyncDispatchJobPayload;
export type EmailSyncUserJob = EmailSyncUserJobPayload;
export type NormalizeRawEmailsJob = NormalizeRawEmailsJobPayload;
export type SenderClassificationJob = SenderClassificationJobPayload;
export type SenderBackfillJob = SenderBackfillJobPayload;
export type MerchantMappingJob = MerchantMappingJobPayload;
export type RawEmailBodyPurgeJob = RawEmailBodyPurgeJobPayload;
export type EmailSyncQueueJob = EmailSyncJobPayload;
export type QueueJob = EmailSyncQueueJob;

const UUID_REGEX =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null;
}

function isNonNegativeSafeInteger(value: unknown): value is number {
	return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isNonEmptyString(value: unknown): value is string {
	return typeof value === 'string' && value.length > 0;
}

function isOptionalNonEmptyString(value: unknown): value is string | undefined {
	return value === undefined || isNonEmptyString(value);
}

function isIsoDateTimeString(value: unknown): value is string {
	return typeof value === 'string' && value.length > 0 && Number.isFinite(Date.parse(value));
}

function isUuidString(value: unknown): value is UUID {
	return typeof value === 'string' && UUID_REGEX.test(value);
}

function isOptionalNonNegativeInteger(value: unknown): boolean {
	if (value === undefined) {
		return true;
	}

	return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isOptionalUuidString(value: unknown): value is UUID | undefined {
	return value === undefined || isUuidString(value);
}

function isNonEmptyUuidArray(value: unknown): value is UUID[] {
	if (
		!Array.isArray(value) ||
		value.length === 0 ||
		value.length > NORMALIZE_RAW_EMAILS_MAX_IDS
	) {
		return false;
	}

	return value.every(entry => isUuidString(entry));
}

function hasValidEmailSyncUserContinuationShape(value: Record<string, unknown>): boolean {
	const continuationConnectionId = value.continuation_connection_id;
	const continuationPageToken = value.continuation_page_token;
	const continuationAfterSeconds = value.continuation_after_seconds;
	const continuationMaxSeen = value.continuation_max_internal_timestamp_seen;

	const hasAnyContinuationField =
		continuationConnectionId !== undefined ||
		continuationPageToken !== undefined ||
		continuationAfterSeconds !== undefined ||
		continuationMaxSeen !== undefined;

	if (!hasAnyContinuationField) {
		return true;
	}

	return (
		isUuidString(continuationConnectionId) &&
		isNonEmptyString(continuationPageToken) &&
		isNonNegativeSafeInteger(continuationAfterSeconds) &&
		isOptionalNonNegativeInteger(continuationMaxSeen)
	);
}

export function isEmailSyncDispatchJob(value: unknown): value is EmailSyncDispatchJob {
	if (!isRecord(value)) {
		return false;
	}

	return (
		value.job_type === 'EMAIL_SYNC_DISPATCH' &&
		isNonNegativeSafeInteger(value.scheduled_time) &&
		isIsoDateTimeString(value.triggered_at) &&
		isNonEmptyString(value.cron) &&
		isOptionalNonNegativeInteger(value.start_offset) &&
		isOptionalUuidString(value.scan_upper_user_id)
	);
}

export function isEmailSyncUserJob(value: unknown): value is EmailSyncUserJob {
	if (!isRecord(value)) {
		return false;
	}

	return (
		value.job_type === 'EMAIL_SYNC_USER' &&
		isUuidString(value.user_id) &&
		isNonNegativeSafeInteger(value.last_sync_timestamp) &&
		isOptionalUuidString(value.sync_run_id) &&
		isOptionalUuidString(value.continuation_connection_id) &&
		isOptionalNonEmptyString(value.continuation_page_token) &&
		isOptionalNonNegativeInteger(value.continuation_after_seconds) &&
		isOptionalNonNegativeInteger(value.continuation_max_internal_timestamp_seen) &&
		hasValidEmailSyncUserContinuationShape(value)
	);
}

export function isNormalizeRawEmailsJob(value: unknown): value is NormalizeRawEmailsJob {
	if (!isRecord(value)) {
		return false;
	}

	return value.job_type === 'NORMALIZE_RAW_EMAILS' && isNonEmptyUuidArray(value.raw_email_ids);
}

export function isSenderClassificationJob(value: unknown): value is SenderClassificationJob {
	if (!isRecord(value)) {
		return false;
	}

	return (
		value.job_type === 'SENDER_CLASSIFICATION' && isIsoDateTimeString(value.triggered_at)
	);
}

export function isSenderBackfillJob(value: unknown): value is SenderBackfillJob {
	if (!isRecord(value)) {
		return false;
	}

	return (
		value.job_type === 'SENDER_BACKFILL' &&
		isUuidString(value.user_id) &&
		Array.isArray(value.raw_email_ids) &&
		value.raw_email_ids.length > 0 &&
		value.raw_email_ids.length <= SENDER_BACKFILL_MAX_IDS &&
		value.raw_email_ids.every(entry => isUuidString(entry))
	);
}

export function buildSenderClassificationJob(
	triggeredAt: string = new Date().toISOString(),
): SenderClassificationJob {
	return {
		job_type: 'SENDER_CLASSIFICATION',
		triggered_at: triggeredAt,
	};
}

export function isMerchantMappingJob(value: unknown): value is MerchantMappingJob {
	if (!isRecord(value)) {
		return false;
	}

	return value.job_type === 'MERCHANT_MAPPING' && isIsoDateTimeString(value.triggered_at);
}

export function buildMerchantMappingJob(
	triggeredAt: string = new Date().toISOString(),
): MerchantMappingJob {
	return {
		job_type: 'MERCHANT_MAPPING',
		triggered_at: triggeredAt,
	};
}

export function parseMerchantMappingJob(value: unknown): MerchantMappingJob {
	if (!isMerchantMappingJob(value)) {
		throw new PoisonMessageError('Invalid MERCHANT_MAPPING payload');
	}

	return value;
}

export function isRawEmailBodyPurgeJob(value: unknown): value is RawEmailBodyPurgeJob {
	if (!isRecord(value)) {
		return false;
	}

	return value.job_type === 'RAW_EMAIL_BODY_PURGE' && isIsoDateTimeString(value.triggered_at);
}

export function buildRawEmailBodyPurgeJob(
	triggeredAt: string = new Date().toISOString(),
): RawEmailBodyPurgeJob {
	return {
		job_type: 'RAW_EMAIL_BODY_PURGE',
		triggered_at: triggeredAt,
	};
}

export function parseRawEmailBodyPurgeJob(value: unknown): RawEmailBodyPurgeJob {
	if (!isRawEmailBodyPurgeJob(value)) {
		throw new PoisonMessageError('Invalid RAW_EMAIL_BODY_PURGE payload');
	}

	return value;
}

export function parseSenderClassificationJob(value: unknown): SenderClassificationJob {
	if (!isSenderClassificationJob(value)) {
		throw new PoisonMessageError('Invalid SENDER_CLASSIFICATION payload');
	}

	return value;
}

export function parseSenderBackfillJob(value: unknown): SenderBackfillJob {
	if (!isSenderBackfillJob(value)) {
		throw new PoisonMessageError('Invalid SENDER_BACKFILL payload');
	}

	return value;
}

export function buildEmailSyncDispatchJob(controller: ScheduledController): EmailSyncDispatchJob {
	return {
		job_type: 'EMAIL_SYNC_DISPATCH',
		scheduled_time: controller.scheduledTime,
		triggered_at: new Date().toISOString(),
		cron: controller.cron || EMAIL_SYNC_CRON,
	};
}

export function buildEmailSyncUserJob(
	userId: UUID,
	lastSyncTimestamp: number,
	syncRunId?: UUID,
	continuation?: Pick<
		EmailSyncUserJob,
		| 'continuation_connection_id'
		| 'continuation_page_token'
		| 'continuation_after_seconds'
		| 'continuation_max_internal_timestamp_seen'
	>,
): EmailSyncUserJob {
	return {
		job_type: 'EMAIL_SYNC_USER',
		user_id: userId,
		last_sync_timestamp: lastSyncTimestamp,
		...(syncRunId ? { sync_run_id: syncRunId } : {}),
		...(continuation ?? {}),
	};
}

export function parseEmailSyncDispatchJob(value: unknown): EmailSyncDispatchJob {
	if (!isEmailSyncDispatchJob(value)) {
		throw new PoisonMessageError('Invalid EMAIL_SYNC_DISPATCH payload');
	}

	return value;
}

export function parseEmailSyncUserJob(value: unknown): EmailSyncUserJob {
	if (!isEmailSyncUserJob(value)) {
		throw new PoisonMessageError('Invalid EMAIL_SYNC_USER payload');
	}

	return value;
}

export function parseNormalizeRawEmailsJob(value: unknown): NormalizeRawEmailsJob {
	if (!isNormalizeRawEmailsJob(value)) {
		throw new PoisonMessageError('Invalid NORMALIZE_RAW_EMAILS payload');
	}

	return value;
}
