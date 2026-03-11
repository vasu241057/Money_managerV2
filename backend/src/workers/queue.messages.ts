import type {
	AiClassificationJobPayload,
	EmailSyncDispatchJobPayload,
	EmailSyncJobPayload,
	EmailSyncUserJobPayload,
	NormalizeRawEmailsJobPayload,
	UUID,
} from '../../../shared/types';
import { NORMALIZE_RAW_EMAILS_MAX_IDS } from '../../../shared/types';
import { PoisonMessageError } from './queue.errors';

export type EmailSyncDispatchJob = EmailSyncDispatchJobPayload;
export type EmailSyncUserJob = EmailSyncUserJobPayload;
export type NormalizeRawEmailsJob = NormalizeRawEmailsJobPayload;
export type AiClassificationJob = AiClassificationJobPayload;
export type EmailSyncQueueJob = EmailSyncJobPayload;
export type QueueJob = EmailSyncQueueJob | AiClassificationJob;

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
		isOptionalUuidString(value.continuation_connection_id) &&
		isOptionalNonEmptyString(value.continuation_page_token) &&
		isOptionalNonNegativeInteger(value.continuation_after_seconds) &&
		isOptionalNonNegativeInteger(value.continuation_max_internal_timestamp_seen) &&
		hasValidEmailSyncUserContinuationShape(value)
	);
}

export function isAiClassificationJob(value: unknown): value is AiClassificationJob {
	if (!isRecord(value)) {
		return false;
	}

	return (
		value.job_type === 'AI_CLASSIFICATION' &&
		isUuidString(value.transaction_id) &&
		isIsoDateTimeString(value.requested_at)
	);
}

export function isNormalizeRawEmailsJob(value: unknown): value is NormalizeRawEmailsJob {
	if (!isRecord(value)) {
		return false;
	}

	return value.job_type === 'NORMALIZE_RAW_EMAILS' && isNonEmptyUuidArray(value.raw_email_ids);
}

export function buildEmailSyncDispatchJob(controller: ScheduledController): EmailSyncDispatchJob {
	return {
		job_type: 'EMAIL_SYNC_DISPATCH',
		scheduled_time: controller.scheduledTime,
		triggered_at: new Date().toISOString(),
		cron: controller.cron,
	};
}

export function buildEmailSyncUserJob(
	userId: UUID,
	lastSyncTimestamp: number,
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
		...(continuation ?? {}),
	};
}

export function buildAiClassificationJob(
	transactionId: UUID,
	requestedAt: string = new Date().toISOString(),
): AiClassificationJob {
	return {
		job_type: 'AI_CLASSIFICATION',
		transaction_id: transactionId,
		requested_at: requestedAt,
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

export function parseAiClassificationJob(value: unknown): AiClassificationJob {
	if (!isAiClassificationJob(value)) {
		throw new PoisonMessageError('Invalid AI_CLASSIFICATION payload');
	}

	return value;
}

export function parseNormalizeRawEmailsJob(value: unknown): NormalizeRawEmailsJob {
	if (!isNormalizeRawEmailsJob(value)) {
		throw new PoisonMessageError('Invalid NORMALIZE_RAW_EMAILS payload');
	}

	return value;
}
