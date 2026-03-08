import type {
	AiClassificationJobPayload,
	EmailSyncDispatchJobPayload,
	EmailSyncJobPayload,
	EmailSyncUserJobPayload,
	UUID,
} from '../../../shared/types';
import { PoisonMessageError } from './queue.errors';

export type EmailSyncDispatchJob = EmailSyncDispatchJobPayload;
export type EmailSyncUserJob = EmailSyncUserJobPayload;
export type AiClassificationJob = AiClassificationJobPayload;
export type EmailSyncQueueJob = EmailSyncJobPayload;
export type QueueJob = EmailSyncQueueJob | AiClassificationJob;

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null;
}

function isFiniteNumber(value: unknown): value is number {
	return typeof value === 'number' && Number.isFinite(value);
}

export function isEmailSyncDispatchJob(value: unknown): value is EmailSyncDispatchJob {
	if (!isRecord(value)) {
		return false;
	}

	return (
		value.job_type === 'EMAIL_SYNC_DISPATCH' &&
		isFiniteNumber(value.scheduled_time) &&
		typeof value.triggered_at === 'string' &&
		typeof value.cron === 'string'
	);
}

export function isEmailSyncUserJob(value: unknown): value is EmailSyncUserJob {
	if (!isRecord(value)) {
		return false;
	}

	return (
		value.job_type === 'EMAIL_SYNC_USER' &&
		typeof value.user_id === 'string' &&
		isFiniteNumber(value.last_sync_timestamp)
	);
}

export function isAiClassificationJob(value: unknown): value is AiClassificationJob {
	if (!isRecord(value)) {
		return false;
	}

	return (
		value.job_type === 'AI_CLASSIFICATION' &&
		typeof value.transaction_id === 'string' &&
		typeof value.requested_at === 'string'
	);
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
): EmailSyncUserJob {
	return {
		job_type: 'EMAIL_SYNC_USER',
		user_id: userId,
		last_sync_timestamp: lastSyncTimestamp,
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
