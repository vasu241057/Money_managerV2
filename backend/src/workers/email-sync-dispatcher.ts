import type { EmailSyncJobPayload, UUID } from '../../../shared/types';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import type { EmailSyncDispatchJob } from './queue.messages';
import { buildEmailSyncUserJob } from './queue.messages';

const DISPATCH_PAGE_LIMIT = 1000;
const USER_JOB_BATCH_SIZE = 100;
const DORMANT_INACTIVITY_DAYS = 45;
const DORMANT_INACTIVITY_MS = DORMANT_INACTIVITY_DAYS * 24 * 60 * 60 * 1000;
const MAX_PAGES_PER_INVOCATION = 250;
const CONTINUATION_SEND_ATTEMPTS = 3;

interface DispatchCandidateRowRaw {
	user_id: unknown;
	last_sync_timestamp: unknown;
	last_app_open_date: unknown;
	has_active_connections: unknown;
	has_dormant_connections: unknown;
}

interface ScanUpperUserIdRowRaw {
	max_user_id: unknown;
}

interface DispatchCandidateRow {
	user_id: UUID;
	last_sync_timestamp: number;
	last_app_open_time_ms: number;
	has_active_connections: boolean;
	has_dormant_connections: boolean;
}

interface EnqueueUserJobsResult {
	batch_count: number;
	enqueued_job_count: number;
	failed_job_count: number;
}

export interface EmailSyncDispatchOptions {
	start_offset?: number;
	scan_upper_user_id?: UUID;
	max_pages_per_invocation?: number;
}

export interface EmailSyncDispatchResult {
	page_count: number;
	scanned_user_count: number;
	enqueued_user_job_count: number;
	failed_user_job_count: number;
	queue_batch_count: number;
	marked_dormant_user_count: number;
	reactivated_user_count: number;
	continuation_offset: number | null;
	scan_upper_user_id: UUID | null;
}

function toErrorMessage(error: unknown): string {
	if (error instanceof Error) {
		return error.message;
	}

	return typeof error === 'string' ? error : 'Unknown error';
}

function parseNonEmptyString(value: unknown, fieldName: string): string {
	if (typeof value !== 'string' || value.length === 0) {
		throw new Error(`${fieldName} must be a non-empty string`);
	}

	return value;
}

function parseOptionalNonEmptyString(value: unknown, fieldName: string): string | null {
	if (value === null || value === undefined) {
		return null;
	}

	return parseNonEmptyString(value, fieldName);
}

function parseSafeInteger(value: unknown, fieldName: string): number {
	if (typeof value === 'number' && Number.isSafeInteger(value)) {
		return value;
	}

	if (typeof value === 'string') {
		const parsed = Number(value);
		if (Number.isSafeInteger(parsed)) {
			return parsed;
		}
	}

	throw new Error(`${fieldName} must be a safe integer`);
}

function parseNonNegativeSafeInteger(value: unknown, fieldName: string): number {
	const parsed = parseSafeInteger(value, fieldName);
	if (parsed < 0) {
		throw new Error(`${fieldName} must be non-negative`);
	}

	return parsed;
}

function parseBoolean(value: unknown, fieldName: string): boolean {
	if (typeof value !== 'boolean') {
		throw new Error(`${fieldName} must be boolean`);
	}

	return value;
}

function parseTimestamp(value: unknown, fieldName: string): number {
	if (value instanceof Date) {
		const timestamp = value.getTime();
		if (!Number.isFinite(timestamp)) {
			throw new Error(`${fieldName} must be a valid datetime`);
		}
		return timestamp;
	}

	if (typeof value === 'string') {
		const timestamp = Date.parse(value);
		if (!Number.isFinite(timestamp)) {
			throw new Error(`${fieldName} must be a valid datetime`);
		}
		return timestamp;
	}

	throw new Error(`${fieldName} must be a datetime`);
}

function parseDispatchCandidateRow(raw: DispatchCandidateRowRaw): DispatchCandidateRow {
	return {
		user_id: parseNonEmptyString(raw.user_id, 'user_id'),
		last_sync_timestamp: parseSafeInteger(raw.last_sync_timestamp, 'last_sync_timestamp'),
		last_app_open_time_ms: parseTimestamp(raw.last_app_open_date, 'last_app_open_date'),
		has_active_connections: parseBoolean(raw.has_active_connections, 'has_active_connections'),
		has_dormant_connections: parseBoolean(raw.has_dormant_connections, 'has_dormant_connections'),
	};
}

async function resolveScanUpperUserId(sql: SqlClient): Promise<UUID | null> {
	const rows = await sql<ScanUpperUserIdRowRaw[]>`
		select max(oc.user_id) as max_user_id
		from public.oauth_connections as oc
		where oc.provider = 'google'
			and oc.sync_status in ('ACTIVE', 'DORMANT')
	`;

	const scanUpperUserId = parseOptionalNonEmptyString(rows[0]?.max_user_id, 'max_user_id');
	return scanUpperUserId;
}

async function listDispatchCandidatePage(
	sql: SqlClient,
	offset: number,
	scanUpperUserId: UUID | null,
): Promise<DispatchCandidateRow[]> {
	if (scanUpperUserId) {
		const rows = await sql<DispatchCandidateRowRaw[]>`
			-- Scan ACTIVE + DORMANT connections under a stable user-id anchor.
			select
				u.id as user_id,
				-- Use the oldest sync cursor per user to avoid skipping lagging linked mailboxes.
				min(oc.last_sync_timestamp) as last_sync_timestamp,
				u.last_app_open_date,
				bool_or(oc.sync_status = 'ACTIVE') as has_active_connections,
				bool_or(oc.sync_status = 'DORMANT') as has_dormant_connections
			from public.oauth_connections as oc
			join public.users as u
				on u.id = oc.user_id
			where oc.provider = 'google'
				and oc.sync_status in ('ACTIVE', 'DORMANT')
				and u.id <= ${scanUpperUserId}
			group by u.id, u.last_app_open_date
			order by u.id asc
			limit ${DISPATCH_PAGE_LIMIT}
			offset ${offset}
		`;

		return rows.map(parseDispatchCandidateRow);
	}

	const rows = await sql<DispatchCandidateRowRaw[]>`
		select
			u.id as user_id,
			min(oc.last_sync_timestamp) as last_sync_timestamp,
			u.last_app_open_date,
			bool_or(oc.sync_status = 'ACTIVE') as has_active_connections,
			bool_or(oc.sync_status = 'DORMANT') as has_dormant_connections
		from public.oauth_connections as oc
		join public.users as u
			on u.id = oc.user_id
		where oc.provider = 'google'
			and oc.sync_status in ('ACTIVE', 'DORMANT')
		group by u.id, u.last_app_open_date
		order by u.id asc
		limit ${DISPATCH_PAGE_LIMIT}
		offset ${offset}
	`;

	return rows.map(parseDispatchCandidateRow);
}

async function markUsersDormant(sql: SqlClient, userIds: UUID[]): Promise<void> {
	if (userIds.length === 0) {
		return;
	}

	await sql`
		update public.oauth_connections as oc
		set sync_status = 'DORMANT'
		where oc.user_id = any(${userIds}::uuid[])
			and oc.provider = 'google'
			and oc.sync_status = 'ACTIVE'
	`;
}

async function reactivateUsers(sql: SqlClient, userIds: UUID[]): Promise<void> {
	if (userIds.length === 0) {
		return;
	}

	await sql`
		update public.oauth_connections as oc
		set sync_status = 'ACTIVE'
		where oc.user_id = any(${userIds}::uuid[])
			and oc.provider = 'google'
			and oc.sync_status = 'DORMANT'
	`;
}

function buildBatchRequests(jobs: ReturnType<typeof buildEmailSyncUserJob>[]) {
	return jobs.map((job) => ({
		body: job,
		contentType: 'json' as const,
	}));
}

async function trySendBatch(
	queue: Queue<EmailSyncJobPayload>,
	jobs: ReturnType<typeof buildEmailSyncUserJob>[],
): Promise<boolean> {
	const requests = buildBatchRequests(jobs);
	try {
		await queue.sendBatch(requests);
		return true;
	} catch (error) {
		console.warn('sendBatch failed for EMAIL_SYNC_USER chunk', {
			chunkSize: jobs.length,
			error: toErrorMessage(error),
		});
		return false;
	}
}

async function enqueueUserJobs(
	queue: Queue<EmailSyncJobPayload>,
	jobs: ReturnType<typeof buildEmailSyncUserJob>[],
): Promise<EnqueueUserJobsResult> {
	const result: EnqueueUserJobsResult = {
		batch_count: 0,
		enqueued_job_count: 0,
		failed_job_count: 0,
	};

	for (let start = 0; start < jobs.length; start += USER_JOB_BATCH_SIZE) {
		const chunk = jobs.slice(start, start + USER_JOB_BATCH_SIZE);
		if (chunk.length === 0) {
			continue;
		}

		const sentAsBatch = await trySendBatch(queue, chunk);
		if (sentAsBatch) {
			result.batch_count += 1;
			result.enqueued_job_count += chunk.length;
			continue;
		}

		result.failed_job_count += chunk.length;
	}

	return result;
}

export async function dispatchEmailSyncUsers(
	sql: SqlClient,
	queue: Queue<EmailSyncJobPayload>,
	nowMs: number,
	options: EmailSyncDispatchOptions = {},
): Promise<EmailSyncDispatchResult> {
	if (!Number.isFinite(nowMs)) {
		throw new Error('dispatch nowMs must be finite');
	}

	const startOffset = parseNonNegativeSafeInteger(
		options.start_offset ?? 0,
		'start_offset',
	);
	const maxPagesPerInvocation = parseNonNegativeSafeInteger(
		options.max_pages_per_invocation ?? MAX_PAGES_PER_INVOCATION,
		'max_pages_per_invocation',
	);
	if (maxPagesPerInvocation === 0) {
		throw new Error('max_pages_per_invocation must be greater than zero');
	}

	const scanUpperUserId =
		parseOptionalNonEmptyString(options.scan_upper_user_id, 'scan_upper_user_id') ??
		(await resolveScanUpperUserId(sql));

	const result: EmailSyncDispatchResult = {
		page_count: 0,
		scanned_user_count: 0,
		enqueued_user_job_count: 0,
		failed_user_job_count: 0,
		queue_batch_count: 0,
		marked_dormant_user_count: 0,
		reactivated_user_count: 0,
		continuation_offset: null,
		scan_upper_user_id: scanUpperUserId,
	};

	let offset = startOffset;
	while (true) {
		if (result.page_count >= maxPagesPerInvocation) {
			result.continuation_offset = offset;
			console.warn('Phase 1 dispatcher page budget reached; scheduling continuation', {
				maxPagesPerInvocation,
				nextOffset: offset,
				scanUpperUserId,
			});
			break;
		}

		let page: DispatchCandidateRow[];
		try {
			page = await listDispatchCandidatePage(sql, offset, scanUpperUserId);
		} catch (error) {
			if (result.enqueued_user_job_count > 0) {
				result.continuation_offset = offset;
				console.error('Dispatcher query failed after partial enqueue; deferring remaining pages', {
					nextOffset: offset,
					error: toErrorMessage(error),
				});
				break;
			}
			throw error;
		}
		if (page.length === 0) {
			break;
		}

		result.page_count += 1;
		result.scanned_user_count += page.length;

		const dormantUserIds: UUID[] = [];
		const activeUserIds: UUID[] = [];
		const jobs: ReturnType<typeof buildEmailSyncUserJob>[] = [];

		for (const candidate of page) {
			const inactivityMs = nowMs - candidate.last_app_open_time_ms;
			const shouldBeDormant = inactivityMs > DORMANT_INACTIVITY_MS;

			if (shouldBeDormant) {
				if (candidate.has_active_connections) {
					dormantUserIds.push(candidate.user_id);
				}
				continue;
			}

			if (candidate.has_dormant_connections) {
				activeUserIds.push(candidate.user_id);
			}

			jobs.push(buildEmailSyncUserJob(candidate.user_id, candidate.last_sync_timestamp));
		}

		try {
			await markUsersDormant(sql, dormantUserIds);
			await reactivateUsers(sql, activeUserIds);
		} catch (error) {
			if (result.enqueued_user_job_count > 0) {
				result.continuation_offset = offset;
				console.error('Dispatcher status update failed after partial enqueue; deferring remaining pages', {
					nextOffset: offset,
					error: toErrorMessage(error),
				});
				break;
			}
			throw error;
		}
		const enqueueResult = await enqueueUserJobs(queue, jobs);

		result.marked_dormant_user_count += dormantUserIds.length;
		result.reactivated_user_count += activeUserIds.length;
		result.queue_batch_count += enqueueResult.batch_count;
		result.enqueued_user_job_count += enqueueResult.enqueued_job_count;
		result.failed_user_job_count += enqueueResult.failed_job_count;

		if (page.length < DISPATCH_PAGE_LIMIT) {
			break;
		}

		offset += DISPATCH_PAGE_LIMIT;
	}

	return result;
}

async function enqueueContinuationDispatchJob(
	queue: Queue<EmailSyncJobPayload>,
	job: EmailSyncDispatchJob,
): Promise<boolean> {
	for (let attempt = 1; attempt <= CONTINUATION_SEND_ATTEMPTS; attempt += 1) {
		try {
			await queue.send(job, { contentType: 'json' });
			return true;
		} catch (error) {
			console.warn('Failed to enqueue dispatcher continuation job', {
				attempt,
				attemptsMax: CONTINUATION_SEND_ATTEMPTS,
				nextOffset: job.start_offset ?? 0,
				error: toErrorMessage(error),
			});
		}
	}

	return false;
}

export async function runEmailSyncDispatchJob(
	dispatchJob: EmailSyncDispatchJob,
	env: Env,
): Promise<EmailSyncDispatchResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const nowMs = Date.now();
	if (dispatchJob.scheduled_time > nowMs + 5 * 60 * 1000) {
		console.warn('EMAIL_SYNC_DISPATCH scheduled_time is unexpectedly in the future', {
			scheduledTime: dispatchJob.scheduled_time,
			nowMs,
		});
	}

	const result = await dispatchEmailSyncUsers(
		sql,
		env.EMAIL_SYNC_QUEUE,
		nowMs,
		{
			start_offset: dispatchJob.start_offset,
			scan_upper_user_id: dispatchJob.scan_upper_user_id,
		},
	);

	if (result.continuation_offset !== null) {
		const continuationJob: EmailSyncDispatchJob = {
			job_type: 'EMAIL_SYNC_DISPATCH',
			scheduled_time: dispatchJob.scheduled_time,
			triggered_at: new Date().toISOString(),
			cron: dispatchJob.cron,
			start_offset: result.continuation_offset,
		};
		if (result.scan_upper_user_id) {
			continuationJob.scan_upper_user_id = result.scan_upper_user_id;
		}

		const continuationQueued = await enqueueContinuationDispatchJob(
			env.EMAIL_SYNC_QUEUE,
			continuationJob,
		);
		if (!continuationQueued) {
			const hasPriorProgress = result.enqueued_user_job_count > 0;
			if (!hasPriorProgress) {
				throw new Error(
					`Failed to enqueue dispatcher continuation at offset ${result.continuation_offset}`,
				);
			}

			console.error('Failed to enqueue continuation after partial progress; next cron run will recover', {
				nextOffset: result.continuation_offset,
			});
		}
	}

	if (result.failed_user_job_count > 0) {
		if (result.enqueued_user_job_count === 0) {
			throw new Error(
				`Failed to enqueue ${result.failed_user_job_count} EMAIL_SYNC_USER jobs`,
			);
		}

		console.warn('Dispatcher could not enqueue all EMAIL_SYNC_USER jobs; next cron run will backfill', {
			failedUserJobCount: result.failed_user_job_count,
		});
	}

	return result;
}
