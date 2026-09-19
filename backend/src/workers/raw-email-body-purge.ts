// Nightly retention job (Task 5). Two duties:
//
//   1. Blank stored email bodies once they are older than the retention
//      window and no longer needed: PROCESSED, IGNORED and UNRECOGNIZED rows
//      have been judged; FAILED rows keep their body because they are the
//      only reprocessable ones. Age is processing_completed_at, falling back
//      to created_at for rows that predate that column. body_purged_at marks
//      the row so the gated-row queries (status IGNORED + empty body) never
//      mistake a purged body for a never-fetched one.
//   2. Retry identity-provider deletions that failed during DELETE /account
//      (user_deletion_log.clerk_deleted = false). A user who signed in again
//      before the retry has a live account under the same id; that tombstone
//      is dropped instead of locking them out.
//   3. Drop idempotency ledger rows past their 24-hour window (Task 7). A
//      key older than that is no longer a retry of anything.
//
// Every loop is bounded so one run can never hold a connection for long; a
// backlog drains over consecutive nights.

import type { RawEmailBodyPurgeJobPayload, UUID } from '../../../shared/types';
import { deleteClerkUser, resolveClerkSecretKey, type FetchLike } from '../lib/clerk-admin';
import { getAppConfig } from '../lib/config';
import { getSqlClient, type SqlClient } from '../lib/db/client';
import { toNullableString, toRequiredString } from '../lib/db/serialization';
import {
	IDEMPOTENCY_RETENTION_HOURS,
	purgeExpiredIdempotencyKeys,
	type IdempotencyPurgeOutcome,
} from '../services/idempotency.service';

export const RAW_EMAIL_BODY_PURGE_BATCH_SIZE = 500;
export const RAW_EMAIL_BODY_PURGE_MAX_BATCHES = 40;
export const CLERK_DELETION_RETRY_LIMIT = 50;

/** Statuses whose body is disposable once the retention window has passed. */
export const PURGEABLE_RAW_EMAIL_STATUSES = ['PROCESSED', 'IGNORED', 'UNRECOGNIZED'] as const;

export interface RawEmailBodyPurgeOutcome {
	purged_count: number;
	batch_count: number;
	/** True when the bounded loop stopped with candidates left for the next run. */
	exhausted: boolean;
}

export interface ClerkDeletionRetryOutcome {
	pending_count: number;
	deleted_count: number;
	user_returned_count: number;
	failed_count: number;
	skipped_reason: string | null;
}

export interface RawEmailBodyPurgeResult {
	retention_days: number;
	purge: RawEmailBodyPurgeOutcome;
	clerk_retry: ClerkDeletionRetryOutcome;
	idempotency: IdempotencyPurgeOutcome;
}

interface PurgeOptions {
	batchSize?: number;
	maxBatches?: number;
}

export async function purgeExpiredRawEmailBodies(
	sql: SqlClient,
	retentionDays: number,
	options: PurgeOptions = {},
): Promise<RawEmailBodyPurgeOutcome> {
	const batchSize = options.batchSize ?? RAW_EMAIL_BODY_PURGE_BATCH_SIZE;
	const maxBatches = options.maxBatches ?? RAW_EMAIL_BODY_PURGE_MAX_BATCHES;
	const statuses = [...PURGEABLE_RAW_EMAIL_STATUSES];
	const outcome: RawEmailBodyPurgeOutcome = { purged_count: 0, batch_count: 0, exhausted: false };

	for (let batch = 0; batch < maxBatches; batch += 1) {
		const rows = await sql<{ id: UUID }[]>`
			with candidates as (
				select re.id
				from public.raw_emails as re
				where re.status = any(${statuses}::text[])
					and re.clean_text <> ''
					and re.body_purged_at is null
					and coalesce(re.processing_completed_at, re.created_at)
						< now() - (${retentionDays} || ' days')::interval
				order by coalesce(re.processing_completed_at, re.created_at) asc
				limit ${batchSize}
			)
			update public.raw_emails as re
			set clean_text = '', body_purged_at = now()
			from candidates
			where re.id = candidates.id
			returning re.id
		`;
		outcome.batch_count += 1;
		outcome.purged_count += rows.length;
		if (rows.length < batchSize) {
			return outcome;
		}
	}

	outcome.exhausted = true;
	return outcome;
}

interface PendingDeletionRowRaw {
	user_id: unknown;
	auth_provider: unknown;
	auth_subject: unknown;
}

export async function retryPendingClerkDeletions(
	sql: SqlClient,
	env: Env,
	fetcher: FetchLike = fetch,
): Promise<ClerkDeletionRetryOutcome> {
	const outcome: ClerkDeletionRetryOutcome = {
		pending_count: 0,
		deleted_count: 0,
		user_returned_count: 0,
		failed_count: 0,
		skipped_reason: null,
	};

	const rows = await sql<PendingDeletionRowRaw[]>`
		select l.user_id, l.auth_provider, l.auth_subject
		from public.user_deletion_log as l
		where l.clerk_deleted = false
			and l.auth_subject is not null
		order by l.deleted_at asc
		limit ${CLERK_DELETION_RETRY_LIMIT}
	`;
	outcome.pending_count = rows.length;
	if (rows.length === 0) {
		return outcome;
	}
	if (!resolveClerkSecretKey(env)) {
		outcome.skipped_reason = 'CLERK_SECRET_KEY not set';
		return outcome;
	}

	for (const row of rows) {
		const userId = toRequiredString(row.user_id, 'user_deletion_log.user_id') as UUID;
		const provider = toNullableString(row.auth_provider, 'user_deletion_log.auth_provider');
		const subject = toNullableString(row.auth_subject, 'user_deletion_log.auth_subject');

		const returned = await sql<{ id: UUID }[]>`
			select u.id from public.users as u where u.id = ${userId} limit 1
		`;
		if (returned.length > 0) {
			// Signed in again after deletion: the middleware re-provisioned an
			// empty account under the same id. Deleting the provider user now
			// would lock a live account out, so the tombstone goes instead.
			await sql`delete from public.user_deletion_log where user_id = ${userId}`;
			outcome.user_returned_count += 1;
			console.warn('ACCOUNT_DELETION_CLERK_RETRY_USER_RETURNED', { userId });
			continue;
		}

		if (provider !== 'clerk' || !subject) {
			outcome.failed_count += 1;
			await sql`
				update public.user_deletion_log
				set clerk_delete_attempts = clerk_delete_attempts + 1,
					clerk_last_attempt_at = now(),
					clerk_last_error = ${'unsupported auth provider'}
				where user_id = ${userId}
			`;
			continue;
		}

		const deletion = await deleteClerkUser(env, subject, fetcher);
		if (deletion.ok) {
			await sql`
				update public.user_deletion_log
				set clerk_deleted = true,
					auth_subject = null,
					clerk_delete_attempts = clerk_delete_attempts + 1,
					clerk_last_attempt_at = now(),
					clerk_last_error = null
				where user_id = ${userId}
			`;
			outcome.deleted_count += 1;
			console.info('ACCOUNT_DELETION_CLERK_RETRY_SUCCEEDED', {
				userId,
				alreadyDeleted: deletion.already_deleted,
			});
		} else {
			await sql`
				update public.user_deletion_log
				set clerk_delete_attempts = clerk_delete_attempts + 1,
					clerk_last_attempt_at = now(),
					clerk_last_error = ${deletion.reason}
				where user_id = ${userId}
			`;
			outcome.failed_count += 1;
			console.warn('ACCOUNT_DELETION_CLERK_RETRY_FAILED', { userId, reason: deletion.reason });
		}
	}

	return outcome;
}

export async function runRawEmailBodyPurgeJob(
	_job: RawEmailBodyPurgeJobPayload,
	env: Env,
	fetcher: FetchLike = fetch,
): Promise<RawEmailBodyPurgeResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);

	const purge = await purgeExpiredRawEmailBodies(sql, config.rawEmailBodyRetentionDays);
	if (purge.exhausted) {
		console.warn('RAW_EMAIL_BODY_PURGE_BACKLOG', {
			purgedCount: purge.purged_count,
			batchCount: purge.batch_count,
		});
	}

	const clerkRetry = await retryPendingClerkDeletions(sql, env, fetcher);

	const idempotency = await purgeExpiredIdempotencyKeys(sql, IDEMPOTENCY_RETENTION_HOURS);
	if (idempotency.exhausted) {
		console.warn('IDEMPOTENCY_PURGE_BACKLOG', { deletedCount: idempotency.deleted_count });
	}

	return {
		retention_days: config.rawEmailBodyRetentionDays,
		purge,
		clerk_retry: clerkRetry,
		idempotency,
	};
}
