import { describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import * as dbClient from '../src/lib/db/client';
import { purgeExpiredIdempotencyKeys } from '../src/services/idempotency.service';
import {
	purgeExpiredRawEmailBodies,
	retryPendingClerkDeletions,
	runRawEmailBodyPurgeJob,
} from '../src/workers/raw-email-body-purge';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function createSqlMock(handler: QueryHandler): SqlClient & { queries: Array<{ query: string; values: unknown[] }> } {
	const queries: Array<{ query: string; values: unknown[] }> = [];
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		queries.push({ query, values });
		return handler(query, values);
	};
	const mock = tag as unknown as SqlClient & { queries: typeof queries };
	mock.queries = queries;
	return mock;
}

describe('purgeExpiredRawEmailBodies', () => {
	it('purges in bounded batches and stops when a batch comes back short', async () => {
		const batches = [3, 3, 1];
		const sql = createSqlMock(() => {
			const count = batches.shift() ?? 0;
			return Array.from({ length: count }, (_, index) => ({ id: `id-${index}` }));
		});

		const outcome = await purgeExpiredRawEmailBodies(sql, 90, { batchSize: 3, maxBatches: 10 });

		expect(outcome).toEqual({ purged_count: 7, batch_count: 3, exhausted: false });
		const first = sql.queries[0];
		expect(first?.query).toContain("set clean_text = '', body_purged_at = now()");
		expect(first?.query).toContain('coalesce(re.processing_completed_at, re.created_at)');
		expect(first?.query).toContain('and re.body_purged_at is null');
		// Statuses, retention days, batch size — FAILED is never in the list.
		expect(first?.values[0]).toEqual(['PROCESSED', 'IGNORED', 'UNRECOGNIZED']);
		expect(first?.values[1]).toBe(90);
		expect(first?.values[2]).toBe(3);
	});

	it('reports a backlog when every batch was full', async () => {
		const sql = createSqlMock(() => [{ id: 'a' }, { id: 'b' }]);

		const outcome = await purgeExpiredRawEmailBodies(sql, 30, { batchSize: 2, maxBatches: 4 });

		expect(outcome).toEqual({ purged_count: 8, batch_count: 4, exhausted: true });
	});
});

describe('retryPendingClerkDeletions', () => {
	const pending = {
		user_id: '11111111-1111-4111-8111-111111111111',
		auth_provider: 'clerk',
		auth_subject: 'user_pending',
	};

	function fetcherReturning(status: number) {
		return vi.fn(async () => new Response('{}', { status }));
	}

	it('skips without calling Clerk when the secret is missing, but still reports the backlog', async () => {
		const sql = createSqlMock(query => (query.includes('from public.user_deletion_log') ? [pending] : []));
		const fetcher = fetcherReturning(200);

		const outcome = await retryPendingClerkDeletions(sql, {} as Env, fetcher);

		expect(outcome).toEqual({
			pending_count: 1,
			deleted_count: 0,
			user_returned_count: 0,
			failed_count: 0,
			skipped_reason: 'CLERK_SECRET_KEY not set',
		});
		expect(fetcher).not.toHaveBeenCalled();
	});

	it('deletes the Clerk user and clears the subject on success', async () => {
		const sql = createSqlMock(query => {
			if (query.includes('from public.user_deletion_log')) return [pending];
			if (query.includes('from public.users as u')) return [];
			return [];
		});
		const fetcher = fetcherReturning(200);

		const outcome = await retryPendingClerkDeletions(sql, { CLERK_SECRET_KEY: 'sk' } as Env, fetcher);

		expect(outcome.deleted_count).toBe(1);
		expect(outcome.failed_count).toBe(0);
		const update = sql.queries.find(entry => entry.query.includes('update public.user_deletion_log'));
		expect(update?.query).toContain('set clerk_deleted = true, auth_subject = null');
		expect(update?.values).toEqual([pending.user_id]);
	});

	it('drops the tombstone instead of locking out a user who signed in again', async () => {
		const sql = createSqlMock(query => {
			if (query.includes('from public.user_deletion_log')) return [pending];
			if (query.includes('from public.users as u')) return [{ id: pending.user_id }];
			return [];
		});
		const fetcher = fetcherReturning(200);

		const outcome = await retryPendingClerkDeletions(sql, { CLERK_SECRET_KEY: 'sk' } as Env, fetcher);

		expect(outcome.user_returned_count).toBe(1);
		expect(fetcher).not.toHaveBeenCalled();
		expect(sql.queries.some(entry => entry.query.includes('delete from public.user_deletion_log where user_id ='))).toBe(true);
	});

	it('counts an attempt and keeps the subject when Clerk still fails', async () => {
		const sql = createSqlMock(query => {
			if (query.includes('from public.user_deletion_log')) return [pending];
			if (query.includes('from public.users as u')) return [];
			return [];
		});

		const outcome = await retryPendingClerkDeletions(sql, { CLERK_SECRET_KEY: 'sk' } as Env, fetcherReturning(503));

		expect(outcome.failed_count).toBe(1);
		const update = sql.queries.find(entry => entry.query.includes('update public.user_deletion_log'));
		expect(update?.query).toContain('clerk_delete_attempts = clerk_delete_attempts + 1');
		expect(update?.query).not.toContain('auth_subject = null');
		expect(update?.values[0]).toBe('http 503');
	});
});

describe('idempotency ledger purge (Task 7)', () => {
	it('keeps deleting until a batch comes back short', async () => {
		const batches = [2, 2, 1];
		const sql = createSqlMock(() => {
			const count = batches.shift() ?? 0;
			return Array.from({ length: count }, (_, index) => ({ key: `k-${index}` }));
		});

		const outcome = await purgeExpiredIdempotencyKeys(sql, 24, 2, 10);

		expect(outcome).toEqual({ deleted_count: 5, exhausted: false });
		const query = sql.queries[0];
		expect(query?.query).toContain('delete from public.idempotency_keys');
		expect(query?.query).toContain("( || ' hours')::interval");
		expect(query?.values).toEqual([24, 2]);
	});

	it('is not exhausted when fewer rows than the bound came back', async () => {
		const sql = createSqlMock(() => [{ key: 'a' }]);

		expect(await purgeExpiredIdempotencyKeys(sql, 24, 5)).toEqual({
			deleted_count: 1,
			exhausted: false,
		});
	});

	it('reports a backlog when every batch it was allowed was full', async () => {
		const sql = createSqlMock(() => [{ key: 'a' }, { key: 'b' }]);

		expect(await purgeExpiredIdempotencyKeys(sql, 24, 2, 3)).toEqual({
			deleted_count: 6,
			exhausted: true,
		});
	});

	it('runs as the third duty of the nightly job', async () => {
		const sql = createSqlMock((query) => {
			if (query.includes('public.idempotency_keys')) return [{ key: 'expired' }];
			if (query.includes('user_deletion_log')) return [];
			return [];
		});
		vi.spyOn(dbClient, 'getSqlClient').mockReturnValue(sql);

		const result = await runRawEmailBodyPurgeJob(
			{ job_type: 'RAW_EMAIL_BODY_PURGE', triggered_at: new Date().toISOString() },
			{ NODE_ENV: 'test' } as unknown as Env,
		);

		expect(result.idempotency).toEqual({ deleted_count: 1, exhausted: false });
		vi.restoreAllMocks();
	});
});
