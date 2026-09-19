// The idempotency ledger on the real schema (Task 7): a retry replays instead
// of writing twice, a reused key with a different body is refused, a claim
// abandoned by a dead worker does not wedge the key forever, keys are
// per-user, and the rows die with their user and with the nightly purge.

import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { HttpError } from '../../src/lib/http/errors';
import {
	claimIdempotencyKey,
	completeIdempotencyKey,
	hashRequestBody,
	IDEMPOTENT_REPLAY_HEADER,
	purgeExpiredIdempotencyKeys,
	releaseIdempotencyKey,
	withIdempotency,
	type IdempotentResult,
} from '../../src/services/idempotency.service';
import { seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { applyMigration, createTenancyDb, type TenancyDb } from './pglite-db';

const MIGRATION = '20260919090000_perimeter_hardening.sql';

let harness: TenancyDb;
let sql: SqlClient;
let tenants: TwoTenantFixture;

const HASH_A = 'a'.repeat(64);
const HASH_B = 'b'.repeat(64);

interface CountRow {
	count: string;
}

interface FakeResponse {
	statusCode: number | null;
	body: unknown;
	headers: Record<string, string>;
}

function fakeResponse(): FakeResponse & import('express').Response {
	const state: FakeResponse = { statusCode: null, body: undefined, headers: {} };
	const res = {
		...state,
		status(code: number) {
			(res as unknown as FakeResponse).statusCode = code;
			return res;
		},
		json(payload: unknown) {
			(res as unknown as FakeResponse).body = payload;
			return res;
		},
		setHeader(name: string, value: string) {
			(res as unknown as FakeResponse).headers[name] = value;
			return res;
		},
	};
	return res as unknown as FakeResponse & import('express').Response;
}

function fakeRequest(body: unknown, key?: string): import('express').Request {
	return {
		body,
		header: (name: string) =>
			name.toLowerCase() === 'idempotency-key' ? key : undefined,
	} as unknown as import('express').Request;
}

async function countKeys(): Promise<number> {
	const rows = await sql<CountRow[]>`select count(*) as count from public.idempotency_keys`;
	return Number(rows[0].count);
}

beforeAll(async () => {
	harness = await createTenancyDb();
	sql = harness.sql;
	tenants = await seedTwoTenants(sql);
}, 120_000);

afterAll(async () => {
	await harness?.close();
});

describe('tenancy: idempotency ledger', () => {
	it('ships the table hardened like every other user-owned table', async () => {
		const rows = await sql<{ relrowsecurity: boolean; policies: string }[]>`
			select c.relrowsecurity, (
				select count(*) from pg_policies as p
				where p.schemaname = 'public' and p.tablename = 'idempotency_keys'
			) as policies
			from pg_class as c
			join pg_namespace as n on n.oid = c.relnamespace
			where n.nspname = 'public' and c.relname = 'idempotency_keys'
		`;
		expect(rows[0].relrowsecurity).toBe(true);
		expect(Number(rows[0].policies)).toBeGreaterThan(0);
	});

	it('re-applies cleanly and keeps the rows it already had', async () => {
		// Part B.1: every migration is re-runnable.
		await claimIdempotencyKey(sql, tenants.a.userId, 'survives-rerun', HASH_A);

		await applyMigration(harness.db, MIGRATION);

		const rows = await sql<{ key: string }[]>`
			select key from public.idempotency_keys where key = ${'survives-rerun'}
		`;
		expect(rows).toHaveLength(1);
		await releaseIdempotencyKey(sql, tenants.a.userId, 'survives-rerun');
	});

	it('claims a fresh key and replays the completed response', async () => {
		const key = 'claim-then-replay';
		expect(await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A)).toEqual({ kind: 'CLAIMED' });

		await completeIdempotencyKey(sql, tenants.a.userId, key, 201, { data: { id: 'txn-1' } });

		expect(await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A)).toEqual({
			kind: 'REPLAY',
			status: 201,
			body: { data: { id: 'txn-1' } },
		});
	});

	it('refuses the same key with a different body', async () => {
		const key = 'reused-key';
		await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A);
		await completeIdempotencyKey(sql, tenants.a.userId, key, 201, { data: 1 });

		expect(await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_B)).toEqual({ kind: 'CONFLICT' });
	});

	it('reports a claim that is still running', async () => {
		const key = 'in-flight';
		await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A);

		expect(await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A)).toEqual({ kind: 'IN_FLIGHT' });
	});

	it('lets a later request take over a claim the worker never finished', async () => {
		const key = 'abandoned';
		await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A);
		await sql`
			update public.idempotency_keys
			set created_at = now() - interval '30 minutes'
			where user_id = ${tenants.a.userId} and key = ${key}
		`;

		expect(await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A)).toEqual({ kind: 'CLAIMED' });
	});

	it('keeps one user’s key out of another user’s way', async () => {
		const key = 'shared-key-name';
		await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A);
		await completeIdempotencyKey(sql, tenants.a.userId, key, 201, { data: 'alpha' });

		// Same key string, different tenant: a fresh claim, never alpha's answer.
		expect(await claimIdempotencyKey(sql, tenants.b.userId, key, HASH_B)).toEqual({ kind: 'CLAIMED' });
		await completeIdempotencyKey(sql, tenants.b.userId, key, 201, { data: 'bravo' });

		expect(await claimIdempotencyKey(sql, tenants.b.userId, key, HASH_B)).toEqual({
			kind: 'REPLAY',
			status: 201,
			body: { data: 'bravo' },
		});
	});

	it('frees the key when the claim is released', async () => {
		const key = 'released';
		await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A);
		await releaseIdempotencyKey(sql, tenants.a.userId, key);

		expect(await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_B)).toEqual({ kind: 'CLAIMED' });
	});

	it('never releases a completed response', async () => {
		const key = 'completed-stays';
		await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A);
		await completeIdempotencyKey(sql, tenants.a.userId, key, 201, { data: 'kept' });
		await releaseIdempotencyKey(sql, tenants.a.userId, key);

		expect(await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A)).toMatchObject({ kind: 'REPLAY' });
	});

	it('purges rows past the retention window and keeps fresh ones', async () => {
		await sql`delete from public.idempotency_keys`;
		await claimIdempotencyKey(sql, tenants.a.userId, 'fresh', HASH_A);
		await claimIdempotencyKey(sql, tenants.a.userId, 'expired', HASH_A);
		await sql`
			update public.idempotency_keys
			set created_at = now() - interval '25 hours'
			where key = ${'expired'}
		`;

		const outcome = await purgeExpiredIdempotencyKeys(sql, 24);

		expect(outcome.deleted_count).toBe(1);
		expect(outcome.exhausted).toBe(false);
		const remaining = await sql<{ key: string }[]>`select key from public.idempotency_keys`;
		expect(remaining.map((row) => row.key)).toEqual(['fresh']);
	});

	it('drains a backlog across batches in one run', async () => {
		await sql`delete from public.idempotency_keys`;
		for (const key of ['old-1', 'old-2', 'old-3', 'old-4', 'old-5']) {
			await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A);
		}
		await sql`update public.idempotency_keys set created_at = now() - interval '25 hours'`;

		// Two at a time: a single batch would leave three behind every night.
		expect(await purgeExpiredIdempotencyKeys(sql, 24, 2, 10)).toEqual({
			deleted_count: 5,
			exhausted: false,
		});
		const left = await sql<CountRow[]>`select count(*) as count from public.idempotency_keys`;
		expect(Number(left[0].count)).toBe(0);
	});

	it('stops at its batch bound and reports the rest for tomorrow', async () => {
		await sql`delete from public.idempotency_keys`;
		for (const key of ['b-1', 'b-2', 'b-3', 'b-4']) {
			await claimIdempotencyKey(sql, tenants.a.userId, key, HASH_A);
		}
		await sql`update public.idempotency_keys set created_at = now() - interval '25 hours'`;

		expect(await purgeExpiredIdempotencyKeys(sql, 24, 1, 2)).toEqual({
			deleted_count: 2,
			exhausted: true,
		});
		const left = await sql<CountRow[]>`select count(*) as count from public.idempotency_keys`;
		expect(Number(left[0].count)).toBe(2);
	});

	it('drops the ledger with the user', async () => {
		const rows = await sql<{ id: string }[]>`
			insert into public.users (email, auth_provider, auth_subject)
			values (${'idem-doomed@example.test'}, ${'clerk'}, ${'user_idem_doomed'})
			returning id
		`;
		const doomedId = rows[0].id;
		await claimIdempotencyKey(sql, doomedId, 'doomed-key', HASH_A);

		await sql`delete from public.users where id = ${doomedId}`;

		const left = await sql<CountRow[]>`
			select count(*) as count from public.idempotency_keys where user_id = ${doomedId}
		`;
		expect(Number(left[0].count)).toBe(0);
	});
});

describe('tenancy: withIdempotency', () => {
	it('runs the handler once across an identical retry', async () => {
		await sql`delete from public.idempotency_keys`;
		const handler = vi.fn(
			async (): Promise<IdempotentResult> => ({ status: 201, body: { data: { id: 'txn-9' } } }),
		);
		const body = { amount_in_paise: 1000, note: 'lunch' };

		const first = fakeResponse();
		await withIdempotency(sql, tenants.a.userId, fakeRequest(body, 'retry-1'), first, handler);

		// The retry arrives with the fields in a different order: the same
		// request as far as the caller is concerned, and the hash agrees.
		const second = fakeResponse();
		await withIdempotency(
			sql,
			tenants.a.userId,
			fakeRequest({ note: 'lunch', amount_in_paise: 1000 }, 'retry-1'),
			second,
			handler,
		);

		expect(handler).toHaveBeenCalledTimes(1);
		expect(first.statusCode).toBe(201);
		expect(second.statusCode).toBe(201);
		expect(second.body).toEqual(first.body);
		expect(first.headers[IDEMPOTENT_REPLAY_HEADER]).toBeUndefined();
		expect(second.headers[IDEMPOTENT_REPLAY_HEADER]).toBe('true');
	});

	it('runs the handler every time when no key is sent', async () => {
		const handler = vi.fn(
			async (): Promise<IdempotentResult> => ({ status: 201, body: { data: 'fresh' } }),
		);

		await withIdempotency(sql, tenants.a.userId, fakeRequest({ a: 1 }), fakeResponse(), handler);
		await withIdempotency(sql, tenants.a.userId, fakeRequest({ a: 1 }), fakeResponse(), handler);

		expect(handler).toHaveBeenCalledTimes(2);
	});

	it('rejects a reused key carrying a different body', async () => {
		const handler = async (): Promise<IdempotentResult> => ({ status: 201, body: { data: 1 } });
		await withIdempotency(sql, tenants.a.userId, fakeRequest({ a: 1 }, 'mixed'), fakeResponse(), handler);

		const error = await withIdempotency(
			sql,
			tenants.a.userId,
			fakeRequest({ a: 2 }, 'mixed'),
			fakeResponse(),
			handler,
		).catch((caught: unknown) => caught);

		expect(error).toBeInstanceOf(HttpError);
		expect((error as HttpError).statusCode).toBe(409);
		expect((error as HttpError).errorCode).toBe('IDEMPOTENCY_KEY_REUSED');
	});

	it('frees the key when the handler fails, so the caller can retry', async () => {
		const before = await countKeys();
		const failing = async (): Promise<IdempotentResult> => {
			throw new Error('write failed');
		};

		await expect(
			withIdempotency(sql, tenants.a.userId, fakeRequest({ a: 1 }, 'failing'), fakeResponse(), failing),
		).rejects.toThrow('write failed');

		expect(await countKeys()).toBe(before);
		const succeeding = async (): Promise<IdempotentResult> => ({ status: 201, body: { data: 'ok' } });
		const res = fakeResponse();
		await withIdempotency(sql, tenants.a.userId, fakeRequest({ a: 1 }, 'failing'), res, succeeding);
		expect(res.statusCode).toBe(201);
	});

	it('refuses a concurrent duplicate instead of running twice', async () => {
		await sql`delete from public.idempotency_keys`;
		let release: (() => void) | null = null;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const slow = vi.fn(async (): Promise<IdempotentResult> => {
			await gate;
			return { status: 201, body: { data: 'slow' } };
		});

		const inFlight = withIdempotency(
			sql,
			tenants.a.userId,
			fakeRequest({ a: 1 }, 'concurrent'),
			fakeResponse(),
			slow,
		);
		// Give the claim time to commit before the duplicate arrives.
		await vi.waitFor(async () => expect(await countKeys()).toBe(1));

		const duplicate = await withIdempotency(
			sql,
			tenants.a.userId,
			fakeRequest({ a: 1 }, 'concurrent'),
			fakeResponse(),
			slow,
		).catch((caught: unknown) => caught);

		expect((duplicate as HttpError).errorCode).toBe('IDEMPOTENCY_KEY_IN_FLIGHT');
		release?.();
		await inFlight;
		expect(slow).toHaveBeenCalledTimes(1);
	});

	it('still answers 201 when recording the response fails', async () => {
		// The transaction was written; turning that into a 500 would make the
		// caller retry and duplicate it.
		await sql`delete from public.idempotency_keys`;
		const failingSql = new Proxy(sql as object, {
			apply(target, thisArg, args: unknown[]) {
				const [strings] = args as [TemplateStringsArray];
				if (strings.join('').includes('set response_status')) {
					return Promise.reject(new Error('ledger write failed'));
				}
				return Reflect.apply(target as never, thisArg, args);
			},
		}) as SqlClient;
		const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});

		const res = fakeResponse();
		await withIdempotency(
			failingSql,
			tenants.a.userId,
			fakeRequest({ a: 1 }, 'complete-fails'),
			res,
			async () => ({ status: 201, body: { data: 'written' } }),
		);

		expect(res.statusCode).toBe(201);
		expect(res.body).toEqual({ data: 'written' });
		expect(errorLog.mock.calls.some((call) => call[0] === 'IDEMPOTENCY_COMPLETE_FAILED')).toBe(true);
		// The claim survives, so an immediate retry is refused rather than re-run.
		const retry = await withIdempotency(
			sql,
			tenants.a.userId,
			fakeRequest({ a: 1 }, 'complete-fails'),
			fakeResponse(),
			async () => ({ status: 201, body: { data: 'second' } }),
		).catch((caught: unknown) => caught);
		expect((retry as HttpError).errorCode).toBe('IDEMPOTENCY_KEY_IN_FLIGHT');
		errorLog.mockRestore();
	});
});
