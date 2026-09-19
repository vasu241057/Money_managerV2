// Account deletion on the real schema (Task 5): one user disappears
// completely (every user-owned table, their private payees, their PRIVATE
// templates, the mapper's memory of keys only they paid), the other tenant is
// byte-for-byte untouched, SHARED templates survive, Google is revoked and the
// Clerk user is deleted — with the tombstone tracking what is still pending
// and the nightly retry finishing (or, for a returned user, standing down).

import type { PGlite } from '@electric-sql/pglite';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { deleteAccount } from '../../src/services/account.service';
import { retryPendingClerkDeletions } from '../../src/workers/raw-email-body-purge';
import { insertLooseEvent, seedTenant, seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';
import { snapshotUser, type UserSnapshot } from './snapshot';

const SENDER = 'alerts@bank.test';

interface CountRow {
	count: string;
}

interface TombstoneRow {
	user_id: string;
	clerk_deleted: boolean;
	google_revoked: boolean;
	auth_provider: string | null;
	auth_subject: string | null;
	clerk_delete_attempts: number;
	clerk_last_error: string | null;
}

function createEnv(overrides: Partial<Env> = {}): Env {
	const keyBytes = new Uint8Array(32).fill(9);
	return {
		GOOGLE_CLIENT_ID: 'google-client-id',
		GOOGLE_CLIENT_SECRET: 'google-client-secret',
		GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY: Buffer.from(keyBytes).toString('base64url'),
		CLERK_SECRET_KEY: 'sk_test_secret',
		...overrides,
	} as Env;
}

interface FakeUpstream {
	fetcher: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
	calls: Array<{ url: string; method: string | undefined; body: string | null }>;
}

function createUpstream(clerkStatus: number, googleStatus = 200): FakeUpstream {
	const calls: FakeUpstream['calls'] = [];
	const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
		const url = typeof input === 'string' ? input : input.toString();
		calls.push({ url, method: init?.method, body: typeof init?.body === 'string' ? init.body : null });
		if (url.startsWith('https://oauth2.googleapis.com/revoke')) {
			return new Response('', { status: googleStatus });
		}
		if (url.startsWith('https://api.clerk.com/v1/users/')) {
			return new Response('{}', { status: clerkStatus });
		}
		throw new Error(`unexpected upstream ${url}`);
	});
	return { fetcher, calls };
}

async function insertTemplate(
	sql: SqlClient,
	skeleton: string,
	owner: string | null,
): Promise<void> {
	await sql`
		insert into public.email_templates (from_address, skeleton_hash, skeleton, slot_map, learned_from_user_id, scope)
		values (
			${SENDER}, ${skeleton}, ${skeleton}, ${'{"version":1,"kind":"IGNORE"}'}::jsonb,
			${owner}, ${owner === null ? 'SHARED' : 'PRIVATE'}
		)
	`;
}

async function countWhere(db: PGlite, table: string, column: string, value: string): Promise<number> {
	if (!/^[a-z_]+$/.test(table) || !/^[a-z_]+$/.test(column)) throw new Error('unsafe identifier');
	const result = await db.query<CountRow>(
		`select count(*)::text as count from public.${table} where ${column} = $1`,
		[value],
	);
	return Number(result.rows[0]?.count ?? 0);
}

async function tombstone(sql: SqlClient, userId: string): Promise<TombstoneRow | undefined> {
	const rows = await sql<TombstoneRow[]>`
		select user_id, clerk_deleted, google_revoked, auth_provider, auth_subject, clerk_delete_attempts, clerk_last_error
		from public.user_deletion_log
		where user_id = ${userId}
	`;
	return rows[0];
}

function nonEmptyTables(snapshot: UserSnapshot): string[] {
	return Object.entries(snapshot)
		.filter(([, rows]) => rows.length > 0)
		.map(([table]) => table)
		.sort();
}

describe('tenancy: account deletion', () => {
	let tenancy: TenancyDb;
	let sql: SqlClient;
	let fx: TwoTenantFixture;
	let bravoBefore: UserSnapshot;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		sql = tenancy.sql;
		fx = await seedTwoTenants(sql);

		// Templates: one SHARED, one PRIVATE per tenant.
		await insertTemplate(sql, 'shared-skeleton', null);
		await insertTemplate(sql, 'alpha-skeleton', fx.a.userId);
		await insertTemplate(sql, 'bravo-skeleton', fx.b.userId);

		// Mapper memory: ALPHAPAYEE is only alpha's, BRAVOPAYEE only bravo's,
		// LOOSEPAYEE is paid by both.
		await insertLooseEvent(sql, fx.a.userId, null, 901);
		await insertLooseEvent(sql, fx.b.userId, null, 902);
		for (const key of [fx.a.searchKey, fx.b.searchKey, 'LOOSEPAYEE']) {
			await sql`
				insert into public.merchant_mapping_attempts (search_key, attempt_count, last_resolution, person_display_name)
				values (${key}, ${1}, ${'P2P'}, ${'Some Person'})
			`;
		}

		// A stored (plain, pre-encryption-format) refresh token so the Google
		// revocation has something to send.
		await sql`
			update public.oauth_connections
			set refresh_token = ${'refresh-alpha'}
			where id = ${fx.a.connectionId}
		`;

		bravoBefore = await snapshotUser(tenancy.db, fx.b.userId);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	it('removes everything alpha owned, leaves bravo untouched, revokes Google and deletes the Clerk user', async () => {
		const upstream = createUpstream(200);
		const alphaBefore = await snapshotUser(tenancy.db, fx.a.userId);
		expect(nonEmptyTables(alphaBefore).length).toBeGreaterThan(10);

		const outcome = await deleteAccount(sql, fx.a.userId, createEnv(), upstream.fetcher);

		expect(outcome).toMatchObject({
			user_id: fx.a.userId,
			google_connection_count: 1,
			google_revoked: true,
			private_template_count: 1,
			mapping_attempt_count: 1,
			clerk_deleted: true,
			clerk_error: null,
		});

		expect(upstream.calls.map((call) => new URL(call.url).host)).toEqual([
			'oauth2.googleapis.com',
			'api.clerk.com',
		]);
		expect(upstream.calls[0]?.body).toBe('token=refresh-alpha');
		expect(upstream.calls[1]?.url).toBe('https://api.clerk.com/v1/users/user_fixture_alpha');
		expect(upstream.calls[1]?.method).toBe('DELETE');

		// Every user-owned table is empty for alpha; only the tombstone remains.
		const alphaAfter = await snapshotUser(tenancy.db, fx.a.userId);
		expect(nonEmptyTables(alphaAfter)).toEqual(['user_deletion_log']);
		expect(await countWhere(tenancy.db, 'users', 'id', fx.a.userId)).toBe(0);
		expect(await countWhere(tenancy.db, 'global_merchants', 'owner_user_id', fx.a.userId)).toBe(0);
		expect(await countWhere(tenancy.db, 'global_merchant_aliases', 'owner_user_id', fx.a.userId)).toBe(0);

		// Bravo: byte-for-byte the same, private payee and template included.
		expect(await snapshotUser(tenancy.db, fx.b.userId)).toEqual(bravoBefore);
		expect(await countWhere(tenancy.db, 'global_merchants', 'id', fx.sharedMerchantId)).toBe(1);

		// Templates: SHARED survives, alpha's PRIVATE is gone, bravo's stays.
		const templates = await sql<Array<{ skeleton: string; scope: string }>>`
			select skeleton, scope from public.email_templates order by skeleton
		`;
		expect(templates).toEqual([
			{ skeleton: 'bravo-skeleton', scope: 'PRIVATE' },
			{ skeleton: 'shared-skeleton', scope: 'SHARED' },
		]);

		// Mapper memory: only the key nobody else ever paid is forgotten.
		const attempts = await sql<Array<{ search_key: string }>>`
			select search_key from public.merchant_mapping_attempts order by search_key
		`;
		expect(attempts.map((row) => row.search_key)).toEqual([fx.b.searchKey, 'LOOSEPAYEE']);

		const row = await tombstone(sql, fx.a.userId);
		expect(row).toMatchObject({
			clerk_deleted: true,
			google_revoked: true,
			auth_provider: 'clerk',
			auth_subject: null,
			clerk_delete_attempts: 1,
			clerk_last_error: null,
		});
	});

	it('refuses to delete an account that is already gone', async () => {
		await expect(deleteAccount(sql, fx.a.userId, createEnv(), createUpstream(200).fetcher)).rejects.toMatchObject({
			statusCode: 404,
		});
	});

	it('keeps the provider subject on the tombstone when Clerk fails and the nightly retry finishes it', async () => {
		const failing = createUpstream(503);

		const outcome = await deleteAccount(sql, fx.b.userId, createEnv(), failing.fetcher);
		expect(outcome.clerk_deleted).toBe(false);
		expect(outcome.clerk_error).toBe('http 503');
		expect(await countWhere(tenancy.db, 'users', 'id', fx.b.userId)).toBe(0);
		expect(await tombstone(sql, fx.b.userId)).toMatchObject({
			clerk_deleted: false,
			auth_subject: 'user_fixture_bravo',
			clerk_delete_attempts: 1,
			clerk_last_error: 'http 503',
		});

		const succeeding = createUpstream(200);
		const retry = await retryPendingClerkDeletions(sql, createEnv(), succeeding.fetcher);
		expect(retry).toEqual({
			pending_count: 1,
			deleted_count: 1,
			user_returned_count: 0,
			failed_count: 0,
			skipped_reason: null,
		});
		expect(succeeding.calls.map((call) => call.url)).toEqual([
			'https://api.clerk.com/v1/users/user_fixture_bravo',
		]);
		expect(await tombstone(sql, fx.b.userId)).toMatchObject({
			clerk_deleted: true,
			auth_subject: null,
			clerk_delete_attempts: 2,
			clerk_last_error: null,
		});
	});

	it('stands down for a user who signed in again before the retry', async () => {
		const charlie = await seedTenant(sql, 'charlie', { sharedMerchantId: fx.sharedMerchantId }, 'Pets');
		const failing = createUpstream(500);
		await deleteAccount(sql, charlie.userId, createEnv(), failing.fetcher);
		expect(await countWhere(tenancy.db, 'users', 'id', charlie.userId)).toBe(0);

		// The auth middleware re-provisions the same id on the next sign-in.
		await sql`
			insert into public.users (id, email, auth_provider, auth_subject)
			values (${charlie.userId}, ${'charlie@example.test'}, ${'clerk'}, ${'user_fixture_charlie'})
		`;

		const upstream = createUpstream(200);
		const retry = await retryPendingClerkDeletions(sql, createEnv(), upstream.fetcher);
		expect(retry).toMatchObject({ pending_count: 1, user_returned_count: 1, deleted_count: 0 });
		expect(upstream.calls).toEqual([]);
		expect(await tombstone(sql, charlie.userId)).toBeUndefined();
		expect(await countWhere(tenancy.db, 'users', 'id', charlie.userId)).toBe(1);
	});
});
