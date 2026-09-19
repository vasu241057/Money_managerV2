import { describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { deleteAccount, parseDeleteAccountRequest } from '../src/services/account.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

interface SqlMock extends SqlClient {
	queries: string[];
}

// Includes-based dispatch: the service runs a fixed sequence of statements
// (identity read, connection read, transaction, tombstone update) and the
// test only cares which of them ran and with what.
function createSqlMock(handler: QueryHandler): SqlMock {
	const queries: string[] = [];
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		queries.push(query);
		return handler(query, values);
	};
	const mock = tag as unknown as SqlMock;
	mock.queries = queries;
	(mock as unknown as { begin: unknown }).begin = async (callback: (tx: SqlClient) => Promise<unknown>) =>
		callback(mock);
	return mock;
}

const USER_ID = '11111111-1111-4111-8111-111111111111';
const CLERK_SUBJECT = 'user_2clerkSubject';

function createEnv(overrides: Partial<Env> = {}): Env {
	const keyBytes = new Uint8Array(32).fill(7);
	return {
		GOOGLE_CLIENT_ID: 'google-client-id',
		GOOGLE_CLIENT_SECRET: 'google-client-secret',
		GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY: Buffer.from(keyBytes).toString('base64url'),
		CLERK_SECRET_KEY: 'sk_test_secret',
		...overrides,
	} as Env;
}

function connectionRow(refreshToken: string | null) {
	return {
		id: '22222222-2222-4222-8222-222222222222',
		user_id: USER_ID,
		provider: 'google',
		email_address: 'person@gmail.test',
		access_token: null,
		refresh_token: refreshToken,
		last_sync_timestamp: 0,
		sync_status: 'ACTIVE',
		created_at: new Date('2026-09-01T00:00:00Z'),
		updated_at: new Date('2026-09-01T00:00:00Z'),
	};
}

interface Scenario {
	clerkStatus?: number;
	googleStatus?: number;
	authSubject?: string | null;
	refreshToken?: string | null;
}

function runScenario(scenario: Scenario = {}) {
	const tombstoneUpdates: Array<{ query: string; values: unknown[] }> = [];
	const sql = createSqlMock((query, values) => {
		if (query.includes('select u.auth_provider, u.auth_subject')) {
			return [{ auth_provider: 'clerk', auth_subject: scenario.authSubject === undefined ? CLERK_SUBJECT : scenario.authSubject }];
		}
		if (query.includes('from public.oauth_connections as oc')) {
			// Plain-text token: decryptTokenValue passes non-prefixed values through.
			return [connectionRow(scenario.refreshToken === undefined ? 'refresh-plain' : scenario.refreshToken)];
		}
		if (query.includes('delete from public.merchant_mapping_attempts')) {
			return [{ search_key: 'ONLYMINE' }];
		}
		if (query.includes('delete from public.email_templates')) {
			return [{ id: 'template-1' }, { id: 'template-2' }];
		}
		if (query.includes('delete from public.user_merchant_rule_categories')) {
			return [];
		}
		if (query.includes('delete from public.users')) {
			return [{ id: USER_ID }];
		}
		if (query.includes('insert into public.user_deletion_log')) {
			return [];
		}
		if (query.includes('update public.user_deletion_log')) {
			tombstoneUpdates.push({ query, values });
			return [];
		}
		throw new Error(`Unexpected query: ${query}`);
	});

	const requests: Array<{ url: string; method: string | undefined; body: string | null }> = [];
	const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
		const url = typeof input === 'string' ? input : input.toString();
		requests.push({ url, method: init?.method, body: typeof init?.body === 'string' ? init.body : null });
		if (url.startsWith('https://oauth2.googleapis.com/revoke')) {
			return new Response('', { status: scenario.googleStatus ?? 200 });
		}
		if (url.startsWith('https://api.clerk.com/v1/users/')) {
			return new Response('{}', { status: scenario.clerkStatus ?? 200 });
		}
		throw new Error(`Unexpected URL: ${url}`);
	});

	return { sql, fetcher, requests, tombstoneUpdates };
}

describe('parseDeleteAccountRequest', () => {
	it('accepts only the exact confirmation word', () => {
		expect(() => parseDeleteAccountRequest({ confirm: 'DELETE' })).not.toThrow();
		for (const body of [{}, { confirm: 'delete' }, { confirm: 'DELETE ' }, { confirm: true }, null, 'DELETE']) {
			expect(() => parseDeleteAccountRequest(body)).toThrow();
		}
	});
});

describe('deleteAccount', () => {
	it('revokes at Google, cascades the user in one transaction, then deletes the Clerk user', async () => {
		const { sql, fetcher, requests, tombstoneUpdates } = runScenario();

		const outcome = await deleteAccount(sql, USER_ID, createEnv(), fetcher);

		expect(outcome).toEqual({
			user_id: USER_ID,
			google_connection_count: 1,
			google_revoked: true,
			private_template_count: 2,
			mapping_attempt_count: 1,
			clerk_deleted: true,
			clerk_error: null,
		});

		// Google before the cascade (tokens still readable), Clerk after it.
		expect(requests.map(request => new URL(request.url).host)).toEqual([
			'oauth2.googleapis.com',
			'api.clerk.com',
		]);
		expect(requests[0]?.body).toBe('token=refresh-plain');
		expect(requests[1]?.method).toBe('DELETE');
		expect(requests[1]?.url).toBe(`https://api.clerk.com/v1/users/${CLERK_SUBJECT}`);

		const order = sql.queries.map(query => query.split(' ').slice(0, 4).join(' '));
		const attemptsIndex = sql.queries.findIndex(query => query.includes('delete from public.merchant_mapping_attempts'));
		const templatesIndex = sql.queries.findIndex(query => query.includes('delete from public.email_templates'));
		const usersIndex = sql.queries.findIndex(query => query.includes('delete from public.users'));
		const ruleCategoriesIndex = sql.queries.findIndex(query => query.includes('delete from public.user_merchant_rule_categories'));
		const tombstoneIndex = sql.queries.findIndex(query => query.includes('insert into public.user_deletion_log'));
		expect(attemptsIndex, order.join(' | ')).toBeLessThan(usersIndex);
		expect(templatesIndex).toBeLessThan(usersIndex);
		expect(ruleCategoriesIndex).toBeLessThan(usersIndex);
		expect(usersIndex).toBeLessThan(tombstoneIndex);

		// Only this user's keys: both an "exists for me" and a "not exists for anyone else" clause.
		const attemptsQuery = sql.queries[attemptsIndex] ?? '';
		expect(attemptsQuery).toContain('where fe.user_id = and fe.search_key = mma.search_key');
		expect(attemptsQuery).toContain('where other.user_id <> and other.search_key = mma.search_key');
		expect(sql.queries[templatesIndex]).toContain("et.scope = 'PRIVATE' and et.learned_from_user_id =");

		expect(tombstoneUpdates).toHaveLength(1);
		expect(tombstoneUpdates[0]?.query).toContain('set clerk_deleted = true, auth_subject = null');
	});

	it('keeps the subject on the tombstone when Clerk fails, so the nightly retry can finish', async () => {
		const { sql, fetcher, tombstoneUpdates } = runScenario({ clerkStatus: 500 });

		const outcome = await deleteAccount(sql, USER_ID, createEnv(), fetcher);

		expect(outcome.clerk_deleted).toBe(false);
		expect(outcome.clerk_error).toBe('http 500');
		expect(tombstoneUpdates).toHaveLength(1);
		expect(tombstoneUpdates[0]?.query).not.toContain('clerk_deleted = true');
		expect(tombstoneUpdates[0]?.query).toContain('clerk_last_error = where user_id =');
		expect(tombstoneUpdates[0]?.values[0]).toBe('http 500');
	});

	it('treats a Clerk 404 as already deleted', async () => {
		const { sql, fetcher } = runScenario({ clerkStatus: 404 });

		const outcome = await deleteAccount(sql, USER_ID, createEnv(), fetcher);

		expect(outcome.clerk_deleted).toBe(true);
	});

	it('still deletes the data when the Clerk secret is not configured', async () => {
		const { sql, fetcher, requests } = runScenario();

		const outcome = await deleteAccount(sql, USER_ID, createEnv({ CLERK_SECRET_KEY: undefined }), fetcher);

		expect(outcome.clerk_deleted).toBe(false);
		expect(outcome.clerk_error).toBe('CLERK_SECRET_KEY is not configured');
		expect(requests.map(request => new URL(request.url).host)).toEqual(['oauth2.googleapis.com']);
		expect(sql.queries.some(query => query.includes('delete from public.users'))).toBe(true);
	});

	it('records an unrevoked Google grant without blocking the deletion', async () => {
		const { sql, fetcher } = runScenario({ googleStatus: 503 });

		const outcome = await deleteAccount(sql, USER_ID, createEnv(), fetcher);

		expect(outcome.google_revoked).toBe(false);
		expect(sql.queries.some(query => query.includes('delete from public.users'))).toBe(true);
		const tombstone = sql.queries.find(query => query.includes('insert into public.user_deletion_log'));
		expect(tombstone).toBeDefined();
	});

	it('does not call Clerk for a row provisioned before the subject was recorded', async () => {
		const { sql, fetcher, requests } = runScenario({ authSubject: null });

		const outcome = await deleteAccount(sql, USER_ID, createEnv(), fetcher);

		expect(outcome.clerk_deleted).toBe(false);
		expect(outcome.clerk_error).toBe('no provider subject on record');
		expect(requests.some(request => request.url.includes('api.clerk.com'))).toBe(false);
	});

	it('fails with 404 when the user row is already gone', async () => {
		const sql = createSqlMock(query => {
			if (query.includes('select u.auth_provider, u.auth_subject')) return [];
			throw new Error(`Unexpected query: ${query}`);
		});

		await expect(deleteAccount(sql, USER_ID, createEnv(), vi.fn())).rejects.toMatchObject({
			statusCode: 404,
			errorCode: 'USER_NOT_FOUND',
		});
	});
});
