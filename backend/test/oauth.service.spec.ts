import { afterEach, describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	completeGoogleOAuthCallback,
	createGoogleOAuthState,
	disconnectGoogleOAuthConnection,
	getGoogleOAuthConnectionStatus,
	parseGoogleOAuthCallbackRequest,
	startGoogleOAuth,
	verifyGoogleOAuthState,
} from '../src/services/oauth.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlMock(handlers: QueryHandler[]): SqlClient {
	let cursor = 0;

	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		const handler = handlers[cursor];
		cursor += 1;

		if (!handler) {
			throw new Error(`Unexpected query execution: ${query}`);
		}

		return handler(query, values);
	};

	return tag as unknown as SqlClient;
}

function createEnv(overrides?: Partial<Env>): Env {
	const keyBytes = new Uint8Array(32).fill(7);
	const encodedKey = Buffer.from(keyBytes).toString('base64url');

	return {
		GOOGLE_CLIENT_ID: 'google-client-id',
		GOOGLE_CLIENT_SECRET: 'google-client-secret',
		GOOGLE_OAUTH_REDIRECT_URI: 'https://app.money-manager.com/oauth/google/callback',
		GOOGLE_OAUTH_STATE_SECRET: 'state-secret-for-tests',
		GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY: encodedKey,
		...overrides,
	} as Env;
}

afterEach(() => {
	vi.restoreAllMocks();
});

describe('oauth.service', () => {
	it('creates and verifies Google OAuth state successfully', async () => {
		const userId = '11111111-1111-4111-8111-111111111111';
		const now = 1_800_000_000;
		const state = await createGoogleOAuthState(userId, 'secret', now);

		await expect(
			verifyGoogleOAuthState(state, userId, 'secret', now + 10),
		).resolves.toBeUndefined();

		await expect(
			verifyGoogleOAuthState(state, '22222222-2222-4222-8222-222222222222', 'secret', now + 10),
		).rejects.toThrow('OAuth state user mismatch');
	});

	it('rejects expired OAuth state', async () => {
		const userId = '11111111-1111-4111-8111-111111111111';
		const now = 1_800_000_000;
		const state = await createGoogleOAuthState(userId, 'secret', now);

		await expect(
			verifyGoogleOAuthState(state, userId, 'secret', now + 1200),
		).rejects.toThrow('OAuth state has expired');
	});

	it('parses callback payload', () => {
		expect(parseGoogleOAuthCallbackRequest({ code: 'abc', state: 'xyz' })).toEqual({
			code: 'abc',
			state: 'xyz',
		});

		expect(() => parseGoogleOAuthCallbackRequest({ code: 'abc' })).toThrow(
			'state must be a string',
		);
	});

	it('builds Google auth URL from start endpoint', async () => {
		const response = await startGoogleOAuth(
			'11111111-1111-4111-8111-111111111111',
			createEnv(),
		);

		const parsed = new URL(response.auth_url);
		expect(parsed.origin).toBe('https://accounts.google.com');
		expect(parsed.searchParams.get('client_id')).toBe('google-client-id');
		expect(parsed.searchParams.get('redirect_uri')).toBe(
			'https://app.money-manager.com/oauth/google/callback',
		);
		expect(parsed.searchParams.get('scope')).toContain('gmail.readonly');
		expect(parsed.searchParams.get('state')).toBeTruthy();
	});

	it('returns null when no Google connection exists', async () => {
		const sql = createSqlMock([
			(query, values) => {
				expect(query).toContain('from public.oauth_connections');
				expect(values[0]).toBe('11111111-1111-4111-8111-111111111111');
				return [];
			},
		]);

		const response = await getGoogleOAuthConnectionStatus(
			sql,
			'11111111-1111-4111-8111-111111111111',
		);
		expect(response.connection).toBeNull();
	});

	it('redacts tokens in returned Google connection status', async () => {
		const sql = createSqlMock([
			() => [
				{
					id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					user_id: '11111111-1111-4111-8111-111111111111',
					provider: 'google',
					email_address: 'test@example.com',
					access_token: 'enc:v1:secret',
					refresh_token: 'enc:v1:secret2',
					last_sync_timestamp: 0,
					sync_status: 'ACTIVE',
					created_at: '2026-01-01T00:00:00.000Z',
					updated_at: '2026-01-01T00:00:00.000Z',
				},
			],
		]);

		const response = await getGoogleOAuthConnectionStatus(
			sql,
			'11111111-1111-4111-8111-111111111111',
		);
		expect(response.connection?.access_token).toBeNull();
		expect(response.connection?.refresh_token).toBeNull();
		expect(response.connection?.sync_status).toBe('ACTIVE');
	});

	it('completes callback, persists encrypted tokens, and redacts response', async () => {
		const now = 1_800_000_000;
		vi.spyOn(Date, 'now').mockReturnValue(now * 1000);
		const userId = '11111111-1111-4111-8111-111111111111';
		const env = createEnv();
		const state = await createGoogleOAuthState(userId, env.GOOGLE_OAUTH_STATE_SECRET as string, now);
		const sql = createSqlMock([
			() => [
					{
						id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
						user_id: userId,
						provider: 'google',
						email_address: 'new@example.com',
						access_token: null,
						refresh_token: 'plain-refresh-token',
						last_sync_timestamp: 0,
					sync_status: 'AUTH_REVOKED',
					created_at: '2026-01-01T00:00:00.000Z',
					updated_at: '2026-01-01T00:00:00.000Z',
				},
			],
			(_query, values) => {
				expect(values[0]).toBe(userId);
				expect(values[2]).toBe('new@example.com');
				expect(typeof values[3]).toBe('string');
				expect(String(values[3])).toContain('enc:v1:');
				expect(typeof values[4]).toBe('string');
				expect(String(values[4])).toContain('enc:v1:');
				return [
					{
						id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
						user_id: userId,
						provider: 'google',
						email_address: 'new@example.com',
						access_token: values[3],
						refresh_token: values[4],
						last_sync_timestamp: 0,
						sync_status: 'ACTIVE',
						created_at: '2026-01-01T00:00:00.000Z',
						updated_at: '2026-01-02T00:00:00.000Z',
					},
				];
			},
			() => [],
		]);

		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValueOnce(
				new Response(JSON.stringify({ access_token: 'access-token-new' }), { status: 200 }),
			)
			.mockResolvedValueOnce(
				new Response(JSON.stringify({ email: 'new@example.com' }), { status: 200 }),
			);

		const response = await completeGoogleOAuthCallback(
			sql,
			userId,
			{
				code: 'auth-code',
				state,
			},
			env,
			fetcher,
		);

		expect(response.connection.email_address).toBe('new@example.com');
		expect(response.connection.sync_status).toBe('ACTIVE');
		expect(response.connection.access_token).toBeNull();
		expect(response.connection.refresh_token).toBeNull();
	});

	it('reuses refresh token from older connection row when latest row lacks one', async () => {
		const now = 1_800_000_000;
		vi.spyOn(Date, 'now').mockReturnValue(now * 1000);
		const userId = '11111111-1111-4111-8111-111111111111';
		const env = createEnv();
		const state = await createGoogleOAuthState(userId, env.GOOGLE_OAUTH_STATE_SECRET as string, now);
		const sql = createSqlMock([
			() => [
				{
					id: 'latest-conn-id',
					user_id: userId,
					provider: 'google',
					email_address: 'latest@example.com',
					access_token: null,
					refresh_token: null,
					last_sync_timestamp: 0,
					sync_status: 'ERROR_PAUSED',
					created_at: '2026-01-03T00:00:00.000Z',
					updated_at: '2026-01-03T00:00:00.000Z',
				},
					{
						id: 'older-conn-id',
						user_id: userId,
						provider: 'google',
						email_address: 'new@example.com',
						access_token: null,
						refresh_token: 'enc:v1:older-refresh',
						last_sync_timestamp: 0,
					sync_status: 'AUTH_REVOKED',
					created_at: '2026-01-01T00:00:00.000Z',
					updated_at: '2026-01-01T00:00:00.000Z',
				},
			],
			(_query, values) => {
				expect(values[4]).toBe('enc:v1:older-refresh');
				return [
					{
						id: 'latest-conn-id',
						user_id: userId,
						provider: 'google',
						email_address: 'new@example.com',
						access_token: values[3],
						refresh_token: values[4],
						last_sync_timestamp: 0,
						sync_status: 'ACTIVE',
						created_at: '2026-01-03T00:00:00.000Z',
						updated_at: '2026-01-04T00:00:00.000Z',
					},
				];
			},
			() => [],
		]);

		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValueOnce(
				new Response(JSON.stringify({ access_token: 'access-token-new' }), { status: 200 }),
			)
			.mockResolvedValueOnce(
				new Response(JSON.stringify({ email: 'new@example.com' }), { status: 200 }),
			);

		const response = await completeGoogleOAuthCallback(
			sql,
			userId,
			{
				code: 'auth-code',
				state,
			},
			env,
			fetcher,
		);

		expect(response.connection.email_address).toBe('new@example.com');
		expect(response.connection.sync_status).toBe('ACTIVE');
	});

	it('does not reuse refresh token from a different Gmail address', async () => {
		const now = 1_800_000_000;
		vi.spyOn(Date, 'now').mockReturnValue(now * 1000);
		const userId = '11111111-1111-4111-8111-111111111111';
		const env = createEnv();
		const state = await createGoogleOAuthState(userId, env.GOOGLE_OAUTH_STATE_SECRET as string, now);
		const sql = createSqlMock([
			() => [
				{
					id: 'other-conn-id',
					user_id: userId,
					provider: 'google',
					email_address: 'other@example.com',
					access_token: null,
					refresh_token: 'enc:v1:other-refresh',
					last_sync_timestamp: 0,
					sync_status: 'AUTH_REVOKED',
					created_at: '2026-01-01T00:00:00.000Z',
					updated_at: '2026-01-01T00:00:00.000Z',
				},
			],
		]);

		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValueOnce(
				new Response(JSON.stringify({ access_token: 'access-token-new' }), { status: 200 }),
			)
			.mockResolvedValueOnce(
				new Response(JSON.stringify({ email: 'new@example.com' }), { status: 200 }),
			);

		await expect(
			completeGoogleOAuthCallback(
				sql,
				userId,
				{
					code: 'auth-code',
					state,
				},
				env,
				fetcher,
			),
		).rejects.toThrow('Google did not return a refresh token');
	});

	it('disconnects all Google connections for a user', async () => {
		const sql = createSqlMock([
			(query, values) => {
				expect(query).toContain("set access_token = null");
				expect(values[0]).toBe('11111111-1111-4111-8111-111111111111');
				return [];
			},
		]);

		await expect(
			disconnectGoogleOAuthConnection(sql, '11111111-1111-4111-8111-111111111111'),
		).resolves.toBeUndefined();
	});
});
