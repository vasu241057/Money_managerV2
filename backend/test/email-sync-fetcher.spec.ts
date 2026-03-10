import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import { runEmailSyncUserJob } from '../src/workers/email-sync-fetcher';
import { TransientMessageError } from '../src/workers/queue.errors';

const { getAppConfigMock, getSqlClientMock } = vi.hoisted(() => ({
	getAppConfigMock: vi.fn(),
	getSqlClientMock: vi.fn(),
}));

vi.mock('../src/lib/config', () => ({
	getAppConfig: getAppConfigMock,
}));

vi.mock('../src/lib/db/client', () => ({
	getSqlClient: getSqlClientMock,
}));

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

interface SqlMock extends SqlClient {
	begin: (cb: (tx: SqlClient) => Promise<unknown>) => Promise<unknown>;
}

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlMock(handler: QueryHandler): SqlMock {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		return handler(query, values);
	};

	const begin = async (cb: (tx: SqlClient) => Promise<unknown>): Promise<unknown> =>
		cb(tag as unknown as SqlClient);

	return Object.assign(tag, { begin }) as unknown as SqlMock;
}

function buildConnectionRow(overrides?: Partial<Record<string, unknown>>): Record<string, unknown> {
	return {
		id: '00000000-0000-4000-8000-0000000000aa',
		user_id: '00000000-0000-4000-8000-000000000001',
		provider: 'google',
		email_address: 'user@example.com',
		access_token: null,
		refresh_token: 'plain-refresh-token',
		last_sync_timestamp: 1_700_000_000_000,
		sync_status: 'ACTIVE',
		created_at: '2026-03-10T00:00:00.000Z',
		updated_at: '2026-03-10T00:00:00.000Z',
		...overrides,
	};
}

function createEnv(overrides?: Partial<Env>): Env {
	return {
		APP_NAME: 'money-manager-backend',
		APP_VERSION: '0.1.0',
		NODE_ENV: 'test',
		SUPABASE_POOLER_URL: 'postgres://postgres:postgres@localhost:6543/postgres',
		DB_MAX_CONNECTIONS: '5',
		DB_CONNECT_TIMEOUT_SECONDS: '5',
		GOOGLE_CLIENT_ID: 'google-client-id',
		GOOGLE_CLIENT_SECRET: 'google-client-secret',
		GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY: btoa('0123456789abcdef0123456789abcdef'),
		EMAIL_SYNC_QUEUE: {
			send: vi.fn().mockResolvedValue(undefined),
		},
		...(overrides ?? {}),
	} as unknown as Env;
}

function toBase64Url(value: string): string {
	return btoa(value).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

describe('email-sync-fetcher', () => {
	const appConfig: AppConfig = {
		appName: 'money-manager-backend',
		appVersion: '0.1.0',
		nodeEnv: 'test',
		supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
		dbMaxConnections: 5,
		dbConnectTimeoutSeconds: 5,
	};

	beforeEach(() => {
		vi.clearAllMocks();
	});

	it('marks connection AUTH_REVOKED and returns success result when refresh token is invalid_grant', async () => {
		const queries: string[] = [];
		const sql = createSqlMock((query) => {
			queries.push(query);
			if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
				return [buildConnectionRow()];
			}
			if (query.includes("set access_token = null") && query.includes("sync_status = 'AUTH_REVOKED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		const fetcher = vi.fn().mockResolvedValue(
			new Response(JSON.stringify({ error: 'invalid_grant' }), {
				status: 400,
				headers: { 'content-type': 'application/json' },
			}),
		);
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(sql);

		const result = await runEmailSyncUserJob(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: '00000000-0000-4000-8000-000000000001',
				last_sync_timestamp: 1_700_000_000_000,
			},
			createEnv(),
			fetcher,
		);

		expect(result.processed_connection_count).toBe(1);
		expect(result.revoked_connection_count).toBe(1);
		expect(result.inserted_or_retried_raw_email_count).toBe(0);
		expect(queries.some(query => query.includes("sync_status = 'AUTH_REVOKED'"))).toBe(true);
	});

	it('persists only new/FAILED raw emails and advances cursor transactionally', async () => {
		const insertedSourceIds: string[] = [];
		let cursorUpdateValue: number | null = null;
		const sql = createSqlMock((query, values) => {
			if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
				return [buildConnectionRow()];
			}
			if (query.includes('and oc.refresh_token is not distinct from') && query.includes("set access_token =")) {
				return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
			}
				if (query.includes('from public.raw_emails as re')) {
					return [
						{
							source_id: 'm2',
							status: 'PROCESSED',
							internal_date: new Date(1_700_000_200_000).toISOString(),
						},
						{
							source_id: 'm3',
							status: 'FAILED',
							internal_date: new Date(1_700_000_300_000).toISOString(),
						},
					];
				}
			if (query.includes('insert into public.raw_emails')) {
				insertedSourceIds.push(values[2] as string);
				return [];
			}
			if (query.includes('last_sync_timestamp = greatest')) {
				cursorUpdateValue = values[0] as number;
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});

		const fetcher = vi.fn(async (input: RequestInfo | URL) => {
			const url = typeof input === 'string' ? input : input.toString();

			if (url.includes('oauth2.googleapis.com/token')) {
				return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.includes('/gmail/v1/users/me/messages?')) {
				return new Response(
					JSON.stringify({ messages: [{ id: 'm1' }, { id: 'm2' }, { id: 'm3' }] }),
					{
						status: 200,
						headers: { 'content-type': 'application/json' },
					},
				);
			}
			if (url.endsWith('/m1?format=full')) {
				return new Response(
					JSON.stringify({
						id: 'm1',
						internalDate: '1700000100000',
						payload: {
							mimeType: 'text/plain',
							body: {
								data: toBase64Url('Transaction A'),
							},
						},
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			if (url.endsWith('/m2?format=full')) {
				return new Response(
					JSON.stringify({
						id: 'm2',
						internalDate: '1700000200000',
						snippet: 'Transaction B',
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			if (url.endsWith('/m3?format=full')) {
				return new Response(
					JSON.stringify({
						id: 'm3',
						internalDate: '1700000300000',
						snippet: 'Retry failed extraction',
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}

			throw new Error(`Unexpected URL: ${url}`);
		});
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(sql);

		const result = await runEmailSyncUserJob(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: '00000000-0000-4000-8000-000000000001',
				last_sync_timestamp: 1_700_000_000_000,
			},
			createEnv(),
			fetcher,
		);

		expect(insertedSourceIds).toEqual(['m1', 'm3']);
		expect(cursorUpdateValue).toBe(1_700_000_300_000);
			expect(result.fetched_message_count).toBe(2);
			expect(result.inserted_or_retried_raw_email_count).toBe(2);
			expect(result.skipped_existing_raw_email_count).toBe(1);
			expect(result.revoked_connection_count).toBe(0);
		});

	it('retries OAuth refresh on OCC conflict and succeeds with reloaded row', async () => {
		let refreshUpdateAttempt = 0;
		const sql = createSqlMock((query) => {
			if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
				return [buildConnectionRow()];
			}
			if (query.includes('and oc.refresh_token is not distinct from') && query.includes("set access_token =")) {
				refreshUpdateAttempt += 1;
				if (refreshUpdateAttempt === 1) {
					return [];
				}
				return [buildConnectionRow({ updated_at: '2026-03-10T00:02:00.000Z' })];
			}
			if (query.includes('where oc.id =') && query.includes('limit 1')) {
				return [buildConnectionRow({ updated_at: '2026-03-10T00:01:30.000Z' })];
			}
			if (query.includes('last_sync_timestamp = greatest')) {
				return [];
			}
			if (query.includes('from public.raw_emails as re')) {
				return [];
			}
			if (query.includes('insert into public.raw_emails')) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});

		const fetcher = vi.fn(async (input: RequestInfo | URL) => {
			const url = typeof input === 'string' ? input : input.toString();
			if (url.includes('oauth2.googleapis.com/token')) {
				return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.includes('/gmail/v1/users/me/messages?')) {
				return new Response(JSON.stringify({ messages: [] }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			throw new Error(`Unexpected URL: ${url}`);
		});
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(sql);

		const result = await runEmailSyncUserJob(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: '00000000-0000-4000-8000-000000000001',
				last_sync_timestamp: 1_700_000_000_000,
			},
			createEnv(),
			fetcher,
		);

			expect(refreshUpdateAttempt).toBe(2);
			expect(result.processed_connection_count).toBe(1);
			expect(result.revoked_connection_count).toBe(0);
		});

		it('exits cleanly when OAuth connection becomes DORMANT during OCC refresh', async () => {
			let refreshUpdateAttempt = 0;
			const sql = createSqlMock((query) => {
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow()];
				}
				if (query.includes('and oc.refresh_token is not distinct from') && query.includes("set access_token =")) {
					refreshUpdateAttempt += 1;
					return [];
				}
				if (query.includes('where oc.id =') && query.includes('limit 1')) {
					return [buildConnectionRow({ sync_status: 'DORMANT' })];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			const result = await runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 1_700_000_000_000,
				},
				createEnv(),
				fetcher,
			);

			expect(refreshUpdateAttempt).toBe(1);
			expect(result.processed_connection_count).toBe(1);
			expect(result.revoked_connection_count).toBe(0);
			expect(result.fetched_message_count).toBe(0);
		});

		it('keeps unicode text during sanitization (e.g. INR symbol)', async () => {
			const insertedTexts: string[] = [];
			const sql = createSqlMock((query, values) => {
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow()];
				}
				if (query.includes('and oc.access_token is not distinct from') && query.includes("set access_token =")) {
					return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
				}
				if (query.includes('from public.raw_emails as re')) {
					return [];
				}
				if (query.includes('insert into public.raw_emails')) {
					insertedTexts.push(values[4] as string);
					return [];
				}
				if (query.includes('last_sync_timestamp = greatest')) {
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					return new Response(JSON.stringify({ messages: [{ id: 'u1' }] }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.endsWith('/u1?format=full')) {
					return new Response(
						JSON.stringify({
							id: 'u1',
							internalDate: '1700000100000',
							snippet: '₹1,250 debited',
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			await runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 1_700_000_000_000,
				},
				createEnv(),
				fetcher,
			);

			expect(insertedTexts).toHaveLength(1);
			expect(insertedTexts[0]).toContain('₹1,250 debited');
		});

		it('marks connection ERROR_PAUSED on non-retryable Gmail permission errors and acks path', async () => {
			const queries: string[] = [];
			const sql = createSqlMock((query) => {
				queries.push(query);
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow()];
				}
				if (query.includes('and oc.access_token is not distinct from') && query.includes("set access_token =")) {
					return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
				}
				if (query.includes("set sync_status = 'ERROR_PAUSED'")) {
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					return new Response(
						JSON.stringify({
							error: { errors: [{ reason: 'domainPolicy' }] },
						}),
						{ status: 403, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			const result = await runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 1_700_000_000_000,
				},
				createEnv(),
				fetcher,
			);

			expect(result.processed_connection_count).toBe(1);
			expect(result.revoked_connection_count).toBe(0);
			expect(queries.some(query => query.includes("set sync_status = 'ERROR_PAUSED'"))).toBe(true);
		});

		it('does not truncate ingestion after 10 pages when Gmail keeps returning nextPageToken', async () => {
			let listCalls = 0;
			let cursorUpdateCalled = 0;
			const sql = createSqlMock((query) => {
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow()];
				}
				if (query.includes('and oc.access_token is not distinct from') && query.includes("set access_token =")) {
					return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
				}
				if (query.includes('last_sync_timestamp = greatest')) {
					cursorUpdateCalled += 1;
					return [];
				}
				if (query.includes('from public.raw_emails as re')) {
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					listCalls += 1;
					const next =
						listCalls < 12
							? `p${listCalls}`
							: undefined;
					return new Response(
						JSON.stringify(
							next
								? { messages: [], nextPageToken: next }
								: { messages: [] },
						),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			await runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 1_700_000_000_000,
				},
				createEnv(),
				fetcher,
			);

			expect(listCalls).toBe(12);
			expect(cursorUpdateCalled).toBe(1);
		});

		it('acks path on permission-denied 403 from gmail.get by pausing connection', async () => {
			const queries: string[] = [];
			const sql = createSqlMock((query) => {
				queries.push(query);
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow()];
				}
				if (query.includes('and oc.access_token is not distinct from') && query.includes("set access_token =")) {
					return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
				}
				if (query.includes('from public.raw_emails as re')) {
					return [];
				}
				if (query.includes("set sync_status = 'ERROR_PAUSED'")) {
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					return new Response(JSON.stringify({ messages: [{ id: 'm-get-403' }] }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.endsWith('/m-get-403?format=full')) {
					return new Response(
						JSON.stringify({
							error: { errors: [{ reason: 'forbidden' }] },
						}),
						{ status: 403, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			const result = await runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 1_700_000_000_000,
				},
				createEnv(),
				fetcher,
			);

			expect(result.processed_connection_count).toBe(1);
			expect(result.revoked_connection_count).toBe(0);
			expect(queries.some(query => query.includes("set sync_status = 'ERROR_PAUSED'"))).toBe(true);
		});

		it('stops at page cap without transient failure and keeps cursor unchanged', async () => {
			let listCalls = 0;
			let cursorUpdateValue: number | null = null;
			const insertedSourceIds: string[] = [];
			const continuationSend = vi.fn().mockResolvedValue(undefined);
			const sql = createSqlMock((query, values) => {
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow({ last_sync_timestamp: 0 })];
				}
				if (query.includes('and oc.access_token is not distinct from') && query.includes("set access_token =")) {
					return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z', last_sync_timestamp: 0 })];
				}
				if (query.includes('from public.raw_emails as re')) {
					return [];
				}
				if (query.includes('insert into public.raw_emails')) {
					insertedSourceIds.push(values[2] as string);
					return [];
				}
				if (query.includes('last_sync_timestamp = greatest')) {
					cursorUpdateValue = values[0] as number;
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const maxSeenTimestamp = 1_700_000_400_000;
			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					listCalls += 1;
					if (listCalls === 1) {
						return new Response(
							JSON.stringify({
								messages: [{ id: 'first-page-message' }],
								nextPageToken: 'p1',
							}),
							{ status: 200, headers: { 'content-type': 'application/json' } },
						);
					}
					return new Response(
						JSON.stringify({
							messages: [],
							nextPageToken: `p${listCalls}`,
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				if (url.endsWith('/first-page-message?format=full')) {
					return new Response(
						JSON.stringify({
							id: 'first-page-message',
							internalDate: String(maxSeenTimestamp),
							snippet: 'first page payload',
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			const result = await runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 0,
				},
				createEnv({
					EMAIL_SYNC_QUEUE: {
						send: continuationSend,
					} as unknown as Queue,
				}),
				fetcher,
			);

			expect(result.processed_connection_count).toBe(1);
			expect(result.inserted_or_retried_raw_email_count).toBe(1);
			expect(result.fetched_message_count).toBe(1);
			expect(insertedSourceIds).toEqual(['first-page-message']);
			expect(listCalls).toBe(1000);
			expect(cursorUpdateValue).toBeNull();
			expect(continuationSend).toHaveBeenCalledTimes(1);
			const continuationPayload = continuationSend.mock.calls[0]?.[0] as
				| Record<string, unknown>
				| undefined;
			expect(continuationPayload?.job_type).toBe('EMAIL_SYNC_USER');
			expect(continuationPayload?.continuation_connection_id).toBe(
				'00000000-0000-4000-8000-0000000000aa',
			);
			expect(continuationPayload?.continuation_after_seconds).toBe(0);
			expect(continuationPayload?.continuation_page_token).toBe('p1000');
			expect(continuationPayload?.continuation_max_internal_timestamp_seen).toBe(
				maxSeenTimestamp,
			);
		});

		it('stops processing additional connections after continuation enqueue to avoid duplicate continuation jobs', async () => {
			let listCalls = 0;
			let refreshUpdateCalls = 0;
			const continuationSend = vi.fn().mockResolvedValue(undefined);
			const firstConnectionId = '00000000-0000-4000-8000-0000000000aa';
			const secondConnectionId = '00000000-0000-4000-8000-0000000000bb';
			const sql = createSqlMock((query) => {
				if (
					query.includes('from public.oauth_connections as oc') &&
					query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")
				) {
					return [
						buildConnectionRow({
							id: firstConnectionId,
							email_address: 'first@example.com',
							refresh_token: 'refresh-token-first',
							last_sync_timestamp: 0,
						}),
						buildConnectionRow({
							id: secondConnectionId,
							email_address: 'second@example.com',
							refresh_token: 'refresh-token-second',
							last_sync_timestamp: 0,
						}),
					];
				}
				if (
					query.includes('and oc.access_token is not distinct from') &&
					query.includes("set access_token =")
				) {
					refreshUpdateCalls += 1;
					if (refreshUpdateCalls > 1) {
						throw new Error('Second connection refresh should not execute after continuation enqueue');
					}
					return [
						buildConnectionRow({
							id: firstConnectionId,
							email_address: 'first@example.com',
							refresh_token: 'refresh-token-first',
							last_sync_timestamp: 0,
							updated_at: '2026-03-10T00:01:00.000Z',
						}),
					];
				}
				if (query.includes('from public.raw_emails as re')) {
					return [];
				}
				if (query.includes('insert into public.raw_emails')) {
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const maxSeenTimestamp = 1_700_000_400_000;
			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					listCalls += 1;
					if (listCalls === 1) {
						return new Response(
							JSON.stringify({
								messages: [{ id: 'first-page-message' }],
								nextPageToken: 'p1',
							}),
							{ status: 200, headers: { 'content-type': 'application/json' } },
						);
					}
					return new Response(
						JSON.stringify({
							messages: [],
							nextPageToken: `p${listCalls}`,
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				if (url.endsWith('/first-page-message?format=full')) {
					return new Response(
						JSON.stringify({
							id: 'first-page-message',
							internalDate: String(maxSeenTimestamp),
							snippet: 'first page payload',
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			const result = await runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 0,
				},
				createEnv({
					EMAIL_SYNC_QUEUE: {
						send: continuationSend,
					} as unknown as Queue,
				}),
				fetcher,
			);

			expect(result.connection_count).toBe(2);
			expect(result.processed_connection_count).toBe(1);
			expect(result.inserted_or_retried_raw_email_count).toBe(1);
			expect(result.fetched_message_count).toBe(1);
			expect(listCalls).toBe(1000);
			expect(refreshUpdateCalls).toBe(1);
			expect(continuationSend).toHaveBeenCalledTimes(1);
		});

		it('continues from continuation page token and preserves max cursor watermark', async () => {
			let cursorUpdateValue: number | null = null;
			let listUrl: string | null = null;
			const sql = createSqlMock((query, values) => {
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow({ last_sync_timestamp: 0 })];
				}
				if (query.includes('and oc.access_token is not distinct from') && query.includes("set access_token =")) {
					return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z', last_sync_timestamp: 0 })];
				}
				if (query.includes('from public.raw_emails as re')) {
					return [];
				}
				if (query.includes('insert into public.raw_emails')) {
					return [];
				}
				if (query.includes('last_sync_timestamp = greatest')) {
					cursorUpdateValue = values[0] as number;
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					listUrl = url;
					return new Response(
						JSON.stringify({
							messages: [{ id: 'old-page-message' }],
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				if (url.endsWith('/old-page-message?format=full')) {
					return new Response(
						JSON.stringify({
							id: 'old-page-message',
							internalDate: '1700000100000',
							snippet: 'older page payload',
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			const result = await runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 0,
					continuation_connection_id: '00000000-0000-4000-8000-0000000000aa',
					continuation_page_token: 'p1000',
					continuation_after_seconds: 0,
					continuation_max_internal_timestamp_seen: 1_700_000_400_000,
				},
				createEnv(),
				fetcher,
			);

			expect(result.processed_connection_count).toBe(1);
			expect(result.inserted_or_retried_raw_email_count).toBe(1);
			expect(result.fetched_message_count).toBe(1);
			expect(listUrl).toContain('pageToken=p1000');
			expect(cursorUpdateValue).toBe(1_700_000_400_000);
		});

		it('counts fetched_message_count from successful gmail.get responses only', async () => {
			const sql = createSqlMock((query) => {
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow()];
				}
				if (query.includes('and oc.access_token is not distinct from') && query.includes("set access_token =")) {
					return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
				}
				if (query.includes('from public.raw_emails as re')) {
					return [];
				}
				if (query.includes('insert into public.raw_emails')) {
					return [];
				}
				if (query.includes('last_sync_timestamp = greatest')) {
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					return new Response(
						JSON.stringify({ messages: [{ id: 'm404' }, { id: 'mbad' }, { id: 'mgood' }] }),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				if (url.endsWith('/m404?format=full')) {
					return new Response(null, { status: 404 });
				}
				if (url.endsWith('/mbad?format=full')) {
					return new Response(
						JSON.stringify({
							id: 'mbad',
							internalDate: 'not-a-number',
							snippet: 'bad internal date',
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				if (url.endsWith('/mgood?format=full')) {
					return new Response(
						JSON.stringify({
							id: 'mgood',
							internalDate: '1700000500000',
							snippet: 'good message',
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			const result = await runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 1_700_000_000_000,
				},
				createEnv(),
				fetcher,
			);

			expect(result.fetched_message_count).toBe(2);
			expect(result.inserted_or_retried_raw_email_count).toBe(1);
		});

		it('keeps retryable Gmail 403 reasons in transient-retry path', async () => {
			const queries: string[] = [];
			const sql = createSqlMock((query) => {
				queries.push(query);
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow()];
				}
				if (query.includes('and oc.access_token is not distinct from') && query.includes("set access_token =")) {
					return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
				}
				if (query.includes('from public.raw_emails as re')) {
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = typeof input === 'string' ? input : input.toString();
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					return new Response(JSON.stringify({ messages: [{ id: 'm-rate-limit' }] }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.endsWith('/m-rate-limit?format=full')) {
					return new Response(
						JSON.stringify({
							error: { errors: [{ reason: 'dailyLimitExceeded' }] },
						}),
						{ status: 403, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			await expect(
				runEmailSyncUserJob(
					{
						job_type: 'EMAIL_SYNC_USER',
						user_id: '00000000-0000-4000-8000-000000000001',
						last_sync_timestamp: 1_700_000_000_000,
					},
					createEnv(),
					fetcher,
				),
			).rejects.toBeInstanceOf(TransientMessageError);

			expect(queries.some(query => query.includes("set sync_status = 'ERROR_PAUSED'"))).toBe(false);
		});
	});
