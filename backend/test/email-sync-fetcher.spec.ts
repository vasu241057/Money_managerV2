import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import {
	runEmailSyncProbe,
	runEmailSyncUserJob,
	runRetroactiveSenderGate,
	runSenderBackfillJob,
	runSenderMetadataBackfill,
	truncateWithoutSplittingSurrogates,
} from '../src/workers/email-sync-fetcher';
import { TransientMessageError } from '../src/workers/queue.errors';
import { withGmailBatchSupport } from './helpers/gmail-batch';

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

const GMAIL_METADATA_SUFFIX = '?format=metadata&metadataHeaders=From&metadataHeaders=Subject';

// The fetcher requests headers (format=metadata) before deciding whether to
// fetch the body. Fixtures serve the same message JSON for both phases, so
// metadata URLs are rewritten to their format=full equivalent here.
function normalizeGmailGetUrl(url: string): string {
	if (url.endsWith(GMAIL_METADATA_SUFFIX)) {
		return `${url.slice(0, -GMAIL_METADATA_SUFFIX.length)}?format=full`;
	}
	return url;
}

/**
 * Task 6 bookkeeping every user job now does. Absent an override the user is
 * far under both caps, nothing is parked on the connection, the usage upsert
 * is a no-op and the hourly self-heal slot is already taken — so the existing
 * cases see exactly the Gmail and raw_emails traffic they always did.
 */
interface QuotaMockOptions {
	bodiesFetched?: number;
	messagesListed?: number;
	aiCalls?: number;
	emailFetchCapOverride?: number | null;
	aiCallCapOverride?: number | null;
	storedContinuation?: { token: string; afterSeconds: number; maxSeen: number } | null;
	/** Whether the hourly self-heal claim succeeds (default: no). */
	requeueClaimed?: boolean;
	stalePendingIds?: string[];
	onUsage?: (values: unknown[]) => void;
	onClaim?: () => void;
	onRelease?: () => void;
}

function quotaIntercept(query: string, values: unknown[], options: QuotaMockOptions): unknown | undefined {
	if (query.includes('with today as') && query.includes('as ai_calls')) {
		return [{
			day: '2026-09-19',
			messages_listed: options.messagesListed ?? 0,
			bodies_fetched: options.bodiesFetched ?? 0,
			ai_calls: String(options.aiCalls ?? 0),
			email_fetch_cap_override: options.emailFetchCapOverride ?? null,
			ai_call_cap_override: options.aiCallCapOverride ?? null,
		}];
	}
	if (query.includes('insert into public.user_usage_daily (user_id, day, messages_listed, bodies_fetched)')) {
		options.onUsage?.(values);
		return [];
	}
	if (query.includes('insert into public.user_usage_daily (user_id, day, pending_requeue_at)')) {
		options.onClaim?.();
		return options.requeueClaimed ? [{ user_id: 'claimed' }] : [];
	}
	if (query.includes('set pending_requeue_at = null')) {
		options.onRelease?.();
		return [];
	}
	if (query.includes("re.status = 'PENDING_EXTRACTION'") && query.includes('order by re.created_at asc') && query.includes('re.user_id =')) {
		return (options.stalePendingIds ?? []).map(id => ({ id }));
	}
	if (query.includes('select oc.email_sync_continuation_token')) {
		const stored = options.storedContinuation ?? null;
		return stored
			? [{
				email_sync_continuation_token: stored.token,
				email_sync_continuation_after_seconds: stored.afterSeconds,
				email_sync_continuation_max_seen: stored.maxSeen,
			}]
			: [{ email_sync_continuation_token: null, email_sync_continuation_after_seconds: null, email_sync_continuation_max_seen: null }];
	}
	return undefined;
}

function createSqlMock(handler: QueryHandler, quota: QuotaMockOptions = {}): SqlMock {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		const intercepted = quotaIntercept(query, values, quota);
		if (intercepted !== undefined) return intercepted;
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
	const appConfig = {
		appName: 'money-manager-backend',
		appVersion: '0.1.0',
		nodeEnv: 'test',
		supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
		dbMaxConnections: 5,
		dbConnectTimeoutSeconds: 5,
		userDailyEmailFetchCap: 500,
		userDailyAiCallCap: 200,
	} as AppConfig;

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
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
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
			withGmailBatchSupport(fetcher),
		);

		expect(result.processed_connection_count).toBe(1);
		expect(result.revoked_connection_count).toBe(1);
		expect(result.inserted_or_retried_raw_email_count).toBe(0);
		expect(queries.some(query => query.includes("sync_status = 'AUTH_REVOKED'"))).toBe(true);
	});

	it('skips a Gmail connection when another cron chain owns its active lease', async () => {
		const sql = createSqlMock((query) => {
			if (
				query.includes('from public.oauth_connections as oc') &&
				query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")
			) {
				return [buildConnectionRow()];
			}
			if (query.includes('set email_sync_lease_id =') && query.includes('returning oc.id')) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		const fetcher = vi.fn();
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(sql);

		const result = await runEmailSyncUserJob(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: '00000000-0000-4000-8000-000000000001',
				last_sync_timestamp: 1_700_000_000_000,
				sync_run_id: '00000000-0000-4000-8000-0000000000f0',
			},
			createEnv(),
			withGmailBatchSupport(fetcher),
		);

		expect(result.connection_count).toBe(1);
		expect(result.processed_connection_count).toBe(0);
		expect(fetcher).not.toHaveBeenCalled();
	});

	it('keeps its Gmail connection lease while Cloudflare retries a transient failure', async () => {
		let leaseReleaseCount = 0;
		const sql = createSqlMock((query) => {
			if (
				query.includes('from public.oauth_connections as oc') &&
				query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")
			) {
				return [buildConnectionRow()];
			}
			if (query.includes('set email_sync_lease_id =') && query.includes('returning oc.id')) {
				return [{ id: '00000000-0000-4000-8000-0000000000aa' }];
			}
			if (query.includes('set email_sync_lease_id = null')) {
				leaseReleaseCount += 1;
				return [];
			}
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		const fetcher = vi.fn().mockResolvedValue(
			new Response('temporary Google outage', {
				status: 503,
				headers: { 'content-type': 'text/plain' },
			}),
		);
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(sql);

		await expect(
			runEmailSyncUserJob(
				{
					job_type: 'EMAIL_SYNC_USER',
					user_id: '00000000-0000-4000-8000-000000000001',
					last_sync_timestamp: 1_700_000_000_000,
					sync_run_id: '00000000-0000-4000-8000-0000000000f0',
				},
				createEnv(),
				withGmailBatchSupport(fetcher),
			),
		).rejects.toThrow('Google OAuth token refresh failed with retryable status 503');

		expect(leaseReleaseCount).toBe(0);
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
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
			}
			throw new Error(`Unexpected query: ${query}`);
		});

		const fetcher = vi.fn(async (input: RequestInfo | URL) => {
			const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());

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
			withGmailBatchSupport(fetcher),
		);

		expect(insertedSourceIds).toEqual(['m1', 'm3']);
		expect(cursorUpdateValue).toBe(1_700_000_300_000);
			expect(result.fetched_message_count).toBe(2);
			expect(result.inserted_or_retried_raw_email_count).toBe(2);
			expect(result.skipped_existing_raw_email_count).toBe(1);
			expect(result.revoked_connection_count).toBe(0);
		});

	it('does not move the cursor when normalize enqueue fails and repairs it on retry', async () => {
		const rawEmailId = '00000000-0000-4000-8000-0000000000e1';
		let rawEmailPersisted = false;
		let cursorUpdateCount = 0;
		const queueSend = vi
			.fn()
			.mockRejectedValueOnce(new Error('normalize unavailable'))
			.mockResolvedValue(undefined);
		const sql = createSqlMock((query, values) => {
			if (
				query.includes('from public.oauth_connections as oc') &&
				query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")
			) {
				return [buildConnectionRow()];
			}
			if (
				query.includes('and oc.refresh_token is not distinct from') &&
				query.includes('set access_token =')
			) {
				return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
			}
			if (query.includes('from public.raw_emails as re')) {
				return rawEmailPersisted
					? [{
							id: rawEmailId,
							source_id: 'retry-m1',
							status: 'PENDING_EXTRACTION',
							internal_date: new Date(1_700_000_500_000).toISOString(),
						}]
					: [];
			}
			if (query.includes('insert into public.raw_emails')) {
				rawEmailPersisted = true;
				return [{ id: rawEmailId }];
			}
			if (query.includes('last_sync_timestamp = greatest')) {
				expect(values[0]).toBe(1_700_000_500_000);
				cursorUpdateCount += 1;
				return [];
			}
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'hdfc' }];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		const fetcher = vi.fn(async (input: RequestInfo | URL) => {
			const url = normalizeGmailGetUrl(
				typeof input === 'string' ? input : input.toString(),
			);
			if (url.includes('oauth2.googleapis.com/token')) {
				return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.includes('/gmail/v1/users/me/messages?')) {
				return new Response(JSON.stringify({ messages: [{ id: 'retry-m1' }] }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.endsWith('/retry-m1?format=full')) {
				return new Response(
					JSON.stringify({
						id: 'retry-m1',
						internalDate: '1700000500000',
						snippet: '₹500 debited',
						payload: {
							headers: [{
								name: 'From',
								value: 'HDFC Bank <alerts@hdfcbank.net>',
							}],
						},
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			throw new Error(`Unexpected URL: ${url}`);
		});
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv({
			EMAIL_SYNC_QUEUE: { send: queueSend } as unknown as Queue,
		});
		const job = {
			job_type: 'EMAIL_SYNC_USER' as const,
			user_id: '00000000-0000-4000-8000-000000000001',
			last_sync_timestamp: 1_700_000_000_000,
		};

		await expect(
			runEmailSyncUserJob(job, env, withGmailBatchSupport(fetcher)),
		).rejects.toThrow('normalize unavailable');
		expect(rawEmailPersisted).toBe(true);
		expect(cursorUpdateCount).toBe(0);

		await runEmailSyncUserJob(job, env, withGmailBatchSupport(fetcher));

		expect(queueSend).toHaveBeenCalledTimes(2);
		expect(queueSend.mock.calls[1]?.[0]).toEqual({
			job_type: 'NORMALIZE_RAW_EMAILS',
			raw_email_ids: [rawEmailId],
		});
		expect(cursorUpdateCount).toBe(1);
	});

	it('caps the initial sync to the lookback window when cursor is zero', async () => {
		const listUrls: string[] = [];
		const insertedSourceIds: string[] = [];
		let cursorUpdateValue: number | null = null;
		const sql = createSqlMock((query, values) => {
			if (
				query.includes("from public.oauth_connections as oc") &&
				query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")
			) {
				return [buildConnectionRow({ last_sync_timestamp: 0 })];
			}
			if (
				query.includes('and oc.refresh_token is not distinct from') &&
				query.includes("set access_token =")
			) {
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
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
			}
			throw new Error(`Unexpected query: ${query}`);
		});

		const fetcher = vi.fn(async (input: RequestInfo | URL) => {
			const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
			if (url.includes('oauth2.googleapis.com/token')) {
				return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.includes('/gmail/v1/users/me/messages?')) {
				listUrls.push(url);
				return new Response(JSON.stringify({ messages: [{ id: 'bootstrap-broad-1' }] }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.endsWith('/bootstrap-broad-1?format=full')) {
				return new Response(
					JSON.stringify({
						id: 'bootstrap-broad-1',
						internalDate: '1700000500000',
						snippet: 'Fallback bootstrap message',
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
			createEnv(),
			withGmailBatchSupport(fetcher),
		);

		expect(listUrls).toHaveLength(1);
		// First sync is bounded: the query filters to the lookback window
		// (default 90 days) instead of listing the whole mailbox.
		expect(listUrls[0]).toContain('q=');
		expect(decodeURIComponent(listUrls[0] ?? '')).toContain('after:');
		const afterMatch = decodeURIComponent(listUrls[0] ?? '').match(/after:(\d+)/);
		const afterSeconds = Number(afterMatch?.[1]);
		const expectedSeconds = Math.floor((Date.now() - 90 * 24 * 60 * 60 * 1000) / 1000);
		expect(Math.abs(afterSeconds - expectedSeconds)).toBeLessThan(24 * 60 * 60);
		expect(listUrls[0]).toContain('includeSpamTrash=true');
		expect(listUrls[0]).toContain('maxResults=25');
		expect(insertedSourceIds).toEqual(['bootstrap-broad-1']);
		expect(cursorUpdateValue).toBe(1_700_000_500_000);
		expect(result.fetched_message_count).toBe(1);
		expect(result.inserted_or_retried_raw_email_count).toBe(1);
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
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
			}
			throw new Error(`Unexpected query: ${query}`);
		});

		const fetcher = vi.fn(async (input: RequestInfo | URL) => {
			const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
			withGmailBatchSupport(fetcher),
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
				withGmailBatchSupport(fetcher),
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
							payload: {
								headers: [
									{ name: 'From', value: 'HDFC Bank InstaAlerts <alerts@hdfcbank.net>' },
								],
							},
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
				withGmailBatchSupport(fetcher),
			);

			expect(insertedTexts).toHaveLength(1);
			expect(insertedTexts[0]).toContain('₹1,250 debited');
		});

		it('persists non-allowlisted senders body-less as IGNORED and keeps bank mail pending', async () => {
			const insertedRows: Array<{
				source_id: string;
				clean_text: string;
				from_address: string | null;
				subject: string | null;
				status: string;
			}> = [];
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
					insertedRows.push({
						source_id: values[2] as string,
						clean_text: values[4] as string,
						from_address: values[5] as string | null,
						subject: values[6] as string | null,
						status: values[7] as string,
					});
					return [];
				}
				if (query.includes('last_sync_timestamp = greatest')) {
					return [];
				}
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					return new Response(JSON.stringify({ messages: [{ id: 'bank1' }, { id: 'personal1' }] }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.endsWith('/bank1?format=full')) {
					return new Response(
						JSON.stringify({
							id: 'bank1',
							internalDate: '1700000100000',
							snippet: 'Rs.500 debited from a/c 1234',
							payload: {
								headers: [
									{ name: 'From', value: 'ICICI Bank <alerts@icicibank.com>' },
									{ name: 'Subject', value: 'Debit alert' },
								],
							},
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				if (url.endsWith('/personal1?format=full')) {
					return new Response(
						JSON.stringify({
							id: 'personal1',
							internalDate: '1700000200000',
							snippet: 'Hey, are we still on for dinner tonight?',
							payload: {
								headers: [
									{ name: 'From', value: 'A Friend <friend@example.com>' },
									{ name: 'Subject', value: 'Dinner plans' },
								],
							},
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
				withGmailBatchSupport(fetcher),
			);

			expect(insertedRows).toHaveLength(2);

			const bankRow = insertedRows.find(row => row.source_id === 'bank1');
			expect(bankRow?.status).toBe('PENDING_EXTRACTION');
			expect(bankRow?.clean_text).toContain('Rs.500 debited');
			expect(bankRow?.from_address).toBe('alerts@icicibank.com');
			expect(bankRow?.subject).toBe('Debit alert');

			const personalRow = insertedRows.find(row => row.source_id === 'personal1');
			expect(personalRow?.status).toBe('IGNORED');
			expect(personalRow?.clean_text).toBe('');
			expect(personalRow?.from_address).toBe('friend@example.com');
			expect(personalRow?.subject).toBe('Dinner plans');

			expect(result.sender_gated_raw_email_count).toBe(1);
		});

		it('backfills gated raw emails: refetches body, marks PENDING_EXTRACTION, enqueues normalize', async () => {
			const rawEmailId = '00000000-0000-4000-8000-00000000000a';
			let updatedCleanText: string | null = null;
			const sql = createSqlMock((query, values) => {
				if (query.includes('re.source_id, re.oauth_connection_id')) {
					return [
						{
							id: rawEmailId,
							source_id: 'g1',
							oauth_connection_id: '00000000-0000-4000-8000-0000000000aa',
						},
					];
				}
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow()];
				}
				if (query.includes('and oc.refresh_token is not distinct from') && query.includes("set access_token =")) {
					return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
				}
				if (query.includes('set clean_text =')) {
					updatedCleanText = values[0] as string;
					return [{ id: rawEmailId }];
				}
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.endsWith('/g1?format=full')) {
					return new Response(
						JSON.stringify({
							id: 'g1',
							internalDate: '1700000100000',
							snippet: 'Rs.999 debited from a/c 4321',
						}),
						{ status: 200, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			const queueSend = vi.fn().mockResolvedValue(undefined);
			const result = await runSenderBackfillJob(
				{
					job_type: 'SENDER_BACKFILL',
					user_id: '00000000-0000-4000-8000-000000000001',
					raw_email_ids: [rawEmailId],
				},
				createEnv({ EMAIL_SYNC_QUEUE: { send: queueSend } as unknown as Queue }),
				withGmailBatchSupport(fetcher),
			);

			expect(result.refetched_count).toBe(1);
			expect(result.skipped_count).toBe(0);
			expect(result.normalize_enqueued).toBe(true);
			expect(updatedCleanText).toContain('Rs.999 debited');
			expect(queueSend).toHaveBeenCalledTimes(1);
			expect(queueSend.mock.calls[0]?.[0]).toEqual({
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: [rawEmailId],
			});
		});

		it('skips backfill rows whose original connection is gone instead of using another mailbox', async () => {
			const orphanRowId = '00000000-0000-4000-8000-00000000000b';
			const sql = createSqlMock((query) => {
				if (query.includes('re.source_id, re.oauth_connection_id')) {
					// Row's connection id is NOT among the eligible connections
					// (deleted / revoked since the mail was gated).
					return [
						{
							id: orphanRowId,
							source_id: 'g9',
							oauth_connection_id: '00000000-0000-4000-8000-00000000dead',
						},
					];
				}
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					// A different, healthy connection exists — it must NOT be used:
					// Gmail ids are per-mailbox, so it would 404 or fetch the
					// wrong account's mail.
					return [buildConnectionRow()];
				}
				if (query.includes('from public.sender_rules')) {
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			// Any network call would mean the wrong mailbox was contacted.
			const fetcher = vi.fn(async () => {
				throw new Error('No Gmail/token call expected for an orphaned row');
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			const queueSend = vi.fn().mockResolvedValue(undefined);
			const result = await runSenderBackfillJob(
				{
					job_type: 'SENDER_BACKFILL',
					user_id: '00000000-0000-4000-8000-000000000001',
					raw_email_ids: [orphanRowId],
				},
				createEnv({ EMAIL_SYNC_QUEUE: { send: queueSend } as unknown as Queue }),
				withGmailBatchSupport(fetcher),
			);

			expect(result.refetched_count).toBe(0);
			expect(result.skipped_count).toBe(1);
			expect(result.normalize_enqueued).toBe(false);
			expect(fetcher).not.toHaveBeenCalled();
			expect(queueSend).not.toHaveBeenCalled();
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
				withGmailBatchSupport(fetcher),
			);

			expect(result.processed_connection_count).toBe(1);
			expect(result.revoked_connection_count).toBe(0);
			expect(queries.some(query => query.includes("set sync_status = 'ERROR_PAUSED'"))).toBe(true);
		});

		it('processes one Gmail page per invocation and continues without advancing the cursor', async () => {
			let listCalls = 0;
			let cursorUpdateCalled = 0;
			const continuationSend = vi.fn().mockResolvedValue(undefined);
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
				createEnv({
					EMAIL_SYNC_QUEUE: {
						send: continuationSend,
					} as unknown as Queue,
				}),
				withGmailBatchSupport(fetcher),
			);

			expect(listCalls).toBe(1);
			expect(cursorUpdateCalled).toBe(0);
			expect(continuationSend).toHaveBeenCalledTimes(1);
			expect(continuationSend.mock.calls[0]?.[0]).toMatchObject({
				job_type: 'EMAIL_SYNC_USER',
				continuation_page_token: 'p1',
			});
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
				withGmailBatchSupport(fetcher),
			);

			expect(result.processed_connection_count).toBe(1);
			expect(result.revoked_connection_count).toBe(0);
			expect(queries.some(query => query.includes("set sync_status = 'ERROR_PAUSED'"))).toBe(true);
		});

		it('commits one page, enqueues its continuation, and keeps cursor unchanged', async () => {
			let listCalls = 0;
			let cursorUpdateValue: number | null = null;
			const insertedSourceIds: string[] = [];
			const continuationSend = vi.fn().mockResolvedValue(undefined);
			const syncRunId = '00000000-0000-4000-8000-0000000000f1';
			const sql = createSqlMock((query, values) => {
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow({ last_sync_timestamp: 0 })];
				}
				if (query.includes('set email_sync_lease_id =') && query.includes('returning oc.id')) {
					expect(values[0]).toBe(syncRunId);
					return [{ id: '00000000-0000-4000-8000-0000000000aa' }];
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const maxSeenTimestamp = 1_700_000_400_000;
			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
					sync_run_id: syncRunId,
				},
				createEnv({
					EMAIL_SYNC_QUEUE: {
						send: continuationSend,
					} as unknown as Queue,
				}),
				withGmailBatchSupport(fetcher),
			);

			expect(result.processed_connection_count).toBe(1);
			expect(result.inserted_or_retried_raw_email_count).toBe(1);
			expect(result.fetched_message_count).toBe(1);
			expect(insertedSourceIds).toEqual(['first-page-message']);
			expect(listCalls).toBe(1);
			expect(cursorUpdateValue).toBeNull();
			expect(continuationSend).toHaveBeenCalledTimes(1);
			const continuationPayload = continuationSend.mock.calls[0]?.[0] as
				| Record<string, unknown>
				| undefined;
			expect(continuationPayload?.job_type).toBe('EMAIL_SYNC_USER');
			expect(continuationPayload?.continuation_connection_id).toBe(
				'00000000-0000-4000-8000-0000000000aa',
			);
			expect(continuationPayload?.sync_run_id).toBe(syncRunId);
			// Zero cursor is capped to the initial lookback window, and the
			// continuation carries that same capped query window forward.
			const continuationAfterSeconds = continuationPayload?.continuation_after_seconds ?? 0;
			const expectedLookbackSeconds = Math.floor(
				(Date.now() - 90 * 24 * 60 * 60 * 1000) / 1000,
			);
			expect(Math.abs(continuationAfterSeconds - expectedLookbackSeconds)).toBeLessThan(
				24 * 60 * 60,
			);
			expect(continuationPayload?.continuation_page_token).toBe('p1');
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const maxSeenTimestamp = 1_700_000_400_000;
			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
				withGmailBatchSupport(fetcher),
			);

			expect(result.connection_count).toBe(2);
			expect(result.processed_connection_count).toBe(1);
			expect(result.inserted_or_retried_raw_email_count).toBe(1);
			expect(result.fetched_message_count).toBe(1);
			expect(listCalls).toBe(1);
			expect(refreshUpdateCalls).toBe(1);
			expect(continuationSend).toHaveBeenCalledTimes(1);
			expect(continuationSend).toHaveBeenCalledWith(
				expect.objectContaining({
					job_type: 'EMAIL_SYNC_USER',
					continuation_page_token: 'p1',
				}),
				{ contentType: 'json', delaySeconds: 30 },
			);
		});

		it('continues from continuation page token and preserves max cursor watermark', async () => {
			let cursorUpdateValue: number | null = null;
			let listUrl: string | null = null;
			let leaseReleaseCount = 0;
			const syncRunId = '00000000-0000-4000-8000-0000000000f2';
			const sql = createSqlMock((query, values) => {
				if (query.includes("from public.oauth_connections as oc") && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
					return [buildConnectionRow({ last_sync_timestamp: 0 })];
				}
				if (query.includes('set email_sync_lease_id =') && query.includes('returning oc.id')) {
					expect(values[0]).toBe(syncRunId);
					return [{ id: '00000000-0000-4000-8000-0000000000aa' }];
				}
				if (query.includes('set email_sync_lease_id = null')) {
					leaseReleaseCount += 1;
					return [];
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
					sync_run_id: syncRunId,
					continuation_connection_id: '00000000-0000-4000-8000-0000000000aa',
					continuation_page_token: 'p1000',
					continuation_after_seconds: 0,
					continuation_max_internal_timestamp_seen: 1_700_000_400_000,
				},
				createEnv(),
				withGmailBatchSupport(fetcher),
			);

			expect(result.processed_connection_count).toBe(1);
			expect(result.inserted_or_retried_raw_email_count).toBe(1);
			expect(result.fetched_message_count).toBe(1);
			expect(listUrl).toContain('pageToken=p1000');
			expect(cursorUpdateValue).toBe(1_700_000_400_000);
			expect(leaseReleaseCount).toBe(1);
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
				withGmailBatchSupport(fetcher),
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});

			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
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
					withGmailBatchSupport(fetcher),
				),
			).rejects.toBeInstanceOf(TransientMessageError);

			expect(queries.some(query => query.includes("set sync_status = 'ERROR_PAUSED'"))).toBe(false);
		});

		it('preserves Gmail batch-part 429 details and requests a minute-window retry', async () => {
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
				if (query.includes('from public.sender_rules')) {
					return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
				}
				throw new Error(`Unexpected query: ${query}`);
			});
			const fetcher = vi.fn(async (input: RequestInfo | URL) => {
				const url = normalizeGmailGetUrl(
					typeof input === 'string' ? input : input.toString(),
				);
				if (url.includes('oauth2.googleapis.com/token')) {
					return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.includes('/gmail/v1/users/me/messages?')) {
					return new Response(JSON.stringify({ messages: [{ id: 'gmail-limited' }] }), {
						status: 200,
						headers: { 'content-type': 'application/json' },
					});
				}
				if (url.endsWith('/gmail-limited?format=full')) {
					return new Response(
						JSON.stringify({
							error: {
								status: 'RESOURCE_EXHAUSTED',
								message: 'User-rate limit exceeded',
							},
						}),
						{ status: 429, headers: { 'content-type': 'application/json' } },
					);
				}
				throw new Error(`Unexpected URL: ${url}`);
			});
			getAppConfigMock.mockReturnValue(appConfig);
			getSqlClientMock.mockReturnValue(sql);

			let caught: unknown;
			try {
				await runEmailSyncUserJob(
					{
						job_type: 'EMAIL_SYNC_USER',
						user_id: '00000000-0000-4000-8000-000000000001',
						last_sync_timestamp: 1_700_000_000_000,
					},
					createEnv(),
					withGmailBatchSupport(fetcher),
				);
			} catch (error) {
				caught = error;
			}

			expect(caught).toBeInstanceOf(TransientMessageError);
			expect(caught).toMatchObject({
				retryAfterSeconds: 65,
				retryJitterSeconds: 5,
			});
			expect((caught as Error).message).toContain(
				'Gmail message batch part for gmail-limited',
			);
			expect((caught as Error).message).toContain('reason=RESOURCE_EXHAUSTED');
			expect((caught as Error).message).toContain('message=User-rate limit exceeded');
		});
	});

describe('internal ops: sender-metadata backfill + sync probe', () => {
	const appConfig = {
		appName: 'money-manager-backend',
		appVersion: '0.1.0',
		nodeEnv: 'test',
		supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
		dbMaxConnections: 5,
		dbConnectTimeoutSeconds: 5,
		userDailyEmailFetchCap: 500,
		userDailyAiCallCap: 200,
	} as AppConfig;

	beforeEach(() => {
		vi.clearAllMocks();
	});

	it('metadata backfill: fills from_address/subject via the row\'s own connection, headers only', async () => {
		const rawEmailId = '00000000-0000-4000-8000-00000000000c';
		let updatedFrom: string | null = null;
		let updatedSubject: string | null = null;
		const sql = createSqlMock((query, values) => {
			if (query.includes('where re.from_address is null') && query.includes('order by re.created_at')) {
				return [
					{
						id: rawEmailId,
						user_id: '00000000-0000-4000-8000-000000000001',
						source_id: 'g1',
						oauth_connection_id: '00000000-0000-4000-8000-0000000000aa',
					},
				];
			}
			if (query.includes('where oc.id =') && query.includes('from public.oauth_connections')) {
				return [buildConnectionRow()];
			}
			if (query.includes('set access_token =')) {
				return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
			}
			if (query.includes('set from_address =') && query.includes('subject = coalesce')) {
				updatedFrom = values[0] as string;
				updatedSubject = values[1] as string;
				return [{ id: rawEmailId }];
			}
			if (query.includes('count(*)::int as remaining')) {
				return [{ remaining: 0 }];
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
			if (url.includes('/g1?format=metadata')) {
				return new Response(
					JSON.stringify({
						id: 'g1',
						internalDate: '1700000100000',
						payload: {
							headers: [
								{ name: 'From', value: 'ICICI Bank <alerts@icicibank.com>' },
								{ name: 'Subject', value: 'Debit alert' },
							],
						},
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			throw new Error(`Unexpected URL (bodies must never be fetched): ${url}`);
		});
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(sql);

		const result = await runSenderMetadataBackfill(200, createEnv(), fetcher);

		expect(result.scanned_count).toBe(1);
		expect(result.updated_count).toBe(1);
		expect(result.skipped_count).toBe(0);
		expect(result.remaining_count).toBe(0);
		expect(updatedFrom).toBe('alerts@icicibank.com');
		expect(updatedSubject).toBe('Debit alert');
	});

	it('metadata backfill: a deleted message (404) is marked tried so it stops re-scanning', async () => {
		const rawEmailId = '00000000-0000-4000-8000-00000000000d';
		let markedTried = false;
		const sql = createSqlMock((query) => {
			if (query.includes('where re.from_address is null') && query.includes('order by re.created_at')) {
				return [
					{
						id: rawEmailId,
						user_id: '00000000-0000-4000-8000-000000000001',
						source_id: 'gone1',
						oauth_connection_id: '00000000-0000-4000-8000-0000000000aa',
					},
				];
			}
			if (query.includes('where oc.id =') && query.includes('from public.oauth_connections')) {
				return [buildConnectionRow()];
			}
			if (query.includes('set access_token =')) {
				return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
			}
			if (query.includes("set from_address = ''")) {
				markedTried = true;
				return [];
			}
			if (query.includes('count(*)::int as remaining')) {
				return [{ remaining: 0 }];
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
			if (url.includes('/gone1?format=metadata')) {
				return new Response('not found', { status: 404 });
			}
			throw new Error(`Unexpected URL: ${url}`);
		});
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(sql);

		const result = await runSenderMetadataBackfill(200, createEnv(), fetcher);

		expect(result.updated_count).toBe(0);
		expect(result.skipped_count).toBe(1);
		expect(markedTried).toBe(true);
	});

	it('sync probe: reports metadata + gate verdicts, persists, enqueues, and never advances the watermark', async () => {
		const queries: string[] = [];
		const sql = createSqlMock((query) => {
			queries.push(query);
			if (query.includes('order by oc.updated_at desc limit 1')) {
				return [buildConnectionRow()];
			}
			if (query.includes('set access_token =')) {
				return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
			}
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'icici' }];
			}
			if (query.includes('re.source_id = any(')) {
				// m3 already exists in raw_emails -> already_known.
				return [{ source_id: 'm3', status: 'PROCESSED', internal_date: '2026-07-01T00:00:00.000Z' }];
			}
			if (query.includes('insert into public.raw_emails')) {
				return [{ id: '00000000-0000-4000-8000-0000000000f1' }];
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
			if (url.includes('users/me/messages?')) {
				expect(url).not.toContain('q=');
				return new Response(
					JSON.stringify({ messages: [{ id: 'm1' }, { id: 'm2' }, { id: 'm3' }] }),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			if (url.includes('/m1?format=metadata')) {
				return new Response(
					JSON.stringify({
						id: 'm1',
						internalDate: '1700000200000',
						payload: {
							headers: [
								{ name: 'From', value: 'ICICI Bank <alerts@icicibank.com>' },
								{ name: 'Subject', value: 'Debit alert' },
							],
						},
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			if (url.includes('/m1?format=full')) {
				return new Response(
					JSON.stringify({
						id: 'm1',
						internalDate: '1700000200000',
						snippet: 'Rs.500 debited from a/c 1234',
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			if (url.includes('/m2?format=metadata')) {
				return new Response(
					JSON.stringify({
						id: 'm2',
						internalDate: '1700000300000',
						payload: {
							headers: [
								{ name: 'From', value: 'Friend <friend@example.com>' },
								{ name: 'Subject', value: 'Dinner plans' },
							],
						},
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			throw new Error(`Unexpected URL (gated bodies must never be fetched): ${url}`);
		});
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(sql);

		const queueSend = vi.fn().mockResolvedValue(undefined);
		const result = await runEmailSyncProbe(
			10,
			createEnv({ EMAIL_SYNC_QUEUE: { send: queueSend } as unknown as Queue }),
			withGmailBatchSupport(fetcher),
		);

		expect(result.listed_count).toBe(3);
		expect(result.enqueued_pending_count).toBe(1);

		const verdictBySourceId = new Map(result.reports.map(r => [r.source_id, r.verdict]));
		expect(verdictBySourceId.get('m1')).toBe('allowed');
		expect(verdictBySourceId.get('m2')).toBe('gated');
		expect(verdictBySourceId.get('m3')).toBe('already_known');

		const allowedReport = result.reports.find(r => r.source_id === 'm1');
		expect(allowedReport?.from_address).toBe('alerts@icicibank.com');
		expect(allowedReport?.subject).toBe('Debit alert');

		// The probe must never move the sync cursor.
		expect(queries.some(q => q.includes('last_sync_timestamp = greatest'))).toBe(false);
		expect(queueSend).toHaveBeenCalledTimes(1);
	});
});

describe('retroactive sender gate', () => {
	const appConfig = {
		appName: 'money-manager-backend',
		appVersion: '0.1.0',
		nodeEnv: 'test',
		supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
		dbMaxConnections: 5,
		dbConnectTimeoutSeconds: 5,
		userDailyEmailFetchCap: 500,
		userDailyAiCallCap: 200,
	} as AppConfig;

	const pendingRows = [
		{ id: '00000000-0000-4000-8000-0000000000e1', from_address: 'alerts@icicibank.com' },
		{ id: '00000000-0000-4000-8000-0000000000e2', from_address: 'friend@example.com' },
		{ id: '00000000-0000-4000-8000-0000000000e3', from_address: '' },
	];

	function createGateSqlMock(onDeniedUpdate: (ids: unknown) => void) {
		return createSqlMock((query, values) => {
			if (query.includes("re.status = 'PENDING_EXTRACTION'") && query.includes('select re.id, re.from_address')) {
				return pendingRows;
			}
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'icici' }];
			}
			if (query.includes("set status = 'IGNORED', clean_text = ''")) {
				onDeniedUpdate(values[0]);
				return [];
			}
			if (query.includes('count(*)::int as remaining')) {
				return [{ remaining: 2 }];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
	}

	beforeEach(() => {
		vi.clearAllMocks();
	});

	it('flips denied senders to IGNORED with purged bodies, keeps allowed + unknown pending', async () => {
		let deniedIds: unknown = null;
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(createGateSqlMock(ids => { deniedIds = ids; }));

		const result = await runRetroactiveSenderGate(2000, false, createEnv());

		expect(result.gate_enabled).toBe(true);
		expect(result.allowed_count).toBe(1);
		expect(result.gated_count).toBe(1);
		expect(result.unknown_sender_count).toBe(1);
		expect(deniedIds).toEqual(['00000000-0000-4000-8000-0000000000e2']);
	});

	it('dry run reports the same counts without mutating anything', async () => {
		let updateRan = false;
		getAppConfigMock.mockReturnValue(appConfig);
		getSqlClientMock.mockReturnValue(createGateSqlMock(() => { updateRan = true; }));

		const result = await runRetroactiveSenderGate(2000, true, createEnv());

		expect(result.dry_run).toBe(true);
		expect(result.allowed_count).toBe(1);
		expect(result.gated_count).toBe(1);
		expect(updateRan).toBe(false);
	});
});

describe('truncateWithoutSplittingSurrogates', () => {
	it('never leaves a dangling high surrogate at the cut', () => {
		// '💰' is a surrogate pair; cutting at length 4 would split it.
		const value = `abc💰xyz`;
		const truncated = truncateWithoutSplittingSurrogates(value, 4);
		expect(truncated).toBe('abc');
		// A valid cut point is untouched.
		expect(truncateWithoutSplittingSurrogates(value, 5)).toBe('abc💰');
		// Well-formedness: no lone surrogates anywhere in the result.
		expect(truncated.isWellFormed()).toBe(true);
	});

	it('returns short strings unchanged', () => {
		expect(truncateWithoutSplittingSurrogates('short', 500)).toBe('short');
	});
});

describe('runStatementHarvest', () => {
	const ROW_A = { id: '00000000-0000-4000-8000-00000000e001', from_address: 'estatement@yes.bank.in' };
	const ROW_B = { id: '00000000-0000-4000-8000-00000000e002', from_address: 'noreply@slice.bank.in' };

	it('dry run reports matches per sender without flipping anything', async () => {
		const queries: string[] = [];
		const sql = createSqlMock((query) => {
			queries.push(query);
			if (query.startsWith('select re.id, re.from_address')) return [ROW_A, ROW_A, ROW_B];
			throw new Error(`Unexpected query: ${query.slice(0, 60)}`);
		});
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv();

		const { runStatementHarvest } = await import('../src/workers/email-sync-fetcher');
		const result = await runStatementHarvest(500, true, env);

		expect(result).toMatchObject({
			dry_run: true,
			matched_count: 3,
			flipped_count: 0,
			enqueued_batches: 0,
			by_sender: { 'estatement@yes.bank.in': 2, 'noreply@slice.bank.in': 1 },
		});
		// Candidates: statement wording, IGNORED/PROCESSED only, and never an
		// email that already produced a statement row.
		expect(queries[0]).toContain("re.status in ('IGNORED', 'PROCESSED')");
		expect(queries[0]).toContain('not exists');
		expect(queries[0]).toContain('card_statements');
		expect(queries.length).toBe(1);
	});

	it('real run flips candidates to PENDING and enqueues normalize batches', async () => {
		const queries: string[] = [];
		const sql = createSqlMock((query) => {
			queries.push(query);
			if (query.startsWith('select re.id, re.from_address')) return [ROW_A, ROW_B];
			if (query.startsWith('update public.raw_emails')) return [{ id: ROW_A.id }, { id: ROW_B.id }];
			throw new Error(`Unexpected query: ${query.slice(0, 60)}`);
		});
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv();

		const { runStatementHarvest } = await import('../src/workers/email-sync-fetcher');
		const result = await runStatementHarvest(500, false, env);

		expect(result).toMatchObject({
			dry_run: false,
			matched_count: 2,
			flipped_count: 2,
			enqueued_batches: 1,
		});
		const flipQuery = queries.find((q) => q.startsWith('update public.raw_emails'));
		expect(flipQuery).toContain("set status = 'PENDING_EXTRACTION'");
		const sendMock = (env as unknown as { EMAIL_SYNC_QUEUE: { send: ReturnType<typeof vi.fn> } })
			.EMAIL_SYNC_QUEUE.send;
		expect(sendMock).toHaveBeenCalledTimes(1);
		expect(sendMock).toHaveBeenCalledWith(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [ROW_A.id, ROW_B.id] },
			{ contentType: 'json' },
		);
	});
});

describe('email-sync-fetcher: fair-use caps (Task 6)', () => {
	const USER_ID = '00000000-0000-4000-8000-000000000001';
	const CONNECTION_ID = '00000000-0000-4000-8000-0000000000aa';
	const SYNC_RUN_ID = '00000000-0000-4000-8000-0000000000f6';
	const PENDING_A = '00000000-0000-4000-8000-00000000000a';
	const PENDING_B = '00000000-0000-4000-8000-00000000000b';
	const appConfig = {
		appName: 'money-manager-backend',
		appVersion: '0.1.0',
		nodeEnv: 'test',
		supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
		dbMaxConnections: 5,
		dbConnectTimeoutSeconds: 5,
		userDailyEmailFetchCap: 500,
		userDailyAiCallCap: 200,
	} as AppConfig;

	const neverGoogle = vi.fn(async (input: RequestInfo | URL) => {
		throw new Error(`Google must not be called: ${typeof input === 'string' ? input : input.toString()}`);
	});

	function queueSendOf(env: Env) {
		return (env.EMAIL_SYNC_QUEUE as unknown as { send: ReturnType<typeof vi.fn> }).send;
	}

	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(appConfig);
	});

	it('defers a continuation page at the fetch cap: parks the token, keeps the cursor, releases the lease, never calls Google', async () => {
		const stored: unknown[][] = [];
		let leaseReleases = 0;
		const onClaim = vi.fn();
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.oauth_connections as oc') && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
				return [buildConnectionRow({ last_sync_timestamp: 0 })];
			}
			if (query.includes('set email_sync_lease_id =') && query.includes('returning oc.id')) {
				return [{ id: CONNECTION_ID }];
			}
			if (query.includes('set email_sync_lease_id = null')) {
				leaseReleases += 1;
				return [];
			}
			if (query.includes('set email_sync_continuation_token =')) {
				stored.push(values);
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		}, { bodiesFetched: 500, onClaim });
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv();

		const result = await runEmailSyncUserJob(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: USER_ID,
				last_sync_timestamp: 0,
				sync_run_id: SYNC_RUN_ID,
				continuation_connection_id: CONNECTION_ID,
				continuation_page_token: 'p7',
				continuation_after_seconds: 1_690_000_000,
				continuation_max_internal_timestamp_seen: 1_700_000_400_000,
			},
			env,
			neverGoogle,
		);

		expect(neverGoogle).not.toHaveBeenCalled();
		expect(result.quota_deferred_connection_count).toBe(1);
		expect(result.processed_connection_count).toBe(1);
		expect(result.fetched_message_count).toBe(0);
		// The token, its query window and the watermark are parked on the connection.
		expect(stored).toEqual([['p7', 1_690_000_000, 1_700_000_400_000, CONNECTION_ID, USER_ID]]);
		// No continuation job, lease handed back, and a continuation page never self-heals.
		expect(queueSendOf(env)).not.toHaveBeenCalled();
		expect(leaseReleases).toBe(1);
		expect(onClaim).not.toHaveBeenCalled();
	});

	it.each([{ emailFetchCapOverride: 10, bodiesFetched: 10 }, { messagesListed: 500 }])('defers a fresh job at either fetch cap without touching the connection (%j)', async (quota) => {
		const sql = createSqlMock((query) => {
			if (query.includes('from public.oauth_connections as oc') && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
				return [buildConnectionRow()];
			}
			throw new Error(`Unexpected query: ${query}`);
		}, quota);
		getSqlClientMock.mockReturnValue(sql);

		const result = await runEmailSyncUserJob(
			{ job_type: 'EMAIL_SYNC_USER', user_id: USER_ID, last_sync_timestamp: 1_700_000_000_000 },
			createEnv(),
			neverGoogle,
		);

		expect(neverGoogle).not.toHaveBeenCalled();
		expect(result.quota_deferred_connection_count).toBe(1);
	});

	it('defers sender backfill to the next IST day when fetching is capped', async () => {
		const sql = createSqlMock(query => {
			if (query.includes('re.source_id, re.oauth_connection_id')) {
				return [{ id: PENDING_A, source_id: 'g1', oauth_connection_id: CONNECTION_ID }];
			}
			if (query.includes('from public.oauth_connections as oc')) return [buildConnectionRow()];
			throw new Error(`Unexpected query: ${query}`);
		}, { bodiesFetched: 500 });
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv();
		const job = { job_type: 'SENDER_BACKFILL' as const, user_id: USER_ID, raw_email_ids: [PENDING_A] };
		const result = await runSenderBackfillJob(job, env, neverGoogle);
		expect(neverGoogle).not.toHaveBeenCalled();
		expect(result.refetched_count).toBe(0);
		expect(result.skipped_count).toBe(1);
		expect(queueSendOf(env)).toHaveBeenCalledWith(job, { contentType: 'json', delaySeconds: expect.any(Number) });
		const delay = queueSendOf(env).mock.calls[0]?.[1].delaySeconds;
		expect(delay).toBeGreaterThanOrEqual(1);
		expect(delay).toBeLessThanOrEqual(86400);
	});

	it('resumes a parked listing on the next fresh job and forgets the token once the page commits', async () => {
		let cursorUpdateValue: number | null = null;
		let listUrl: string | null = null;
		let cleared = 0;
		const usage: unknown[][] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.oauth_connections as oc') && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
				return [buildConnectionRow({ last_sync_timestamp: 0 })];
			}
			if (query.includes('set email_sync_lease_id =') && query.includes('returning oc.id')) {
				return [{ id: CONNECTION_ID }];
			}
			if (query.includes('set email_sync_lease_id = null')) {
				return [];
			}
			if (query.includes('and oc.access_token is not distinct from') && query.includes('set access_token =')) {
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
			if (query.includes('email_sync_continuation_token = null')) {
				cleared += 1;
				return [];
			}
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'hdfc' }];
			}
			throw new Error(`Unexpected query: ${query}`);
		}, {
			storedContinuation: { token: 'parked-token', afterSeconds: 1_690_000_000, maxSeen: 1_700_000_300_000 },
			onUsage: values => usage.push(values),
		});
		const fetcher = vi.fn(async (input: RequestInfo | URL) => {
			const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
			if (url.includes('oauth2.googleapis.com/token')) {
				return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.includes('/gmail/v1/users/me/messages?')) {
				listUrl = url;
				return new Response(JSON.stringify({ messages: [{ id: 'resumed-message' }] }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.endsWith('/resumed-message?format=full')) {
				return new Response(
					JSON.stringify({ id: 'resumed-message', internalDate: '1700000100000', snippet: 'later page' }),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			throw new Error(`Unexpected URL: ${url}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runEmailSyncUserJob(
			{ job_type: 'EMAIL_SYNC_USER', user_id: USER_ID, last_sync_timestamp: 0, sync_run_id: SYNC_RUN_ID },
			createEnv(),
			withGmailBatchSupport(fetcher),
		);

		expect(result.processed_connection_count).toBe(1);
		expect(result.quota_deferred_connection_count).toBe(0);
		// The parked token and its exact query window, not a fresh listing from the cursor.
		expect(listUrl).toContain('pageToken=parked-token');
		expect(listUrl).toContain('after%3A1690000000');
		// The chain finished: the cursor lands on the parked watermark and the token is gone.
		expect(cursorUpdateValue).toBe(1_700_000_300_000);
		expect(cleared).toBe(1);
		// One id listed, no body fetched (no sender header, so the gate held it).
		expect(usage).toEqual([[USER_ID, 'Asia/Kolkata', 1, 0]]);
	});

	it.each([false, true])('counts page usage and parks the next token immediately at the cap (boundary=%s)', async (atBoundary) => {
		const usage: unknown[][] = [];
		const parked: unknown[][] = [];
		let cursorAdvanced = false;
		let pageSize = '';
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.oauth_connections as oc') && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
				return [buildConnectionRow()];
			}
			if (query.includes('and oc.access_token is not distinct from') && query.includes('set access_token =')) {
				return [buildConnectionRow({ updated_at: '2026-03-10T00:01:00.000Z' })];
			}
			if (query.includes('from public.raw_emails as re')) return [];
			if (query.includes('insert into public.raw_emails')) return [];
			if (query.includes('last_sync_timestamp = greatest')) { cursorAdvanced = true; return []; }
			if (query.includes('set email_sync_continuation_token =')) { parked.push(values); return []; }
			if (query.includes('from public.sender_rules')) return [{ pattern: 'icici' }];
			throw new Error(`Unexpected query: ${query}`);
		}, { bodiesFetched: atBoundary ? 499 : 0, onUsage: values => usage.push(values) });
		const message = (id: string, from: string, internalDate: string) =>
			new Response(
				JSON.stringify({
					id,
					internalDate,
					snippet: 'body',
					payload: { headers: [{ name: 'From', value: from }, { name: 'Subject', value: 'Subject' }] },
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } },
			);
		const fetcher = vi.fn(async (input: RequestInfo | URL) => {
			const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());
			if (url.includes('oauth2.googleapis.com/token')) {
				return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.includes('/gmail/v1/users/me/messages?')) {
				pageSize = new URL(url).searchParams.get('maxResults') ?? '';
				return new Response(JSON.stringify({
					messages: atBoundary ? [{ id: 'bank1' }] : [{ id: 'bank1' }, { id: 'personal1' }],
					...(atBoundary ? { nextPageToken: 'tomorrow-page' } : {}),
				}), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				});
			}
			if (url.endsWith('/bank1?format=full')) return message('bank1', 'ICICI Bank <alerts@icicibank.com>', '1700000100000');
			if (url.endsWith('/personal1?format=full')) return message('personal1', 'A Friend <friend@example.com>', '1700000200000');
			throw new Error(`Unexpected URL: ${url}`);
		});
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv();

		const result = await runEmailSyncUserJob(
			{ job_type: 'EMAIL_SYNC_USER', user_id: USER_ID, last_sync_timestamp: 1_700_000_000_000 },
			env,
			withGmailBatchSupport(fetcher),
		);

		expect(result.sender_gated_raw_email_count).toBe(atBoundary ? 0 : 1);
		expect(usage).toEqual([[USER_ID, 'Asia/Kolkata', atBoundary ? 1 : 2, 1]]);
		expect(pageSize).toBe(atBoundary ? '1' : '25');
		expect(cursorAdvanced).toBe(!atBoundary);
		expect(result.quota_deferred_connection_count).toBe(atBoundary ? 1 : 0);
		expect(parked).toHaveLength(atBoundary ? 1 : 0);
		if (atBoundary) {
			expect(parked[0]?.[0]).toBe('tomorrow-page');
			expect(queueSendOf(env).mock.calls.every(call => call[0].job_type !== 'EMAIL_SYNC_USER')).toBe(true);
		}
	});

	it('re-enqueues this user’s stale PENDING rows once the hourly slot is claimed', async () => {
		const sql = createSqlMock((query) => {
			if (query.includes('from public.oauth_connections as oc') && query.includes("sync_status in ('ACTIVE', 'ERROR_PAUSED')")) {
				return []; // nothing to sync; the self-heal still runs
			}
			throw new Error(`Unexpected query: ${query}`);
		}, { requeueClaimed: true, stalePendingIds: [PENDING_A, PENDING_B] });
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv();

		const result = await runEmailSyncUserJob(
			{ job_type: 'EMAIL_SYNC_USER', user_id: USER_ID, last_sync_timestamp: 0 },
			env,
			neverGoogle,
		);

		expect(result.pending_requeued_count).toBe(2);
		expect(queueSendOf(env)).toHaveBeenCalledTimes(1);
		expect(queueSendOf(env)).toHaveBeenCalledWith(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [PENDING_A, PENDING_B] },
			{ contentType: 'json' },
		);
	});

	it('stands down when the hour is already taken', async () => {
		const sql = createSqlMock(() => [], { requeueClaimed: false, stalePendingIds: [PENDING_A] });
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv();

		const result = await runEmailSyncUserJob(
			{ job_type: 'EMAIL_SYNC_USER', user_id: USER_ID, last_sync_timestamp: 0 },
			env,
			neverGoogle,
		);

		expect(result.pending_requeued_count).toBe(0);
		expect(queueSendOf(env)).not.toHaveBeenCalled();
	});

	it('never claims the slot while the user is over the AI cap', async () => {
		const onClaim = vi.fn();
		const sql = createSqlMock(() => [], { aiCalls: 200, requeueClaimed: true, stalePendingIds: [PENDING_A], onClaim });
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv();

		const result = await runEmailSyncUserJob(
			{ job_type: 'EMAIL_SYNC_USER', user_id: USER_ID, last_sync_timestamp: 0 },
			env,
			neverGoogle,
		);

		expect(result.pending_requeued_count).toBe(0);
		expect(onClaim).not.toHaveBeenCalled();
		expect(queueSendOf(env)).not.toHaveBeenCalled();
	});

	it('hands the hour back when the enqueue fails', async () => {
		const onRelease = vi.fn();
		const sql = createSqlMock(() => [], { requeueClaimed: true, stalePendingIds: [PENDING_A], onRelease });
		getSqlClientMock.mockReturnValue(sql);
		const env = createEnv({
			EMAIL_SYNC_QUEUE: { send: vi.fn().mockRejectedValue(new Error('queue down')) },
		} as unknown as Partial<Env>);

		const result = await runEmailSyncUserJob(
			{ job_type: 'EMAIL_SYNC_USER', user_id: USER_ID, last_sync_timestamp: 0 },
			env,
			neverGoogle,
		);

		expect(result.pending_requeued_count).toBe(0);
		expect(onRelease).toHaveBeenCalledTimes(1);
	});
});
