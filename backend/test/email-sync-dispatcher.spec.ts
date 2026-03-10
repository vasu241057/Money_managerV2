import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { EmailSyncJobPayload } from '../../shared/types';
import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import {
	dispatchEmailSyncUsers,
	runEmailSyncDispatchJob,
} from '../src/workers/email-sync-dispatcher';

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

interface DispatchCandidateFixture {
	user_id: string;
	last_sync_timestamp: number | string;
	last_app_open_date: string;
	has_active_connections: boolean;
	has_dormant_connections: boolean;
}

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

function createQueueMock() {
	const sendBatch = vi.fn().mockResolvedValue(undefined);
	const send = vi.fn().mockResolvedValue(undefined);
	const queue = {
		sendBatch,
		send,
	} as unknown as Queue<EmailSyncJobPayload>;

	return { queue, sendBatch, send };
}

function buildUserId(index: number): string {
	return `00000000-0000-4000-8000-${index.toString(16).padStart(12, '0')}`;
}

function createCandidate(
	index: number,
	overrides?: Partial<DispatchCandidateFixture>,
): DispatchCandidateFixture {
	const nowIso = '2026-03-09T00:00:00.000Z';
	return {
		user_id: buildUserId(index),
		last_sync_timestamp: index,
		last_app_open_date: nowIso,
		has_active_connections: true,
		has_dormant_connections: false,
		...overrides,
	};
}

const TEST_APP_CONFIG: AppConfig = {
	appName: 'money-manager-backend',
	appVersion: '0.1.0',
	nodeEnv: 'test',
	supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
	dbMaxConnections: 5,
	dbConnectTimeoutSeconds: 5,
};

beforeEach(() => {
	vi.clearAllMocks();
	getAppConfigMock.mockReturnValue(TEST_APP_CONFIG);
});

describe('email-sync-dispatcher', () => {
	it('paginates with LIMIT/OFFSET and chunks queue writes into 100-message batches', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const scanUpperUserId = buildUserId(9_999);
		const firstPage = Array.from({ length: 1000 }, (_, index) => createCandidate(index + 1));
		const secondPage = Array.from({ length: 250 }, (_, index) => createCandidate(index + 1001));

		const sql = createSqlMock([
			(query, values) => {
				expect(query).toContain('limit');
				expect(query).toContain('offset');
				expect(query).toContain("oc.sync_status in ('ACTIVE', 'DORMANT')");
				expect(query).toContain('oc.created_at <= to_timestamp');
				expect(query).toContain('u.created_at <= to_timestamp');
				expect(values[0]).toBe(nowMs);
				expect(values[1]).toBe(nowMs);
				expect(values[2]).toBe(scanUpperUserId);
				expect(values[3]).toBe(1000);
				expect(values[4]).toBe(0);
				return firstPage;
			},
			(query, values) => {
				expect(query).toContain('limit');
				expect(query).toContain('offset');
				expect(query).toContain("oc.sync_status in ('ACTIVE', 'DORMANT')");
				expect(query).toContain('oc.created_at <= to_timestamp');
				expect(query).toContain('u.created_at <= to_timestamp');
				expect(values[0]).toBe(nowMs);
				expect(values[1]).toBe(nowMs);
				expect(values[2]).toBe(scanUpperUserId);
				expect(values[3]).toBe(1000);
				expect(values[4]).toBe(1000);
				return secondPage;
			},
		]);
		const { queue, sendBatch } = createQueueMock();

		const result = await dispatchEmailSyncUsers(sql, queue, nowMs, {
			scan_upper_user_id: scanUpperUserId,
		});

		expect(sendBatch).toHaveBeenCalledTimes(13);
		const chunkSizes = sendBatch.mock.calls.map((call) => (call[0] as unknown[]).length);
		expect(chunkSizes.every((size) => size <= 100)).toBe(true);
		expect(chunkSizes[0]).toBe(100);
		expect(chunkSizes[12]).toBe(50);

		const firstBatch = sendBatch.mock.calls[0]?.[0] as Array<{ body: { job_type: string } }>;
		expect(firstBatch[0]?.body.job_type).toBe('EMAIL_SYNC_USER');

		expect(result).toEqual({
			page_count: 2,
			scanned_user_count: 1250,
			enqueued_user_job_count: 1250,
			failed_user_job_count: 0,
			queue_batch_count: 13,
			marked_dormant_user_count: 0,
			reactivated_user_count: 0,
			continuation_offset: null,
			scan_upper_user_id: scanUpperUserId,
		});
	});

	it('resolves a snapshot-bounded scan anchor when scan_upper_user_id is omitted', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const scanUpperUserId = buildUserId(9_999);
		const sql = createSqlMock([
			(query, values) => {
				expect(query).toContain('select max(oc.user_id) as max_user_id');
				expect(query).toContain('oc.created_at <= to_timestamp');
				expect(values[0]).toBe(nowMs);
				return [{ max_user_id: scanUpperUserId }];
			},
			(query, values) => {
				expect(query).toContain('limit');
				expect(query).toContain('offset');
				expect(query).toContain('oc.created_at <= to_timestamp');
				expect(query).toContain('u.created_at <= to_timestamp');
				expect(values[0]).toBe(nowMs);
				expect(values[1]).toBe(nowMs);
				expect(values[2]).toBe(scanUpperUserId);
				expect(values[3]).toBe(1000);
				expect(values[4]).toBe(0);
				return [createCandidate(1)];
			},
		]);
		const { queue, sendBatch } = createQueueMock();

		const result = await dispatchEmailSyncUsers(sql, queue, nowMs);

		expect(sendBatch).toHaveBeenCalledTimes(1);
		expect(result.scan_upper_user_id).toBe(scanUpperUserId);
		expect(result.enqueued_user_job_count).toBe(1);
	});

	it('marks stale users as dormant and skips enqueueing them', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const dormantCutoffMs = 45 * 24 * 60 * 60 * 1000;
		const staleUserId = buildUserId(1);
		const activeUserId = buildUserId(2);
		const scanUpperUserId = buildUserId(9_999);

		const sql = createSqlMock([
			() => [
				createCandidate(1, {
					user_id: staleUserId,
					last_sync_timestamp: 10,
					last_app_open_date: new Date(nowMs - dormantCutoffMs - 1).toISOString(),
					has_active_connections: true,
					has_dormant_connections: false,
				}),
				createCandidate(2, {
					user_id: activeUserId,
					last_sync_timestamp: 20,
					last_app_open_date: new Date(nowMs - dormantCutoffMs + 1).toISOString(),
					has_active_connections: true,
					has_dormant_connections: false,
				}),
			],
			(query, values) => {
				expect(query).toContain("set sync_status = 'DORMANT'");
				expect(values[0]).toEqual([staleUserId]);
				return [];
			},
		]);
		const { queue, sendBatch } = createQueueMock();

		const result = await dispatchEmailSyncUsers(sql, queue, nowMs, {
			scan_upper_user_id: scanUpperUserId,
		});

		expect(sendBatch).toHaveBeenCalledTimes(1);
		const firstBatch = sendBatch.mock.calls[0]?.[0] as Array<{ body: { user_id: string } }>;
		expect(firstBatch).toHaveLength(1);
		expect(firstBatch[0]?.body.user_id).toBe(activeUserId);
		expect(result.marked_dormant_user_count).toBe(1);
		expect(result.enqueued_user_job_count).toBe(1);
		expect(result.failed_user_job_count).toBe(0);
	});

	it('reactivates dormant users that are within inactivity threshold', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const dormantCutoffMs = 45 * 24 * 60 * 60 * 1000;
		const userA = buildUserId(3);
		const userB = buildUserId(4);
		const scanUpperUserId = buildUserId(9_999);

		const sql = createSqlMock([
			() => [
				createCandidate(3, {
					user_id: userA,
					last_sync_timestamp: '30',
					last_app_open_date: new Date(nowMs - dormantCutoffMs).toISOString(),
					has_active_connections: false,
					has_dormant_connections: true,
				}),
				createCandidate(4, {
					user_id: userB,
					last_sync_timestamp: 40,
					last_app_open_date: new Date(nowMs - 1_000).toISOString(),
					has_active_connections: true,
					has_dormant_connections: true,
				}),
			],
			(query, values) => {
				expect(query).toContain("set sync_status = 'ACTIVE'");
				expect(values[0]).toEqual([userA, userB]);
				return [];
			},
		]);
		const { queue, sendBatch } = createQueueMock();

		const result = await dispatchEmailSyncUsers(sql, queue, nowMs, {
			scan_upper_user_id: scanUpperUserId,
		});

		expect(sendBatch).toHaveBeenCalledTimes(1);
		const firstBatch = sendBatch.mock.calls[0]?.[0] as Array<{ body: { user_id: string } }>;
		expect(firstBatch).toHaveLength(2);
		expect(firstBatch[0]?.body.user_id).toBe(userA);
		expect(firstBatch[1]?.body.user_id).toBe(userB);
		expect(result.reactivated_user_count).toBe(2);
		expect(result.enqueued_user_job_count).toBe(2);
		expect(result.failed_user_job_count).toBe(0);
	});

	it('clamps negative last_sync_timestamp to zero before enqueueing user jobs', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const scanUpperUserId = buildUserId(9_999);
		const userId = buildUserId(7);
		const sql = createSqlMock([
			() => [
				createCandidate(7, {
					user_id: userId,
					last_sync_timestamp: -42,
					has_active_connections: true,
					has_dormant_connections: false,
				}),
			],
		]);
		const { queue, sendBatch } = createQueueMock();

		const result = await dispatchEmailSyncUsers(sql, queue, nowMs, {
			scan_upper_user_id: scanUpperUserId,
		});

		expect(sendBatch).toHaveBeenCalledTimes(1);
		const firstBatch = sendBatch.mock.calls[0]?.[0] as Array<{
			body: { user_id: string; last_sync_timestamp: number };
		}>;
		expect(firstBatch[0]?.body.user_id).toBe(userId);
		expect(firstBatch[0]?.body.last_sync_timestamp).toBe(0);
		expect(result.enqueued_user_job_count).toBe(1);
		expect(result.failed_user_job_count).toBe(0);
	});

	it('fails fast when a candidate row has invalid last_sync_timestamp', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const scanUpperUserId = buildUserId(9_999);
		const sql = createSqlMock([
			() => [
				createCandidate(1, {
					last_sync_timestamp: 'invalid',
				}),
			],
		]);
		const { queue, sendBatch } = createQueueMock();

		await expect(
			dispatchEmailSyncUsers(sql, queue, nowMs, {
				scan_upper_user_id: scanUpperUserId,
			}),
		).rejects.toThrow('last_sync_timestamp must be a safe integer');
		expect(sendBatch).not.toHaveBeenCalled();
	});

	it('fails fast when scan_upper_user_id is not a UUID', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const sql = createSqlMock([]);
		const { queue, sendBatch } = createQueueMock();

		await expect(
			dispatchEmailSyncUsers(sql, queue, nowMs, {
				scan_upper_user_id: 'invalid-id',
			}),
		).rejects.toThrow('scan_upper_user_id must be a valid UUID');
		expect(sendBatch).not.toHaveBeenCalled();
	});

	it('returns continuation offset when page budget is reached', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const scanUpperUserId = buildUserId(9_999);
		const firstPage = Array.from({ length: 1000 }, (_, index) => createCandidate(index + 1));
		const sql = createSqlMock([
			() => firstPage,
		]);
		const { queue, sendBatch } = createQueueMock();

		const result = await dispatchEmailSyncUsers(sql, queue, nowMs, {
			scan_upper_user_id: scanUpperUserId,
			max_pages_per_invocation: 1,
		});

		expect(sendBatch).toHaveBeenCalledTimes(10);
		expect(result.page_count).toBe(1);
		expect(result.continuation_offset).toBe(1000);
		expect(result.scan_upper_user_id).toBe(scanUpperUserId);
	});

	it('counts chunk as failed when sendBatch fails', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const scanUpperUserId = buildUserId(9_999);
		const sql = createSqlMock([
			() => [
				createCandidate(1),
				createCandidate(2),
			],
		]);
		const { queue, sendBatch, send } = createQueueMock();
		sendBatch.mockRejectedValue(new Error('queue unavailable'));
		send.mockResolvedValue(undefined);

		const result = await dispatchEmailSyncUsers(sql, queue, nowMs, {
			scan_upper_user_id: scanUpperUserId,
		});

		expect(sendBatch).toHaveBeenCalledTimes(1);
		expect(send).not.toHaveBeenCalled();
		expect(result.enqueued_user_job_count).toBe(0);
		expect(result.failed_user_job_count).toBe(2);
		expect(result.queue_batch_count).toBe(0);
	});

	it('defers remaining pages instead of throwing when later query fails after partial enqueue', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const scanUpperUserId = buildUserId(9_999);
		const firstPage = Array.from({ length: 1000 }, (_, index) => createCandidate(index + 1));
		const sql = createSqlMock([
			() => firstPage,
			() => {
				throw new Error('temporary db issue');
			},
		]);
		const { queue, sendBatch } = createQueueMock();

		const result = await dispatchEmailSyncUsers(sql, queue, nowMs, {
			scan_upper_user_id: scanUpperUserId,
		});

		expect(sendBatch).toHaveBeenCalledTimes(10);
		expect(result.enqueued_user_job_count).toBe(1000);
		expect(result.continuation_offset).toBe(1000);
	});
});

describe('runEmailSyncDispatchJob', () => {
	it('enqueues a continuation dispatch job when the dispatcher returns continuation_offset', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const scanUpperUserId = buildUserId(9_999);
		const page = Array.from({ length: 1000 }, (_, index) => createCandidate(index + 1));
		const sql = createSqlMock([
			() => page,
			() => {
				throw new Error('temporary db issue');
			},
		]);
		const { queue, sendBatch, send } = createQueueMock();
		getSqlClientMock.mockReturnValue(sql);
		const dateNowSpy = vi.spyOn(Date, 'now').mockReturnValue(nowMs);

		const dispatchJob = {
			job_type: 'EMAIL_SYNC_DISPATCH' as const,
			scheduled_time: nowMs - 60_000,
			triggered_at: new Date(nowMs - 60_000).toISOString(),
			cron: '*/10 * * * *',
			scan_upper_user_id: scanUpperUserId,
		};

		const result = await runEmailSyncDispatchJob(dispatchJob, {
			EMAIL_SYNC_QUEUE: queue,
		} as unknown as Env);

		expect(sendBatch).toHaveBeenCalledTimes(10);
		expect(send).toHaveBeenCalledTimes(1);
		const [payload, options] = send.mock.calls[0] as [
			{
				job_type: string;
				scheduled_time: number;
				triggered_at: string;
				cron: string;
				start_offset: number;
				scan_upper_user_id?: string;
			},
			{ contentType: string },
		];
		expect(options).toEqual({ contentType: 'json' });
		expect(payload.job_type).toBe('EMAIL_SYNC_DISPATCH');
		expect(payload.scheduled_time).toBe(dispatchJob.scheduled_time);
		expect(payload.cron).toBe(dispatchJob.cron);
		expect(payload.start_offset).toBe(1000);
		expect(payload.scan_upper_user_id).toBe(scanUpperUserId);
		expect(Number.isFinite(Date.parse(payload.triggered_at))).toBe(true);
		expect(result.continuation_offset).toBe(1000);

		dateNowSpy.mockRestore();
	});

	it('throws when all EMAIL_SYNC_USER enqueue attempts fail and skips continuation enqueue', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const sql = createSqlMock([
			() => [
				createCandidate(1),
				createCandidate(2),
			],
		]);
		const { queue, sendBatch, send } = createQueueMock();
		sendBatch.mockRejectedValue(new Error('queue unavailable'));
		getSqlClientMock.mockReturnValue(sql);
		const dateNowSpy = vi.spyOn(Date, 'now').mockReturnValue(nowMs);

		const dispatchJob = {
			job_type: 'EMAIL_SYNC_DISPATCH' as const,
			scheduled_time: nowMs - 60_000,
			triggered_at: new Date(nowMs - 60_000).toISOString(),
			cron: '*/10 * * * *',
			scan_upper_user_id: buildUserId(9_999),
		};

		await expect(
			runEmailSyncDispatchJob(dispatchJob, {
				EMAIL_SYNC_QUEUE: queue,
			} as unknown as Env),
		).rejects.toThrow('Failed to enqueue 2 EMAIL_SYNC_USER jobs');

		expect(send).not.toHaveBeenCalled();
		dateNowSpy.mockRestore();
	});

	it('does not throw when continuation enqueue retries fail after partial progress', async () => {
		const nowMs = Date.parse('2026-03-09T00:00:00.000Z');
		const scanUpperUserId = buildUserId(9_999);
		const page = Array.from({ length: 1000 }, (_, index) => createCandidate(index + 1));
		const sql = createSqlMock([
			() => page,
			() => {
				throw new Error('temporary db issue');
			},
		]);
		const { queue, sendBatch, send } = createQueueMock();
		send.mockRejectedValue(new Error('queue send unavailable'));
		getSqlClientMock.mockReturnValue(sql);
		const dateNowSpy = vi.spyOn(Date, 'now').mockReturnValue(nowMs);

		const dispatchJob = {
			job_type: 'EMAIL_SYNC_DISPATCH' as const,
			scheduled_time: nowMs - 60_000,
			triggered_at: new Date(nowMs - 60_000).toISOString(),
			cron: '*/10 * * * *',
			scan_upper_user_id: scanUpperUserId,
		};

		const result = await runEmailSyncDispatchJob(dispatchJob, {
			EMAIL_SYNC_QUEUE: queue,
		} as unknown as Env);

		expect(sendBatch).toHaveBeenCalledTimes(10);
		expect(send).toHaveBeenCalledTimes(3);
		for (const call of send.mock.calls) {
			const [payload] = call as [{ start_offset?: number; scan_upper_user_id?: string }];
			expect(payload.start_offset).toBe(1000);
			expect(payload.scan_upper_user_id).toBe(scanUpperUserId);
		}
		expect(result.enqueued_user_job_count).toBe(1000);
		expect(result.continuation_offset).toBe(1000);

		dateNowSpy.mockRestore();
	});
});
