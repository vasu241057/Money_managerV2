import { describe, expect, it, vi } from 'vitest';

import type { EmailSyncJobPayload } from '../../shared/types';
import type { SqlClient } from '../src/lib/db/client';
import { dispatchEmailSyncUsers } from '../src/workers/email-sync-dispatcher';

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
				expect(values[0]).toBe(scanUpperUserId);
				expect(values[1]).toBe(1000);
				expect(values[2]).toBe(0);
				return firstPage;
			},
			(query, values) => {
				expect(query).toContain('limit');
				expect(query).toContain('offset');
				expect(query).toContain("oc.sync_status in ('ACTIVE', 'DORMANT')");
				expect(values[0]).toBe(scanUpperUserId);
				expect(values[1]).toBe(1000);
				expect(values[2]).toBe(1000);
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
