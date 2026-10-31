import { beforeEach, describe, expect, it, vi } from 'vitest';

const runSenderClassificationJobMock = vi.fn().mockResolvedValue({
	candidate_count: 0,
	candidate_backlog_count: 0,
	candidate_cap: 0,
	classified_count: 0,
	allow_rule_count: 0,
	deny_rule_count: 0,
	backfill_raw_email_count: 0,
	backfill_job_count: 0,
	skipped_reason: null,
});
const runSenderBackfillJobMock = vi.fn().mockResolvedValue({
	requested_count: 1,
	refetched_count: 1,
	skipped_count: 0,
	normalize_enqueued: 1,
});
const runEmailSyncUserJobMock = vi.fn().mockResolvedValue({
	connection_count: 0,
	processed_connection_count: 0,
	revoked_connection_count: 0,
	fetched_message_count: 0,
	inserted_or_retried_raw_email_count: 0,
	skipped_existing_raw_email_count: 0,
});

vi.mock('../src/workers/sender-classifier', () => ({
	runSenderClassificationJob: (...args: unknown[]) => runSenderClassificationJobMock(...args),
}));
vi.mock('../src/workers/email-sync-fetcher', () => ({
	releaseEmailSyncJobLeases: vi.fn().mockResolvedValue(1),
	runEmailSyncUserJob: (...args: unknown[]) => runEmailSyncUserJobMock(...args),
	runSenderBackfillJob: (...args: unknown[]) => runSenderBackfillJobMock(...args),
}));

import { isNightlyJobType, NIGHTLY_JOB_TYPES, QUEUE_NAMES } from '../src/lib/infra';
import { handleQueue } from '../src/workers/queue.worker';
import {
	resetNightlyQueueBindingWarning,
	resolveNightlyQueue,
} from '../src/workers/queue.producers';

function batchOf(queue: string, body: unknown) {
	const ack = vi.fn();
	const retry = vi.fn();
	const message = {
		id: 'msg-1',
		timestamp: new Date(),
		body,
		attempts: 1,
		ack,
		retry,
	} as unknown as Message<unknown>;

	return {
		ack,
		retry,
		batch: {
			queue,
			messages: [message],
			ackAll: vi.fn(),
			retryAll: vi.fn(),
		} as unknown as MessageBatch<unknown>,
	};
}

describe('nightly queue routing', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		resetNightlyQueueBindingWarning();
	});

	it('names the four job types that belong on the nightly queue', () => {
		expect([...NIGHTLY_JOB_TYPES]).toEqual([
			'SENDER_CLASSIFICATION',
			'SENDER_BACKFILL',
			'MERCHANT_MAPPING',
			'RAW_EMAIL_BODY_PURGE',
		]);
		expect(isNightlyJobType('MERCHANT_MAPPING')).toBe(true);
		expect(isNightlyJobType('EMAIL_SYNC_USER')).toBe(false);
		expect(isNightlyJobType(undefined)).toBe(false);
	});

	it('prefers the nightly binding when it exists', () => {
		const nightly = { send: vi.fn() } as unknown as Queue;
		const sync = { send: vi.fn() } as unknown as Queue;

		expect(resolveNightlyQueue({ EMAIL_SYNC_QUEUE: sync, NIGHTLY_QUEUE: nightly })).toBe(nightly);
	});

	// The queue must exist before a producer can send to it, so an unbound
	// NIGHTLY_QUEUE has to keep working rather than fail the send.
	it('falls back to the sync queue when the binding is missing, warning once', () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
		const sync = { send: vi.fn() } as unknown as Queue;

		expect(resolveNightlyQueue({ EMAIL_SYNC_QUEUE: sync })).toBe(sync);
		expect(resolveNightlyQueue({ EMAIL_SYNC_QUEUE: sync })).toBe(sync);

		const warnings = warn.mock.calls.filter((call) => call[0] === 'NIGHTLY_QUEUE_BINDING_MISSING');
		expect(warnings).toHaveLength(1);
		warn.mockRestore();
	});

	it('runs a nightly job delivered on the nightly queue', async () => {
		const harness = batchOf(QUEUE_NAMES.NIGHTLY, {
			job_type: 'SENDER_CLASSIFICATION',
			triggered_at: new Date().toISOString(),
		});

		await handleQueue(harness.batch, {} as Env, {} as ExecutionContext);

		expect(runSenderClassificationJobMock).toHaveBeenCalledTimes(1);
		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	// Messages enqueued before the split, and anything the producer fallback
	// routed, must still drain on the main queue.
	it('still runs a nightly job delivered on the sync queue', async () => {
		const harness = batchOf(QUEUE_NAMES.EMAIL_SYNC, {
			job_type: 'SENDER_CLASSIFICATION',
			triggered_at: new Date().toISOString(),
		});

		await handleQueue(harness.batch, {} as Env, {} as ExecutionContext);

		expect(runSenderClassificationJobMock).toHaveBeenCalledTimes(1);
		expect(harness.ack).toHaveBeenCalledTimes(1);
	});

	it('handles a misrouted sync job on the nightly queue, but says so', async () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
		const harness = batchOf(QUEUE_NAMES.NIGHTLY, {
			job_type: 'EMAIL_SYNC_USER',
			user_id: '00000000-0000-4000-8000-000000000001',
			last_sync_timestamp: 1_700_000_000,
		});

		await handleQueue(harness.batch, {} as Env, {} as ExecutionContext);

		expect(runEmailSyncUserJobMock).toHaveBeenCalledTimes(1);
		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(
			warn.mock.calls.filter((call) => call[0] === 'QUEUE_ROUTING_UNEXPECTED'),
		).toHaveLength(1);
		warn.mockRestore();
	});

	it('logs and acks a dead letter on the nightly DLQ under its own queue name', async () => {
		const error = vi.spyOn(console, 'error').mockImplementation(() => undefined);
		const harness = batchOf(QUEUE_NAMES.NIGHTLY_DLQ, {
			job_type: 'MERCHANT_MAPPING',
			triggered_at: new Date().toISOString(),
		});

		await handleQueue(harness.batch, {} as Env, {} as ExecutionContext);

		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
		const dlqLine = error.mock.calls.find((call) => call[0] === 'DLQ_MESSAGE');
		expect(dlqLine?.[1]).toMatchObject({
			queue: QUEUE_NAMES.NIGHTLY_DLQ,
			jobType: 'MERCHANT_MAPPING',
		});
		error.mockRestore();
	});

	it('reports a dead-letter batch once, not once per message', async () => {
		const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 } as Response);
		vi.stubGlobal('fetch', fetchMock);
		vi.spyOn(console, 'error').mockImplementation(() => undefined);

		const ack = vi.fn();
		const messages = [1, 2, 3].map(
			(index) =>
				({
					id: `msg-${index}`,
					timestamp: new Date(),
					body: { job_type: 'MERCHANT_MAPPING' },
					attempts: 1,
					ack,
					retry: vi.fn(),
				}) as unknown as Message<unknown>,
		);

		await handleQueue(
			{
				queue: QUEUE_NAMES.NIGHTLY_DLQ,
				messages,
				ackAll: vi.fn(),
				retryAll: vi.fn(),
			} as unknown as MessageBatch<unknown>,
			{ ERROR_REPORTING_WEBHOOK_URL: 'https://alerts.example.com/hook' } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(ack).toHaveBeenCalledTimes(3);
		expect(fetchMock).toHaveBeenCalledTimes(1);
		const body = JSON.parse((fetchMock.mock.calls[0][1] as RequestInit).body as string);
		expect(body.event).toBe('DLQ_MESSAGE');
		expect(body.context.deadLetterCount).toBe(3);
		vi.unstubAllGlobals();
	});
});
