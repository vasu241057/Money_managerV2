import { describe, expect, it, vi } from 'vitest';

import { QUEUE_NAMES } from '../src/lib/infra';
import { handleQueue } from '../src/workers/queue.worker';

interface MessageHarness {
	message: Message<unknown>;
	ack: ReturnType<typeof vi.fn>;
	retry: ReturnType<typeof vi.fn>;
}

function createMessage(body: unknown, attempts: number): MessageHarness {
	const ack = vi.fn();
	const retry = vi.fn();

	return {
		ack,
		retry,
		message: {
			id: 'msg-1',
			timestamp: new Date(),
			body,
			attempts,
			ack,
			retry,
		} as unknown as Message<unknown>,
	};
}

describe('queue.worker', () => {
	it('acks valid EMAIL_SYNC_DISPATCH messages', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_DISPATCH',
				scheduled_time: 1_700_000_000_000,
				triggered_at: new Date().toISOString(),
				cron: '*/10 * * * *',
			},
			1,
		);

		await handleQueue(
			{
				queue: QUEUE_NAMES.EMAIL_SYNC,
				messages: [harness.message],
				ackAll: vi.fn(),
				retryAll: vi.fn(),
			} as unknown as MessageBatch<unknown>,
			{} as Env,
			{} as ExecutionContext,
		);

		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	it('acks valid EMAIL_SYNC_USER messages', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: 'user-1',
				last_sync_timestamp: 1_700_000_000,
			},
			1,
		);

		await handleQueue(
			{
				queue: QUEUE_NAMES.EMAIL_SYNC,
				messages: [harness.message],
				ackAll: vi.fn(),
				retryAll: vi.fn(),
			} as unknown as MessageBatch<unknown>,
			{} as Env,
			{} as ExecutionContext,
		);

		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	it('acks valid AI_CLASSIFICATION messages', async () => {
		const harness = createMessage(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: 'txn-1',
				requested_at: new Date().toISOString(),
			},
			1,
		);

		await handleQueue(
			{
				queue: QUEUE_NAMES.AI_CLASSIFICATION,
				messages: [harness.message],
				ackAll: vi.fn(),
				retryAll: vi.fn(),
			} as unknown as MessageBatch<unknown>,
			{} as Env,
			{} as ExecutionContext,
		);

		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	it('acks poison messages instead of retrying forever', async () => {
		const harness = createMessage({ job_type: 'EMAIL_SYNC_USER' }, 1);

		await handleQueue(
			{
				queue: QUEUE_NAMES.EMAIL_SYNC,
				messages: [harness.message],
				ackAll: vi.fn(),
				retryAll: vi.fn(),
			} as unknown as MessageBatch<unknown>,
			{} as Env,
			{} as ExecutionContext,
		);

		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	it('acks poison messages when EMAIL_SYNC_DISPATCH has non-finite scheduled_time', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_DISPATCH',
				scheduled_time: Number.NaN,
				triggered_at: new Date().toISOString(),
				cron: '*/10 * * * *',
			},
			1,
		);

		await handleQueue(
			{
				queue: QUEUE_NAMES.EMAIL_SYNC,
				messages: [harness.message],
				ackAll: vi.fn(),
				retryAll: vi.fn(),
			} as unknown as MessageBatch<unknown>,
			{} as Env,
			{} as ExecutionContext,
		);

		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	it('acks poison messages when EMAIL_SYNC_USER has non-finite last_sync_timestamp', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: 'user-1',
				last_sync_timestamp: Number.POSITIVE_INFINITY,
			},
			1,
		);

		await handleQueue(
			{
				queue: QUEUE_NAMES.EMAIL_SYNC,
				messages: [harness.message],
				ackAll: vi.fn(),
				retryAll: vi.fn(),
			} as unknown as MessageBatch<unknown>,
			{} as Env,
			{} as ExecutionContext,
		);

		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	it('retries transient failures with exponential backoff', async () => {
		const harness = createMessage({ job_type: 'EMAIL_SYNC_DISPATCH' }, 1);

		await handleQueue(
			{
				queue: 'unsupported-queue',
				messages: [harness.message],
				ackAll: vi.fn(),
				retryAll: vi.fn(),
			} as unknown as MessageBatch<unknown>,
			{} as Env,
			{} as ExecutionContext,
		);

		expect(harness.ack).not.toHaveBeenCalled();
		expect(harness.retry).toHaveBeenCalledTimes(1);
		expect(harness.retry).toHaveBeenCalledWith({ delaySeconds: 2 });
	});

	it('retries exhausted transient failures to hand off to DLQ', async () => {
		const harness = createMessage({ job_type: 'EMAIL_SYNC_DISPATCH' }, 5);

		await handleQueue(
			{
				queue: 'unsupported-queue',
				messages: [harness.message],
				ackAll: vi.fn(),
				retryAll: vi.fn(),
			} as unknown as MessageBatch<unknown>,
			{} as Env,
			{} as ExecutionContext,
		);

		expect(harness.ack).not.toHaveBeenCalled();
		expect(harness.retry).toHaveBeenCalledTimes(1);
		expect(harness.retry).toHaveBeenCalledWith();
	});
});
