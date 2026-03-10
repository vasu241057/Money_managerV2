import { beforeEach, describe, expect, it, vi } from 'vitest';

const {
	runEmailSyncDispatchJobMock,
	runEmailSyncUserJobMock,
	runNormalizeRawEmailsJobMock,
} = vi.hoisted(() => ({
	runEmailSyncDispatchJobMock: vi.fn().mockResolvedValue({
		page_count: 0,
		scanned_user_count: 0,
		enqueued_user_job_count: 0,
		failed_user_job_count: 0,
		queue_batch_count: 0,
		marked_dormant_user_count: 0,
		reactivated_user_count: 0,
		continuation_offset: null,
		scan_upper_user_id: null,
	}),
	runEmailSyncUserJobMock: vi.fn().mockResolvedValue({
		user_id: '00000000-0000-4000-8000-000000000001',
		connection_count: 1,
		processed_connection_count: 1,
		revoked_connection_count: 0,
		fetched_message_count: 1,
		inserted_or_retried_raw_email_count: 1,
		skipped_existing_raw_email_count: 0,
	}),
	runNormalizeRawEmailsJobMock: vi.fn().mockResolvedValue({
		requested_raw_email_count: 1,
		processed_raw_email_count: 1,
		ignored_raw_email_count: 0,
		unrecognized_raw_email_count: 0,
		skipped_raw_email_count: 0,
		created_transaction_count: 1,
		needs_review_transaction_count: 0,
		ai_enqueued_count: 0,
	}),
}));

vi.mock('../src/workers/email-sync-dispatcher', () => ({
	runEmailSyncDispatchJob: runEmailSyncDispatchJobMock,
}));

vi.mock('../src/workers/email-sync-fetcher', () => ({
	runEmailSyncUserJob: runEmailSyncUserJobMock,
}));

vi.mock('../src/workers/email-normalizer', () => ({
	runNormalizeRawEmailsJob: runNormalizeRawEmailsJobMock,
}));

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
	beforeEach(() => {
		vi.clearAllMocks();
	});

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

		expect(runEmailSyncDispatchJobMock).toHaveBeenCalledTimes(1);
		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	it('acks valid EMAIL_SYNC_USER messages', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: '00000000-0000-4000-8000-000000000001',
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
		expect(runEmailSyncUserJobMock).toHaveBeenCalledTimes(1);
	});

	it('acks valid EMAIL_SYNC_USER continuation messages', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: '00000000-0000-4000-8000-000000000001',
				last_sync_timestamp: 1_700_000_000,
				continuation_connection_id: '00000000-0000-4000-8000-0000000000aa',
				continuation_page_token: 'p1000',
				continuation_after_seconds: 0,
				continuation_max_internal_timestamp_seen: 1_700_000_400_000,
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
		expect(runEmailSyncUserJobMock).toHaveBeenCalledTimes(1);
	});

	it('acks valid NORMALIZE_RAW_EMAILS messages', async () => {
		const harness = createMessage(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000aa'],
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
		expect(runNormalizeRawEmailsJobMock).toHaveBeenCalledTimes(1);
	});

	it('acks valid AI_CLASSIFICATION messages', async () => {
		const harness = createMessage(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-00000000000a',
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

	it('acks poison messages when EMAIL_SYNC_DISPATCH has negative scheduled_time', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_DISPATCH',
				scheduled_time: -1,
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
				user_id: '00000000-0000-4000-8000-000000000001',
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

	it('acks poison messages when EMAIL_SYNC_USER has non-integer last_sync_timestamp', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: '00000000-0000-4000-8000-000000000001',
				last_sync_timestamp: 1.5,
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

	it('acks poison messages when EMAIL_SYNC_DISPATCH has invalid scan_upper_user_id', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_DISPATCH',
				scheduled_time: 1_700_000_000_000,
				triggered_at: new Date().toISOString(),
				cron: '*/10 * * * *',
				scan_upper_user_id: 'invalid-id',
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

		expect(runEmailSyncDispatchJobMock).not.toHaveBeenCalled();
		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	it('acks valid continuation dispatch payload without scan_upper_user_id', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_DISPATCH',
				scheduled_time: 1_700_000_000_000,
				triggered_at: new Date().toISOString(),
				cron: '*/10 * * * *',
				start_offset: 1000,
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

		expect(runEmailSyncDispatchJobMock).toHaveBeenCalledTimes(1);
		expect(harness.ack).toHaveBeenCalledTimes(1);
		expect(harness.retry).not.toHaveBeenCalled();
	});

	it('acks poison messages when EMAIL_SYNC_USER has invalid user_id', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: 'not-a-uuid',
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

	it('acks poison messages when EMAIL_SYNC_USER continuation shape is partial/invalid', async () => {
		const harness = createMessage(
			{
				job_type: 'EMAIL_SYNC_USER',
				user_id: '00000000-0000-4000-8000-000000000001',
				last_sync_timestamp: 1_700_000_000,
				continuation_connection_id: '00000000-0000-4000-8000-0000000000aa',
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

	it('acks poison messages when NORMALIZE_RAW_EMAILS payload has empty raw_email_ids', async () => {
		const harness = createMessage(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: [],
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
		expect(runNormalizeRawEmailsJobMock).not.toHaveBeenCalled();
	});

	it('acks poison messages when NORMALIZE_RAW_EMAILS payload exceeds raw_email_ids max size', async () => {
		const oversizedRawEmailIds = Array.from({ length: 251 }, (_, index) =>
			`00000000-0000-4000-8000-${String(index).padStart(12, '0')}`,
		);
		const harness = createMessage(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: oversizedRawEmailIds,
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
		expect(runNormalizeRawEmailsJobMock).not.toHaveBeenCalled();
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
