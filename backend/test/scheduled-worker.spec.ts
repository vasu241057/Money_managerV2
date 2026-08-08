import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EMAIL_SYNC_CRON, SENDER_CLASSIFICATION_CRON } from '../src/lib/infra';

const claimDueScheduledJobs = vi.fn();
const releaseScheduledJobClaim = vi.fn();

vi.mock('../src/lib/config', () => ({
	getAppConfig: () => ({ databaseUrl: 'postgres://test' }),
}));

vi.mock('../src/lib/db/client', () => ({
	getSqlClient: () => ({}),
}));

vi.mock('../src/services/job-schedules.service', () => ({
	claimDueScheduledJobs: (...args: unknown[]) => claimDueScheduledJobs(...args),
	releaseScheduledJobClaim: (...args: unknown[]) => releaseScheduledJobClaim(...args),
}));

const { handleScheduled } = await import('../src/workers/scheduled.worker');

function controllerFor(cron: string): ScheduledController {
	return { cron, scheduledTime: 1_700_000_000_000 } as ScheduledController;
}

describe('scheduled.worker', () => {
	beforeEach(() => {
		claimDueScheduledJobs.mockReset().mockResolvedValue([]);
		releaseScheduledJobClaim.mockReset().mockResolvedValue(undefined);
	});

	it('enqueues EMAIL_SYNC_DISPATCH job for expected cron', async () => {
		const send = vi.fn().mockResolvedValue(undefined);
		const waitUntil = vi.fn();

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil } as unknown as ExecutionContext,
		);

		expect(send).toHaveBeenCalledTimes(1);
		const [payload, options] = send.mock.calls[0] as [
			Record<string, unknown>,
			Record<string, unknown>,
		];
		expect(payload.job_type).toBe('EMAIL_SYNC_DISPATCH');
		expect(payload.cron).toBe(EMAIL_SYNC_CRON);
		expect(payload.scheduled_time).toBe(1_700_000_000_000);
		expect(options).toEqual({ contentType: 'json' });
	});

	it('still enqueues dispatch jobs when cron expression drifts', async () => {
		const send = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor('0 * * * *'),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(send).toHaveBeenCalledTimes(1);
	});

	it('does not enqueue email sync on the nightly cron', async () => {
		const send = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(SENDER_CLASSIFICATION_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(send).not.toHaveBeenCalled();
		expect(claimDueScheduledJobs).toHaveBeenCalledTimes(1);
	});

	// The dispatcher no longer decides *when* — it only asks the schedule what
	// is due, so nothing is enqueued on a tick where nothing was claimed.
	it('enqueues nothing extra when no job is due', async () => {
		const send = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(send.mock.calls.map(([payload]) => payload.job_type)).toEqual([
			'EMAIL_SYNC_DISPATCH',
		]);
	});

	it('enqueues each claimed job', async () => {
		claimDueScheduledJobs.mockResolvedValue([
			{ job: 'sender_classification', previous_enqueued_on: null, previous_enqueued_at: null },
			{ job: 'merchant_mapping', previous_enqueued_on: null, previous_enqueued_at: null },
		]);
		const send = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(send.mock.calls.map(([payload]) => payload.job_type).sort()).toEqual([
			'EMAIL_SYNC_DISPATCH',
			'MERCHANT_MAPPING',
			'SENDER_CLASSIFICATION',
		]);
	});

	it('never offers merchant mapping to the claim while the kill switch is set', async () => {
		const send = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{
				EMAIL_SYNC_QUEUE: { send },
				MERCHANT_MAPPING_ENABLED: 'off',
			} as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(claimDueScheduledJobs.mock.calls[0]?.[1]).toEqual(['sender_classification']);
	});

	it('offers both jobs to the claim by default', async () => {
		const send = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(claimDueScheduledJobs.mock.calls[0]?.[1]).toEqual([
			'sender_classification',
			'merchant_mapping',
		]);
	});

	// A consumed day with no job run is the worst outcome: it would look
	// scheduled and simply never happen until tomorrow.
	it('hands the day back when the queue send fails', async () => {
		const claim = {
			job: 'merchant_mapping',
			previous_enqueued_on: '2026-08-08',
			previous_enqueued_at: '2026-08-08T21:30:00.000Z',
		};
		claimDueScheduledJobs.mockResolvedValue([claim]);
		const send = vi.fn().mockRejectedValue(new Error('queue unavailable'));

		await handleScheduled(
			controllerFor(SENDER_CLASSIFICATION_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(releaseScheduledJobClaim).toHaveBeenCalledTimes(1);
		expect(releaseScheduledJobClaim.mock.calls[0]?.[1]).toEqual(claim);
	});

	it('keeps email sync dispatching when the schedule lookup fails', async () => {
		claimDueScheduledJobs.mockRejectedValue(new Error('database unreachable'));
		const send = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(send).toHaveBeenCalledTimes(1);
		expect(send.mock.calls[0]?.[0]).toEqual(
			expect.objectContaining({ job_type: 'EMAIL_SYNC_DISPATCH' }),
		);
	});
});
