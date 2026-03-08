import { describe, expect, it, vi } from 'vitest';

import { EMAIL_SYNC_CRON } from '../src/lib/infra';
import { handleScheduled } from '../src/workers/scheduled.worker';

describe('scheduled.worker', () => {
	it('enqueues EMAIL_SYNC_DISPATCH job for expected cron', async () => {
		const send = vi.fn().mockResolvedValue(undefined);
		const waitUntil = vi.fn();
		const env = {
			EMAIL_SYNC_QUEUE: { send },
		} as unknown as Env;

		await handleScheduled(
			{
				cron: EMAIL_SYNC_CRON,
				scheduledTime: 1_700_000_000_000,
			} as ScheduledController,
			env,
			{ waitUntil } as unknown as ExecutionContext,
		);

		expect(send).toHaveBeenCalledTimes(1);
		const [payload, options] = send.mock.calls[0] as [Record<string, unknown>, Record<string, unknown>];
		expect(payload.job_type).toBe('EMAIL_SYNC_DISPATCH');
		expect(payload.cron).toBe(EMAIL_SYNC_CRON);
		expect(payload.scheduled_time).toBe(1_700_000_000_000);
		expect(options).toEqual({ contentType: 'json' });
		expect(waitUntil).toHaveBeenCalledTimes(1);
	});

	it('ignores unknown cron expressions', async () => {
		const send = vi.fn().mockResolvedValue(undefined);
		const env = {
			EMAIL_SYNC_QUEUE: { send },
		} as unknown as Env;

		await handleScheduled(
			{
				cron: '0 * * * *',
				scheduledTime: 1_700_000_000_000,
			} as ScheduledController,
			env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(send).not.toHaveBeenCalled();
	});
});
