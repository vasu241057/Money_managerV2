import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	claimDueScheduledJobs,
	isScheduledJobName,
	isTickAlignedTime,
	listJobSchedules,
	releaseScheduledJobClaim,
	updateJobSchedule,
} from '../src/services/job-schedules.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function createSqlMock(handler: QueryHandler): SqlClient {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		return handler(query, values);
	};
	return tag as unknown as SqlClient;
}

const SCHEDULE_ROW = {
	job: 'merchant_mapping',
	enabled: true,
	run_at_local: '10:00',
	timezone: 'Asia/Kolkata',
	last_enqueued_at: new Date('2026-08-08T04:30:00.000Z'),
	next_run_at: new Date('2026-08-09T04:30:00.000Z'),
};

describe('scheduled job names', () => {
	it.each(['merchant_mapping', 'sender_classification', 'raw_email_body_purge'])('accepts %s', (job) => {
		expect(isScheduledJobName(job)).toBe(true);
	});

	it.each([undefined, '', 'email_sync', 'MERCHANT_MAPPING', 42])('rejects %s', (job) => {
		expect(isScheduledJobName(job)).toBe(false);
	});
});

describe('schedule time validation', () => {
	// The dispatcher ticks every 10 minutes, so anything it cannot honour must
	// be rejected rather than silently rounded.
	it.each(['00:00', '03:00', '10:30', '23:50'])('accepts tick-aligned %s', (time) => {
		expect(isTickAlignedTime(time)).toBe(true);
	});

	it.each(['10:07', '10:05', '24:00', '9:00', '10:00:00', '', 'ten', undefined, 1000])(
		'rejects %s',
		(time) => {
			expect(isTickAlignedTime(time)).toBe(false);
		},
	);
});

describe('listJobSchedules', () => {
	it('reports the kill switch only against merchant mapping', async () => {
		const sql = createSqlMock(() => [
			SCHEDULE_ROW,
			{ ...SCHEDULE_ROW, job: 'sender_classification' },
		]);

		const schedules = await listJobSchedules(sql, {
			MERCHANT_MAPPING_ENABLED: 'off',
		} as unknown as Env);

		expect(schedules.map((schedule) => schedule.kill_switched)).toEqual([true, false]);
		expect(schedules[0].run_at_local).toBe('10:00');
		expect(schedules[0].next_run_at).toBe('2026-08-09T04:30:00.000Z');
	});

	it('carries a disabled job through with no next run', async () => {
		const sql = createSqlMock(() => [
			{ ...SCHEDULE_ROW, enabled: false, next_run_at: null, last_enqueued_at: null },
		]);

		const [schedule] = await listJobSchedules(sql, {} as unknown as Env);

		expect(schedule.enabled).toBe(false);
		expect(schedule.next_run_at).toBeNull();
		expect(schedule.last_enqueued_at).toBeNull();
	});
});

describe('updateJobSchedule', () => {
	it('stamps the claim date so a slot that already passed waits for tomorrow', async () => {
		const queries: string[] = [];
		const sql = createSqlMock((query) => {
			queries.push(query);
			return query.startsWith('update') ? [{ job: 'merchant_mapping' }] : [SCHEDULE_ROW];
		});

		const schedule = await updateJobSchedule(sql, {} as unknown as Env, 'merchant_mapping', {
			enabled: true,
			run_at_local: '10:00',
		});

		expect(schedule?.job).toBe('merchant_mapping');
		expect(queries[0]).toContain('last_enqueued_on = case');
		expect(queries[0]).toContain('>= coalesce(');
	});

	it('returns null when the job row does not exist', async () => {
		const sql = createSqlMock(() => []);

		const schedule = await updateJobSchedule(sql, {} as unknown as Env, 'merchant_mapping', {
			enabled: true,
		});

		expect(schedule).toBeNull();
	});
});

describe('claimDueScheduledJobs', () => {
	it('returns the prior claim state so a failed enqueue can be undone', async () => {
		const sql = createSqlMock(() => [
			{
				job: 'merchant_mapping',
				previous_enqueued_on: '2026-08-08',
				previous_enqueued_at: new Date('2026-08-08T04:30:00.000Z'),
			},
		]);

		const claimed = await claimDueScheduledJobs(sql, ['merchant_mapping']);

		expect(claimed).toEqual([
			{
				job: 'merchant_mapping',
				previous_enqueued_on: '2026-08-08',
				previous_enqueued_at: '2026-08-08T04:30:00.000Z',
			},
		]);
	});

	it('handles a first-ever claim with no prior state', async () => {
		const sql = createSqlMock(() => [
			{ job: 'sender_classification', previous_enqueued_on: null, previous_enqueued_at: null },
		]);

		const [claim] = await claimDueScheduledJobs(sql, ['sender_classification']);

		expect(claim.previous_enqueued_on).toBeNull();
		expect(claim.previous_enqueued_at).toBeNull();
	});

	it('does not query at all when no job is eligible', async () => {
		let queried = false;
		const sql = createSqlMock(() => {
			queried = true;
			return [];
		});

		expect(await claimDueScheduledJobs(sql, [])).toEqual([]);
		expect(queried).toBe(false);
	});

	it('restores both fields when handing a day back', async () => {
		const values: unknown[][] = [];
		const sql = createSqlMock((_query, queryValues) => {
			values.push(queryValues);
			return [];
		});

		await releaseScheduledJobClaim(sql, {
			job: 'merchant_mapping',
			previous_enqueued_on: '2026-08-08',
			previous_enqueued_at: '2026-08-08T04:30:00.000Z',
		});

		expect(values[0]).toEqual([
			'2026-08-08',
			'2026-08-08T04:30:00.000Z',
			'merchant_mapping',
		]);
	});
});
