import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { SCHEDULED_JOB_NAMES } from '../../shared/types';
import {
	EMAIL_SYNC_CRON,
	isNightlyJobType,
	SENDER_CLASSIFICATION_CRON,
} from '../src/lib/infra';

const claimDueScheduledJobs = vi.fn();
const releaseScheduledJobClaim = vi.fn();

vi.mock('../src/lib/config', () => ({
	getAppConfig: () => ({ databaseUrl: 'postgres://test' }),
}));

// The tick reads the stale-extraction MV, and once a day the Google
// test-user count; these stand in for those two queries, routed by the SQL
// they carry so each test can drive them independently.
const stalePendingQuery = vi.fn();
const capacityQuery = vi.fn();
vi.mock('../src/lib/db/client', () => ({
	getSqlClient: () => (...args: unknown[]) => {
		const text = Array.isArray(args[0]) ? (args[0] as string[]).join(' ') : '';
		return text.includes('oauth_connections')
			? capacityQuery(...args)
			: stalePendingQuery(...args);
	},
}));

vi.mock('../src/services/job-schedules.service', () => ({
	claimDueScheduledJobs: (...args: unknown[]) => claimDueScheduledJobs(...args),
	releaseScheduledJobClaim: (...args: unknown[]) => releaseScheduledJobClaim(...args),
}));

const { handleScheduled } = await import('../src/workers/scheduled.worker');
const { resetErrorReportingState } = await import('../src/lib/error-reporting');
const { resetNightlyQueueBindingWarning } = await import('../src/workers/queue.producers');

function controllerFor(cron: string, scheduledTime = 1_700_000_000_000): ScheduledController {
	return { cron, scheduledTime } as ScheduledController;
}

/** 1_700_000_000_000 is 22:13 UTC: neither the hourly nor the daily window. */
const AT_0200_UTC = Date.UTC(2026, 8, 19, 2, 0, 0);
const AT_0220_UTC = Date.UTC(2026, 8, 19, 2, 20, 0);
const AT_0900_UTC = Date.UTC(2026, 8, 19, 9, 0, 0);

describe('scheduled.worker', () => {
	beforeEach(() => {
		claimDueScheduledJobs.mockReset().mockResolvedValue([]);
		releaseScheduledJobClaim.mockReset().mockResolvedValue(undefined);
		stalePendingQuery.mockReset().mockResolvedValue([
			{
				user_count: 0,
				stale_count: 0,
				oldest_created_at: null,
				snapshot_at: null,
				snapshot_age_seconds: null,
			},
		]);
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

		expect(claimDueScheduledJobs.mock.calls[0]?.[1]).toEqual([
			'sender_classification',
			'raw_email_body_purge',
		]);
	});

	it('offers every job to the claim by default', async () => {
		const send = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(claimDueScheduledJobs.mock.calls[0]?.[1]).toEqual([
			'sender_classification',
			'raw_email_body_purge',
			'merchant_mapping',
		]);
	});

	it('enqueues the body purge job when the schedule claims it', async () => {
		claimDueScheduledJobs.mockResolvedValue([
			{ job: 'raw_email_body_purge', previous_enqueued_on: null, previous_enqueued_at: null },
		]);
		const send = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(send.mock.calls.map(([payload]) => payload.job_type).sort()).toEqual([
			'EMAIL_SYNC_DISPATCH',
			'RAW_EMAIL_BODY_PURGE',
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

describe('scheduled.worker stale extraction alert', () => {
	beforeEach(() => {
		claimDueScheduledJobs.mockReset().mockResolvedValue([]);
		releaseScheduledJobClaim.mockReset().mockResolvedValue(undefined);
		stalePendingQuery.mockReset();
	});

	async function runTick(): Promise<void> {
		const pending: Promise<unknown>[] = [];
		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send: vi.fn().mockResolvedValue(undefined) } } as unknown as Env,
			{
				waitUntil: (promise: Promise<unknown>) => {
					pending.push(promise);
				},
			} as unknown as ExecutionContext,
		);
		await Promise.all(pending);
	}

	it('logs STALE_PENDING_EXTRACTION when the view has rows', async () => {
		stalePendingQuery.mockResolvedValue([
			{
				user_count: 2,
				stale_count: '17',
				oldest_created_at: '2026-09-19T01:00:00.000Z',
				snapshot_at: '2026-09-19T01:30:00.000Z',
				snapshot_age_seconds: '120',
			},
		]);
		const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});

		await runTick();

		const alert = errorLog.mock.calls.find((call) => call[0] === 'STALE_PENDING_EXTRACTION');
		expect(alert).toBeDefined();
		expect(alert?.[1]).toMatchObject({ userCount: 2, staleCount: 17 });
		errorLog.mockRestore();
	});

	it('stays quiet when nothing is stale', async () => {
		stalePendingQuery.mockResolvedValue([
			{
				user_count: 0,
				stale_count: '0',
				oldest_created_at: null,
				snapshot_at: null,
				snapshot_age_seconds: null,
			},
		]);
		const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});

		await runTick();

		expect(errorLog.mock.calls.some((call) => call[0] === 'STALE_PENDING_EXTRACTION')).toBe(false);
		errorLog.mockRestore();
	});

	it('still dispatches when the view read fails', async () => {
		stalePendingQuery.mockRejectedValue(new Error('relation missing'));
		const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
		const send = vi.fn().mockResolvedValue(undefined);
		const pending: Promise<unknown>[] = [];

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send } } as unknown as Env,
			{
				waitUntil: (promise: Promise<unknown>) => {
					pending.push(promise);
				},
			} as unknown as ExecutionContext,
		);
		await Promise.all(pending);

		expect(send).toHaveBeenCalledTimes(1);
		expect(
			errorLog.mock.calls.some((call) => call[0] === 'STALE_PENDING_EXTRACTION_CHECK_FAILED'),
		).toBe(true);
		errorLog.mockRestore();
	});

	it('reports a frozen view instead of alerting on its numbers', async () => {
		// pg_cron refreshes the view; if it stops, the counts describe an
		// earlier moment and must not be raised as current.
		stalePendingQuery.mockResolvedValue([
			{
				user_count: 5,
				stale_count: '99',
				oldest_created_at: '2026-09-01T00:00:00.000Z',
				snapshot_at: '2026-09-01T00:00:00.000Z',
				snapshot_age_seconds: '1600000',
			},
		]);
		const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});

		await runTick();

		expect(
			errorLog.mock.calls.some((call) => call[0] === 'STALE_PENDING_EXTRACTION_SNAPSHOT_STALE'),
		).toBe(true);
		expect(errorLog.mock.calls.some((call) => call[0] === 'STALE_PENDING_EXTRACTION')).toBe(false);
		errorLog.mockRestore();
	});
});

describe('scheduled.worker nightly queue routing (Task 8)', () => {
	beforeEach(() => {
		claimDueScheduledJobs.mockReset().mockResolvedValue([
			{ job: 'merchant_mapping', previous_enqueued_on: null, previous_enqueued_at: null },
		]);
		releaseScheduledJobClaim.mockReset().mockResolvedValue(undefined);
		stalePendingQuery.mockReset().mockResolvedValue([
			{
				user_count: 0,
				stale_count: 0,
				oldest_created_at: null,
				snapshot_at: null,
				snapshot_age_seconds: null,
			},
		]);
		resetNightlyQueueBindingWarning();
	});

	it('sends the claimed nightly job to the nightly queue, sync dispatch to the sync queue', async () => {
		const syncSend = vi.fn().mockResolvedValue(undefined);
		const nightlySend = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{
				EMAIL_SYNC_QUEUE: { send: syncSend },
				NIGHTLY_QUEUE: { send: nightlySend },
			} as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(syncSend.mock.calls.map(([payload]) => payload.job_type)).toEqual([
			'EMAIL_SYNC_DISPATCH',
		]);
		expect(nightlySend.mock.calls.map(([payload]) => payload.job_type)).toEqual([
			'MERCHANT_MAPPING',
		]);
	});

	// Drift guard: a new nightly job added to SCHEDULED_JOB_NAMES must also
	// be registered in NIGHTLY_JOB_TYPES, or the consumer logs it as
	// misrouted on the very queue it was sent to.
	it('routes every schedulable job to the nightly queue as a registered nightly type', async () => {
		claimDueScheduledJobs.mockResolvedValue(
			SCHEDULED_JOB_NAMES.map((job) => ({
				job,
				previous_enqueued_on: null,
				previous_enqueued_at: null,
			})),
		);
		const nightlySend = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{
				EMAIL_SYNC_QUEUE: { send: vi.fn().mockResolvedValue(undefined) },
				NIGHTLY_QUEUE: { send: nightlySend },
			} as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		const sent = nightlySend.mock.calls.map(([payload]) => payload.job_type);
		expect(sent).toHaveLength(SCHEDULED_JOB_NAMES.length);
		for (const jobType of sent) {
			expect(isNightlyJobType(jobType)).toBe(true);
		}
	});

	// Deployable in one step: the binding does not exist until the deploy
	// that introduces it lands.
	it('falls back to the sync queue when the nightly binding is absent', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => undefined);
		const syncSend = vi.fn().mockResolvedValue(undefined);

		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON),
			{ EMAIL_SYNC_QUEUE: { send: syncSend } } as unknown as Env,
			{ waitUntil: vi.fn() } as unknown as ExecutionContext,
		);

		expect(syncSend.mock.calls.map(([payload]) => payload.job_type).sort()).toEqual([
			'EMAIL_SYNC_DISPATCH',
			'MERCHANT_MAPPING',
		]);
		vi.restoreAllMocks();
	});
});

describe('scheduled.worker Google test-user capacity (Task 8)', () => {
	async function runTickAt(scheduledTime: number, env: Record<string, unknown> = {}) {
		const pending: Promise<unknown>[] = [];
		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON, scheduledTime),
			{
				EMAIL_SYNC_QUEUE: { send: vi.fn().mockResolvedValue(undefined) },
				...env,
			} as unknown as Env,
			{
				waitUntil: (promise: Promise<unknown>) => {
					pending.push(promise);
				},
			} as unknown as ExecutionContext,
		);
		await Promise.all(pending);
	}

	beforeEach(() => {
		claimDueScheduledJobs.mockReset().mockResolvedValue([]);
		releaseScheduledJobClaim.mockReset().mockResolvedValue(undefined);
		stalePendingQuery.mockReset().mockResolvedValue([
			{
				user_count: 0,
				stale_count: 0,
				oldest_created_at: null,
				snapshot_at: null,
				snapshot_age_seconds: null,
			},
		]);
		capacityQuery.mockReset().mockResolvedValue([{ connected_user_count: 3 }]);
		resetErrorReportingState();
	});

	afterEach(() => {
		vi.restoreAllMocks();
		vi.unstubAllGlobals();
	});

	it('asks once a day, on the first tick of the chosen hour', async () => {
		await runTickAt(AT_0200_UTC);
		expect(capacityQuery).toHaveBeenCalledTimes(1);

		// Same hour, later tick: the daily question was already asked.
		await runTickAt(AT_0220_UTC);
		await runTickAt(AT_0900_UTC);
		expect(capacityQuery).toHaveBeenCalledTimes(1);
	});

	it('records the count quietly while there is room', async () => {
		const info = vi.spyOn(console, 'info').mockImplementation(() => undefined);
		const error = vi.spyOn(console, 'error').mockImplementation(() => undefined);

		await runTickAt(AT_0200_UTC);

		expect(info.mock.calls.find((call) => call[0] === 'GOOGLE_TEST_USER_CAPACITY')?.[1]).toMatchObject({
			connectedUserCount: 3,
			cap: 100,
		});
		expect(error.mock.calls.find((call) => call[0] === 'GOOGLE_TEST_USER_CAP_WARNING')).toBeUndefined();
	});

	it('warns and reports as the list approaches Google 100-user ceiling', async () => {
		capacityQuery.mockResolvedValue([{ connected_user_count: 85 }]);
		const error = vi.spyOn(console, 'error').mockImplementation(() => undefined);
		const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 } as Response);
		vi.stubGlobal('fetch', fetchMock);

		await runTickAt(AT_0200_UTC, {
			ERROR_REPORTING_WEBHOOK_URL: 'https://alerts.example.com/hook',
		});

		expect(error.mock.calls.find((call) => call[0] === 'GOOGLE_TEST_USER_CAP_WARNING')?.[1]).toMatchObject({
			connectedUserCount: 85,
			cap: 100,
			countIsLowerBound: true,
		});
		expect(fetchMock).toHaveBeenCalledTimes(1);
		const body = JSON.parse((fetchMock.mock.calls[0][1] as RequestInit).body as string);
		expect(body.event).toBe('GOOGLE_TEST_USER_CAP_WARNING');
		expect(body.severity).toBe('warning');
	});

	it('never lets a failed capacity read cost a tick', async () => {
		capacityQuery.mockRejectedValue(new Error('database unreachable'));
		const error = vi.spyOn(console, 'error').mockImplementation(() => undefined);

		await expect(runTickAt(AT_0200_UTC)).resolves.toBeUndefined();
		expect(
			error.mock.calls.find((call) => call[0] === 'GOOGLE_TEST_USER_CAPACITY_CHECK_FAILED'),
		).toBeDefined();
	});
});

describe('scheduled.worker alert notification throttle (Task 8)', () => {
	async function runTickAt(scheduledTime: number) {
		const pending: Promise<unknown>[] = [];
		await handleScheduled(
			controllerFor(EMAIL_SYNC_CRON, scheduledTime),
			{
				EMAIL_SYNC_QUEUE: { send: vi.fn().mockResolvedValue(undefined) },
				ERROR_REPORTING_WEBHOOK_URL: 'https://alerts.example.com/hook',
			} as unknown as Env,
			{
				waitUntil: (promise: Promise<unknown>) => {
					pending.push(promise);
				},
			} as unknown as ExecutionContext,
		);
		await Promise.all(pending);
	}

	beforeEach(() => {
		claimDueScheduledJobs.mockReset().mockResolvedValue([]);
		releaseScheduledJobClaim.mockReset().mockResolvedValue(undefined);
		capacityQuery.mockReset().mockResolvedValue([{ connected_user_count: 1 }]);
		stalePendingQuery.mockReset().mockResolvedValue([
			{
				user_count: 2,
				stale_count: '17',
				oldest_created_at: '2026-09-19T01:00:00.000Z',
				snapshot_at: '2026-09-19T01:30:00.000Z',
				snapshot_age_seconds: '120',
			},
		]);
		resetErrorReportingState();
		vi.spyOn(console, 'error').mockImplementation(() => undefined);
		vi.spyOn(console, 'info').mockImplementation(() => undefined);
	});

	afterEach(() => {
		vi.restoreAllMocks();
		vi.unstubAllGlobals();
	});

	// A stuck backlog stays stuck for hours; an alert channel that repeats
	// every ten minutes stops being read.
	it('pushes the stale-extraction alert at most hourly, while logging every tick', async () => {
		const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 } as Response);
		vi.stubGlobal('fetch', fetchMock);

		await runTickAt(Date.UTC(2026, 8, 19, 9, 5, 0));
		expect(fetchMock).toHaveBeenCalledTimes(1);

		await runTickAt(Date.UTC(2026, 8, 19, 9, 15, 0));
		await runTickAt(Date.UTC(2026, 8, 19, 9, 45, 0));
		expect(fetchMock).toHaveBeenCalledTimes(1);

		// Every one of those ticks still wrote the console line.
		const lines = (console.error as unknown as ReturnType<typeof vi.fn>).mock.calls.filter(
			(call) => call[0] === 'STALE_PENDING_EXTRACTION',
		);
		expect(lines).toHaveLength(3);
	});
});
