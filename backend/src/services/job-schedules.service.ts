import type { JobSchedule, ScheduledJobName } from '../../../shared/types';
import { SCHEDULED_JOB_NAMES } from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import {
	toBoolean,
	toIsoDateTime,
	toNullableString,
	toRequiredString,
} from '../lib/db/serialization';
import { isMerchantMappingKillSwitched } from '../lib/feature-flags';

/** Minutes between dispatcher ticks — the granularity a picked time can be honoured to. */
export const SCHEDULE_TICK_MINUTES = 10;

const RUN_AT_PATTERN = /^([01]\d|2[0-3]):([0-5]\d)$/;

export function isScheduledJobName(value: unknown): value is ScheduledJobName {
	return (
		typeof value === 'string' &&
		(SCHEDULED_JOB_NAMES as readonly string[]).includes(value)
	);
}

/**
 * A time the dispatcher can actually honour. Rejecting 10:07 here (and again in
 * a table constraint) is the difference between "runs at 10:00" and "silently
 * runs at 10:10 and the UI lies about it".
 */
export function isTickAlignedTime(value: unknown): value is string {
	if (typeof value !== 'string') return false;
	const match = RUN_AT_PATTERN.exec(value);
	if (match === null) return false;
	return Number(match[2]) % SCHEDULE_TICK_MINUTES === 0;
}

interface JobScheduleRowRaw {
	job: unknown;
	enabled: unknown;
	run_at_local: unknown;
	timezone: unknown;
	last_enqueued_at: unknown;
	next_run_at: unknown;
}

function mapRow(row: JobScheduleRowRaw, env: Env): JobSchedule {
	const job = toRequiredString(row.job, 'job_schedules.job') as ScheduledJobName;
	return {
		job,
		enabled: toBoolean(row.enabled, 'job_schedules.enabled'),
		run_at_local: toRequiredString(row.run_at_local, 'job_schedules.run_at_local'),
		timezone: toRequiredString(row.timezone, 'job_schedules.timezone'),
		last_enqueued_at:
			row.last_enqueued_at === null
				? null
				: toIsoDateTime(row.last_enqueued_at, 'job_schedules.last_enqueued_at'),
		next_run_at:
			row.next_run_at === null
				? null
				: toIsoDateTime(row.next_run_at, 'job_schedules.next_run_at'),
		kill_switched: job === 'merchant_mapping' && isMerchantMappingKillSwitched(env),
	};
}

/**
 * next_run_at is derived from the *same* predicate the claim query uses, so the
 * time this promises and the time the dispatcher fires cannot drift:
 *   - already claimed today -> tomorrow's slot
 *   - not claimed today     -> today's slot, which being in the past means
 *                              "due now, on the next tick" (the UI says so).
 */
export async function listJobSchedules(sql: SqlClient, env: Env): Promise<JobSchedule[]> {
	const rows = await sql<JobScheduleRowRaw[]>`
		select
			js.job,
			js.enabled,
			to_char(js.run_at_local, 'HH24:MI') as run_at_local,
			js.timezone,
			js.last_enqueued_at,
			case
				when not js.enabled then null
				else (
					(
						case
							when js.last_enqueued_on
								is distinct from (now() at time zone js.timezone)::date
								then (now() at time zone js.timezone)::date
							else (now() at time zone js.timezone)::date + 1
						end
					) + js.run_at_local
				) at time zone js.timezone
			end as next_run_at
		from public.job_schedules as js
		order by js.job
	`;
	return rows.map(row => mapRow(row, env));
}

export interface JobScheduleUpdate {
	enabled?: boolean;
	run_at_local?: string;
}

/**
 * Enabling a job at 23:00 whose slot is 10:00 must schedule tomorrow, not fire
 * within ten minutes. Stamping last_enqueued_on for a slot that has already
 * passed locally is what makes that true, and it is applied on every change so
 * moving 03:00 -> 10:00 at noon behaves the same way.
 */
export async function updateJobSchedule(
	sql: SqlClient,
	env: Env,
	job: ScheduledJobName,
	update: JobScheduleUpdate,
): Promise<JobSchedule | null> {
	const updated = await sql<Array<{ job: unknown }>>`
		update public.job_schedules as js
		set
			enabled = coalesce(${update.enabled ?? null}::boolean, js.enabled),
			run_at_local = coalesce(${update.run_at_local ?? null}::time, js.run_at_local),
			last_enqueued_on = case
				when (now() at time zone js.timezone)::time
					>= coalesce(${update.run_at_local ?? null}::time, js.run_at_local)
					then (now() at time zone js.timezone)::date
				else js.last_enqueued_on
			end,
			updated_at = now()
		where js.job = ${job}
		returning js.job
	`;
	if (updated.length === 0) return null;

	const schedules = await listJobSchedules(sql, env);
	return schedules.find(schedule => schedule.job === job) ?? null;
}

export interface ClaimedJob {
	job: ScheduledJobName;
	/** Prior claim date, so a failed enqueue can hand the day back. */
	previous_enqueued_on: string | null;
	previous_enqueued_at: string | null;
}

/**
 * Atomically claims every job whose local slot has arrived and that has not
 * already been claimed for this local day. The UPDATE itself is the mutual
 * exclusion — `for update skip locked` means a second tick running at the same
 * instant walks away empty rather than blocking or double-enqueueing.
 */
export async function claimDueScheduledJobs(
	sql: SqlClient,
	jobs: readonly ScheduledJobName[],
): Promise<ClaimedJob[]> {
	if (jobs.length === 0) return [];
	const rows = await sql<
		Array<{ job: unknown; previous_enqueued_on: unknown; previous_enqueued_at: unknown }>
	>`
		with candidate as (
			select js.job, js.last_enqueued_on, js.last_enqueued_at
			from public.job_schedules as js
			where js.job = any(${[...jobs]}::text[])
				and js.enabled
				and (now() at time zone js.timezone)::time >= js.run_at_local
				and js.last_enqueued_on is distinct from (now() at time zone js.timezone)::date
			for update skip locked
		)
		update public.job_schedules as js
		set
			last_enqueued_on = (now() at time zone js.timezone)::date,
			last_enqueued_at = now()
		from candidate as c
		where c.job = js.job
		returning
			js.job,
			to_char(c.last_enqueued_on, 'YYYY-MM-DD') as previous_enqueued_on,
			c.last_enqueued_at as previous_enqueued_at
	`;

	const claimed: ClaimedJob[] = [];
	for (const row of rows) {
		const job = toRequiredString(row.job, 'job_schedules.job');
		if (!isScheduledJobName(job)) continue;
		claimed.push({
			job,
			previous_enqueued_on: toNullableString(
				row.previous_enqueued_on,
				'job_schedules.last_enqueued_on',
			),
			previous_enqueued_at:
				row.previous_enqueued_at === null
					? null
					: toIsoDateTime(row.previous_enqueued_at, 'job_schedules.last_enqueued_at'),
		});
	}
	return claimed;
}

/**
 * Hands a claimed day back when the queue send failed. Without this a transient
 * queue error would consume the day and the job would simply not run until
 * tomorrow, silently.
 */
export async function releaseScheduledJobClaim(
	sql: SqlClient,
	claim: ClaimedJob,
): Promise<void> {
	await sql`
		update public.job_schedules
		set
			last_enqueued_on = ${claim.previous_enqueued_on}::date,
			last_enqueued_at = ${claim.previous_enqueued_at}::timestamptz
		where job = ${claim.job}
	`;
}
