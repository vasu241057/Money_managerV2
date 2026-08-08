import type { JobSchedule } from '../../../shared/types';

/** Minutes between dispatcher ticks — mirrors SCHEDULE_TICK_MINUTES on the server. */
export const SCHEDULE_TICK_MINUTES = 10;

export function formatIstDateTime(iso: string): string {
  return new Date(iso).toLocaleString('en-IN', {
    timeZone: 'Asia/Kolkata',
    day: 'numeric',
    month: 'short',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  });
}

/**
 * What the card promises about the next run. next_run_at comes from the same
 * predicate the dispatcher claims on, so the only judgement made here is
 * whether that moment has already arrived — a slot in the past means the job is
 * due and the next tick takes it, which must not be shown as a past date.
 */
export function describeNextRun(
  schedule: Pick<JobSchedule, 'enabled' | 'next_run_at' | 'kill_switched'>,
  now: number,
): string {
  if (schedule.kill_switched) {
    return 'Forced off by MERCHANT_MAPPING_ENABLED in the environment — the toggle cannot override it.';
  }
  if (!schedule.enabled || schedule.next_run_at === null) {
    return 'Off — nothing is scheduled.';
  }
  if (new Date(schedule.next_run_at).getTime() <= now) {
    return `Due now — the next ${SCHEDULE_TICK_MINUTES}-minute check will start it.`;
  }
  return `Next run ${formatIstDateTime(schedule.next_run_at)} IST`;
}

export function isDueNow(
  schedule: Pick<JobSchedule, 'enabled' | 'next_run_at' | 'kill_switched'>,
  now: number,
): boolean {
  if (!schedule.enabled || schedule.kill_switched || schedule.next_run_at === null) return false;
  return new Date(schedule.next_run_at).getTime() <= now;
}
