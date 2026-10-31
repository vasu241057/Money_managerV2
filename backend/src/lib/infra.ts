export const EMAIL_SYNC_CRON = '*/10 * * * *';
// 21:30 UTC = 03:00 IST. No longer a registered trigger: nightly work is
// scheduled in job_schedules and claimed by the 10-minute tick. Kept so the
// dispatcher still recognizes it (rather than logging drift) if it is ever
// re-added as a trigger.
export const SENDER_CLASSIFICATION_CRON = '30 21 * * *';

export const QUEUE_NAMES = {
	EMAIL_SYNC: 'money-manager-email-sync',
	EMAIL_SYNC_DLQ: 'money-manager-email-sync-dlq',
	// Task 8: nightly work runs on its own queue so a long batch job cannot
	// sit in front of a user's email sync, and so the two can be given
	// different concurrency and different retry behaviour.
	NIGHTLY: 'money-manager-nightly',
	NIGHTLY_DLQ: 'money-manager-nightly-dlq',
} as const;

export type QueueName = (typeof QUEUE_NAMES)[keyof typeof QUEUE_NAMES];

/**
 * Job types that belong on the nightly queue: the three claimed from
 * job_schedules, plus the per-user backfill the sender classifier fans out
 * during its own run.
 *
 * The main consumer still accepts every one of them. That is deliberate —
 * messages enqueued before the split has to drain somewhere, and the queue a
 * job arrives on never changes how it is handled.
 */
export const NIGHTLY_JOB_TYPES = [
	'SENDER_CLASSIFICATION',
	'SENDER_BACKFILL',
	'MERCHANT_MAPPING',
	'RAW_EMAIL_BODY_PURGE',
] as const;

export type NightlyJobType = (typeof NIGHTLY_JOB_TYPES)[number];

export function isNightlyJobType(jobType: unknown): jobType is NightlyJobType {
	return (NIGHTLY_JOB_TYPES as readonly unknown[]).includes(jobType);
}

/**
 * Consumer concurrency ceilings (Task 8), asserted against wrangler.jsonc by
 * the infra contract test.
 *
 * The binding constraint is pooler slots, not CPU: every consumer invocation
 * builds its own postgres client with `max: DB_MAX_CONNECTIONS` (default 5),
 * so the worst case is roughly concurrency x 5 client connections against
 * PgBouncer. 10 and 2 keep that near 60 while letting ten users sync in
 * parallel — and, more to the point, they keep nightly batch work from
 * taking concurrency the interactive path needs.
 */
export const QUEUE_MAX_CONCURRENCY = {
	EMAIL_SYNC: 10,
	NIGHTLY: 2,
} as const;
