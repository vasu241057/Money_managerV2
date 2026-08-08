export const EMAIL_SYNC_CRON = '*/10 * * * *';
// 21:30 UTC = 03:00 IST. No longer a registered trigger: nightly work is
// scheduled in job_schedules and claimed by the 10-minute tick. Kept so the
// dispatcher still recognizes it (rather than logging drift) if it is ever
// re-added as a trigger.
export const SENDER_CLASSIFICATION_CRON = '30 21 * * *';

export const QUEUE_NAMES = {
	EMAIL_SYNC: 'money-manager-email-sync',
	EMAIL_SYNC_DLQ: 'money-manager-email-sync-dlq',
} as const;

export type QueueName = (typeof QUEUE_NAMES)[keyof typeof QUEUE_NAMES];
