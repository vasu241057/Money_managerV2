export const EMAIL_SYNC_CRON = '*/10 * * * *';
// 21:30 UTC = 03:00 IST — end-of-day batch for Indian users.
export const SENDER_CLASSIFICATION_CRON = '30 21 * * *';

export const QUEUE_NAMES = {
	EMAIL_SYNC: 'money-manager-email-sync',
	EMAIL_SYNC_DLQ: 'money-manager-email-sync-dlq',
} as const;

export type QueueName = (typeof QUEUE_NAMES)[keyof typeof QUEUE_NAMES];
