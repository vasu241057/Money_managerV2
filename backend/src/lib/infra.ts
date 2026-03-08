export const EMAIL_SYNC_CRON = '*/10 * * * *';

export const QUEUE_NAMES = {
	EMAIL_SYNC: 'money-manager-email-sync',
	EMAIL_SYNC_DLQ: 'money-manager-email-sync-dlq',
	AI_CLASSIFICATION: 'money-manager-ai-classification',
	AI_CLASSIFICATION_DLQ: 'money-manager-ai-classification-dlq',
} as const;

export type QueueName = (typeof QUEUE_NAMES)[keyof typeof QUEUE_NAMES];
