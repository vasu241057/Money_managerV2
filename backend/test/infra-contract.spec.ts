import { readFileSync } from 'node:fs';
import path from 'node:path';

import { describe, expect, it } from 'vitest';

import { EMAIL_SYNC_CRON, QUEUE_NAMES } from '../src/lib/infra';

interface WranglerQueueConfig {
	producers: Array<{ binding: string; queue: string }>;
	consumers: Array<{ queue: string }>;
}

interface WranglerConfig {
	triggers?: {
		crons?: string[];
	};
	queues?: WranglerQueueConfig;
}

function parseJsonc(content: string): WranglerConfig {
	const withoutBlockComments = content.replace(/\/\*[\s\S]*?\*\//g, '');
	const withoutLineComments = withoutBlockComments.replace(/^\s*\/\/.*$/gm, '');
	return JSON.parse(withoutLineComments) as WranglerConfig;
}

describe('infra contract', () => {
	it('keeps cron + queue names aligned with wrangler.jsonc', () => {
		const wranglerConfigPath = path.resolve(__dirname, '../wrangler.jsonc');
		const rawContent = readFileSync(wranglerConfigPath, 'utf8');
		const config = parseJsonc(rawContent);

		expect(config.triggers?.crons).toContain(EMAIL_SYNC_CRON);

		const producerQueues = new Set((config.queues?.producers ?? []).map((producer) => producer.queue));
		const consumerQueues = new Set((config.queues?.consumers ?? []).map((consumer) => consumer.queue));

		expect(producerQueues.has(QUEUE_NAMES.EMAIL_SYNC)).toBe(true);
		expect(producerQueues.has(QUEUE_NAMES.AI_CLASSIFICATION)).toBe(true);
		expect(consumerQueues.has(QUEUE_NAMES.EMAIL_SYNC)).toBe(true);
		expect(consumerQueues.has(QUEUE_NAMES.AI_CLASSIFICATION)).toBe(true);
	});
});
