import { readFileSync } from 'node:fs';
import path from 'node:path';

import { describe, expect, it } from 'vitest';

import {
	IP_RATE_LIMIT_PER_WINDOW,
	RATE_LIMIT_WINDOW_SECONDS,
	USER_RATE_LIMIT_PER_WINDOW,
} from '../src/lib/http/rate-limit';
import { EMAIL_SYNC_CRON, QUEUE_MAX_CONCURRENCY, QUEUE_NAMES } from '../src/lib/infra';

interface WranglerQueueConfig {
	producers: Array<{ binding: string; queue: string }>;
	consumers: Array<{
		queue: string;
		dead_letter_queue?: string;
		max_retries?: number;
		max_concurrency?: number;
	}>;
}

interface WranglerRateLimit {
	name: string;
	namespace_id: string;
	simple: { limit: number; period: number };
}

interface WranglerConfig {
	triggers?: {
		crons?: string[];
	};
	queues?: WranglerQueueConfig;
	ratelimits?: WranglerRateLimit[];
	vars?: Record<string, unknown>;
}

function parseJsonc(content: string): WranglerConfig {
	let output = '';
	let inString = false;
	let escaping = false;
	let inLineComment = false;
	let inBlockComment = false;

	for (let index = 0; index < content.length; index += 1) {
		const character = content[index];
		const nextCharacter = content[index + 1];

		if (inLineComment) {
			if (character === '\n') {
				inLineComment = false;
				output += character;
			}
			continue;
		}

		if (inBlockComment) {
			if (character === '*' && nextCharacter === '/') {
				inBlockComment = false;
				index += 1;
			} else if (character === '\n') {
				output += character;
			}
			continue;
		}

		if (inString) {
			output += character;
			if (escaping) {
				escaping = false;
			} else if (character === '\\') {
				escaping = true;
			} else if (character === '"') {
				inString = false;
			}
			continue;
		}

		if (character === '"') {
			inString = true;
			output += character;
		} else if (character === '/' && nextCharacter === '/') {
			inLineComment = true;
			index += 1;
		} else if (character === '/' && nextCharacter === '*') {
			inBlockComment = true;
			index += 1;
		} else {
			output += character;
		}
	}

	return JSON.parse(output) as WranglerConfig;
}

function loadWranglerConfig(): WranglerConfig {
	const wranglerConfigPath = path.resolve(__dirname, '../wrangler.jsonc');
	return parseJsonc(readFileSync(wranglerConfigPath, 'utf8'));
}

describe('infra contract', () => {
	it('keeps cron + queue names aligned with wrangler.jsonc', () => {
		const config = loadWranglerConfig();

		expect(config.triggers?.crons).toContain(EMAIL_SYNC_CRON);

		const producerQueues = new Set((config.queues?.producers ?? []).map((producer) => producer.queue));
		const consumerQueues = new Set((config.queues?.consumers ?? []).map((consumer) => consumer.queue));

		expect(producerQueues.has(QUEUE_NAMES.EMAIL_SYNC)).toBe(true);
		expect(consumerQueues.has(QUEUE_NAMES.EMAIL_SYNC)).toBe(true);
	});

	it('consumes the dead-letter queue the sync consumer routes to', () => {
		const config = loadWranglerConfig();
		const consumers = config.queues?.consumers ?? [];

		const syncConsumer = consumers.find((consumer) => consumer.queue === QUEUE_NAMES.EMAIL_SYNC);
		expect(syncConsumer?.dead_letter_queue).toBe(QUEUE_NAMES.EMAIL_SYNC_DLQ);

		// Without this consumer a dead job is only a queue-depth number.
		const dlqConsumer = consumers.find((consumer) => consumer.queue === QUEUE_NAMES.EMAIL_SYNC_DLQ);
		expect(dlqConsumer).toBeDefined();
		// Nothing catches a failure here, so the handler must never retry.
		expect(dlqConsumer?.max_retries).toBe(0);
	});

	it('binds both rate limiters the Worker reads by name', () => {
		const config = loadWranglerConfig();
		const byName = new Map((config.ratelimits ?? []).map((entry) => [entry.name, entry]));

		expect(byName.get('USER_RATE_LIMITER')?.simple).toEqual({
			limit: USER_RATE_LIMIT_PER_WINDOW,
			period: RATE_LIMIT_WINDOW_SECONDS,
		});
		expect(byName.get('IP_RATE_LIMITER')?.simple).toEqual({
			limit: IP_RATE_LIMIT_PER_WINDOW,
			period: RATE_LIMIT_WINDOW_SECONDS,
		});
		// Distinct namespaces, or the two limits share one counter.
		expect(byName.get('USER_RATE_LIMITER')?.namespace_id).not.toBe(
			byName.get('IP_RATE_LIMITER')?.namespace_id,
		);
	});

	it('binds a producer and a consumer for the nightly queue', () => {
		const config = loadWranglerConfig();
		const producers = config.queues?.producers ?? [];
		const consumers = config.queues?.consumers ?? [];

		// The code resolves this binding by name; a rename here silently
		// sends every nightly job back to the sync queue.
		const nightlyProducer = producers.find(
			(producer) => producer.queue === QUEUE_NAMES.NIGHTLY,
		);
		expect(nightlyProducer?.binding).toBe('NIGHTLY_QUEUE');

		const nightlyConsumer = consumers.find((consumer) => consumer.queue === QUEUE_NAMES.NIGHTLY);
		expect(nightlyConsumer?.dead_letter_queue).toBe(QUEUE_NAMES.NIGHTLY_DLQ);

		const nightlyDlqConsumer = consumers.find(
			(consumer) => consumer.queue === QUEUE_NAMES.NIGHTLY_DLQ,
		);
		expect(nightlyDlqConsumer).toBeDefined();
		expect(nightlyDlqConsumer?.max_retries).toBe(0);
	});

	it('caps consumer concurrency at the numbers the code documents', () => {
		const consumers = loadWranglerConfig().queues?.consumers ?? [];
		const byQueue = new Map(consumers.map((consumer) => [consumer.queue, consumer]));

		// Unset means auto-scale, which is what these numbers exist to stop:
		// concurrency x DB_MAX_CONNECTIONS is the pooler-slot ceiling.
		expect(byQueue.get(QUEUE_NAMES.EMAIL_SYNC)?.max_concurrency).toBe(
			QUEUE_MAX_CONCURRENCY.EMAIL_SYNC,
		);
		expect(byQueue.get(QUEUE_NAMES.NIGHTLY)?.max_concurrency).toBe(
			QUEUE_MAX_CONCURRENCY.NIGHTLY,
		);
	});

	it('keeps the admin user id out of the committed config', () => {
		// Task 7 moved it to a secret; keep_vars preserves the deployed value.
		expect(Object.keys(loadWranglerConfig().vars ?? {})).not.toContain('AI_USAGE_ADMIN_USER_ID');
	});
});
