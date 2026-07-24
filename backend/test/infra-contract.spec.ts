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

describe('infra contract', () => {
	it('keeps cron + queue names aligned with wrangler.jsonc', () => {
		const wranglerConfigPath = path.resolve(__dirname, '../wrangler.jsonc');
		const rawContent = readFileSync(wranglerConfigPath, 'utf8');
		const config = parseJsonc(rawContent);

		expect(config.triggers?.crons).toContain(EMAIL_SYNC_CRON);

		const producerQueues = new Set((config.queues?.producers ?? []).map((producer) => producer.queue));
		const consumerQueues = new Set((config.queues?.consumers ?? []).map((consumer) => consumer.queue));

		expect(producerQueues.has(QUEUE_NAMES.EMAIL_SYNC)).toBe(true);
		expect(consumerQueues.has(QUEUE_NAMES.EMAIL_SYNC)).toBe(true);
	});
});
