import { describe, expect, it, vi } from 'vitest';

import {
	assertAiRequiresWebhookSecret,
	enqueueAiRequiresWebhookJob,
	parseAiRequiresWebhookRequest,
} from '../src/services/ai-webhook.service';

function createEnv(overrides?: Partial<Env>): Env {
	return {
		APP_NAME: 'money-manager-backend',
		APP_VERSION: '0.1.0',
		NODE_ENV: 'test',
		SUPABASE_POOLER_URL: 'postgres://postgres:postgres@localhost:6543/postgres',
		DB_MAX_CONNECTIONS: '5',
		DB_CONNECT_TIMEOUT_SECONDS: '5',
		AI_CLASSIFICATION_QUEUE: {
			send: vi.fn().mockResolvedValue(undefined),
		},
		...(overrides ?? {}),
	} as unknown as Env;
}

describe('ai webhook service', () => {
	it('parses valid webhook payload with optional requested_at', () => {
		const parsed = parseAiRequiresWebhookRequest({
			job_type: 'REQUIRES_AI',
			transaction_id: '00000000-0000-4000-8000-0000000000a1',
			requested_at: '2026-03-11T10:00:00.000Z',
		});

		expect(parsed).toEqual({
			job_type: 'REQUIRES_AI',
			transaction_id: '00000000-0000-4000-8000-0000000000a1',
			requested_at: '2026-03-11T10:00:00.000Z',
		});
	});

	it('rejects invalid webhook payload transaction_id', () => {
		expect(() =>
			parseAiRequiresWebhookRequest({
				transaction_id: 'not-a-uuid',
			}),
		).toThrow('transaction_id must be a valid UUID');
	});

	it('rejects unsupported webhook job_type values', () => {
		expect(() =>
			parseAiRequiresWebhookRequest({
				job_type: 'OTHER',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
			}),
		).toThrow('job_type must be REQUIRES_AI when provided');
	});

	it('validates webhook secret using timing-safe comparison', () => {
		expect(() =>
			assertAiRequiresWebhookSecret('secret-1', 'secret-1'),
		).not.toThrow();

		expect(() => assertAiRequiresWebhookSecret('secret-2', 'secret-1')).toThrow(
			'Invalid AI webhook secret',
		);
	});

	it('enqueues AI_CLASSIFICATION job with delay from environment', async () => {
		const send = vi.fn().mockResolvedValue(undefined);
		const env = createEnv({
			AI_QUEUE_DELAY_SECONDS: '45',
			AI_CLASSIFICATION_QUEUE: { send } as unknown as Queue,
		});

		const response = await enqueueAiRequiresWebhookJob(env, {
			transaction_id: '00000000-0000-4000-8000-0000000000a1',
			requested_at: '2026-03-11T10:00:00.000Z',
		});

		expect(response).toEqual({
			accepted: true,
			queued_at: '2026-03-11T10:00:00.000Z',
		});

		expect(send).toHaveBeenCalledTimes(1);
		expect(send).toHaveBeenCalledWith(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:00:00.000Z',
			},
			{
				contentType: 'json',
				delaySeconds: 45,
			},
		);
	});

	it('falls back to default queue delay for invalid configuration', async () => {
		const send = vi.fn().mockResolvedValue(undefined);
		const env = createEnv({
			AI_QUEUE_DELAY_SECONDS: 'invalid',
			AI_CLASSIFICATION_QUEUE: { send } as unknown as Queue,
		});

		await enqueueAiRequiresWebhookJob(env, {
			transaction_id: '00000000-0000-4000-8000-0000000000a1',
		});

		expect(send).toHaveBeenCalledTimes(1);
		expect(send).toHaveBeenCalledWith(
			expect.objectContaining({
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
			}),
			expect.objectContaining({
				delaySeconds: 30,
			}),
		);
	});
});
