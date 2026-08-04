import { afterEach, describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { callWithTieredFallback } from '../src/workers/email-ai-normalizer';

const USER_ID = '00000000-0000-4000-8000-000000000001';
const CALL_ID = '11111111-1111-4111-8111-111111111111';

function createUsageSqlMock(records: Array<{ query: string; values: unknown[] }>): SqlClient {
	return (async (strings: TemplateStringsArray, ...values: unknown[]) => {
		records.push({ query: strings.join(' ').replace(/\s+/g, ' ').trim(), values });
		return [];
	}) as unknown as SqlClient;
}

function successfulResponse(): Response {
	return new Response(
		JSON.stringify({
			choices: [
				{
					message: {
						content: JSON.stringify({ transactions: [] }),
					},
				},
			],
		}),
		{ status: 200, headers: { 'content-type': 'application/json' } },
	);
}

describe('OpenRouter normalizer fallback', () => {
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('rotates immediately on 402 through DeepSeek, Nemotron, and GPT-OSS', async () => {
		const records: Array<{ query: string; values: unknown[] }> = [];
		const sql = createUsageSqlMock(records);
		const fetchMock = vi
			.fn()
			.mockResolvedValueOnce(new Response('{"error":{"code":402}}', { status: 402 }))
			.mockResolvedValueOnce(new Response('{"error":{"code":402}}', { status: 402 }))
			.mockResolvedValueOnce(successfulResponse());
		vi.stubGlobal('fetch', fetchMock);

		const response = await callWithTieredFallback(
			{ AI_PROVIDER: 'openrouter' } as unknown as Env,
			'dummy email',
			'test-key',
			'https://openrouter.ai/api/v1/chat/completions',
			'dummy system prompt',
			{
				sql,
				userId: USER_ID,
				callId: CALL_ID,
				provider: 'openrouter',
				operation: 'EMAIL_NORMALIZATION',
				itemCount: 4,
			},
		);

		expect(response).toEqual({ transactions: [] });
		expect(fetchMock).toHaveBeenCalledTimes(3);
		expect(
			fetchMock.mock.calls.map(call => {
				const request = call[1] as RequestInit;
				return (JSON.parse(request.body as string) as { model: string }).model;
			}),
		).toEqual([
			'deepseek/deepseek-v4-flash',
			'nvidia/nemotron-3-ultra-550b-a55b:free',
			'openai/gpt-oss-120b:free',
		]);

		const attempts = records.filter(record =>
			record.query.includes('insert into public.ai_usage_events'),
		);
		expect(attempts.map(record => record.values[2])).toEqual([1, 2, 3]);
		expect(attempts.map(record => record.values[6])).toEqual(['ERROR', 'ERROR', 'SUCCESS']);
		const finish = records.find(record => record.query.includes('update public.ai_call_runs'));
		expect(finish?.values.slice(0, 2)).toEqual(['SUCCESS', 3]);
	});

	it('marks the logical call failed only after every provider attempt is exhausted', async () => {
		const records: Array<{ query: string; values: unknown[] }> = [];
		const sql = createUsageSqlMock(records);
		vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
			new Response('{"error":{"code":402}}', { status: 402 }),
		));

		await expect(callWithTieredFallback(
			{ AI_PROVIDER: 'openrouter' } as unknown as Env,
			'dummy email',
			'test-key',
			'https://openrouter.ai/api/v1/chat/completions',
			'dummy system prompt',
			{
				sql,
				userId: USER_ID,
				callId: CALL_ID,
				provider: 'openrouter',
				operation: 'EMAIL_NORMALIZATION',
				itemCount: 1,
			},
		)).rejects.toThrow('All AI model tiers exhausted');

		const finish = records.find(record => record.query.includes('update public.ai_call_runs'));
		expect(finish?.values.slice(0, 2)).toEqual(['FAILED', 3]);
	});
});
