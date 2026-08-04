import { describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	captureAiUsageAttempt,
	extractAiUsageMetrics,
	fetchOpenRouterKeyUsage,
	getAiUsageDashboard,
	getAiUsageSummary,
	isAiUsageAdmin,
	isValidAiUsageMonth,
} from '../src/services/ai-usage.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function createSqlMock(handler: QueryHandler): SqlClient {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		return handler(query, values);
	};
	return tag as unknown as SqlClient;
}

describe('AI usage capture', () => {
	it('extracts provider-reported cost and detailed token counts', () => {
		expect(
			extractAiUsageMetrics({
				usage: {
					prompt_tokens: 120,
					completion_tokens: 30,
					total_tokens: 150,
					cost: 0.00042,
					completion_tokens_details: { reasoning_tokens: 18 },
					prompt_tokens_details: { cached_tokens: 40 },
				},
			}),
		).toEqual({
			promptTokens: 120,
			completionTokens: 30,
			reasoningTokens: 18,
			cachedTokens: 40,
			totalTokens: 150,
			costUsd: 0.00042,
		});
	});

	it('persists one metadata-only receipt and makes rejected HTTP calls zero-cost', async () => {
		let insertValues: unknown[] = [];
		const sql = createSqlMock((query, values) => {
			expect(query).toContain('insert into public.ai_usage_events');
			expect(query).not.toContain('messages');
			insertValues = values;
			return [];
		});

		await captureAiUsageAttempt({
			sql,
			userId: 'admin-id',
			callId: '22222222-2222-4222-8222-222222222222',
			attemptNumber: 1,
			provider: 'openrouter',
			model: 'deepseek/deepseek-v4-flash',
			operation: 'EMAIL_NORMALIZATION',
			itemCount: 4,
			startedAtMs: Date.now() - 20,
			status: 'ERROR',
			httpStatus: 402,
			payload: { usage: { cost: 99 } },
			errorCode: 'HTTP_402',
		});

		expect(insertValues[0]).toBe('admin-id');
		expect(insertValues[1]).toBe('22222222-2222-4222-8222-222222222222');
		expect(insertValues[2]).toBe(1);
		expect(insertValues[3]).toBe('openrouter');
		expect(insertValues[4]).toBe('deepseek/deepseek-v4-flash');
		expect(insertValues[5]).toBe('EMAIL_NORMALIZATION');
		expect(insertValues[6]).toBe('ERROR');
		expect(insertValues[7]).toBe(402);
		expect(insertValues[8]).toBe(4);
		expect(insertValues[14]).toBe(0);
		expect(insertValues[16]).toBe('HTTP_402');
	});

	it('keeps key totals available when the optional credits lookup is malformed', async () => {
		const fetcher = vi.fn().mockImplementation(async (input: string | URL | Request) => {
			if (String(input).endsWith('/credits')) {
				return new Response('{', { status: 200 });
			}
			return new Response(JSON.stringify({
				data: { usage: 1.75, usage_monthly: 0.4, limit: null, limit_remaining: null },
			}), { status: 200 });
		});

		await expect(
			fetchOpenRouterKeyUsage('test-key', fetcher, 'management-key'),
		).resolves.toMatchObject({
			all_time_cost_usd: 1.75,
			current_month_cost_usd: 0.4,
			purchased_credits_usd: null,
			credits_remaining_usd: null,
		});
	});

	it('does not request account credits without a management key', async () => {
		const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify({
			data: { usage: 1.75, usage_monthly: 0.4, limit: null, limit_remaining: null },
		}), { status: 200 }));

		const result = await fetchOpenRouterKeyUsage('test-key', fetcher);

		expect(result?.all_time_cost_usd).toBe(1.75);
		expect(result?.purchased_credits_usd).toBeNull();
		expect(fetcher).toHaveBeenCalledTimes(1);
		expect(fetcher).toHaveBeenCalledWith(
			'https://openrouter.ai/api/v1/key',
			expect.any(Object),
		);
	});
});

describe('AI usage dashboard queries', () => {
	it('combines exact OpenRouter key usage with locally tracked totals', async () => {
		const sql = createSqlMock((query) => {
			expect(query).toContain('from public.ai_usage_events');
			return [{
				tracked_since: '2026-08-01T00:00:00.000Z',
				all_time_cost: '0.125',
				month_cost: '0.025',
				all_time_requests: '9',
				month_requests: '3',
			}];
		});
		const fetcher = vi.fn().mockImplementation(async (input: string | URL | Request) => {
			const url = String(input);
			if (url.endsWith('/credits')) {
				return new Response(JSON.stringify({
					data: { total_credits: 5, total_usage: 1.8 },
				}), { status: 200 });
			}
			return new Response(JSON.stringify({
				data: {
					usage: 1.75,
					usage_monthly: 0.4,
					limit: 5,
					limit_remaining: 3.25,
				},
			}), { status: 200 });
		});

		const summary = await getAiUsageSummary(
			sql,
			{
				OPENROUTER_API_KEY: 'test-key',
				OPENROUTER_MANAGEMENT_API_KEY: 'management-key',
			} as unknown as Env,
			'admin-id',
			fetcher,
		);

		expect(summary.account).toMatchObject({
			provider: 'openrouter',
			all_time_cost_usd: 1.8,
			current_month_cost_usd: 0.4,
			purchased_credits_usd: 5,
			credits_remaining_usd: 3.2,
			limit_usd: 5,
			remaining_usd: 3.25,
		});
		expect(summary.tracked_all_time_cost_usd).toBe(0.125);
		expect(summary.tracked_current_month_request_count).toBe(3);
		expect(fetcher).toHaveBeenCalledWith(
			'https://openrouter.ai/api/v1/key',
			expect.objectContaining({ headers: { Authorization: 'Bearer test-key' } }),
		);
		expect(fetcher).toHaveBeenCalledWith(
			'https://openrouter.ai/api/v1/credits',
			expect.objectContaining({ headers: { Authorization: 'Bearer management-key' } }),
		);
	});

	it('returns month, model, and paginated event receipts', async () => {
		const sql = createSqlMock((query, values) => {
			if (query.includes('group by 1')) {
				return [{
					month: '2026-08',
					request_count: '2',
					success_count: '1',
					cost: '0.01',
					item_count: '5',
					average_latency_ms: '1250',
					total_tokens: '500',
				}];
			}
			if (query.includes('group by aue.provider, aue.model')) {
				expect(values).toContain('2026-08-01');
				return [{
					provider: 'openrouter',
					model: 'deepseek/deepseek-v4-flash',
					request_count: '2',
					success_count: '1',
					cost: '0.01',
					prompt_tokens: '400',
					completion_tokens: '100',
					reasoning_tokens: '40',
					cached_tokens: '0',
					total_tokens: '500',
					item_count: '5',
					average_latency_ms: '1250',
				}];
			}
			if (query.includes('count(*) over ()')) {
				expect(values.at(-1)).toBe(30);
				return [{
					id: '11111111-1111-4111-8111-111111111111',
					call_id: '22222222-2222-4222-8222-222222222222',
					attempt_number: 1,
					provider: 'openrouter',
					model: 'deepseek/deepseek-v4-flash',
					operation: 'EMAIL_NORMALIZATION',
					status: 'SUCCESS',
					http_status: 200,
					item_count: 4,
					prompt_tokens: '400',
					completion_tokens: '100',
					reasoning_tokens: '40',
					cached_tokens: '0',
					total_tokens: '500',
					cost_usd: '0.01',
					latency_ms: 1200,
					error_code: null,
					generation_id: 'gen-1',
					created_at: '2026-08-04T10:00:00.000Z',
					total_event_count: '2',
				}];
			}
			if (query.includes('with email_scope as')) {
				return [{
					emails_handled: '5',
					template_hits: '3',
					ai_routed_emails: '2',
					email_final_failures: '0',
					transactions_created: '4',
					logical_calls: '1',
					provider_attempts: '2',
					first_attempt_successes: '0',
					recovered_calls: '1',
					final_failures: '0',
					in_progress_calls: '0',
				}];
			}
			if (query.includes('with recent_calls as')) {
				return [{
					run_id: '22222222-2222-4222-8222-222222222222',
					run_item_count: '2',
					run_status: 'SUCCESS',
					run_attempt_count: '2',
					run_started_at: '2026-08-04T10:00:00.000Z',
					run_completed_at: '2026-08-04T10:00:02.000Z',
					attempt_id: '11111111-1111-4111-8111-111111111111',
					attempt_number: '1',
					model: 'deepseek/deepseek-v4-flash',
					attempt_status: 'ERROR',
					http_status: 402,
					error_code: 'HTTP_402',
					cost_usd: '0',
					latency_ms: 400,
				}];
			}
			throw new Error(`Unexpected query: ${query}`);
		});

		const dashboard = await getAiUsageDashboard(sql, 'admin-id', '2026-08', 2);

		expect(dashboard).toMatchObject({
			selected_month: '2026-08',
			page: 2,
			page_size: 30,
			total_event_count: 2,
		});
		expect(dashboard.models[0]).toMatchObject({
			request_count: 2,
			cost_usd: 0.01,
			item_count: 5,
			average_latency_ms: 1250,
		});
		expect(dashboard.months[0]).toMatchObject({
			item_count: 5,
			average_latency_ms: 1250,
		});
		expect(dashboard.events[0]).toMatchObject({
			call_id: '22222222-2222-4222-8222-222222222222',
			attempt_number: 1,
			generation_id: 'gen-1',
			item_count: 4,
			reasoning_tokens: 40,
		});
		expect(dashboard.processing).toMatchObject({
			emails_handled: 5,
			template_hits: 3,
			ai_routed_emails: 2,
			recovered_calls: 1,
			provider_attempts: 2,
		});
		expect(dashboard.processing.recent_runs[0]).toMatchObject({
			status: 'SUCCESS',
			attempt_count: 2,
		});
	});
});

describe('AI usage access and month validation', () => {
	it('grants access only to the configured authenticated user', () => {
		const env = { AI_USAGE_ADMIN_USER_ID: 'admin-id' } as unknown as Env;
		expect(isAiUsageAdmin(env, 'admin-id')).toBe(true);
		expect(isAiUsageAdmin(env, 'someone-else')).toBe(false);
		expect(isAiUsageAdmin({} as Env, 'admin-id')).toBe(false);
	});

	it('accepts only bounded YYYY-MM values', () => {
		expect(isValidAiUsageMonth('2026-08')).toBe(true);
		expect(isValidAiUsageMonth('2026-13')).toBe(false);
		expect(isValidAiUsageMonth('08-2026')).toBe(false);
	});
});
