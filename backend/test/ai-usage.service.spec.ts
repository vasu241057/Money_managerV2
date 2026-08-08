import { describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	captureAiUsageAttempt,
	extractAiUsageMetrics,
	fetchOpenRouterKeyUsage,
	fetchTavilyKeysUsage,
	getAiUsageDashboard,
	getAiUsageSummary,
	getMerchantMappingSummary,
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
			if (query.includes('with day_counts as')) {
				return [{
					day: '2026-07-19',
					tavily_searches: '42',
					merchant_ai_calls: '2',
					merchants_mapped: '18',
					merchants_skipped_unsure: '3',
					counters_available: true,
				}];
			}
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
			if (query.includes('with day_counts as')) {
				return [{
					day: '2026-07-19',
					tavily_searches: '42',
					merchant_ai_calls: '2',
					merchants_mapped: '18',
					merchants_skipped_unsure: '3',
					counters_available: true,
				}];
			}
			if (query.includes('to_char(date(gma.created_at)')) {
				return [{
					day: '2026-07-19',
					search_key: 'BUNDL TECHNOLOGIES',
					canonical_name: 'Swiggy',
					merchant_type: 'MERCHANT',
					default_category_name: 'Food',
					confirmed: true,
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
		// Tavily credits are no longer bundled here — the dashboard must stay a
		// pure database read, with key balances fetched only by the mapping tab.
		expect(dashboard.merchant_mapping).toMatchObject({
			mapper_enabled: false,
			truncated: false,
		});
		expect('tavily_keys' in dashboard.merchant_mapping).toBe(false);
		expect(dashboard.merchant_mapping.days[0]).toMatchObject({
			day: '2026-07-19',
			tavily_searches: 42,
			merchant_ai_calls: 2,
			merchants_mapped: 18,
			merchants_skipped_unsure: 3,
		});
		expect(dashboard.merchant_mapping.days[0].mappings[0]).toMatchObject({
			search_key: 'BUNDL TECHNOLOGIES',
			canonical_name: 'Swiggy',
			merchant_type: 'MERCHANT',
			default_category: 'Food',
			confirmed: true,
		});
	});
});

describe('Tavily key credit reporting', () => {
	it('reports per-key usage with the account plan as the fallback ceiling', async () => {
		const fetcher = vi.fn().mockImplementation(async (input: string | URL | Request) => {
			return new Response(JSON.stringify({
				key: { usage: 150, limit: 1000, search_usage: 150 },
				account: { current_plan: 'Bootstrap', plan_usage: 500, plan_limit: 15000 },
			}), { status: 200 });
		});

		const keys = await fetchTavilyKeysUsage(
			{ TAVILY_API_KEY: 'tvly-one,tvly-two' } as unknown as Env,
			fetcher,
		);

		expect(keys).toHaveLength(2);
		expect(keys[0]).toMatchObject({
			label: 'Key 1',
			is_live: true,
			used_credits: 150,
			limit_credits: 1000,
			remaining_credits: 850,
			plan_name: 'Bootstrap',
			plan_used_credits: 500,
			plan_limit_credits: 15000,
			unavailable: false,
		});
		expect(keys[0].fetched_at).toBeTruthy();
		expect(keys[1]).toMatchObject({ label: 'Key 2', is_live: false });
		expect(fetcher).toHaveBeenCalledTimes(2);
		expect(String(fetcher.mock.calls[0][0])).toBe('https://api.tavily.com/usage');
	});

	it('marks the first key with credits left as live, not simply the first key', async () => {
		// Key 1 is spent, so Key 2 is what the mapper's rotation will actually
		// reach. Reporting Key 1 as live would hide exactly the exhaustion this
		// panel exists to surface.
		const fetcher = vi.fn().mockImplementation(async (_url, init) => {
			const auth = String((init as RequestInit | undefined)?.headers
				? new Headers((init as RequestInit).headers).get('Authorization')
				: '');
			const spent = auth.includes('tvly-one');
			return new Response(JSON.stringify({
				key: { usage: spent ? 1000 : 20, limit: 1000 },
				account: { current_plan: 'Free', plan_usage: 1020, plan_limit: 2000 },
			}), { status: 200 });
		});

		const keys = await fetchTavilyKeysUsage(
			{ TAVILY_API_KEY: 'tvly-one,tvly-two' } as unknown as Env,
			fetcher,
		);

		expect(keys[0]).toMatchObject({ label: 'Key 1', remaining_credits: 0, is_live: false });
		expect(keys[1]).toMatchObject({ label: 'Key 2', remaining_credits: 980, is_live: true });
	});

	it('falls back to the first key when every balance is exhausted', async () => {
		// A fresh Response per call: one body can only be read once, so a shared
		// instance would make the second key look unreachable rather than spent.
		const fetcher = vi.fn().mockImplementation(async () =>
			new Response(JSON.stringify({
				key: { usage: 1000, limit: 1000 },
				account: { current_plan: 'Free', plan_usage: 1000, plan_limit: 1000 },
			}), { status: 200 }),
		);

		const keys = await fetchTavilyKeysUsage(
			{ TAVILY_API_KEY: 'tvly-one,tvly-two' } as unknown as Env,
			fetcher,
		);

		expect(keys.filter(key => key.is_live)).toHaveLength(1);
		expect(keys[0].is_live).toBe(true);
	});

	it('uses the account plan when a free-tier key has no per-key limit', async () => {
		const fetcher = vi.fn().mockResolvedValue(
			new Response(JSON.stringify({
				key: { usage: 210, limit: null },
				account: { current_plan: 'Bootstrap', plan_usage: 210, plan_limit: 1000 },
			}), { status: 200 }),
		);

		const keys = await fetchTavilyKeysUsage(
			{ TAVILY_API_KEY: 'tvly-free' } as unknown as Env,
			fetcher,
		);

		expect(keys[0]).toMatchObject({
			used_credits: 210,
			limit_credits: null,
			remaining_credits: 790,
			plan_name: 'Bootstrap',
		});
	});

	it('marks a key unavailable instead of failing when the usage call errors', async () => {
		const fetcher = vi.fn().mockImplementation(
			async (input: string | URL | Request, init?: { headers?: Record<string, string> }) => {
				const auth = init?.headers?.Authorization ?? '';
				if (auth.includes('tvly-dead')) {
					throw new Error('network down');
				}
				return new Response(JSON.stringify({
					key: { usage: 40, limit: 1000 },
					account: { current_plan: 'Bootstrap', plan_usage: 40, plan_limit: 1000 },
				}), { status: 200 });
			},
		);

		const keys = await fetchTavilyKeysUsage(
			{ TAVILY_API_KEY: 'tvly-live,tvly-dead' } as unknown as Env,
			fetcher,
		);

		expect(keys[0]).toMatchObject({ unavailable: false, used_credits: 40 });
		expect(keys[1]).toMatchObject({
			unavailable: true,
			used_credits: null,
			remaining_credits: null,
			fetched_at: null,
		});
	});

	it('treats a revoked key (401) as unavailable', async () => {
		const fetcher = vi.fn().mockResolvedValue(new Response('Unauthorized', { status: 401 }));

		const keys = await fetchTavilyKeysUsage(
			{ TAVILY_API_KEY: 'tvly-revoked' } as unknown as Env,
			fetcher,
		);

		expect(keys[0]).toMatchObject({ unavailable: true, used_credits: null });
	});

	it('returns an empty list when no Tavily key is configured', async () => {
		const fetcher = vi.fn();
		const keys = await fetchTavilyKeysUsage(
			{ TAVILY_API_KEY: '  ,  ' } as unknown as Env,
			fetcher,
		);
		expect(keys).toEqual([]);
		expect(fetcher).not.toHaveBeenCalled();
	});
});

describe('Merchant mapping summary', () => {
	it('merges counter days with AI-alias days and attaches mappings', async () => {
		let queryCount = 0;
		const sql = createSqlMock((query) => {
			queryCount += 1;
			if (query.includes('with day_counts as')) {
				// The day query only admits alias days backed by an AI-created
				// merchant, so the seed migration's burst never appears here.
				expect(query).toContain("gm.source = 'AI'");
				return [
					{
						day: '2026-07-19',
						tavily_searches: '7',
						merchant_ai_calls: '1',
						merchants_mapped: '2',
						merchants_skipped_unsure: '1',
						counters_available: true,
					},
					{
						day: '2026-07-18',
						tavily_searches: '0',
						merchant_ai_calls: '0',
						merchants_mapped: '0',
						merchants_skipped_unsure: '0',
						counters_available: false,
					},
				];
			}
			if (query.includes('to_char(date(gma.created_at)')) {
				// Mappings are scoped to the days actually being rendered.
				expect(query).toContain('= any(');
				// A run also aliases seeded and manual merchants, so the
				// mapping list itself must not filter on merchant source.
				expect(query).not.toContain("gm.source = 'AI'");
				return [
					{
						day: '2026-07-19',
						search_key: 'BUNDLTECHNOLOGIES',
						raw_payee: 'BUNDL TECHNOLOGIES',
						canonical_name: 'Swiggy',
						merchant_type: 'MERCHANT',
						default_category_name: 'Food',
						confirmed: false,
					},
					{
						day: '2026-07-19',
						search_key: 'ANURAGS',
						raw_payee: null,
						canonical_name: 'Anurag S',
						merchant_type: 'P2P',
						default_category_name: null,
						confirmed: false,
					},
				];
			}
			throw new Error(`Unexpected query: ${query}`);
		});

		const summary = await getMerchantMappingSummary(sql, undefined);

		expect(queryCount).toBe(2);
		expect(summary.mapper_enabled).toBe(false);
		expect(summary.truncated).toBe(false);
		expect(summary.days).toHaveLength(2);
		expect(summary.days[0]).toMatchObject({
			day: '2026-07-19',
			tavily_searches: 7,
			merchants_mapped: 2,
			counters_available: true,
		});
		expect(summary.days[0].mappings).toHaveLength(2);
		expect(summary.days[0].mappings[0]).toMatchObject({
			search_key: 'BUNDLTECHNOLOGIES',
			raw_payee: 'BUNDL TECHNOLOGIES',
			canonical_name: 'Swiggy',
		});
		expect(summary.days[0].mappings[1]).toMatchObject({
			search_key: 'ANURAGS',
			raw_payee: null,
			merchant_type: 'P2P',
			default_category: null,
		});
		// A day whose counters were never written must not read as zeros.
		expect(summary.days[1].counters_available).toBe(false);
		expect(summary.days[1].mappings).toEqual([]);
	});

	it('skips the mapping query and reports the flag when no day qualifies', async () => {
		let mappingQueried = false;
		const sql = createSqlMock((query) => {
			if (query.includes('with day_counts as')) return [];
			if (query.includes('to_char(date(gma.created_at)')) {
				mappingQueried = true;
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});

		const summary = await getMerchantMappingSummary(
			sql,
			{ MERCHANT_MAPPING_ENABLED: '1' } as unknown as Env,
		);

		expect(summary.mapper_enabled).toBe(true);
		expect(summary.days).toEqual([]);
		expect(summary.truncated).toBe(false);
		// No days means nothing to scope mappings to; the second query is skipped.
		expect(mappingQueried).toBe(false);
	});

	it('caps the day window and flags that older days remain', async () => {
		const sql = createSqlMock((query) => {
			if (query.includes('with day_counts as')) {
				return Array.from({ length: 31 }, (_unused, index) => ({
					day: `2026-07-${String(31 - index).padStart(2, '0')}`,
					tavily_searches: '1',
					merchant_ai_calls: '1',
					merchants_mapped: '0',
					merchants_skipped_unsure: '0',
					counters_available: true,
				}));
			}
			if (query.includes('to_char(date(gma.created_at)')) return [];
			throw new Error(`Unexpected query: ${query}`);
		});

		const summary = await getMerchantMappingSummary(sql, undefined);

		expect(summary.days).toHaveLength(30);
		expect(summary.truncated).toBe(true);
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
