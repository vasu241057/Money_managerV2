import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import { previewMerchantMappings, runMerchantMappingJob } from '../src/workers/merchant-mapper';

const { getAppConfigMock, getSqlClientMock } = vi.hoisted(() => ({
	getAppConfigMock: vi.fn(),
	getSqlClientMock: vi.fn(),
}));

vi.mock('../src/lib/config', () => ({
	getAppConfig: getAppConfigMock,
}));

vi.mock('../src/lib/db/client', () => ({
	getSqlClient: getSqlClientMock,
}));

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function createSqlMock(handler: QueryHandler): SqlClient {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		if (query.includes('insert into public.ai_usage_events')) return [];
		return handler(query, values);
	};
	return tag as unknown as SqlClient;
}

const appConfig = {
	appName: 'money-manager-backend',
	appVersion: '0.1.0',
	nodeEnv: 'test',
	supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
	dbMaxConnections: 5,
	dbConnectTimeoutSeconds: 5,
} as AppConfig;

function createEnv(overrides?: Record<string, unknown>): Env {
	return {
		OPENCODE_GO_API_KEY: 'test-key',
		...(overrides ?? {}),
	} as unknown as Env;
}

function modelResponse(merchants: unknown): Response {
	return new Response(
		JSON.stringify({
			choices: [{ message: { content: JSON.stringify({ merchants }) } }],
		}),
		{ status: 200, headers: { 'content-type': 'application/json' } },
	);
}

function triageResponse(personKeys: string[], confidence = 0.95): Response {
	return new Response(
		JSON.stringify({
			choices: [{
				message: {
					content: JSON.stringify({
						assessments: personKeys.map(key => ({ key, kind: 'PERSON', confidence })),
					}),
				},
			}],
		}),
		{ status: 200, headers: { 'content-type': 'application/json' } },
	);
}

function emptyTavilyResponse(): Response {
	return new Response(JSON.stringify({ results: [] }), {
		status: 200,
		headers: { 'content-type': 'application/json' },
	});
}

/**
 * The provider probe, the person triage call and the mapping call all POST to
 * the same URL, so the stub tells them apart by what the request body asks for.
 * Every call gets its OWN Response: a body can only be read once, so sharing a
 * single instance silently starves whichever reader comes second.
 */
function stubModelFetch(options: {
	merchants?: unknown;
	mappingResponse?: () => Response;
	/** Candidate keys triage should report as private individuals. */
	persons?: string[];
	tavily?: () => Response;
} = {}) {
	const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
		if (String(url).includes('tavily')) return (options.tavily ?? emptyTavilyResponse)();
		const body = typeof init?.body === 'string' ? init.body : '';
		if (body.includes('assessments')) return triageResponse(options.persons ?? []);
		return options.mappingResponse
			? options.mappingResponse()
			: modelResponse(options.merchants ?? []);
	});
	vi.stubGlobal('fetch', fetchMock);
	return fetchMock;
}

const JOB = {
	job_type: 'MERCHANT_MAPPING',
	triggered_at: '2026-07-07T21:30:00.000Z',
} as const;

const MERCHANT_ID = '00000000-0000-4000-8000-0000000000fe';
const GROCERIES_CATEGORY_ID = '00000000-0000-0000-0000-000000000117';
const OLD_MERCHANT_ID = '00000000-0000-4000-8000-0000000000f0';

// Handler for the system expense taxonomy the mapper offers the model.
function expenseCategoriesHandler(query: string): unknown[] | null {
	if (query.includes('from public.categories as c') && query.includes("c.type = 'expense'")) {
		return [
			{ id: GROCERIES_CATEGORY_ID, type: 'expense', name: 'Groceries' },
			{ id: '00000000-0000-0000-0000-000000000106', type: 'expense', name: 'Shopping' },
		];
	}
	return null;
}

describe('merchant-mapper worker', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(appConfig);
	});

	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('skips when OPENCODE_GO_API_KEY is not set', async () => {
		const result = await runMerchantMappingJob(
			JOB,
			createEnv({ OPENCODE_GO_API_KEY: undefined }),
		);

		expect(result.skipped_reason).toContain('OPENCODE_GO_API_KEY');
		expect(result.candidate_count).toBe(0);
	});

	it('maps counterparties with an AI-picked default category and relinks transactions', async () => {
		const insertedMerchants: Array<{ name: string; categoryId: unknown; type: string }> = [];
		const insertedAliases: Array<{ key: string; merchantId: string }> = [];
		let relinkQuery = '';
		let relinkKeys: unknown = null;

		const sql = createSqlMock((query, values) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;

			if (query.includes('from public.financial_events as fe') && query.includes('group by fe.search_key')) {
				return [
					{
						search_key: 'FSNECOMMERCEVENTURES',
						counterparty_raw: 'FSN E-COMMERCE VENTURES LTD',
					},
				];
			}
			if (query.includes('from public.global_merchants as gm') && query.includes('default_category_id is null')) {
				// No category backfill backlog in this test.
				return [];
			}
			if (query.includes('insert into public.global_merchants')) {
				insertedMerchants.push({
					name: values[0] as string,
					categoryId: values[1],
					type: values[2] as string,
				});
				return [{ id: MERCHANT_ID }];
			}
			if (query.includes('insert into public.global_merchant_category_defaults')) return [];
			if (query.includes('insert into public.global_merchant_aliases')) {
				insertedAliases.push({ key: values[0] as string, merchantId: values[1] as string });
				return [{ search_key: values[0] }];
			}
			if (query.includes('update public.transactions as t')) {
				relinkQuery = query;
				relinkKeys = values[0];
				return [{ id: '00000000-0000-4000-8000-00000000000t' }];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		stubModelFetch({
			merchants: [
				{
					key: 'FSNECOMMERCEVENTURES',
					canonical_name: 'Nykaa',
					type: 'MERCHANT',
					category: 'Shopping',
				},
				// Injection attempt: unknown key, must be dropped.
				{ key: 'EVILKEY', canonical_name: 'Evil Corp', type: 'MERCHANT', category: 'Shopping' },
			],
		});

		const result = await runMerchantMappingJob(JOB, createEnv());

		expect(result.candidate_count).toBe(1);
		expect(result.mapped_count).toBe(1);
		expect(result.alias_created_count).toBe(1);
		expect(result.relinked_transaction_count).toBe(1);

		expect(insertedMerchants).toEqual([
			{
				name: 'Nykaa',
				categoryId: '00000000-0000-0000-0000-000000000106',
				type: 'MERCHANT',
			},
		]);
		expect(insertedAliases).toEqual([
			{ key: 'FSNECOMMERCEVENTURES', merchantId: MERCHANT_ID },
		]);
		expect(relinkKeys).toEqual(['FSNECOMMERCEVENTURES']);
		// The relink must only fill missing merchant links; it must NOT filter
		// on classification_source (user-rule-classified rows carry USER yet
		// still deserve the merchant name).
		expect(relinkQuery).toContain('merchant_id is null');
		expect(relinkQuery).not.toContain('classification_source');
	});

	it('drops hallucinated categories and gives aggregators no category', async () => {
		const insertedMerchants: Array<{ name: string; categoryId: unknown; type: string }> = [];

		const sql = createSqlMock((query, values) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;

			if (query.includes('from public.financial_events as fe') && query.includes('group by fe.search_key')) {
				return [
					{ search_key: 'SOMESHOP', counterparty_raw: 'Some Shop' },
					{ search_key: 'RAZORPAYSOMETHING', counterparty_raw: 'RAZORPAY SOMETHING' },
				];
			}
			if (query.includes('from public.global_merchants as gm') && query.includes('default_category_id is null')) {
				return [];
			}
			if (query.includes('insert into public.global_merchants')) {
				insertedMerchants.push({
					name: values[0] as string,
					categoryId: values[1],
					type: values[2] as string,
				});
				return [{ id: MERCHANT_ID }];
			}
			if (query.includes('insert into public.global_merchant_aliases')) {
				return [{ search_key: values[0] }];
			}
			if (query.includes('update public.transactions as t')) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		stubModelFetch({
			merchants: [
				// Category not in the offered enum → dropped to null.
				{ key: 'SOMESHOP', canonical_name: 'Some Shop', type: 'MERCHANT', category: 'Crypto' },
				// Aggregators never get a default category, even if the model sends one.
				{ key: 'RAZORPAYSOMETHING', canonical_name: 'Razorpay', type: 'AGGREGATOR', category: 'Shopping' },
			],
		});

		const result = await runMerchantMappingJob(JOB, createEnv());

		expect(result.mapped_count).toBe(2);
		expect(insertedMerchants).toEqual([
			{ name: 'Some Shop', categoryId: null, type: 'MERCHANT' },
			{ name: 'Razorpay', categoryId: null, type: 'AGGREGATOR' },
		]);
	});

	it('backfills categories for pre-existing merchants without one', async () => {
		const categoryUpdates: Array<{ merchantId: unknown; categoryId: unknown }> = [];

		const sql = createSqlMock((query, values) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;

			if (query.includes('from public.financial_events as fe') && query.includes('group by fe.search_key')) {
				return [];
			}
			if (query.includes('from public.global_merchants as gm') && query.includes('default_category_id is null')) {
				return [{ id: OLD_MERCHANT_ID, canonical_name: 'Zepto' }];
			}
			if (query.includes('update public.global_merchants as gm')) {
				categoryUpdates.push({ categoryId: values[0], merchantId: values[1] });
				return [{ id: OLD_MERCHANT_ID }];
			}
			if (query.includes('insert into public.global_merchant_category_defaults')) return [];
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		stubModelFetch({
			merchants: [
				{ key: OLD_MERCHANT_ID, canonical_name: 'Zepto', type: 'MERCHANT', category: 'Groceries' },
			],
		});

		const result = await runMerchantMappingJob(JOB, createEnv());

		expect(result.candidate_count).toBe(0);
		expect(result.category_backfill_candidate_count).toBe(1);
		expect(result.category_backfilled_count).toBe(1);
		expect(categoryUpdates).toEqual([
			{ categoryId: GROCERIES_CATEGORY_ID, merchantId: OLD_MERCHANT_ID },
		]);
	});

	it('returns skipped_reason without writes when the model response is malformed', async () => {
		const sql = createSqlMock((query) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;

			if (query.includes('from public.financial_events as fe')) {
				return [{ search_key: 'SOMEKEY', counterparty_raw: 'Some Merchant' }];
			}
			if (query.includes('from public.global_merchants as gm') && query.includes('default_category_id is null')) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		stubModelFetch({
			mappingResponse: () => new Response(
				JSON.stringify({ choices: [{ message: { content: 'not json at all {' } }] }),
				{ status: 200, headers: { 'content-type': 'application/json' } },
			),
		});

		const result = await runMerchantMappingJob(JOB, createEnv());

		expect(result.skipped_reason).toBe('invalid model response');
		expect(result.alias_created_count).toBe(0);
	});
});

describe('sure-gate and web context', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(appConfig);
	});

	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('skips unsure identifications and records the attempt for cooldown', async () => {
		const insertedMerchants: string[] = [];
		let attemptKeys: unknown = null;
		let candidateQuery = '';
		const sql = createSqlMock((query, values) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;
			if (query.includes('group by fe.search_key')) {
				candidateQuery = query;
				return [
					{ search_key: 'SURESHOP', counterparty_raw: 'FSN E-COMMERCE VENTURES' },
					{ search_key: 'MYSTERYCO', counterparty_raw: 'MYSTERY TRADING CO' },
				];
			}
			if (query.includes('default_category_id is null')) return [];
			if (query.includes('insert into public.global_merchants')) {
				insertedMerchants.push(values[0] as string);
				return [{ id: MERCHANT_ID }];
			}
			if (query.includes('insert into public.global_merchant_category_defaults')) return [];
			if (query.includes('insert into public.global_merchant_aliases')) {
				return [{ search_key: values[0] }];
			}
			if (query.includes('update public.transactions as t')) return [];
			if (query.includes('insert into public.merchant_mapping_attempts')) {
				attemptKeys = values[0];
				return [];
			}
			if (query.includes('insert into public.usage_counters')) return [];
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		stubModelFetch({
			merchants: [
				{ key: 'SURESHOP', canonical_name: 'Nykaa', type: 'MERCHANT', category: 'Shopping', sure: true },
				{ key: 'MYSTERYCO', canonical_name: 'Mystery Brand Guess', type: 'MERCHANT', category: 'Shopping', sure: false },
			],
		});

		const result = await runMerchantMappingJob(JOB, createEnv());

		expect(result.mapped_count).toBe(1);
		expect(result.unsure_skipped_count).toBe(1);
		expect(insertedMerchants).toEqual(['Nykaa']);
		// Both keys rest in the cooldown table; the candidate query excludes
		// recently attempted keys.
		expect(attemptKeys).toEqual(['SURESHOP', 'MYSTERYCO']);
		expect(candidateQuery).toContain('merchant_mapping_attempts');
		expect(candidateQuery).toContain('last_attempt_at >');
	});

	it('rotates Tavily keys on quota exhaustion and counts real searches', async () => {
		const { searchMerchantWebContext } = await import('../src/workers/merchant-mapper');
		const calls: string[] = [];
		const fetcher = vi.fn().mockImplementation(async (_url: unknown, init?: RequestInit) => {
			const auth = (init?.headers as Record<string, string>)?.Authorization ?? '';
			calls.push(auth);
			if (auth.includes('key-one')) {
				return new Response('{}', { status: 432 });
			}
			return new Response(
				JSON.stringify({ results: [{ title: 'Swiggy', content: 'Bundl Technologies is Swiggy' }] }),
				{ status: 200, headers: { 'content-type': 'application/json' } },
			);
		});

		const outcome = await searchMerchantWebContext(
			createEnv({ TAVILY_API_KEY: 'key-one, key-two' }),
			[
				{ key: 'BUNDLTECH', name: 'BUNDL TECHNOLOGIES' },
				{ key: 'ZEPTOMKT', name: 'ZEPTO MARKETPLACE' },
			],
			fetcher as unknown as typeof fetch,
		);

		// key-one died on the first candidate; key-two answered both.
		expect(calls.filter((auth) => auth.includes('key-one'))).toHaveLength(1);
		expect(calls.filter((auth) => auth.includes('key-two'))).toHaveLength(2);
		expect(outcome.searches_performed).toBe(2);
		expect(outcome.context.get('BUNDLTECH')).toContain('Swiggy');
		// The exhausted key was never billed; both searches land on key two, and
		// the ledger has to attribute them to that position or the derived
		// balance drains the wrong account.
		expect(outcome.credits_by_key).toEqual([0, 2]);
		const body = JSON.parse(
			String((fetcher.mock.calls[0][1] as RequestInit).body),
		) as Record<string, unknown>;
		expect(body.include_usage).toBe(true);
	});

	it('records what Tavily says it billed rather than assuming one per search', async () => {
		const { searchMerchantWebContext } = await import('../src/workers/merchant-mapper');
		const fetcher = vi.fn().mockImplementation(async () =>
			new Response(
				JSON.stringify({ results: [{ title: 'X', content: 'Y' }], usage: { credits: 2 } }),
				{ status: 200, headers: { 'content-type': 'application/json' } },
			),
		);

		const outcome = await searchMerchantWebContext(
			createEnv({ TAVILY_API_KEY: 'key-one' }),
			[{ key: 'A', name: 'A LTD' }, { key: 'B', name: 'B LTD' }],
			fetcher as unknown as typeof fetch,
		);

		expect(outcome.searches_performed).toBe(2);
		expect(outcome.credits_by_key).toEqual([4]);
	});

	// Older responses, or a plan that stops reporting, must not silently record
	// zero spend — that would show a full balance while credits drain.
	it('falls back to one credit per search when usage is absent or malformed', async () => {
		const { searchMerchantWebContext } = await import('../src/workers/merchant-mapper');
		const fetcher = vi.fn().mockImplementation(async () =>
			new Response(
				JSON.stringify({ results: [{ title: 'X', content: 'Y' }], usage: { credits: 'lots' } }),
				{ status: 200, headers: { 'content-type': 'application/json' } },
			),
		);

		const outcome = await searchMerchantWebContext(
			createEnv({ TAVILY_API_KEY: 'key-one' }),
			[{ key: 'A', name: 'A LTD' }],
			fetcher as unknown as typeof fetch,
		);

		expect(outcome.credits_by_key).toEqual([1]);
	});

	it('returns empty context without a key and never searches', async () => {
		const { searchMerchantWebContext } = await import('../src/workers/merchant-mapper');
		const fetcher = vi.fn();
		const outcome = await searchMerchantWebContext(
			createEnv(),
			[{ key: 'K', name: 'N' }],
			fetcher as unknown as typeof fetch,
		);
		expect(outcome.context.size).toBe(0);
		expect(outcome.searches_performed).toBe(0);
		expect(outcome.credits_by_key).toEqual([]);
		expect(fetcher).not.toHaveBeenCalled();
	});
});

describe('merchant mapping preview (dry run)', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(appConfig);
	});

	afterEach(() => {
		vi.unstubAllGlobals();
	});

	// A dead key used to be discovered only after every Tavily credit had been
	// spent, and then reported as "invalid model response".
	it('probes the model before buying any search context', async () => {
		const sql = createSqlMock((query) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;
			if (query.includes('from public.financial_events as fe')) {
				return [{ search_key: 'BUNDLTECHNOLOGIES', counterparty_raw: 'BUNDL TECHNOLOGIES' }];
			}
			return [];
		});
		getSqlClientMock.mockReturnValue(sql);

		const fetchMock = vi.fn(async () => new Response('Forbidden', { status: 403 }));
		vi.stubGlobal('fetch', fetchMock);

		const { previewMerchantMappings } = await import('../src/workers/merchant-mapper');
		const result = await previewMerchantMappings(createEnv({ TAVILY_API_KEY: 'tvly-key' }));

		expect(result.candidate_count).toBe(1);
		expect(result.web_search_count).toBe(0);
		expect(result.skipped_reason).toContain('HTTP 403');
		// Exactly one call: the probe. Nothing reached Tavily.
		expect(fetchMock).toHaveBeenCalledTimes(1);
		expect(
			fetchMock.mock.calls.filter(([url]) => String(url).includes('tavily')),
		).toHaveLength(0);
	});

	it('returns the searched context even when the model answer is unusable', async () => {
		const sql = createSqlMock((query) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;
			if (query.includes('from public.financial_events as fe')) {
				return [{ search_key: 'BUNDLTECHNOLOGIES', counterparty_raw: 'BUNDL TECHNOLOGIES' }];
			}
			return [];
		});
		getSqlClientMock.mockReturnValue(sql);

		let probed = false;
		const fetchMock = vi.fn(async (url: string) => {
			if (String(url).includes('tavily')) {
				return new Response(
					JSON.stringify({ results: [{ content: 'Bundl Technologies operates Swiggy.' }] }),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			if (!probed) {
				probed = true;
				return modelResponse([]);
			}
			// The real mapping call comes back as unparseable content.
			return new Response(
				JSON.stringify({ choices: [{ message: { content: 'not json at all' } }] }),
				{ status: 200, headers: { 'content-type': 'application/json' } },
			);
		});
		vi.stubGlobal('fetch', fetchMock);

		const { previewMerchantMappings } = await import('../src/workers/merchant-mapper');
		const result = await previewMerchantMappings(createEnv({ TAVILY_API_KEY: 'tvly-key' }));

		expect(result.proposals).toHaveLength(0);
		expect(result.skipped_reason).toBe('invalid model response');
		// The credits were spent, so what they bought must come back.
		expect(result.searched).toEqual([
			{
				search_key: 'BUNDLTECHNOLOGIES',
				raw_name: 'BUNDL TECHNOLOGIES',
				web_context: 'Bundl Technologies operates Swiggy.',
			},
		]);
	});

	it('returns proposals with web context and writes nothing to mapping state', async () => {
		const writes: string[] = [];
		let candidateQuery = '';
		let candidateValues: unknown[] = [];

		const sql = createSqlMock((query, values) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;

			if (query.includes('from public.financial_events as fe') && query.includes('group by fe.search_key')) {
				candidateQuery = query;
				candidateValues = values;
				return [
					{ search_key: 'BUNDLTECHNOLOGIES', counterparty_raw: 'BUNDL TECHNOLOGIES' },
					{ search_key: 'ETERNALLIMITED', counterparty_raw: 'ETERNAL LIMITED' },
				];
			}
			// Day-bucketed cost counters are the one write a preview keeps: the
			// Tavily and model calls really happened.
			if (query.includes('usage')) return [];
			writes.push(query);
			return [];
		});
		getSqlClientMock.mockReturnValue(sql);

		const fetchMock = vi.fn(async (url: string) => {
			if (String(url).includes('tavily')) {
				return new Response(
					JSON.stringify({ results: [{ content: 'Bundl Technologies operates Swiggy.' }] }),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				);
			}
			return modelResponse([
				{ key: 'BUNDLTECHNOLOGIES', canonical_name: 'Swiggy', type: 'MERCHANT', category: 'Shopping' },
				{ key: 'ETERNALLIMITED', canonical_name: 'Zomato', type: 'MERCHANT', category: 'Shopping' },
			]);
		});
		vi.stubGlobal('fetch', fetchMock);

		const result = await previewMerchantMappings(createEnv({ TAVILY_API_KEY: 'tav-key' }));

		expect(result.candidate_count).toBe(2);
		expect(result.proposal_count).toBe(2);
		expect(result.ai_call_count).toBe(1);
		expect(result.unanswered_keys).toEqual([]);
		expect(result.proposals.map(p => p.canonical_name)).toEqual(['Swiggy', 'Zomato']);
		expect(result.proposals[0]?.raw_name).toBe('BUNDL TECHNOLOGIES');
		expect(result.proposals[0]?.web_context).toContain('Swiggy');
		// Category ids are resolved back to names so the output is readable.
		expect(result.proposals[0]?.default_category).toBe('Shopping');

		// Nothing that the real job would persist may be written here.
		expect(writes).toEqual([]);
		// Defaults widen the candidate query past both gates that strand keys:
		// applyLookback (first bind) and applyCooldown (fifth) both arrive false.
		expect(candidateQuery).toContain('from public.financial_events as fe');
		expect(candidateValues[0]).toBe(false);
		expect(candidateValues[4]).toBe(false);
	});

	it('keeps confident person transfers away from Tavily but still maps them', async () => {
		const sql = createSqlMock((query) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;
			if (query.includes('from public.financial_events as fe')) {
				return [
					{ search_key: 'AJAYKUMAR', counterparty_raw: 'AJAY KUMAR' },
					{ search_key: 'BUNDLTECHNOLOGIES', counterparty_raw: 'BUNDL TECHNOLOGIES' },
				];
			}
			return [];
		});
		getSqlClientMock.mockReturnValue(sql);

		const fetchMock = stubModelFetch({
			persons: ['AJAYKUMAR'],
			merchants: [
				{ key: 'AJAYKUMAR', canonical_name: 'Ajay Kumar', type: 'P2P', category: null },
				{ key: 'BUNDLTECHNOLOGIES', canonical_name: 'Swiggy', type: 'MERCHANT', category: 'Shopping' },
			],
			tavily: () => new Response(
				JSON.stringify({ results: [{ content: 'Bundl Technologies operates Swiggy.' }] }),
				{ status: 200, headers: { 'content-type': 'application/json' } },
			),
		});

		const result = await previewMerchantMappings(createEnv({ TAVILY_API_KEY: 'tvly-key' }));

		expect(result.person_skipped_count).toBe(1);
		expect(result.person_skipped_names).toEqual(['AJAY KUMAR']);
		// One credit spent, on the business — not two.
		expect(result.web_search_count).toBe(1);
		const searchedNames = fetchMock.mock.calls
			.filter(([url]) => String(url).includes('tavily'))
			.map(([, init]) => String((init as RequestInit).body));
		expect(searchedNames).toHaveLength(1);
		expect(searchedNames[0]).toContain('BUNDL TECHNOLOGIES');
		expect(searchedNames[0]).not.toContain('AJAY KUMAR');
		// The person is still mapped: skipping the search is not skipping the work.
		expect(result.proposals.map(p => p.canonical_name).sort()).toEqual(['Ajay Kumar', 'Swiggy']);
	});

	it('overrides a confident person verdict when the payee carries a business marker', async () => {
		const sql = createSqlMock((query) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;
			if (query.includes('from public.financial_events as fe')) {
				return [{ search_key: 'AJAYSINGHAM', counterparty_raw: 'Ajay Singham Private Limited' }];
			}
			return [];
		});
		getSqlClientMock.mockReturnValue(sql);

		// The model is fooled by the personal name and says PERSON with high
		// confidence; the legal suffix must send it back to be researched.
		stubModelFetch({
			persons: ['AJAYSINGHAM'],
			merchants: [
				{ key: 'AJAYSINGHAM', canonical_name: 'Ajay Singham', type: 'MERCHANT', category: 'Shopping' },
			],
		});

		const result = await previewMerchantMappings(createEnv({ TAVILY_API_KEY: 'tvly-key' }));

		expect(result.person_skipped_count).toBe(0);
		expect(result.web_search_count).toBe(1);
	});

	it('searches everything when triage is not confident enough', async () => {
		const sql = createSqlMock((query) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;
			if (query.includes('from public.financial_events as fe')) {
				return [{ search_key: 'MAYBEPERSON', counterparty_raw: 'S KUMAR' }];
			}
			return [];
		});
		getSqlClientMock.mockReturnValue(sql);

		const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
			if (String(url).includes('tavily')) return emptyTavilyResponse();
			const body = typeof init?.body === 'string' ? init.body : '';
			// Below the 0.9 bar: a maybe is worth a credit, not a guess.
			if (body.includes('assessments')) return triageResponse(['MAYBEPERSON'], 0.7);
			return modelResponse([]);
		});
		vi.stubGlobal('fetch', fetchMock);

		const result = await previewMerchantMappings(createEnv({ TAVILY_API_KEY: 'tvly-key' }));

		expect(result.person_skipped_count).toBe(0);
		expect(result.web_search_count).toBe(1);
	});

	it('searches everything when the triage call itself fails', async () => {
		const sql = createSqlMock((query) => {
			const categories = expenseCategoriesHandler(query);
			if (categories) return categories;
			if (query.includes('from public.financial_events as fe')) {
				return [{ search_key: 'AJAYKUMAR', counterparty_raw: 'AJAY KUMAR' }];
			}
			return [];
		});
		getSqlClientMock.mockReturnValue(sql);

		const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
			if (String(url).includes('tavily')) return emptyTavilyResponse();
			const body = typeof init?.body === 'string' ? init.body : '';
			if (body.includes('assessments')) return new Response('nope', { status: 500 });
			return modelResponse([]);
		});
		vi.stubGlobal('fetch', fetchMock);

		const result = await previewMerchantMappings(createEnv({ TAVILY_API_KEY: 'tvly-key' }));

		// Fail open: an optimisation must never cost the run a merchant.
		expect(result.person_skipped_count).toBe(0);
		expect(result.web_search_count).toBe(1);
		expect(result.skipped_reason).not.toBe('invalid model response');
	});

	it('skips without an AI key and never searches', async () => {
		const fetchMock = vi.fn();
		vi.stubGlobal('fetch', fetchMock);

		const result = await previewMerchantMappings(
			createEnv({ OPENCODE_GO_API_KEY: undefined, TAVILY_API_KEY: 'tav-key' }),
		);

		expect(result.skipped_reason).toContain('OPENCODE_GO_API_KEY');
		expect(result.candidate_count).toBe(0);
		expect(fetchMock).not.toHaveBeenCalled();
	});
});
