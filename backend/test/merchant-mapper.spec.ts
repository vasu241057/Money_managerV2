import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import { runMerchantMappingJob } from '../src/workers/merchant-mapper';

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

const JOB = {
	job_type: 'MERCHANT_MAPPING',
	triggered_at: '2026-07-07T21:30:00.000Z',
} as const;

const MERCHANT_ID = '00000000-0000-4000-8000-0000000000fe';

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

	it('maps unmatched counterparties, creates merchant + alias, and relinks transactions', async () => {
		const insertedMerchants: Array<{ name: string; type: string }> = [];
		const insertedAliases: Array<{ key: string; merchantId: string }> = [];
		let relinkKeys: unknown = null;

		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.financial_events as fe') && query.includes('group by fe.search_key')) {
				return [
					{
						search_key: 'FSNECOMMERCEVENTURES',
						counterparty_raw: 'FSN E-COMMERCE VENTURES LTD',
					},
				];
			}
			if (query.includes('insert into public.global_merchants')) {
				insertedMerchants.push({ name: values[0] as string, type: values[1] as string });
				return [{ id: MERCHANT_ID }];
			}
			if (query.includes('insert into public.global_merchant_aliases')) {
				insertedAliases.push({ key: values[0] as string, merchantId: values[1] as string });
				return [{ search_key: values[0] }];
			}
			if (query.includes('update public.transactions as t')) {
				relinkKeys = values[0];
				return [{ id: '00000000-0000-4000-8000-00000000000t' }];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				modelResponse([
					{ key: 'FSNECOMMERCEVENTURES', canonical_name: 'Nykaa', type: 'MERCHANT' },
					// Injection attempt: unknown key, must be dropped.
					{ key: 'EVILKEY', canonical_name: 'Evil Corp', type: 'MERCHANT' },
				]),
			),
		);

		const result = await runMerchantMappingJob(JOB, createEnv());

		expect(result.candidate_count).toBe(1);
		expect(result.mapped_count).toBe(1);
		expect(result.alias_created_count).toBe(1);
		expect(result.relinked_transaction_count).toBe(1);

		expect(insertedMerchants).toEqual([{ name: 'Nykaa', type: 'MERCHANT' }]);
		expect(insertedAliases).toEqual([
			{ key: 'FSNECOMMERCEVENTURES', merchantId: MERCHANT_ID },
		]);
		expect(relinkKeys).toEqual(['FSNECOMMERCEVENTURES']);
	});

	it('returns skipped_reason without writes when the model response is malformed', async () => {
		const sql = createSqlMock((query) => {
			if (query.includes('from public.financial_events as fe')) {
				return [{ search_key: 'SOMEKEY', counterparty_raw: 'Some Merchant' }];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response(
					JSON.stringify({ choices: [{ message: { content: 'not json at all {' } }] }),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				),
			),
		);

		const result = await runMerchantMappingJob(JOB, createEnv());

		expect(result.skipped_reason).toBe('invalid model response');
		expect(result.alias_created_count).toBe(0);
	});
});
