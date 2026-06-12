import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import { runAiClassificationJob } from '../src/workers/ai-classifier';

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

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlMock(handler: QueryHandler): SqlClient {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		return handler(query, values);
	};

	return tag as unknown as SqlClient;
}

function createEnv(overrides?: Partial<Env>): Env {
	return {
		APP_NAME: 'money-manager-backend',
		APP_VERSION: '0.1.0',
		NODE_ENV: 'test',
		SUPABASE_POOLER_URL: 'postgres://postgres:postgres@localhost:6543/postgres',
		DB_MAX_CONNECTIONS: '5',
		DB_CONNECT_TIMEOUT_SECONDS: '5',
		OPENROUTER_API_KEY: 'test-openrouter-key',
		...(overrides ?? {}),
	} as unknown as Env;
}

function candidateRow(
	overrides?: Partial<Record<string, unknown>>,
): Record<string, unknown> {
	return {
		transaction_id: '00000000-0000-4000-8000-0000000000a1',
		user_id: '00000000-0000-4000-8000-000000000001',
		transaction_status: 'NEEDS_REVIEW',
		transaction_type: 'expense',
		transaction_amount_in_paise: 45000,
		transaction_txn_date: '2026-03-11T10:00:00.000Z',
		transaction_category_id: '00000000-0000-4000-8000-000000000102',
		transaction_merchant_id: null,
		transaction_user_note: null,
		financial_event_status: 'ACTIVE',
		financial_event_payment_method: 'upi',
		financial_event_counterparty_raw: 'Unknown Shop',
		financial_event_search_key: 'UNKNOWNSHOP',
		raw_email_clean_text: 'Rs 450 debited to Unknown Shop via UPI ref UTR1234',
		...(overrides ?? {}),
	};
}

const TEST_APP_CONFIG: AppConfig = {
	appName: 'money-manager-backend',
	appVersion: '0.1.0',
	nodeEnv: 'test',
	supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
	dbMaxConnections: 5,
	dbConnectTimeoutSeconds: 5,
};

describe('ai-classifier worker', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		vi.unstubAllGlobals();
		getAppConfigMock.mockReturnValue(TEST_APP_CONFIG);
	});

	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('skips when transaction is missing', async () => {
		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv(),
		);

		expect(result.outcome).toBe('SKIPPED_MISSING_TRANSACTION');
		expect(result.transaction_status).toBeNull();
	});

	it('skips when transaction no longer needs review', async () => {
		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow({ transaction_status: 'VERIFIED' })];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv(),
		);

		expect(result.outcome).toBe('SKIPPED_ALREADY_REVIEWED');
		expect(result.transaction_status).toBe('VERIFIED');
	});

	it('skips when AI key is not configured', async () => {
		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow()];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv({ OPENROUTER_API_KEY: ' ' }),
		);

		expect(result.outcome).toBe('SKIPPED_AI_DISABLED');
	});

	it('updates transaction to VERIFIED when AI returns high-confidence VERIFY', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response(
					JSON.stringify({
						choices: [
							{
								message: {
									content:
										'{"action":"VERIFY","confidence_score":0.98,"reason":"clear upi merchant"}',
								},
							},
						],
					}),
					{ status: 200 },
				),
			),
		);

		let updateValues: unknown[] = [];
		getSqlClientMock.mockReturnValue(
			createSqlMock((query, values) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow()];
				}
				if (query.includes('update public.transactions as t')) {
					updateValues = values;
					return [
						{
							transaction_id: '00000000-0000-4000-8000-0000000000a1',
							transaction_status: 'VERIFIED',
						},
					];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv(),
		);

		expect(result.outcome).toBe('UPDATED_VERIFIED');
		expect(result.transaction_status).toBe('VERIFIED');
		expect(result.confidence_score).toBe(0.98);
		expect(result.action).toBe('VERIFY');
		expect(updateValues).toContain(0.98);
		expect(updateValues).toContain(true);
	});

	it('keeps transaction in review when confidence is below auto-verify threshold', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response(
					JSON.stringify({
						choices: [
							{
								message: {
									content:
										'{"action":"VERIFY","confidence_score":0.70,"reason":"possible merchant"}',
								},
							},
						],
					}),
					{ status: 200 },
				),
			),
		);

		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow()];
				}
				if (query.includes('update public.transactions as t')) {
					return [
						{
							transaction_id: '00000000-0000-4000-8000-0000000000a1',
							transaction_status: 'NEEDS_REVIEW',
						},
					];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv(),
		);

		expect(result.outcome).toBe('UPDATED_NEEDS_REVIEW');
		expect(result.transaction_status).toBe('NEEDS_REVIEW');
		expect(result.confidence_score).toBe(0.7);
	});

	it('skips update on invalid AI response payload', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response(
					JSON.stringify({
						choices: [
							{
								message: {
									content: '{"action":"VERIFY"}',
								},
							},
						],
					}),
					{ status: 200 },
				),
			),
		);

		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow()];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv(),
		);

		expect(result.outcome).toBe('SKIPPED_INVALID_AI_RESPONSE');
	});

	it('skips update when model returns unsupported action value', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response(
					JSON.stringify({
						choices: [
							{
								message: {
									content:
										'{"action":"VERFIY","confidence_score":0.95,"reason":"model typo"}',
								},
							},
						],
					}),
					{ status: 200 },
				),
			),
		);

		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow()];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv(),
		);

		expect(result.outcome).toBe('SKIPPED_INVALID_AI_RESPONSE');
		expect(result.reason).toContain('missing required action/confidence');
	});

	it('skips update when model returns out-of-range numeric confidence', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response(
					JSON.stringify({
						choices: [
							{
								message: {
									content:
										'{"action":"VERIFY","confidence_score":95,"reason":"model drift"}',
								},
							},
						],
					}),
					{ status: 200 },
				),
			),
		);

		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow()];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv(),
		);

		expect(result.outcome).toBe('SKIPPED_INVALID_AI_RESPONSE');
		expect(result.reason).toContain('missing required action/confidence');
	});

	it('skips update when model returns out-of-range string confidence', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response(
					JSON.stringify({
						choices: [
							{
								message: {
									content:
										'{"action":"VERIFY","confidence_score":"-0.2","reason":"model drift"}',
								},
							},
						],
					}),
					{ status: 200 },
				),
			),
		);

		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow()];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv(),
		);

		expect(result.outcome).toBe('SKIPPED_INVALID_AI_RESPONSE');
		expect(result.reason).toContain('missing required action/confidence');
	});

	it('skips update when model returns malformed confidence string', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response(
					JSON.stringify({
						choices: [
							{
								message: {
									content:
										'{"action":"VERIFY","confidence_score":"0.95%","reason":"model drift"}',
								},
							},
						],
					}),
					{ status: 200 },
				),
			),
		);

		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow()];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		const result = await runAiClassificationJob(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:01:00.000Z',
			},
			createEnv(),
		);

		expect(result.outcome).toBe('SKIPPED_INVALID_AI_RESPONSE');
		expect(result.reason).toContain('missing required action/confidence');
	});

	it('throws transient error for rate-limited OpenRouter response', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response('rate limited', {
					status: 429,
				}),
			),
		);

		getSqlClientMock.mockReturnValue(
			createSqlMock((query) => {
				if (query.includes('from public.transactions as t')) {
					return [candidateRow()];
				}
				throw new Error(`Unexpected query: ${query}`);
			}),
		);

		await expect(
			runAiClassificationJob(
				{
					job_type: 'AI_CLASSIFICATION',
					transaction_id: '00000000-0000-4000-8000-0000000000a1',
					requested_at: '2026-03-11T10:01:00.000Z',
				},
				createEnv(),
			),
		).rejects.toThrow('OpenRouter transient HTTP 429');
	});
});
