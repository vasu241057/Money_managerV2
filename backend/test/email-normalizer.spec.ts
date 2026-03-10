import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import { runNormalizeRawEmailsJob } from '../src/workers/email-normalizer';

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

interface SqlMock extends SqlClient {
	begin: (cb: (tx: SqlClient) => Promise<unknown>) => Promise<unknown>;
}

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlMock(handler: QueryHandler): SqlMock {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		return handler(query, values);
	};

	const begin = async (cb: (tx: SqlClient) => Promise<unknown>): Promise<unknown> =>
		cb(tag as unknown as SqlClient);

	return Object.assign(tag, { begin }) as unknown as SqlMock;
}

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

function pendingRawEmailRow(id: string, cleanText: string): Record<string, unknown> {
	return {
		id,
		user_id: '00000000-0000-4000-8000-000000000001',
		internal_date: '2026-03-10T10:00:00.000Z',
		clean_text: cleanText,
		status: 'PENDING_EXTRACTION',
	};
}

const SYSTEM_CATEGORIES = [
	{ id: '00000000-0000-4000-8000-000000000101', type: 'income' },
	{ id: '00000000-0000-4000-8000-000000000102', type: 'expense' },
	{ id: '00000000-0000-4000-8000-000000000103', type: 'transfer' },
];

const TEST_APP_CONFIG: AppConfig = {
	appName: 'money-manager-backend',
	appVersion: '0.1.0',
	nodeEnv: 'test',
	supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
	dbMaxConnections: 5,
	dbConnectTimeoutSeconds: 5,
};

describe('email-normalizer (milestone 10 pipeline)', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(TEST_APP_CONFIG);
	});

	it('respects kill switch before any DB work', async () => {
		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000aa'],
			},
			createEnv({ NORMALIZATION_KILL_SWITCH: 'true' } as unknown as Partial<Env>),
		);

		expect(result.kill_switch_enabled).toBe(true);
		expect(result.skipped_raw_email_count).toBe(1);
		expect(getAppConfigMock).not.toHaveBeenCalled();
		expect(getSqlClientMock).not.toHaveBeenCalled();
	});

	it('processes debit emails through extractor -> canonicalization -> identity graph -> persistence', async () => {
		const sql = createSqlMock((query) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000aa',
						'Rs 123.45 debited to Amazon via UPI ref AXIS123456',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [
					{
						search_key: 'AMAZON',
						merchant_id: '00000000-0000-4000-8000-0000000000c1',
						default_category_id: '00000000-0000-4000-8000-000000000102',
						merchant_type: 'MERCHANT',
					},
				];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000aa',
						'Rs 123.45 debited to Amazon via UPI ref AXIS123456',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: '00000000-0000-4000-8000-0000000000f1' }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: '00000000-0000-4000-8000-0000000000b1', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const aiSend = vi.fn().mockResolvedValue(undefined);
		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000aa'],
			},
			createEnv({ AI_CLASSIFICATION_QUEUE: { send: aiSend } as unknown as Queue }),
		);

		expect(result.processed_raw_email_count).toBe(1);
		expect(result.extracted_fact_count).toBe(1);
		expect(result.reconciled_fact_count).toBe(1);
		expect(result.persisted_financial_event_count).toBe(1);
		expect(result.created_transaction_count).toBe(1);
		expect(result.needs_review_transaction_count).toBe(0);
		expect(result.ai_enqueued_count).toBe(0);
		expect(aiSend).not.toHaveBeenCalled();
	});

	it('prefers transactional amount when a balance amount is also present', async () => {
		let insertedFinancialEventAmount: unknown = null;
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a1',
						'Avl bal Rs 50,000. Rs 1,200 debited to Zomato via UPI ref ZOMA123456',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a1',
						'Avl bal Rs 50,000. Rs 1,200 debited to Zomato via UPI ref ZOMA123456',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedFinancialEventAmount = values[4];
				return [{ id: '00000000-0000-4000-8000-0000000000f9' }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: '00000000-0000-4000-8000-0000000000b9', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a1'],
			},
			createEnv(),
		);

		expect(result.processed_raw_email_count).toBe(1);
		expect(result.created_transaction_count).toBe(1);
		expect(insertedFinancialEventAmount).toBe(120000);
	});

	it('ignores balance-only lines even when global direction is present elsewhere', async () => {
		const insertedAmounts: number[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a0',
						'Avl bal Rs 50,000\nRs 1,200 debited to Zomato via UPI ref ZOMA123456',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a0',
						'Avl bal Rs 50,000\nRs 1,200 debited to Zomato via UPI ref ZOMA123456',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedAmounts.push(Number(values[4]));
				return [{ id: '00000000-0000-4000-8000-0000000000f0' }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: '00000000-0000-4000-8000-0000000000b0', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a0'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(1);
		expect(result.reconciled_fact_count).toBe(1);
		expect(result.created_transaction_count).toBe(1);
		expect(insertedAmounts).toEqual([120000]);
	});

	it('does not parse non-currency rs substrings (for example, offers/orders) as amounts', async () => {
		const terminalUpdates: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a4',
						'Special offers 500 cashback credited today',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a4',
						'Special offers 500 cashback credited today',
					),
				];
			}
			if (query.includes('update public.raw_emails as re') && !query.includes("'PROCESSED'")) {
				terminalUpdates.push(String(values[0]));
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a4'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(0);
		expect(result.created_transaction_count).toBe(0);
		expect(result.ignored_raw_email_count).toBe(1);
		expect(terminalUpdates).toEqual(['IGNORED']);
	});

	it('ignores promotional cashback emails even when amount and purchase wording are present', async () => {
		const terminalUpdates: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e1',
						'Special offer: Get Rs 500 cashback on next purchase',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e1',
						'Special offer: Get Rs 500 cashback on next purchase',
					),
				];
			}
			if (query.includes('update public.raw_emails as re') && !query.includes("'PROCESSED'")) {
				terminalUpdates.push(String(values[0]));
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000e1'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(0);
		expect(result.created_transaction_count).toBe(0);
		expect(result.ignored_raw_email_count).toBe(1);
		expect(terminalUpdates).toEqual(['IGNORED']);
	});

	it('ignores cashback rows even when debit wording is present', async () => {
		const terminalUpdates: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e5',
						'Cashback Rs 200 debited from rewards bucket',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e5',
						'Cashback Rs 200 debited from rewards bucket',
					),
				];
			}
			if (query.includes('update public.raw_emails as re') && !query.includes("'PROCESSED'")) {
				terminalUpdates.push(String(values[0]));
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000e5'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(0);
		expect(result.created_transaction_count).toBe(0);
		expect(result.ignored_raw_email_count).toBe(1);
		expect(terminalUpdates).toEqual(['IGNORED']);
	});

	it('does not parse alphanumeric-embedded currency tokens (for example, abc500INR)', async () => {
		const terminalUpdates: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a9',
						'promoabc500INR credited and offers500rs cashback',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a9',
						'promoabc500INR credited and offers500rs cashback',
					),
				];
			}
			if (query.includes('update public.raw_emails as re') && !query.includes("'PROCESSED'")) {
				terminalUpdates.push(String(values[0]));
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a9'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(0);
		expect(result.created_transaction_count).toBe(0);
		expect(result.ignored_raw_email_count).toBe(1);
		expect(terminalUpdates).toEqual(['IGNORED']);
	});

	it('parses no-space INR/RS currency formats without false negatives', async () => {
		const insertedAmounts: number[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a7',
						'INR500 debited to Alpha via UPI\n700INR credited from Beta',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a7',
						'INR500 debited to Alpha via UPI\n700INR credited from Beta',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedAmounts.push(Number(values[4]));
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: crypto.randomUUID(), status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a7'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(2);
		expect(result.created_transaction_count).toBe(2);
		expect(insertedAmounts.sort((a, b) => a - b)).toEqual([50000, 70000]);
	});

	it('reconciles reversal pairs deterministically (drop mirrored debit)', async () => {
		const sql = createSqlMock((query) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ab',
						'Rs 500 debited to XYZ UPI ref U1\nRs 500 credited back from XYZ reversal',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ab',
						'Rs 500 debited to XYZ UPI ref U1\nRs 500 credited back from XYZ reversal',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: '00000000-0000-4000-8000-0000000000f2' }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: '00000000-0000-4000-8000-0000000000b2', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000ab'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(2);
		expect(result.reconciled_fact_count).toBe(1);
		expect(result.processed_raw_email_count).toBe(1);
		expect(result.created_transaction_count).toBe(1);
	});

	it('extracts transaction when amount and direction are split across adjacent lines', async () => {
		let insertedAmount: number | null = null;
		let insertedPaymentMethod: string | null = null;
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e6',
						'Amount Rs 1,200\nDebited to Zomato via UPI ref ZOMA123456',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e6',
						'Amount Rs 1,200\nDebited to Zomato via UPI ref ZOMA123456',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedAmount = Number(values[4]);
				insertedPaymentMethod = String(values[6]);
				return [{ id: '00000000-0000-4000-8000-0000000000f6' }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: '00000000-0000-4000-8000-0000000000b6', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000e6'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(1);
		expect(result.created_transaction_count).toBe(1);
		expect(insertedAmount).toBe(120000);
		expect(insertedPaymentMethod).toBe('unknown');
	});

	it('does not dedupe legitimate repeated debit events that share the same attributes', async () => {
		let insertedFinancialEvents = 0;
		let insertedTransactions = 0;
		const sql = createSqlMock((query) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e2',
						'Rs 500 debited to Alpha via UPI ref RPT111111\nRs 500 debited to Alpha via UPI ref RPT111111',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e2',
						'Rs 500 debited to Alpha via UPI ref RPT111111\nRs 500 debited to Alpha via UPI ref RPT111111',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedFinancialEvents += 1;
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedTransactions += 1;
				return [{ id: crypto.randomUUID(), status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000e2'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(2);
		expect(result.reconciled_fact_count).toBe(2);
		expect(result.created_transaction_count).toBe(2);
		expect(insertedFinancialEvents).toBe(2);
		expect(insertedTransactions).toBe(2);
	});

	it('reconciles reversal when one side omits counterparty/instrument but reversal cue is explicit', async () => {
		const sql = createSqlMock((query) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a2',
						'Rs 500 debited for UPI transaction\nRs 500 credited back due to reversal',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a2',
						'Rs 500 debited for UPI transaction\nRs 500 credited back due to reversal',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: '00000000-0000-4000-8000-0000000000fa' }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: '00000000-0000-4000-8000-0000000000ba', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a2'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(2);
		expect(result.reconciled_fact_count).toBe(1);
		expect(result.processed_raw_email_count).toBe(1);
		expect(result.created_transaction_count).toBe(1);
	});

	it('does not drop unrelated debits in multi-event emails during reversal fallback', async () => {
		let insertedFinancialEvents = 0;
		let insertedTransactions = 0;
		const sql = createSqlMock((query) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a5',
						'Rs 500 debited to Alpha via UPI\nRs 700 debited to Beta via UPI\nRs 500 credited back due to reversal',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a5',
						'Rs 500 debited to Alpha via UPI\nRs 700 debited to Beta via UPI\nRs 500 credited back due to reversal',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedFinancialEvents += 1;
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedTransactions += 1;
				return [{ id: crypto.randomUUID(), status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a5'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(3);
		expect(result.reconciled_fact_count).toBe(3);
		expect(result.created_transaction_count).toBe(3);
		expect(insertedFinancialEvents).toBe(3);
		expect(insertedTransactions).toBe(3);
	});

	it('does not pair two-fact same-amount rows when counterparties conflict', async () => {
		let insertedFinancialEvents = 0;
		let insertedTransactions = 0;
		const sql = createSqlMock((query) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a8',
						'Rs 500 debited to Alpha via UPI\nRs 500 credited back from Beta due to reversal',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a8',
						'Rs 500 debited to Alpha via UPI\nRs 500 credited back from Beta due to reversal',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedFinancialEvents += 1;
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedTransactions += 1;
				return [{ id: crypto.randomUUID(), status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a8'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(2);
		expect(result.reconciled_fact_count).toBe(2);
		expect(result.created_transaction_count).toBe(2);
		expect(insertedFinancialEvents).toBe(2);
		expect(insertedTransactions).toBe(2);
	});

	it('does not pair two-fact same-amount rows when only one side has identifiers', async () => {
		let insertedFinancialEvents = 0;
		let insertedTransactions = 0;
		const sql = createSqlMock((query) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ab',
						'Rs 500 debited to Alpha via UPI\nRs 500 credited back due to reversal',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ab',
						'Rs 500 debited to Alpha via UPI\nRs 500 credited back due to reversal',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedFinancialEvents += 1;
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedTransactions += 1;
				return [{ id: crypto.randomUUID(), status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000ab'],
			},
			createEnv(),
		);

		expect(result.extracted_fact_count).toBe(2);
		expect(result.reconciled_fact_count).toBe(2);
		expect(result.created_transaction_count).toBe(2);
		expect(insertedFinancialEvents).toBe(2);
		expect(insertedTransactions).toBe(2);
	});

	it('applies transfer interception per fact and does not leak transfer hint across lines', async () => {
		const insertedTransactionTypes: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e3',
						'Rs 1200 debited for credit card payment to HDFC CREDIT CARD\nRs 700 debited to Grocery Store via UPI ref GRO123456',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e3',
						'Rs 1200 debited for credit card payment to HDFC CREDIT CARD\nRs 700 debited to Grocery Store via UPI ref GRO123456',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedTransactionTypes.push(String(values[5]));
				return [{ id: crypto.randomUUID(), status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000e3'],
			},
			createEnv(),
		);

		expect(result.created_transaction_count).toBe(2);
		expect(insertedTransactionTypes).toEqual(['transfer', 'expense']);
	});

	it('does not leak aggregator flags across unrelated facts in the same email', async () => {
		const insertedStatuses: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e7',
						'Rs 500 debited to Grocery via UPI ref GRO123456\nRs 700 debited to Vendor via Razorpay ref RAZ123456',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e7',
						'Rs 500 debited to Grocery via UPI ref GRO123456\nRs 700 debited to Vendor via Razorpay ref RAZ123456',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedStatuses.push(String(values[7]));
				return [{ id: crypto.randomUUID(), status: String(values[7]) }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000e7'],
			},
			createEnv(),
		);

		expect(result.created_transaction_count).toBe(2);
		expect(insertedStatuses).toEqual(['VERIFIED', 'NEEDS_REVIEW']);
	});

	it('does not bleed global instrument/payment fallbacks across multi-line events', async () => {
		const insertedInstruments: Array<string | null> = [];
		const insertedPaymentMethods: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e4',
						'Rs 100 debited to Alpha via UPI ref REFABC123\nRs 200 debited to Beta',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e4',
						'Rs 100 debited to Alpha via UPI ref REFABC123\nRs 200 debited to Beta',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedPaymentMethods.push(String(values[6]));
				insertedInstruments.push((values[7] as string | null) ?? null);
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: crypto.randomUUID(), status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000e4'],
			},
			createEnv(),
		);

		expect(result.created_transaction_count).toBe(2);
		expect(insertedPaymentMethods).toEqual(['upi', 'unknown']);
		expect(insertedInstruments).toEqual(['REFABC123', null]);
	});

	it('does not leak VPA-derived user rule classification across unrelated facts', async () => {
		const insertedClassificationSources: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e8',
						'Rs 500 debited to Grocery Store via UPI ref GRO123456\nRs 300 debited to abc@okicici via UPI ref ABC123456',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [
					{
						user_id: '00000000-0000-4000-8000-000000000001',
						search_key: 'ABCOKICICI',
						merchant_id: null,
						custom_category_id: '00000000-0000-4000-8000-000000000102',
					},
				];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000e8',
						'Rs 500 debited to Grocery Store via UPI ref GRO123456\nRs 300 debited to abc@okicici via UPI ref ABC123456',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedClassificationSources.push(String(values[8]));
				return [{ id: crypto.randomUUID(), status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000e8'],
			},
			createEnv(),
		);

		expect(result.created_transaction_count).toBe(2);
		expect(insertedClassificationSources).toEqual(['SYSTEM_DEFAULT', 'USER']);
	});

	it('sets classification_source USER when matched by user_merchant_rules', async () => {
		let insertedClassificationSource: unknown = null;
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a3',
						'Rs 220 debited to Swiggy via UPI ref SWI220123',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [
					{
						user_id: '00000000-0000-4000-8000-000000000001',
						search_key: 'SWIGGY',
						merchant_id: null,
						custom_category_id: '00000000-0000-4000-8000-000000000102',
					},
				];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a3',
						'Rs 220 debited to Swiggy via UPI ref SWI220123',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: '00000000-0000-4000-8000-0000000000fb' }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedClassificationSource = values[8];
				return [{ id: '00000000-0000-4000-8000-0000000000bb', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a3'],
			},
			createEnv(),
		);

		expect(insertedClassificationSource).toBe('USER');
	});

	it('keeps classification_source USER when transfer intercept also matches', async () => {
		let insertedClassificationSource: unknown = null;
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a6',
						'Rs 1200 debited for credit card payment to HDFC CREDIT CARD',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [
					{
						user_id: '00000000-0000-4000-8000-000000000001',
						search_key: 'HDFCCREDITCARD',
						merchant_id: null,
						custom_category_id: '00000000-0000-4000-8000-000000000103',
					},
				];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000a6',
						'Rs 1200 debited for credit card payment to HDFC CREDIT CARD',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: '00000000-0000-4000-8000-0000000000fc' }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedClassificationSource = values[8];
				return [{ id: '00000000-0000-4000-8000-0000000000bc', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000a6'],
			},
			createEnv(),
		);

		expect(insertedClassificationSource).toBe('USER');
	});

	it('rejects oversized raw_email_ids when runNormalizeRawEmailsJob is called directly', async () => {
		const oversizedRawEmailIds = Array.from({ length: 251 }, (_, index) =>
			`00000000-0000-4000-8000-${String(index).padStart(12, '0')}`,
		);

		await expect(
			runNormalizeRawEmailsJob(
				{
					job_type: 'NORMALIZE_RAW_EMAILS',
					raw_email_ids: oversizedRawEmailIds,
				},
				createEnv(),
			),
		).rejects.toThrow('raw_email_ids exceeds max');

		expect(getAppConfigMock).not.toHaveBeenCalled();
		expect(getSqlClientMock).not.toHaveBeenCalled();
	});

	it('rejects oversized raw_email_ids even when all ids are duplicates', async () => {
		const duplicatedOversizedRawEmailIds = Array.from(
			{ length: 251 },
			() => '00000000-0000-4000-8000-0000000000ff',
		);

		await expect(
			runNormalizeRawEmailsJob(
				{
					job_type: 'NORMALIZE_RAW_EMAILS',
					raw_email_ids: duplicatedOversizedRawEmailIds,
				},
				createEnv(),
			),
		).rejects.toThrow('raw_email_ids exceeds max');

		expect(getAppConfigMock).not.toHaveBeenCalled();
		expect(getSqlClientMock).not.toHaveBeenCalled();
	});

	it('runs identity graph lookup in batch across multiple pending rows', async () => {
		let userRuleQueries = 0;
		let aliasQueries = 0;
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ac',
						'Rs 100 debited to Alpha via UPI',
					),
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ad',
						'Rs 200 debited to Beta via UPI',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				userRuleQueries += 1;
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				aliasQueries += 1;
				return [];
			}
			if (query.includes('for update')) {
				const id = String(values[0]);
				if (id.includes('0ac')) {
					return [pendingRawEmailRow('00000000-0000-4000-8000-0000000000ac', 'Rs 100 debited to Alpha via UPI')];
				}
				return [pendingRawEmailRow('00000000-0000-4000-8000-0000000000ad', 'Rs 200 debited to Beta via UPI')];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: crypto.randomUUID(), status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: [
					'00000000-0000-4000-8000-0000000000ac',
					'00000000-0000-4000-8000-0000000000ad',
				],
			},
			createEnv(),
		);

		expect(result.processed_raw_email_count).toBe(2);
		expect(userRuleQueries).toBe(1);
		expect(aliasQueries).toBe(1);
	});

	it('applies transfer interceptor and persists transfer transaction type', async () => {
		let insertedTransactionType: unknown = null;
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ae',
						'Rs 1200 debited for credit card payment to HDFC CREDIT CARD',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [
					{
						search_key: 'HDFCCREDITCARD',
						merchant_id: '00000000-0000-4000-8000-0000000000d1',
						default_category_id: '00000000-0000-4000-8000-000000000103',
						merchant_type: 'TRANSFER_INSTITUTION',
					},
				];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ae',
						'Rs 1200 debited for credit card payment to HDFC CREDIT CARD',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: '00000000-0000-4000-8000-0000000000f4' }];
			}
			if (query.includes('insert into public.transactions')) {
				insertedTransactionType = values[5];
				return [{ id: '00000000-0000-4000-8000-0000000000b4', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000ae'],
			},
			createEnv(),
		);

		expect(insertedTransactionType).toBe('transfer');
	});

	it('keeps idempotency on conflict do nothing and still marks raw email PROCESSED', async () => {
		const sql = createSqlMock((query) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000af',
						'Rs 330 debited to Store via UPI ref UAF123456',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000af',
						'Rs 330 debited to Store via UPI ref UAF123456',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [];
			}
			if (query.includes('from public.financial_events as fe where fe.raw_email_id')) {
				return [{ id: '00000000-0000-4000-8000-0000000000f5' }];
			}
			if (query.includes('insert into public.transactions')) {
				return [];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000af'],
			},
			createEnv(),
		);

		expect(result.processed_raw_email_count).toBe(1);
		expect(result.persisted_financial_event_count).toBe(0);
		expect(result.created_transaction_count).toBe(0);
	});

	it('moves no-fact rows to IGNORED/UNRECOGNIZED terminal statuses', async () => {
		const terminalUpdates: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ba',
						'Your OTP is 123456. Do not share.',
					),
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000bb',
						'Meeting reminder for tomorrow 6 PM.',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('for update')) {
				return [
					values[0] === '00000000-0000-4000-8000-0000000000ba'
						? pendingRawEmailRow(
								'00000000-0000-4000-8000-0000000000ba',
								'Your OTP is 123456. Do not share.',
							)
						: pendingRawEmailRow(
								'00000000-0000-4000-8000-0000000000bb',
								'Meeting reminder for tomorrow 6 PM.',
							),
				];
			}
			if (query.includes('update public.raw_emails as re') && !query.includes("'PROCESSED'")) {
				terminalUpdates.push(String(values[0]));
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: [
					'00000000-0000-4000-8000-0000000000ba',
					'00000000-0000-4000-8000-0000000000bb',
				],
			},
			createEnv(),
		);

		expect(result.ignored_raw_email_count).toBe(1);
		expect(result.unrecognized_raw_email_count).toBe(1);
		expect(terminalUpdates.sort()).toEqual(['IGNORED', 'UNRECOGNIZED']);
	});

	it('marks failing rows as FAILED and continues processing remaining rows', async () => {
		let insertCount = 0;
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000ca',
						'Rs 600 debited to Fail Merchant via UPI',
					),
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000cb',
						'Rs 700 debited to Pass Merchant via UPI',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				if (values[0] === '00000000-0000-4000-8000-0000000000ca') {
					return [
						pendingRawEmailRow(
							'00000000-0000-4000-8000-0000000000ca',
							'Rs 600 debited to Fail Merchant via UPI',
						),
					];
				}
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000cb',
						'Rs 700 debited to Pass Merchant via UPI',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				insertCount += 1;
				if (insertCount === 1) {
					throw new Error('simulated insert failure');
				}
				return [{ id: '00000000-0000-4000-8000-0000000000f7' }];
			}
			if (query.includes("set status = 'FAILED'")) {
				return [];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: '00000000-0000-4000-8000-0000000000b7', status: 'VERIFIED' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: [
					'00000000-0000-4000-8000-0000000000ca',
					'00000000-0000-4000-8000-0000000000cb',
				],
			},
			createEnv(),
		);

		expect(result.failed_raw_email_count).toBe(1);
		expect(result.processed_raw_email_count).toBe(1);
		expect(result.created_transaction_count).toBe(1);
	});

	it('enqueues AI_CLASSIFICATION only for newly created NEEDS_REVIEW transactions', async () => {
		const aiSend = vi.fn().mockResolvedValue(undefined);
		const sql = createSqlMock((query) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000da',
						'Rs 450 debited to UnknownShop ref X12345',
					),
				];
			}
			if (query.includes('from public.categories as c')) {
				return SYSTEM_CATEGORIES;
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('for update')) {
				return [
					pendingRawEmailRow(
						'00000000-0000-4000-8000-0000000000da',
						'Rs 450 debited to UnknownShop ref X12345',
					),
				];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: '00000000-0000-4000-8000-0000000000f8' }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: '00000000-0000-4000-8000-0000000000b8', status: 'NEEDS_REVIEW' }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const result = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: ['00000000-0000-4000-8000-0000000000da'],
			},
			createEnv({ AI_CLASSIFICATION_QUEUE: { send: aiSend } as unknown as Queue }),
		);

		expect(result.needs_review_transaction_count).toBe(1);
		expect(result.ai_enqueued_count).toBe(1);
		expect(aiSend).toHaveBeenCalledTimes(1);
		expect(aiSend).toHaveBeenCalledWith(
			expect.objectContaining({
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000b8',
			}),
			{ contentType: 'json' },
		);
	});
});
