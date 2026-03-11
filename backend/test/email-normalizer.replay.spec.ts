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

interface SqlMock extends SqlClient {
	begin: (cb: (tx: SqlClient) => Promise<unknown>) => Promise<unknown>;
}

interface ReplayCase {
	name: string;
	clean_text: string;
	expected_terminal_status: 'PROCESSED' | 'IGNORED' | 'UNRECOGNIZED';
	expected_amounts_in_paise: number[];
}

const TEST_APP_CONFIG: AppConfig = {
	appName: 'money-manager-backend',
	appVersion: '0.1.0',
	nodeEnv: 'test',
	supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
	dbMaxConnections: 5,
	dbConnectTimeoutSeconds: 5,
};

const SYSTEM_CATEGORIES = [
	{ id: '00000000-0000-4000-8000-000000000101', type: 'income' },
	{ id: '00000000-0000-4000-8000-000000000102', type: 'expense' },
	{ id: '00000000-0000-4000-8000-000000000103', type: 'transfer' },
];

const REPLAY_CASES: ReplayCase[] = [
	{
		name: 'does not parse coupon-like RS500OFF as currency',
		clean_text: 'Card debited with promotion code RS500OFF at merchant',
		expected_terminal_status: 'IGNORED',
		expected_amounts_in_paise: [],
	},
	{
		name: 'suppresses promotional cashback pseudo-transactions',
		clean_text: 'Special offer: Get Rs 500 cashback on next purchase',
		expected_terminal_status: 'IGNORED',
		expected_amounts_in_paise: [],
	},
	{
		name: 'suppresses reward-points credit notifications',
		clean_text: 'Rs 500 credited as reward points for card usage',
		expected_terminal_status: 'IGNORED',
		expected_amounts_in_paise: [],
	},
	{
		name: 'retains genuine debit when cashback wording co-exists',
		clean_text: 'Rs 1200 debited to Zomato via UPI ref ZOMA1200 cashback offer active',
		expected_terminal_status: 'PROCESSED',
		expected_amounts_in_paise: [120000],
	},
	{
		name: 'prefers transaction amount over nearby balance amount',
		clean_text: 'Avl bal Rs 50,000. Rs 1,200 debited to Zomato via UPI ref ZOMA123456',
		expected_terminal_status: 'PROCESSED',
		expected_amounts_in_paise: [120000],
	},
	{
		name: 'drops mirrored debit from reversal pair deterministically',
		clean_text: 'Rs 500 debited to XYZ UPI ref U1\nRs 500 credited back from XYZ reversal',
		expected_terminal_status: 'PROCESSED',
		expected_amounts_in_paise: [50000],
	},
	{
		name: 'suppresses noisy promo+currency-like token blends',
		clean_text: 'promoabc500INR credited and offers500rs cashback',
		expected_terminal_status: 'IGNORED',
		expected_amounts_in_paise: [],
	},
	{
		name: 'does not infer direction from adjacent promotional line',
		clean_text: 'Rs 500\nUse this on your next purchase',
		expected_terminal_status: 'UNRECOGNIZED',
		expected_amounts_in_paise: [],
	},
];

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createEnv(): Env {
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
	} as unknown as Env;
}

function createSqlHarness(params: {
	rawEmailId: string;
	cleanText: string;
	insertedAmounts: number[];
	insertedTransactionStatuses: string[];
	terminalStatusUpdates: string[];
}): SqlMock {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);

		if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
			return [
				{
					id: params.rawEmailId,
					user_id: '00000000-0000-4000-8000-000000000001',
					internal_date: '2026-03-11T10:00:00.000Z',
					clean_text: params.cleanText,
					status: 'PENDING_EXTRACTION',
				},
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
				{
					id: params.rawEmailId,
					user_id: '00000000-0000-4000-8000-000000000001',
					internal_date: '2026-03-11T10:00:00.000Z',
					clean_text: params.cleanText,
					status: 'PENDING_EXTRACTION',
				},
			];
		}

		if (query.includes('insert into public.financial_events')) {
			params.insertedAmounts.push(Number(values[4]));
			return [{ id: crypto.randomUUID() }];
		}

		if (query.includes('insert into public.transactions')) {
			const status = String(values[7]);
			params.insertedTransactionStatuses.push(status);
			return [{ id: crypto.randomUUID(), status }];
		}

		if (query.includes("set status = 'PROCESSED'")) {
			params.terminalStatusUpdates.push('PROCESSED');
			return [];
		}

		if (query.includes('update public.raw_emails as re') && !query.includes("'PROCESSED'")) {
			params.terminalStatusUpdates.push(String(values[0]));
			return [];
		}

		if (query.includes("set status = 'FAILED'")) {
			throw new Error('FAILED status update should not run in replay cases');
		}

		throw new Error(`Unexpected query: ${query}`);
	};

	const begin = async (cb: (tx: SqlClient) => Promise<unknown>): Promise<unknown> =>
		cb(tag as unknown as SqlClient);

	return Object.assign(tag, { begin }) as unknown as SqlMock;
}

describe('email-normalizer regex replay suite', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(TEST_APP_CONFIG);
	});

	for (const replayCase of REPLAY_CASES) {
		it(replayCase.name, async () => {
			const rawEmailId = `00000000-0000-4000-8000-${(2000 + REPLAY_CASES.indexOf(replayCase))
				.toString()
				.padStart(12, '0')}`;
			const insertedAmounts: number[] = [];
			const insertedTransactionStatuses: string[] = [];
			const terminalStatusUpdates: string[] = [];
			const sql = createSqlHarness({
				rawEmailId,
				cleanText: replayCase.clean_text,
				insertedAmounts,
				insertedTransactionStatuses,
				terminalStatusUpdates,
			});
			getSqlClientMock.mockReturnValue(sql);

			const result = await runNormalizeRawEmailsJob(
				{
					job_type: 'NORMALIZE_RAW_EMAILS',
					raw_email_ids: [rawEmailId],
				},
				createEnv(),
			);

			expect(result.failed_raw_email_count).toBe(0);
			expect(result.persisted_financial_event_count).toBe(replayCase.expected_amounts_in_paise.length);
			expect(result.created_transaction_count).toBe(replayCase.expected_amounts_in_paise.length);
			expect(insertedAmounts).toEqual(replayCase.expected_amounts_in_paise);

			switch (replayCase.expected_terminal_status) {
				case 'PROCESSED':
					expect(result.processed_raw_email_count).toBe(1);
					expect(result.ignored_raw_email_count).toBe(0);
					expect(result.unrecognized_raw_email_count).toBe(0);
					expect(terminalStatusUpdates).toContain('PROCESSED');
					expect(insertedTransactionStatuses).toHaveLength(
						replayCase.expected_amounts_in_paise.length,
					);
					expect(
						insertedTransactionStatuses.every(
							(status) => status === 'VERIFIED' || status === 'NEEDS_REVIEW',
						),
					).toBe(true);
					break;
				case 'IGNORED':
					expect(result.processed_raw_email_count).toBe(0);
					expect(result.ignored_raw_email_count).toBe(1);
					expect(result.unrecognized_raw_email_count).toBe(0);
					expect(terminalStatusUpdates).toContain('IGNORED');
					expect(insertedTransactionStatuses).toHaveLength(0);
					break;
				case 'UNRECOGNIZED':
					expect(result.processed_raw_email_count).toBe(0);
					expect(result.ignored_raw_email_count).toBe(0);
					expect(result.unrecognized_raw_email_count).toBe(1);
					expect(terminalStatusUpdates).toContain('UNRECOGNIZED');
					expect(insertedTransactionStatuses).toHaveLength(0);
					break;
			}
		});
	}
});
