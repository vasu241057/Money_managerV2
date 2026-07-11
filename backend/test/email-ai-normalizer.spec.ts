import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import { induceExtractTemplate } from '../src/lib/email-templates';
import { maskNumericTokens } from '../src/lib/masking';
import { runNormalizeRawEmailsJob } from '../src/workers/email-ai-normalizer';

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

function createSqlMock(handler: QueryHandler): SqlMock {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		return handler(query, values);
	};
	const begin = async (cb: (tx: SqlClient) => Promise<unknown>): Promise<unknown> =>
		cb(tag as unknown as SqlClient);
	return Object.assign(tag, { begin }) as unknown as SqlMock;
}

const appConfig = {
	appName: 'money-manager-backend',
	appVersion: '0.1.0',
	nodeEnv: 'test',
	supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
	dbMaxConnections: 5,
	dbConnectTimeoutSeconds: 5,
} as AppConfig;

const USER_ID = '00000000-0000-4000-8000-000000000001';
const RAW_EMAIL_ID = '00000000-0000-4000-8000-0000000000e1';
const RULE_MERCHANT_ID = '00000000-0000-4000-8000-0000000000fe';
const RULE_CATEGORY_ID = '00000000-0000-4000-8000-0000000000fc';
const EXPENSE_CATEGORY_ID = '00000000-0000-4000-8000-0000000000fd';
const FE_ID = '00000000-0000-4000-8000-0000000000aa';
const TXN_ID = '00000000-0000-4000-8000-0000000000ab';

// toSearchKey('ZEPTO MARKETPLACE PRIVATE LIMITED')
const EXPECTED_SEARCH_KEY = 'ZEPTOMARKETPLACEPRIVATELIMITED';

function pendingRawEmailRow(): Record<string, unknown> {
	return {
		id: RAW_EMAIL_ID,
		user_id: USER_ID,
		internal_date: '2026-07-01T10:00:00.000Z',
		clean_text: 'Rs.167.00 debited to ZEPTO MARKETPLACE PRIVATE LIMITED',
		status: 'PENDING_EXTRACTION',
	};
}

function aiModelResponse(): Response {
	return new Response(
		JSON.stringify({
			choices: [
				{
					message: {
						content: JSON.stringify({
							transactions: [
								{
									direction: 'debit',
									amount_in_paise: 16700,
									txn_timestamp: '2026-07-01T10:00:00.000Z',
									payment_method: 'upi',
									counterparty_raw: 'ZEPTO MARKETPLACE PRIVATE LIMITED',
									instrument_last4: '5960',
									category_guess: null,
									category_id: null,
									sub_category_id: null,
									// Below the 0.85 auto-verify threshold on purpose:
									// VERIFIED must come from the user rule, not confidence.
									confidence: 0.5,
								},
							],
						}),
					},
				},
			],
		}),
		{ status: 200, headers: { 'content-type': 'application/json' } },
	);
}

describe('email-ai-normalizer identity graph integration', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(appConfig);
	});

	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('applies user_merchant_rules with canonical search keys: merchant, category, USER source, VERIFIED', async () => {
		let financialEventValues: unknown[] = [];
		let transactionValues: unknown[] = [];

		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('for update')) {
				return [pendingRawEmailRow()];
			}
			if (query.includes('from public.raw_emails as re')) {
				return [pendingRawEmailRow()];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = false')) {
				return [];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = true')) {
				return [{ id: EXPENSE_CATEGORY_ID, type: 'expense' }];
			}
			if (query.includes('from public.accounts as a')) {
				return [];
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [
					{
						user_id: USER_ID,
						search_key: EXPECTED_SEARCH_KEY,
						merchant_id: RULE_MERCHANT_ID,
						custom_category_id: RULE_CATEGORY_ID,
					},
				];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('fe.amount_in_paise =')) {
				return [];
			}
			if (query.includes('insert into public.financial_events')) {
				financialEventValues = values;
				return [{ id: FE_ID }];
			}
			if (query.includes('insert into public.transactions')) {
				transactionValues = values;
				return [{ id: TXN_ID, status: values[8] }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);
		vi.stubGlobal('fetch', vi.fn().mockResolvedValue(aiModelResponse()));

		const result = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [RAW_EMAIL_ID] },
			{
				OPENCODE_GO_API_KEY: 'test-key',
				AI_NORMALIZER_MASK_NUMBERS: 'false',
			} as unknown as Env,
		);

		expect(result.processed_raw_email_count).toBe(1);
		expect(result.created_transaction_count).toBe(1);

		// financial_events.search_key uses the canonical (deterministic-engine)
		// format, so review rules and aliases match across engines.
		expect(financialEventValues[9]).toBe(EXPECTED_SEARCH_KEY);

		// transactions insert: category/merchant from the user rule, USER
		// source, VERIFIED despite confidence 0.5.
		expect(transactionValues[3]).toBe(RULE_CATEGORY_ID);
		expect(transactionValues[4]).toBe(RULE_MERCHANT_ID);
		expect(transactionValues[8]).toBe('VERIFIED');
		expect(transactionValues[9]).toBe('USER');

		// VERIFIED via user rule → no review needed.
		expect(result.needs_review_transaction_count).toBe(0);
	});

	it('masked mode: no digits reach the model; amount/date/last4 resolved locally; alias hit auto-verifies', async () => {
		let financialEventValues: unknown[] = [];
		let transactionValues: unknown[] = [];

		const maskedEmailRow = {
			id: RAW_EMAIL_ID,
			user_id: USER_ID,
			internal_date: '2026-07-01T10:00:00.000Z',
			clean_text:
				'Rs.167.00 has been debited from account 5960 to ZEPTO MARKETPLACE PRIVATE LIMITED on 03-03-26. Avl Bal Rs.45,230.10',
			status: 'PENDING_EXTRACTION',
		};

		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re')) {
				return [maskedEmailRow];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = false')) {
				return [];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = true')) {
				return [{ id: EXPENSE_CATEGORY_ID, type: 'expense' }];
			}
			if (query.includes('from public.accounts as a')) {
				return [];
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [
					{
						search_key: EXPECTED_SEARCH_KEY,
						merchant_id: RULE_MERCHANT_ID,
						default_category_id: null,
						merchant_type: 'MERCHANT',
					},
				];
			}
			if (query.includes('fe.amount_in_paise =')) {
				return [];
			}
			if (query.includes('insert into public.financial_events')) {
				financialEventValues = values;
				return [{ id: FE_ID }];
			}
			if (query.includes('insert into public.transactions')) {
				transactionValues = values;
				return [{ id: TXN_ID, status: values[8] }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		// Masking order in the fixture text:
		// N1=167.00, N2=5960, N3=03, N4=03, N5=26, N6=45,230.10
		const fetchMock = vi.fn().mockResolvedValue(
			new Response(
				JSON.stringify({
					choices: [
						{
							message: {
								content: JSON.stringify({
									transactions: [
										{
											direction: 'debit',
											amount_token: 'N1',
											date_text: '<N3>-<N4>-<N5>',
											payment_method: 'upi',
											counterparty_raw: 'ZEPTO MARKETPLACE PRIVATE LIMITED',
											instrument_token: 'N2',
											category_id: null,
											sub_category_id: null,
											confidence: 0.6,
										},
									],
								}),
							},
						},
					],
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } },
			),
		);
		vi.stubGlobal('fetch', fetchMock);

		const result = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [RAW_EMAIL_ID] },
			{ OPENCODE_GO_API_KEY: 'test-key' } as unknown as Env,
		);

		expect(result.processed_raw_email_count).toBe(1);

		// The user message sent to the provider must contain no digits outside
		// the <N#> placeholder ids themselves.
		const requestBody = JSON.parse(fetchMock.mock.calls[0]?.[1]?.body as string) as {
			messages: Array<{ role: string; content: string }>;
		};
		const userMessage = requestBody.messages.find(m => m.role === 'user')?.content ?? '';
		expect(userMessage).toContain('<N1>');
		expect(userMessage.replace(/<N\d+>/g, '')).not.toMatch(/\d/);

		// Amount, instrument, and date resolved locally from the token map.
		expect(financialEventValues[4]).toBe(16700);
		expect(financialEventValues[7]).toBe('5960');
		// 03-03-26 midnight IST → 2026-03-02T18:30:00.000Z
		expect(financialEventValues[5]).toBe('2026-03-02T18:30:00.000Z');
		expect(financialEventValues[9]).toBe(EXPECTED_SEARCH_KEY);

		// D3: global-alias hit + resolved category → VERIFIED without any
		// confidence involvement (0.6 would previously stay in review).
		expect(transactionValues[4]).toBe(RULE_MERCHANT_ID);
		expect(transactionValues[8]).toBe('VERIFIED');
	});

	it('downgrades to NEEDS_REVIEW when another raw email already produced the same event', async () => {
		let transactionValues: unknown[] = [];

		const sql = createSqlMock((query, values) => {
			// Cross-email dedup probe finds a matching event from another email.
			if (query.includes('fe.amount_in_paise =')) {
				return [{ id: '00000000-0000-4000-8000-0000000000dd' }];
			}
			if (query.includes('from public.raw_emails as re')) {
				return [pendingRawEmailRow()];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = false')) {
				return [];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = true')) {
				return [{ id: EXPENSE_CATEGORY_ID, type: 'expense' }];
			}
			if (query.includes('from public.accounts as a')) {
				return [];
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [
					{
						user_id: USER_ID,
						search_key: EXPECTED_SEARCH_KEY,
						merchant_id: RULE_MERCHANT_ID,
						custom_category_id: RULE_CATEGORY_ID,
					},
				];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: FE_ID }];
			}
			if (query.includes('insert into public.transactions')) {
				transactionValues = values;
				return [{ id: TXN_ID, status: values[8] }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);
		vi.stubGlobal('fetch', vi.fn().mockResolvedValue(aiModelResponse()));

		const result = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [RAW_EMAIL_ID] },
			{
				OPENCODE_GO_API_KEY: 'test-key',
				AI_NORMALIZER_MASK_NUMBERS: 'false',
			} as unknown as Env,
		);

		expect(result.processed_raw_email_count).toBe(1);
		// The user rule would auto-verify, but the duplicate probe wins.
		expect(transactionValues[8]).toBe('NEEDS_REVIEW');
		expect(result.needs_review_transaction_count).toBe(1);
	});

	it('batches same-user emails into one model call with globally unique placeholders', async () => {
		const RAW_EMAIL_ID_2 = '00000000-0000-4000-8000-0000000000e2';
		const emailRows = [
			{
				id: RAW_EMAIL_ID,
				user_id: USER_ID,
				internal_date: '2026-07-01T10:00:00.000Z',
				clean_text: 'Rs.167.00 debited to ZEPTO MARKETPLACE on 03-03-26',
				status: 'PENDING_EXTRACTION',
			},
			{
				id: RAW_EMAIL_ID_2,
				user_id: USER_ID,
				internal_date: '2026-07-01T11:00:00.000Z',
				clean_text: 'Rs.550.00 debited to SWIGGY on 03-03-26',
				status: 'PENDING_EXTRACTION',
			},
		];

		const insertedAmounts: number[] = [];
		const failedIds: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re') && query.includes('where re.id = any')) {
				return emailRows;
			}
			if (query.includes('from public.raw_emails as re') && query.includes('for update')) {
				const id = values[0] as string;
				return emailRows.filter(row => row.id === id);
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = false')) {
				return [];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = true')) {
				return [{ id: EXPENSE_CATEGORY_ID, type: 'expense' }];
			}
			if (query.includes('from public.accounts as a')) {
				return [];
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('fe.amount_in_paise =')) {
				return [];
			}
			if (query.includes('insert into public.financial_events')) {
				insertedAmounts.push(Number(values[4]));
				return [{ id: crypto.randomUUID() }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: crypto.randomUUID(), status: values[8] }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			if (query.includes("set status = 'FAILED'")) {
				failedIds.push(values[0] as string);
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		// Email 1 masks to N1 (167.00), N2 (03), N3 (03), N4 (26);
		// email 2 continues with N5 (550.00), N6..N8 — globally unique.
		// Email 3's entry is deliberately absent to exercise the missing-entry
		// path... only 2 emails here; respond for email 1 only so email 2
		// exercises the missing-entry failure.
		const fetchMock = vi.fn().mockResolvedValue(
			new Response(
				JSON.stringify({
					choices: [
						{
							message: {
								content: JSON.stringify({
									emails: [
										{
											email: 1,
											transactions: [
												{
													direction: 'debit',
													amount_token: 'N1',
													date_text: '<N2>-<N3>-<N4>',
													payment_method: 'upi',
													counterparty_raw: 'ZEPTO MARKETPLACE',
													instrument_token: null,
													category_id: null,
													sub_category_id: null,
													confidence: 0.9,
												},
											],
										},
									],
								}),
							},
						},
					],
				}),
				{ status: 200, headers: { 'content-type': 'application/json' } },
			),
		);
		vi.stubGlobal('fetch', fetchMock);

		const result = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [RAW_EMAIL_ID, RAW_EMAIL_ID_2] },
			{ OPENCODE_GO_API_KEY: 'test-key' } as unknown as Env,
		);

		// One model call for both emails.
		expect(fetchMock).toHaveBeenCalledTimes(1);

		const requestBody = JSON.parse(fetchMock.mock.calls[0]?.[1]?.body as string) as {
			messages: Array<{ role: string; content: string }>;
		};
		const userMessage = requestBody.messages.find(m => m.role === 'user')?.content ?? '';
		expect(userMessage).toContain('=== EMAIL 1 ===');
		expect(userMessage).toContain('=== EMAIL 2 ===');
		// No digits outside placeholder ids and the email section headers.
		expect(
			userMessage.replace(/<N\d+>/g, '').replace(/=== EMAIL \d+ ===/g, ''),
		).not.toMatch(/\d/);
		// Placeholder ids never restart across emails.
		const ids = [...userMessage.matchAll(/<N(\d+)>/g)].map(m => Number(m[1]));
		expect(new Set(ids).size).toBe(ids.length);

		// Email 1 processed with locally resolved amount; email 2 (missing
		// from the response) is failed visibly, not silently ignored.
		expect(result.processed_raw_email_count).toBe(1);
		expect(insertedAmounts).toEqual([16700]);
		expect(result.failed_raw_email_count).toBe(1);
		expect(result.failed_raw_email_ids).toEqual([RAW_EMAIL_ID_2]);
		expect(failedIds).toEqual([RAW_EMAIL_ID_2]);
	});

	it('falls back to system category and AI source when no rules match', async () => {
		let transactionValues: unknown[] = [];

		const sql = createSqlMock((query, values) => {
			if (query.includes('from public.raw_emails as re')) {
				return [pendingRawEmailRow()];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = false')) {
				return [];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = true')) {
				return [{ id: EXPENSE_CATEGORY_ID, type: 'expense' }];
			}
			if (query.includes('from public.accounts as a')) {
				return [];
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: FE_ID }];
			}
			if (query.includes('insert into public.transactions')) {
				transactionValues = values;
				return [{ id: TXN_ID, status: values[8] }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);
		vi.stubGlobal('fetch', vi.fn().mockResolvedValue(aiModelResponse()));

		const result = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [RAW_EMAIL_ID] },
			{
				OPENCODE_GO_API_KEY: 'test-key',
				AI_NORMALIZER_MASK_NUMBERS: 'false',
			} as unknown as Env,
		);

		expect(result.processed_raw_email_count).toBe(1);
		expect(transactionValues[3]).toBe(EXPENSE_CATEGORY_ID);
		expect(transactionValues[4]).toBeNull();
		// Confidence 0.5 < 0.85 with no user rule → stays in review.
		expect(transactionValues[8]).toBe('NEEDS_REVIEW');
		expect(transactionValues[9]).toBe('AI');
		expect(result.needs_review_transaction_count).toBe(1);
	});

	// Masking order for the template fixtures: N1=amount, N2=account last4,
	// N3/N4/N5=date parts, N6=UPI ref, N7=helpline.
	const TEMPLATE_SOURCE_TEXT =
		'Dear Customer, Rs.123.45 has been debited from your account **9876 on 05-07-26 to VPA zepto@ybl. UPI Ref No 5544332211. Not you? Call 18002586161.';
	const TEMPLATE_VARIANT_TEXT =
		'Dear Customer, Rs.999.00 has been debited from your account **9876 on 06-07-26 to VPA swiggy@icici. UPI Ref No 9988776655. Not you? Call 18002586161.';
	const TEMPLATE_ID = '00000000-0000-4000-8000-0000000000c1';
	const HDFC_FROM = 'alerts@hdfcbank.net';

	function templateSourceEmailRow(text: string): Record<string, unknown> {
		return {
			id: RAW_EMAIL_ID,
			user_id: USER_ID,
			internal_date: '2026-07-01T10:00:00.000Z',
			clean_text: text,
			from_address: HDFC_FROM,
			status: 'PENDING_EXTRACTION',
		};
	}

	it('learns a self-verified template from a masked single-transaction answer', async () => {
		let templateInsertValues: unknown[] = [];

		const sql = createSqlMock((query, values) => {
			if (query.includes('insert into public.email_templates')) {
				templateInsertValues = values;
				return [{ id: TEMPLATE_ID }];
			}
			if (query.includes('delete from public.email_templates')) {
				return [];
			}
			if (query.includes('from public.email_templates')) {
				return [];
			}
			if (query.includes('from public.raw_emails as re')) {
				return [templateSourceEmailRow(TEMPLATE_SOURCE_TEXT)];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = false')) {
				return [];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = true')) {
				return [{ id: EXPENSE_CATEGORY_ID, type: 'expense' }];
			}
			if (query.includes('from public.accounts as a')) {
				return [];
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('fe.amount_in_paise =')) {
				return [];
			}
			if (query.includes('insert into public.financial_events')) {
				return [{ id: FE_ID }];
			}
			if (query.includes('insert into public.transactions')) {
				return [{ id: TXN_ID, status: values[8] }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				new Response(
					JSON.stringify({
						choices: [
							{
								message: {
									content: JSON.stringify({
										transactions: [
											{
												direction: 'debit',
												amount_token: 'N1',
												date_text: '<N3>-<N4>-<N5>',
												payment_method: 'upi',
												counterparty_raw: 'zepto@ybl',
												instrument_token: 'N2',
												category_id: null,
												sub_category_id: null,
												confidence: 0.9,
											},
										],
									}),
								},
							},
						],
					}),
					{ status: 200, headers: { 'content-type': 'application/json' } },
				),
			),
		);

		const result = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [RAW_EMAIL_ID] },
			{ OPENCODE_GO_API_KEY: 'test-key' } as unknown as Env,
		);

		expect(result.processed_raw_email_count).toBe(1);
		expect(result.template_learned_count).toBe(1);

		expect(templateInsertValues[0]).toBe(HDFC_FROM);
		const skeleton = templateInsertValues[2] as string;
		expect(skeleton).toContain('has been debited from your account');
		expect(skeleton).toContain('{{M}}');
		expect(skeleton).not.toContain('zepto');

		const slotMap = JSON.parse(templateInsertValues[3] as string) as Record<string, unknown>;
		expect(slotMap.kind).toBe('EXTRACT');
		expect(slotMap.amount_ordinal).toBe(1);
		expect(slotMap.instrument_ordinal).toBe(2);
		expect(slotMap.date_pattern).toBe('{{3}}-{{4}}-{{5}}');
	});

	it('resolves a template hit locally without calling the model', async () => {
		const masked = maskNumericTokens(TEMPLATE_SOURCE_TEXT);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N1',
			date_text: '<N3>-<N4>-<N5>',
			instrument_token: 'N2',
			counterparty_raw_masked: 'zepto@ybl',
			direction: 'debit',
			payment_method: 'upi',
			confidence: 0.9,
		});
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		let financialEventValues: unknown[] = [];
		let transactionValues: unknown[] = [];
		let hitTemplateId: unknown = null;

		const sql = createSqlMock((query, values) => {
			if (query.includes('set hit_count = et.hit_count + 1')) {
				hitTemplateId = values[0];
				return [];
			}
			if (query.includes('from public.email_templates')) {
				return [
					{
						id: TEMPLATE_ID,
						from_address: HDFC_FROM,
						skeleton: induced.skeleton,
						slot_map: induced.slot_map,
						hit_count: 3,
						failure_count: 0,
					},
				];
			}
			if (query.includes('from public.raw_emails as re')) {
				return [templateSourceEmailRow(TEMPLATE_VARIANT_TEXT)];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = false')) {
				return [];
			}
			if (query.includes('from public.categories as c') && query.includes('c.is_system = true')) {
				return [{ id: EXPENSE_CATEGORY_ID, type: 'expense' }];
			}
			if (query.includes('from public.accounts as a')) {
				return [];
			}
			if (query.includes('from public.user_merchant_rules as umr')) {
				return [];
			}
			if (query.includes('from public.global_merchant_aliases as gma')) {
				return [];
			}
			if (query.includes('fe.amount_in_paise =')) {
				return [];
			}
			if (query.includes('insert into public.financial_events')) {
				financialEventValues = values;
				return [{ id: FE_ID }];
			}
			if (query.includes('insert into public.transactions')) {
				transactionValues = values;
				return [{ id: TXN_ID, status: values[8] }];
			}
			if (query.includes("set status = 'PROCESSED'")) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		const fetchMock = vi.fn().mockRejectedValue(new Error('model must not be called'));
		vi.stubGlobal('fetch', fetchMock);

		const result = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [RAW_EMAIL_ID] },
			{ OPENCODE_GO_API_KEY: 'test-key' } as unknown as Env,
		);

		expect(fetchMock).not.toHaveBeenCalled();
		expect(result.template_matched_count).toBe(1);
		expect(result.template_learned_count).toBe(0);
		expect(result.processed_raw_email_count).toBe(1);
		expect(hitTemplateId).toBe(TEMPLATE_ID);

		// Amount/merchant/date/last4 come from the variant email's own tokens.
		expect(financialEventValues[4]).toBe(99_900);
		expect(financialEventValues[7]).toBe('9876');
		expect(financialEventValues[8]).toBe('swiggy@icici');
		// 06-07-26 (DD-MM-YY) midnight IST.
		expect(financialEventValues[5]).toBe('2026-07-05T18:30:00.000Z');

		expect(transactionValues[8]).toBe('NEEDS_REVIEW');
		expect(transactionValues[9]).toBe('AI');
		expect(transactionValues[10]).toBe(0.9);
	});
});
