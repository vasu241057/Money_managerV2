import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { reviewTransaction, updateTransaction } from '../src/services/transactions.service';

type QueryResult = unknown;
type QueryHandler = (query: string, values: unknown[]) => QueryResult | Promise<QueryResult>;

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlForReviewTests(handlers: QueryHandler[]): {
	sql: SqlClient;
	queries: string[];
	values: unknown[][];
} {
	const queries: string[] = [];
	const capturedValues: unknown[][] = [];
	let index = 0;

	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		queries.push(query);
		capturedValues.push(values);
		const handler = handlers[index];
		index += 1;

		if (!handler) {
			throw new Error(`Unexpected query execution: ${query}`);
		}

		return handler(query, values);
	};

	const sql = tag as unknown as SqlClient;
	(sql as unknown as { begin: (cb: (tx: SqlClient) => Promise<unknown>) => Promise<unknown> }).begin =
		async (callback: (tx: SqlClient) => Promise<unknown>) => callback(sql);

	return { sql, queries, values: capturedValues };
}

function transactionFeedRow(overrides?: Partial<Record<string, unknown>>): Record<string, unknown> {
	return {
		transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
		transaction_user_id: '11111111-1111-4111-8111-111111111111',
		transaction_financial_event_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
		transaction_account_id: null,
		transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
		transaction_merchant_id: null,
		transaction_credit_card_id: null,
		transaction_amount_in_paise: 45000,
		transaction_type: 'expense',
		transaction_txn_date: '2026-03-11T09:00:00.000Z',
		transaction_user_note: null,
		transaction_status: 'VERIFIED',
		transaction_classification_source: 'USER',
		transaction_ai_confidence_score: null,
		transaction_created_at: '2026-03-11T09:00:00.000Z',
		transaction_updated_at: '2026-03-11T09:10:00.000Z',
		financial_event_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
		financial_event_user_id: '11111111-1111-4111-8111-111111111111',
		financial_event_raw_email_id: 'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
		financial_event_extraction_index: 0,
		financial_event_direction: 'debit',
		financial_event_amount_in_paise: 45000,
		financial_event_currency: 'INR',
		financial_event_txn_timestamp: '2026-03-11T09:00:00.000Z',
		financial_event_payment_method: 'upi',
		financial_event_instrument_id: null,
		financial_event_counterparty_raw: 'Unknown Shop',
		financial_event_search_key: 'UNKNOWNSHOP',
		financial_event_status: 'ACTIVE',
		financial_event_created_at: '2026-03-11T09:00:00.000Z',
		account_id: null,
		account_name: null,
		account_type: null,
		credit_card_id: null,
		credit_card_card_label: null,
		credit_card_first4: null,
		credit_card_last4: null,
		category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
		category_name: 'Food',
		category_type: 'expense',
		category_icon: 'Utensils',
		merchant_id: null,
		merchant_canonical_name: null,
		merchant_type: null,
		merchant_logo_public_url: null,
		merchant_logo_brand_color: null,
		raw_email_id: 'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
		raw_email_source_id: 'gmail-msg-1',
		raw_email_internal_date: '2026-03-11T09:00:00.000Z',
		raw_email_status: 'PROCESSED',
		...(overrides ?? {}),
	};
}

describe('reviewTransaction', () => {
	it('verifies NEEDS_REVIEW transaction and returns updated feed item', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'NEEDS_REVIEW',
					transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
					transaction_merchant_id: null,
					transaction_type: 'expense',
					financial_event_direction: 'debit',
					financial_event_search_key: 'unknown-shop',
				},
			],
			() => [],
			() => [transactionFeedRow()],
			// probation lookup: no alias for this key
			() => [],
		]);

		const result = await reviewTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{},
		);

		expect(result.rule_applied).toBe(false);
		expect(result.applied_search_key).toBeNull();
		expect(result.transaction.transaction.status).toBe('VERIFIED');
		expect(result.transaction.transaction.classification_source).toBe('USER');
		expect(queries).toHaveLength(4);
	});

	it('applies user merchant rule when requested', async () => {
		let ruleInsertValues: unknown[] = [];
		const { sql, queries } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'NEEDS_REVIEW',
					transaction_category_id: null,
					transaction_merchant_id: null,
					transaction_type: 'expense',
					financial_event_direction: 'debit',
					financial_event_search_key: 'zomato-pay',
				},
			],
			() => [{ type: 'expense' }],
			() => [],
			() => [
				transactionFeedRow({
					transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
					category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
				}),
			],
			// probation lookup: no alias for this key
			() => [],
			(_query, values) => {
				ruleInsertValues = values;
				return [{ id: 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee' }];
			},
			(query, values) => {
				expect(query).toContain('insert into public.user_merchant_rule_categories');
				expect(values).toContain('expense');
				return [];
			},
			// Backfill carries the answer over rows sharing this descriptor.
			() => [{ id: 'f1' }, { id: 'f2' }],
		]);

		const result = await reviewTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{
				category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
				apply_rule: true,
			},
		);

		expect(result.rule_applied).toBe(true);
		expect(result.applied_search_key).toBe('ZOMATOPAY');
		expect(ruleInsertValues).toContain('ZOMATOPAY');
		expect(ruleInsertValues).toContain('cccccccc-cccc-4ccc-8ccc-cccccccccccc');
		expect(queries[5]).toContain('insert into public.user_merchant_rules');
		expect(queries[6]).toContain('insert into public.user_merchant_rule_categories');
		expect(result.backfilled_count).toBe(2);
	});

	it('scopes the review backfill to the descriptor, never the whole merchant', async () => {
		const { sql, queries, values } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'NEEDS_REVIEW',
					transaction_category_id: null,
					transaction_merchant_id: 'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
					transaction_type: 'expense',
					financial_event_direction: 'debit',
					financial_event_search_key: 'AMAZONPAYINGROCERY',
				},
			],
			() => [{ type: 'expense' }],
			() => [],
			() => [
				transactionFeedRow({
					transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
					category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
				}),
			],
			() => [],
			() => [{ id: 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee' }],
			() => [],
			() => [{ id: 'f1' }],
		]);

		await reviewTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{
				category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
				apply_rule: true,
			},
		);

		const backfill = queries[7] ?? '';
		expect(backfill).toContain('update public.transactions');
		expect(backfill).toContain('fe.search_key =');
		// The whole point: one raw counterparty, not every descriptor the
		// merchant owns, and no merchant-wide rule written from a review.
		expect(backfill).not.toContain('t.merchant_id');
		expect(values[7]).toContain('AMAZONPAYINGROCERY');
		expect(
			queries.some(query => query.includes('user_merchant_category_rules')),
		).toBe(false);
	});

	it('rejects rule write when no search key is available', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'NEEDS_REVIEW',
					transaction_category_id: null,
					transaction_merchant_id: null,
					transaction_type: 'expense',
					financial_event_direction: 'debit',
					financial_event_search_key: null,
				},
			],
			() => [{ type: 'expense' }],
			() => [],
			() => [transactionFeedRow()],
		]);

		await expect(
			reviewTransaction(
				sql,
				'11111111-1111-4111-8111-111111111111',
				'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
				{
					category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
					apply_rule: true,
				},
			),
		).rejects.toMatchObject({
			statusCode: 400,
			errorCode: 'REVIEW_RULE_SEARCH_KEY_REQUIRED',
		});

		expect(queries).toHaveLength(4);
	});

	it('lets the user change a debit expense to transfer with a transfer category', async () => {
		const TRANSFER_CATEGORY = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
		let updateValues: unknown[] = [];
		const { sql } = createSqlForReviewTests([
			() => [{
				transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
				transaction_status: 'NEEDS_REVIEW',
				transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
				transaction_merchant_id: null,
				transaction_type: 'expense',
				financial_event_direction: 'debit',
				financial_event_search_key: 'icicibank',
			}],
			() => [{ type: 'transfer' }],
			(_query, values) => {
				updateValues = values;
				return [];
			},
			() => [transactionFeedRow({
				transaction_type: 'transfer',
				transaction_category_id: TRANSFER_CATEGORY,
				category_id: TRANSFER_CATEGORY,
				category_name: 'Card Bill Payment',
				category_type: 'transfer',
			})],
			() => [],
		]);

		const result = await reviewTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{ type: 'transfer', category_id: TRANSFER_CATEGORY },
		);

		expect(updateValues).toContain('transfer');
		expect(updateValues).toContain(TRANSFER_CATEGORY);
		expect(result.transaction.transaction.type).toBe('transfer');
		expect(result.transaction.transaction.classification_source).toBe('USER');
	});

	it('rejects changing a debit transaction directly to income', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [{
				transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
				transaction_status: 'NEEDS_REVIEW',
				transaction_category_id: null,
				transaction_merchant_id: null,
				transaction_type: 'expense',
				financial_event_direction: 'debit',
				financial_event_search_key: null,
			}],
		]);

		await expect(reviewTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{ type: 'income', category_id: null },
		)).rejects.toMatchObject({
			statusCode: 400,
			errorCode: 'TRANSACTION_TYPE_DIRECTION_MISMATCH',
		});
		expect(queries).toHaveLength(1);
	});

	it('rejects review when transaction is already resolved', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'VERIFIED',
					transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
					transaction_merchant_id: null,
					transaction_type: 'expense',
					financial_event_direction: 'debit',
					financial_event_search_key: 'zomato',
				},
			],
		]);

		await expect(
			reviewTransaction(
				sql,
				'11111111-1111-4111-8111-111111111111',
				'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
				{},
			),
		).rejects.toMatchObject({
			statusCode: 409,
			errorCode: 'TRANSACTION_REVIEW_CLOSED',
		});

		expect(queries).toHaveLength(1);
	});

	it('confirms an unconfirmed AI mapping when the review keeps its merchant', async () => {
		const MERCHANT = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee';
		const { sql, queries, values } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'NEEDS_REVIEW',
					transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
					transaction_merchant_id: MERCHANT,
					transaction_type: 'expense',
					financial_event_direction: 'debit',
					financial_event_search_key: 'bundltech',
				},
			],
			() => [],
			() => [transactionFeedRow({
				transaction_merchant_id: MERCHANT,
				merchant_id: MERCHANT,
				merchant_canonical_name: 'Swiggy',
				merchant_type: 'MERCHANT',
				merchant_logo_public_url: 'https://example.supabase.co/storage/v1/object/public/merchant-logos/swiggy.svg',
				merchant_logo_brand_color: '#FC8019',
			})],
			// probation lookup: AI-sourced alias pointing at the kept merchant
			() => [{ merchant_id: MERCHANT, source: 'AI' }],
			// this user's CONFIRM verdict, then the recompute from every verdict
			() => [],
			() => [{ confirmed: true, confirms: 1, dissents: 0 }],
		]);

		const result = await reviewTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{},
			{ merchantConfirmationThreshold: 1 },
		);

		const verdict = queries[4] ?? '';
		expect(verdict).toContain('insert into public.merchant_confirmations');
		expect(values[4]).toEqual([MERCHANT, '11111111-1111-4111-8111-111111111111', 'CONFIRM']);
		const recompute = queries[5] ?? '';
		expect(recompute).toContain('update public.global_merchants as gm');
		expect(recompute).toContain('set confirmed = (tally.confirms >=');
		expect(values[5]).toContain(1);
		expect(result.transaction.merchant?.logo).toEqual({
			public_url: 'https://example.supabase.co/storage/v1/object/public/merchant-logos/swiggy.svg',
			brand_color: '#FC8019',
		});
	});

	it('dissents (demotes) an AI mapping when the review replaces its merchant', async () => {
		const ALIAS_MERCHANT = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee';
		const USER_PICK = 'ffffffff-ffff-4fff-8fff-ffffffffffff';
		const { sql, queries, values } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'NEEDS_REVIEW',
					transaction_category_id: null,
					transaction_merchant_id: ALIAS_MERCHANT,
					transaction_type: 'expense',
					financial_event_direction: 'debit',
					financial_event_search_key: 'bundltech',
				},
			],
			// merchant visibility: the user's pick is a shared brand or their own payee
			() => [{ id: USER_PICK }],
			() => [],
			() => [transactionFeedRow({ transaction_merchant_id: USER_PICK, merchant_id: USER_PICK, merchant_canonical_name: 'Zepto', merchant_type: 'MERCHANT' })],
			() => [{ merchant_id: ALIAS_MERCHANT, source: 'AI' }],
			// this user's DISSENT verdict, then the recompute
			() => [],
			() => [{ confirmed: false, confirms: 0, dissents: 1 }],
		]);

		await reviewTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{ merchant_id: USER_PICK },
			{ merchantConfirmationThreshold: 1 },
		);

		const visibility = queries[1] ?? '';
		expect(visibility).toContain('gm.owner_user_id is null or gm.owner_user_id =');
		const probation = queries[4] ?? '';
		expect(probation).toContain('gma.owner_user_id is null or gma.owner_user_id =');
		const verdict = queries[5] ?? '';
		expect(verdict).toContain('insert into public.merchant_confirmations');
		expect(values[5]).toEqual([ALIAS_MERCHANT, '11111111-1111-4111-8111-111111111111', 'DISSENT']);
		const recompute = queries[6] ?? '';
		expect(recompute).toContain('dissent_count = tally.dissents');
	});

	it('never touches SEED/MANUAL merchants from the probation path', async () => {
		const MERCHANT = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee';
		const { sql, queries } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'NEEDS_REVIEW',
					transaction_category_id: null,
					transaction_merchant_id: MERCHANT,
					transaction_type: 'expense',
					financial_event_direction: 'debit',
					financial_event_search_key: 'zomato',
				},
			],
			() => [],
			() => [transactionFeedRow()],
			() => [{ merchant_id: MERCHANT, source: 'SEED' }],
		]);

		await reviewTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{ merchant_id: null },
		);

		// probation select ran, but no confirm/dissent write followed
		expect(queries).toHaveLength(4);
	});
});

describe('updateTransaction classification', () => {
	it('rejects a direction-crossing type patch before writing', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [{
				transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
				transaction_status: 'VERIFIED',
				financial_event_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
				raw_email_id: 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',
				transaction_category_id: null,
				transaction_type: 'expense',
				financial_event_direction: 'debit',
				transaction_account_id: null,
				transaction_credit_card_id: null,
				current_credit_card_account_id: null,
			}],
		]);

		await expect(updateTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{ type: 'income', category_id: null },
		)).rejects.toMatchObject({
			statusCode: 400,
			errorCode: 'TRANSACTION_TYPE_DIRECTION_MISMATCH',
		});
		expect(queries).toHaveLength(1);
	});

	it('rejects a category whose type differs from the selected type', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [{
				transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
				transaction_status: 'VERIFIED',
				financial_event_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
				raw_email_id: 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',
				transaction_category_id: null,
				transaction_type: 'expense',
				financial_event_direction: 'debit',
				transaction_account_id: null,
				transaction_credit_card_id: null,
				current_credit_card_account_id: null,
			}],
			() => [{ type: 'income' }],
		]);

		await expect(updateTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{ category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc' },
		)).rejects.toMatchObject({
			statusCode: 400,
			errorCode: 'TRANSACTION_CATEGORY_TYPE_MISMATCH',
		});
		expect(queries).toHaveLength(2);
	});

	it('allows a note-only edit on an untouched historical mismatch', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [{
				transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
				transaction_status: 'VERIFIED',
				financial_event_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
				raw_email_id: 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',
				transaction_category_id: null,
				transaction_type: 'income',
				financial_event_direction: 'debit',
				transaction_account_id: null,
				transaction_credit_card_id: null,
				current_credit_card_account_id: null,
			}],
			() => [],
			() => [transactionFeedRow({
				transaction_type: 'income',
				transaction_category_id: null,
				category_id: null,
				category_name: null,
				category_type: null,
				transaction_user_note: 'keep this note',
			})],
		]);

		const result = await updateTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{ user_note: 'keep this note' },
		);

		expect(result.transaction.user_note).toBe('keep this note');
		expect(queries).toHaveLength(3);
	});

	it('patches a compatible imported type change and marks it as user-classified', async () => {
		const TRANSFER_CATEGORY = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
		let updateValues: unknown[] = [];
		const { sql, queries } = createSqlForReviewTests([
			() => [{
				transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
				transaction_status: 'NEEDS_REVIEW',
				financial_event_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
				raw_email_id: 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',
				transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
				transaction_type: 'expense',
				financial_event_direction: 'debit',
				transaction_account_id: null,
				transaction_credit_card_id: null,
				current_credit_card_account_id: null,
			}],
			() => [{ type: 'transfer' }],
			(_query, values) => {
				updateValues = values;
				return [];
			},
			() => [transactionFeedRow({
				transaction_type: 'transfer',
				transaction_category_id: TRANSFER_CATEGORY,
				transaction_classification_source: 'USER',
				transaction_ai_confidence_score: null,
				category_id: TRANSFER_CATEGORY,
				category_name: 'Card Bill Payment',
				category_type: 'transfer',
			})],
		]);

		const result = await updateTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{ type: 'transfer', category_id: TRANSFER_CATEGORY },
		);

		expect(updateValues).toContain('transfer');
		expect(queries[2]).toContain('recurring_series_id');
		expect(result.transaction.type).toBe('transfer');
		expect(result.transaction.classification_source).toBe('USER');
		expect(result.transaction.ai_confidence_score).toBeNull();
	});

	it('rejects changing the type of a linked expense before writing', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [{
				transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
				transaction_status: 'VERIFIED',
				financial_event_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
				raw_email_id: 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',
				transaction_category_id: null,
				transaction_type: 'expense',
				financial_event_direction: 'debit',
				budget_relationship_as_source: false,
				budget_relationship_as_target: true,
				transaction_account_id: null,
				transaction_credit_card_id: null,
				current_credit_card_account_id: null,
			}],
		]);

		await expect(updateTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			{ type: 'transfer', category_id: null },
		)).rejects.toMatchObject({
			statusCode: 400,
			errorCode: 'TRANSACTION_RELATIONSHIP_TYPE_MISMATCH',
		});
		expect(queries).toHaveLength(1);
	});
});
