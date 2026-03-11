import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { reviewTransaction } from '../src/services/transactions.service';

type QueryResult = unknown;
type QueryHandler = (query: string, values: unknown[]) => QueryResult | Promise<QueryResult>;

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlForReviewTests(handlers: QueryHandler[]): {
	sql: SqlClient;
	queries: string[];
} {
	const queries: string[] = [];
	let index = 0;

	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		queries.push(query);
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

	return { sql, queries };
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
					financial_event_search_key: 'unknown-shop',
				},
			],
			() => [],
			() => [transactionFeedRow()],
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
		expect(queries).toHaveLength(3);
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
					financial_event_search_key: 'zomato-pay',
				},
			],
			() => [],
			() => [
				transactionFeedRow({
					transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
					category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
				}),
			],
			(_query, values) => {
				ruleInsertValues = values;
				return [];
			},
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
		expect(queries[3]).toContain('insert into public.user_merchant_rules');
	});

	it('rejects rule write when no search key is available', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'NEEDS_REVIEW',
					transaction_category_id: null,
					transaction_merchant_id: null,
					financial_event_search_key: null,
				},
			],
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

		expect(queries).toHaveLength(3);
	});

	it('rejects review when transaction is already resolved', async () => {
		const { sql, queries } = createSqlForReviewTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'VERIFIED',
					transaction_category_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
					transaction_merchant_id: null,
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
});
