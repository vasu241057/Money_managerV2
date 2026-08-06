import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	backfillMerchantCategory,
	listGlobalMerchants,
	listUserMerchants,
	parseListGlobalMerchantsQuery,
	parseSetUserMerchantCategoryRequest,
	setUserMerchantCategory,
} from '../src/services/merchants.service';

const USER_ID = '00000000-0000-4000-8000-0000000000a1';
const MERCHANT_ID = '00000000-0000-4000-8000-0000000000a2';
const CATEGORY_ID = '00000000-0000-4000-8000-0000000000a3';

type QueryHandler = (query: string, values: unknown[]) => unknown;

function scriptedSql(handlers: QueryHandler[]): {
	sql: SqlClient;
	queries: string[];
	values: unknown[][];
} {
	const queries: string[] = [];
	const values: unknown[][] = [];
	let index = 0;
	const tag = async (strings: TemplateStringsArray, ...queryValues: unknown[]): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		queries.push(query);
		values.push(queryValues);
		const handler = handlers[index++];
		if (!handler) throw new Error(`Unexpected query execution: ${query}`);
		return handler(query, queryValues);
	};
	const sql = tag as unknown as SqlClient;
	(sql as unknown as { begin: (callback: (tx: SqlClient) => Promise<unknown>) => Promise<unknown> }).begin =
		async callback => callback(sql);
	return { sql, queries, values };
}

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlMock(handler: (query: string, values: unknown[]) => unknown): SqlClient {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		return handler(normalizeQuery(strings), values);
	};
	return tag as unknown as SqlClient;
}

describe('merchants service', () => {
	it('parses query defaults', () => {
		expect(parseListGlobalMerchantsQuery({})).toEqual({
			q: null,
			limit: 200,
		});
	});

	it('rejects non-string merchant search query', () => {
		expect(() =>
			parseListGlobalMerchantsQuery({
				q: 123,
			}),
		).toThrow('q must be a string');
	});

	it('rejects merchant list limit above cap', () => {
		expect(() =>
			parseListGlobalMerchantsQuery({
				limit: '1001',
			}),
		).toThrow('limit must be less than or equal to 1000');
	});

	it('rejects merchant search query that is too long', () => {
		expect(() =>
			parseListGlobalMerchantsQuery({
				q: 'a'.repeat(121),
			}),
		).toThrow('q must be at most 120 characters');
	});

	it('queries and maps merchants list', async () => {
		const sql = createSqlMock((query, values) => {
			expect(query).toContain('from public.global_merchants as gm');
			expect(query).toContain("gm.canonical_name ilike");
			expect(values).toContain('%shop%');
			expect(values).toContain(25);

			return [
				{
					merchant_id: '00000000-0000-4000-8000-000000000001',
					merchant_canonical_name: 'Shop One',
					merchant_type: 'MERCHANT',
				},
				{
					merchant_id: '00000000-0000-4000-8000-000000000002',
					merchant_canonical_name: 'Shop Two',
					merchant_type: 'P2P',
				},
			];
		});

		const query = parseListGlobalMerchantsQuery({
			q: 'shop',
			limit: '25',
		});

		const result = await listGlobalMerchants(sql, query);
		expect(result).toEqual([
			{
				id: '00000000-0000-4000-8000-000000000001',
				canonical_name: 'Shop One',
				type: 'MERCHANT',
			},
			{
				id: '00000000-0000-4000-8000-000000000002',
				canonical_name: 'Shop Two',
				type: 'P2P',
			},
		]);
	});
});

describe('parseSetUserMerchantCategoryRequest', () => {
	it('accepts a well-formed payload', () => {
		expect(
			parseSetUserMerchantCategoryRequest({
				transaction_type: 'expense',
				category_id: CATEGORY_ID,
				backfill: true,
			}),
		).toEqual({ transaction_type: 'expense', category_id: CATEGORY_ID, backfill: true });
	});

	it('rejects an unknown transaction type', () => {
		expect(() =>
			parseSetUserMerchantCategoryRequest({
				transaction_type: 'refund',
				category_id: CATEGORY_ID,
				backfill: false,
			}),
		).toThrow(/income, expense or transfer/);
	});

	it('requires backfill to be stated, never inferred', () => {
		expect(() =>
			parseSetUserMerchantCategoryRequest({
				transaction_type: 'expense',
				category_id: CATEGORY_ID,
			}),
		).toThrow(/backfill must be a boolean/);
	});
});

describe('backfillMerchantCategory', () => {
	it('spares hand-set categories under AI_ONLY and reports the row count', async () => {
		const { sql, queries, values } = scriptedSql([() => [{ id: 'a' }, { id: 'b' }]]);

		const count = await backfillMerchantCategory(
			sql,
			USER_ID,
			MERCHANT_ID,
			'expense',
			CATEGORY_ID,
			'AI_ONLY',
			'already-updated',
		);

		expect(count).toBe(2);
		expect(queries[0]).toContain('update public.transactions');
		expect(queries[0]).toContain("status = 'VERIFIED'");
		expect(queries[0]).toContain("classification_source <> 'USER'");
		// classification_source stays as it was, so a later decision for the
		// same merchant is still allowed to move these rows.
		expect(queries[0]).not.toContain('set classification_source');
		// true arms the "only AI-assigned" guard; the excluded id is the row
		// the caller already wrote itself.
		expect(values[0]).toContain(true);
		expect(values[0]).toContain('already-updated');
	});

	it('passes the ALL scope through so Settings can overwrite everything', async () => {
		const { sql, values } = scriptedSql([() => []]);

		const count = await backfillMerchantCategory(
			sql,
			USER_ID,
			MERCHANT_ID,
			'expense',
			CATEGORY_ID,
			'ALL',
		);

		expect(count).toBe(0);
		expect(values[0]).toContain(false);
	});
});

describe('setUserMerchantCategory', () => {
	it('saves the rule without touching history when backfill is off', async () => {
		const { sql, queries } = scriptedSql([
			() => [{ id: MERCHANT_ID }],
			() => [{ type: 'expense' }],
			() => [],
		]);

		const count = await setUserMerchantCategory(sql, USER_ID, MERCHANT_ID, {
			transaction_type: 'expense',
			category_id: CATEGORY_ID,
			backfill: false,
		});

		expect(count).toBe(0);
		expect(queries[2]).toContain('insert into public.user_merchant_category_rules');
		expect(queries.some(query => query.includes('update public.transactions'))).toBe(false);
	});

	it('saves the rule and rewrites history when backfill is on', async () => {
		const { sql, queries } = scriptedSql([
			() => [{ id: MERCHANT_ID }],
			() => [{ type: 'expense' }],
			() => [],
			() => [{ id: 'a' }, { id: 'b' }, { id: 'c' }],
		]);

		const count = await setUserMerchantCategory(sql, USER_ID, MERCHANT_ID, {
			transaction_type: 'expense',
			category_id: CATEGORY_ID,
			backfill: true,
		});

		expect(count).toBe(3);
		expect(queries[3]).toContain('update public.transactions');
	});

	it('refuses a category whose type does not match the slice being mapped', async () => {
		const { sql } = scriptedSql([() => [{ id: MERCHANT_ID }], () => [{ type: 'income' }]]);

		await expect(
			setUserMerchantCategory(sql, USER_ID, MERCHANT_ID, {
				transaction_type: 'expense',
				category_id: CATEGORY_ID,
				backfill: false,
			}),
		).rejects.toThrow(/Category type must match expense/);
	});

	it('refuses a category the user cannot use', async () => {
		const { sql } = scriptedSql([() => [{ id: MERCHANT_ID }], () => []]);

		await expect(
			setUserMerchantCategory(sql, USER_ID, MERCHANT_ID, {
				transaction_type: 'expense',
				category_id: CATEGORY_ID,
				backfill: false,
			}),
		).rejects.toThrow(/not available to this user/);
	});

	it('refuses an unknown merchant', async () => {
		const { sql } = scriptedSql([() => []]);

		await expect(
			setUserMerchantCategory(sql, USER_ID, MERCHANT_ID, {
				transaction_type: 'expense',
				category_id: CATEGORY_ID,
				backfill: false,
			}),
		).rejects.toThrow(/Merchant not found/);
	});
});

describe('listUserMerchants', () => {
	it('folds the per-type rows into one entry per merchant and totals the counts', async () => {
		const { sql } = scriptedSql([
			() => [
				{
					merchant_id: MERCHANT_ID,
					canonical_name: 'Swiggy',
					merchant_type: 'MERCHANT',
					logo_url: null,
					transaction_type: 'expense',
					transaction_count: 61,
					rule_category_id: CATEGORY_ID,
					rule_category_name: 'Food Delivery',
					common_category_id: CATEGORY_ID,
					common_category_name: 'Food Delivery',
				},
				{
					merchant_id: MERCHANT_ID,
					canonical_name: 'Swiggy',
					merchant_type: 'MERCHANT',
					logo_url: null,
					transaction_type: 'income',
					transaction_count: 4,
					rule_category_id: null,
					rule_category_name: null,
					common_category_id: null,
					common_category_name: null,
				},
			],
		]);

		const merchants = await listUserMerchants(sql, USER_ID, 500);

		expect(merchants).toHaveLength(1);
		expect(merchants[0]?.canonical_name).toBe('Swiggy');
		expect(merchants[0]?.transaction_count).toBe(65);
		expect(merchants[0]?.breakdown.map(entry => entry.transaction_type)).toEqual([
			'expense',
			'income',
		]);
		expect(merchants[0]?.breakdown[0]?.rule_category_name).toBe('Food Delivery');
		expect(merchants[0]?.breakdown[1]?.rule_category_id).toBeNull();
	});
});
