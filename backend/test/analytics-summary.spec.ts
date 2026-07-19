import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { getAnalyticsSummary } from '../src/services/analytics.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

// The service issues its queries via Promise.all in a fixed array order, so
// a cursor-based mock stays deterministic.
function createSqlMock(handlers: QueryHandler[]): SqlClient {
	let cursor = 0;

	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		const handler = handlers[cursor];
		cursor += 1;
		if (!handler) {
			throw new Error(`Unexpected query execution: ${query}`);
		}
		return handler(query, values);
	};

	return tag as unknown as SqlClient;
}

const USER_ID = '11111111-1111-4111-8111-111111111111';
const CAT_ID = '22222222-2222-4222-8222-222222222222';
const ACC_ID = '33333333-3333-4333-8333-333333333333';

describe('getAnalyticsSummary', () => {
	it('assembles months ascending with nested breakdowns and derived all-time totals', async () => {
		const sql = createSqlMock([
			// monthly totals by type (bigint sums arrive as strings)
			() => [
				{ month: '2026-05', type: 'expense', amount: '150000', cnt: 3 },
				{ month: '2026-05', type: 'income', amount: '500000', cnt: 1 },
				{ month: '2026-04', type: 'expense', amount: '20000', cnt: 1 },
				{ month: '2026-05', type: 'transfer', amount: '77700', cnt: 1 },
			],
			// expense by category
			() => [
				{ month: '2026-05', category_id: CAT_ID, category_name: 'Groceries', amount: '90000', cnt: 2 },
				{ month: '2026-05', category_id: null, category_name: null, amount: '60000', cnt: 1 },
			],
			// expense by account
			() => [
				{ month: '2026-05', account_id: ACC_ID, amount: '150000', cnt: 3 },
			],
			// per-account all-time
			() => [
				{ account_id: ACC_ID, income: '500000', expense: '170000' },
			],
			// review counters
			() => [{ total: 4, last_3_days: 1 }],
		]);

		const summary = await getAnalyticsSummary(sql, USER_ID);

		expect(summary.months.map(m => m.month)).toEqual(['2026-04', '2026-05']);

		const may = summary.months[1];
		expect(may).toMatchObject({
			income_in_paise: 500000,
			expense_in_paise: 150000,
			transfer_in_paise: 77700,
			txn_count: 5,
		});
		// Breakdown slices sort largest-first; null buckets survive
		expect(may?.expense_by_category).toEqual([
			{ category_id: CAT_ID, category_name: 'Groceries', amount_in_paise: 90000, txn_count: 2 },
			{ category_id: null, category_name: null, amount_in_paise: 60000, txn_count: 1 },
		]);
		expect(may?.expense_by_account).toEqual([
			{ account_id: ACC_ID, amount_in_paise: 150000, txn_count: 3 },
		]);

		expect(summary.all_time).toEqual({ income_in_paise: 500000, expense_in_paise: 170000 });
		expect(summary.accounts).toEqual([
			{ account_id: ACC_ID, income_in_paise: 500000, expense_in_paise: 170000 },
		]);
		expect(summary.review).toEqual({ total: 4, last_3_days: 1 });
	});

	it('returns an empty shell for a user with no transactions', async () => {
		const sql = createSqlMock([() => [], () => [], () => [], () => [], () => []]);

		const summary = await getAnalyticsSummary(sql, USER_ID);

		expect(summary.months).toEqual([]);
		expect(summary.all_time).toEqual({ income_in_paise: 0, expense_in_paise: 0 });
		expect(summary.accounts).toEqual([]);
		expect(summary.review).toEqual({ total: 0, last_3_days: 0 });
	});
});
