import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';

import {
	confirmRecurringPattern,
	createTransactionRelationship,
	currentBudgetMonth,
	getBudgetOverview,
	parseBudgetMonth,
	parseConfirmRecurringPatternRequest,
	parseCreateTransactionRelationshipRequest,
	parseDismissBudgetSuggestionRequest,
	parseSetCategoryBudgetRuleRequest,
	parseUpsertMonthlyBudgetRequest,
	parseUpsertBudgetPlanRequest,
	parseUpdateRecurringSeriesRequest,
	upsertBudgetPlan,
} from '../src/services/budget.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function scriptedSql(handlers: QueryHandler[]): { sql: SqlClient; queries: string[]; values: unknown[][] } {
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

function budgetOverviewSql(): SqlClient {
	const tag = async (strings: TemplateStringsArray): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		if (query.includes('from public.monthly_budgets')) {
			return [{ limit_in_paise: 20_000_00, month_start: '2026-06-01' }];
		}
		if (query.includes('from public.user_category_budget_rules as ucbr')) return [];
		if (query.includes('from public.transaction_relationships as rel')) {
			return [{ source_transaction_id: 'refund', target_transaction_id: 'purchase' }];
		}
		if (query.includes('from public.transactions as source')) return [];
		if (query.includes('t.recurring_series_id is null')) return [];
		if (query.includes('from public.recurring_series as rs')) return [];
		if (query.includes('from public.budget_suggestion_dismissals')) return [];
		if (query.includes('from public.transactions as t')) {
			return [
				{
					id: 'purchase',
					amount_in_paise: 6_000_00,
					type: 'expense',
					direction: 'debit',
					txn_date: '2026-07-29T12:00:00.000Z',
					budget_treatment: 'AUTO',
					category_id: 'food',
					category_name: 'Food',
					category_icon: 'Utensils',
					category_treatment: null,
					recurring_treatment: null,
					recurring_status: null,
				},
				{
					id: 'refund',
					amount_in_paise: 2_000_00,
					type: 'income',
					direction: 'credit',
					txn_date: '2026-08-04T12:00:00.000Z',
					budget_treatment: 'AUTO',
					category_id: null,
					category_name: null,
					category_icon: null,
					category_treatment: null,
					recurring_treatment: null,
					recurring_status: null,
				},
			];
		}
		throw new Error(`Unexpected budget query: ${query}`);
	};
	return tag as unknown as SqlClient;
}

describe('budget service validation', () => {
	it('attributes a later linked refund back to the purchase month', async () => {
		const overview = await getBudgetOverview(
			budgetOverviewSql(),
			'11111111-1111-4111-8111-111111111111',
			'2026-07',
		);
		expect(overview.summary).toMatchObject({
			limit_in_paise: 20_000_00,
			limit_source_month: '2026-06',
			counted_spending_in_paise: 6_000_00,
			adjustments_in_paise: 2_000_00,
			used_in_paise: 4_000_00,
			remaining_in_paise: 16_000_00,
		});
		expect(overview.summary.categories).toEqual([
			expect.objectContaining({
				category_name: 'Food',
				spent_in_paise: 6_000_00,
				adjustment_in_paise: 2_000_00,
				net_in_paise: 4_000_00,
			}),
		]);
	});

	it('formats the current month in Asia/Kolkata', () => {
		expect(currentBudgetMonth(new Date('2026-01-31T20:00:00.000Z'))).toBe('2026-02');
	});

	it('accepts real YYYY-MM values and rejects malformed months', () => {
		expect(parseBudgetMonth('2026-08')).toBe('2026-08');
		expect(() => parseBudgetMonth('2026-13')).toThrow('month must use YYYY-MM');
		expect(() => parseBudgetMonth('August')).toThrow('month must use YYYY-MM');
	});

	it('requires a positive monthly limit in paise', () => {
		expect(parseUpsertMonthlyBudgetRequest({ limit_in_paise: 2_000_00 })).toEqual({
			limit_in_paise: 2_000_00,
		});
		expect(() => parseUpsertMonthlyBudgetRequest({ limit_in_paise: 0 })).toThrow(
			'limit_in_paise must be a positive integer',
		);
	});

	it('validates an atomic plan update and rejects duplicate categories', () => {
		const categoryId = '11111111-1111-4111-8111-111111111111';
		expect(parseUpsertBudgetPlanRequest({
			limit_in_paise: 20_000_00,
			category_rules: [{ category_id: categoryId, treatment: 'EXCLUDE' }],
		})).toEqual({
			limit_in_paise: 20_000_00,
			category_rules: [{ category_id: categoryId, treatment: 'EXCLUDE' }],
		});
		expect(() => parseUpsertBudgetPlanRequest({
			limit_in_paise: 20_000_00,
			category_rules: [
				{ category_id: categoryId, treatment: null },
				{ category_id: categoryId, treatment: 'EXCLUDE' },
			],
		})).toThrow('must not contain duplicate categories');
	});

	it('saves the limit and fixed-cost rules in one transaction', async () => {
		const categoryIds = [
			'11111111-1111-4111-8111-111111111111',
			'22222222-2222-4222-8222-222222222222',
		];
		const { sql, queries, values } = scriptedSql([
			() => categoryIds.map(id => ({ id })),
			() => [],
			() => [],
			() => [],
		]);

		await upsertBudgetPlan(sql, 'user-1', '2026-08', {
			limit_in_paise: 20_000_00,
			category_rules: [
				{ category_id: categoryIds[0], treatment: 'EXCLUDE' },
				{ category_id: categoryIds[1], treatment: null },
			],
		});

		expect(queries).toHaveLength(4);
		expect(queries[0]).toContain('from public.categories as c');
		expect(queries[1]).toContain('insert into public.monthly_budgets');
		expect(queries[2]).toContain('delete from public.user_category_budget_rules');
		expect(queries[3]).toContain('insert into public.user_category_budget_rules');
		expect(values[3]).toContainEqual([categoryIds[0]]);
		expect(values[3]).toContainEqual(['EXCLUDE']);
	});

	it('locks a purchase before validating and linking a manual refund', async () => {
		const sourceId = '11111111-1111-4111-8111-111111111111';
		const targetId = '22222222-2222-4222-8222-222222222222';
		const { sql, queries } = scriptedSql([
			() => [{ id: targetId }],
			() => [{
				source_type: 'income',
				source_direction: 'credit',
				source_amount: 2_000_00,
				source_date: '2026-08-04T12:00:00.000Z',
				target_type: 'expense',
				target_direction: 'debit',
				target_remaining: 4_000_00,
				target_date: '2026-07-29T12:00:00.000Z',
			}],
			() => [],
		]);

		await createTransactionRelationship(sql, 'user-1', {
			source_transaction_id: sourceId,
			target_transaction_id: targetId,
			kind: 'REFUND',
		});

		expect(queries).toHaveLength(3);
		expect(queries[0]).toContain('for update');
		expect(queries[1]).toContain('target_remaining');
		expect(queries[2]).toContain('insert into public.transaction_relationships');
	});

	it('rejects extra transactions outside the confirmed recurring pattern', async () => {
		const matchingIds = [
			'11111111-1111-4111-8111-111111111111',
			'22222222-2222-4222-8222-222222222222',
			'33333333-3333-4333-8333-333333333333',
		];
		const unrelatedId = '44444444-4444-4444-8444-444444444444';
		const recurringRow = (id: string, date: string, merchantId: string) => ({
			id,
			amount_in_paise: 499_00,
			txn_date: date,
			direction: 'debit',
			merchant_id: merchantId,
			search_key: merchantId.toUpperCase(),
			category_id: '55555555-5555-4555-8555-555555555555',
			label: merchantId,
			category_treatment: null,
		});
		const { sql, queries } = scriptedSql([
			() => [
				recurringRow(matchingIds[0], '2026-06-05T00:00:00.000Z', 'netflix'),
				recurringRow(matchingIds[1], '2026-07-05T00:00:00.000Z', 'netflix'),
				recurringRow(matchingIds[2], '2026-08-05T00:00:00.000Z', 'netflix'),
				recurringRow(unrelatedId, '2026-08-06T00:00:00.000Z', 'spotify'),
			],
		]);

		await expect(confirmRecurringPattern(sql, 'user-1', {
			fingerprint: 'recurring:debit:netflix:55555555-5555-4555-8555-555555555555:MONTHLY',
			transaction_ids: [...matchingIds, unrelatedId],
			treatment: 'EXCLUDE',
		})).rejects.toThrow('must exactly match one recurring pattern');
		expect(queries).toHaveLength(1);
	});

	it('only accepts explicit category treatments', () => {
		expect(parseSetCategoryBudgetRuleRequest({ treatment: 'EXCLUDE' })).toEqual({
			treatment: 'EXCLUDE',
		});
		expect(() => parseSetCategoryBudgetRuleRequest({ treatment: 'AUTO' })).toThrow(
			'treatment must be INCLUDE or EXCLUDE',
		);
	});

	it('validates refund relationship identifiers and kind', () => {
		const source = '11111111-1111-4111-8111-111111111111';
		const target = '22222222-2222-4222-8222-222222222222';
		expect(parseCreateTransactionRelationshipRequest({
			source_transaction_id: source,
			target_transaction_id: target,
			kind: 'REIMBURSEMENT',
		})).toEqual({ source_transaction_id: source, target_transaction_id: target, kind: 'REIMBURSEMENT' });
		expect(() => parseCreateTransactionRelationshipRequest({
			source_transaction_id: source,
			target_transaction_id: target,
			kind: 'LOAN',
		})).toThrow('kind must be REFUND or REIMBURSEMENT');
	});

	it('requires three unique transactions to confirm recurrence', () => {
		const ids = [
			'11111111-1111-4111-8111-111111111111',
			'22222222-2222-4222-8222-222222222222',
			'33333333-3333-4333-8333-333333333333',
		];
		expect(parseConfirmRecurringPatternRequest({
			fingerprint: 'recurring:debit:test',
			transaction_ids: ids,
			treatment: 'EXCLUDE',
		})).toMatchObject({ transaction_ids: ids, treatment: 'EXCLUDE' });
		expect(() => parseConfirmRecurringPatternRequest({
			fingerprint: 'recurring:debit:test',
			transaction_ids: [ids[0], ids[0], ids[1]],
			treatment: 'INCLUDE',
		})).toThrow('at least 3 unique transactions');
	});

	it('validates recurring updates and suggestion dismissals', () => {
		expect(parseUpdateRecurringSeriesRequest({ status: 'PAUSED' })).toEqual({ status: 'PAUSED' });
		expect(() => parseUpdateRecurringSeriesRequest({})).toThrow('Provide budget_treatment or status');
		expect(parseDismissBudgetSuggestionRequest({
			type: 'REFUND_MATCH',
			fingerprint: 'refund:source:target',
		})).toEqual({ type: 'REFUND_MATCH', fingerprint: 'refund:source:target' });
	});
});
