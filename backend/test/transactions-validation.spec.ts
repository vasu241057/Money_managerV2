import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';

import {
	isTransactionTypeAllowedForDirection,
	listTransactions,
	normalizeTransactionSearchAmount,
	parseCreateManualTransactionRequest,
	parseListTransactionsQuery,
	parseReviewTransactionRequest,
	parseTransactionSearchAmountInPaise,
	parseUpdateTransactionRequest,
	prepareTransactionRelationUpdate,
	resolveManualTransactionDirection,
} from '../src/services/transactions.service';

describe('transactions payload/query validation', () => {
	it('rejects list query where from > to', () => {
		expect(() =>
			parseListTransactionsQuery({
				from: '2026-03-08T10:00:00.000Z',
				to: '2026-03-07T10:00:00.000Z',
			}),
		).toThrow('from must be earlier than or equal to to');
	});

	it('normalizes an optional transaction search query', () => {
		expect(parseListTransactionsQuery({ q: '  Amazon India  ' })).toMatchObject({
			q: 'Amazon India',
		});
		expect(parseListTransactionsQuery({ q: '   ' })).toMatchObject({ q: null });
		expect(() => parseListTransactionsQuery({ q: 'x'.repeat(121) })).toThrow(
			'q must be at most 120 characters',
		);
	});

	it('recognizes exact rupee amount searches without confusing dates', () => {
		expect(parseTransactionSearchAmountInPaise('₹1,250.50')).toBe(125_050);
		expect(parseTransactionSearchAmountInPaise('6000')).toBe(600_000);
		expect(parseTransactionSearchAmountInPaise('04/08/2026')).toBeNull();
		expect(parseTransactionSearchAmountInPaise('12.345')).toBeNull();
	});

	it('normalizes partial amount searches without treating dates as amounts', () => {
		expect(normalizeTransactionSearchAmount('  ₹2,575.50 ')).toBe('2575.50');
		expect(normalizeTransactionSearchAmount('257')).toBe('257');
		expect(normalizeTransactionSearchAmount('0')).toBe('0');
		expect(normalizeTransactionSearchAmount('04/08/2026')).toBeNull();
		expect(normalizeTransactionSearchAmount('merchant 257')).toBeNull();
	});

	it('applies partial text and amount search to count and page queries', async () => {
		const queries: string[] = [];
		const values: unknown[][] = [];
		const sql = (async (strings: TemplateStringsArray, ...queryValues: unknown[]) => {
			const query = strings.join(' ').replace(/\s+/g, ' ').trim();
			queries.push(query);
			values.push(queryValues);
			return query.startsWith('select count') ? [{ total: 0 }] : [];
		}) as unknown as SqlClient;

		const response = await listTransactions(
			sql,
			'11111111-1111-4111-8111-111111111111',
			parseListTransactionsQuery({ q: 'Amazon', page: '1', limit: '25' }),
		);

		expect(response).toMatchObject({ data: [], total: 0, page: 1, limit: 25, has_more: false });
		expect(queries).toHaveLength(2);
		for (const query of queries) {
			expect(query).toContain('t.user_note ilike');
			expect(query).toContain('gm.canonical_name ilike');
			expect(query).toContain('fe.counterparty_raw ilike');
			expect(query).toContain('t.amount_in_paise');
			expect(query).not.toContain("at time zone 'Asia/Kolkata'");
		}
		expect(values[0]).toContain('%Amazon%');
		expect(values[1]).toContain('%Amazon%');
	});

	it('matches a partial numeric query against the displayed rupee amount', async () => {
		const queries: string[] = [];
		const values: unknown[][] = [];
		const sql = (async (strings: TemplateStringsArray, ...queryValues: unknown[]) => {
			const query = strings.join(' ').replace(/\s+/g, ' ').trim();
			queries.push(query);
			values.push(queryValues);
			return query.startsWith('select count') ? [{ total: 0 }] : [];
		}) as unknown as SqlClient;

		await listTransactions(
			sql,
			'11111111-1111-4111-8111-111111111111',
			parseListTransactionsQuery({ q: '257', page: '1', limit: '25' }),
		);

		for (const query of queries) {
			expect(query).toContain("to_char(t.amount_in_paise / 100.0, 'FM999999999999990.00') ilike");
		}
		expect(values[0]).toContain('%257%');
		expect(values[1]).toContain('%257%');
	});

	it('rejects create payload with non-positive amount', () => {
		expect(() =>
			parseCreateManualTransactionRequest({
				amount_in_paise: 0,
				type: 'expense',
				txn_date: new Date().toISOString(),
			}),
		).toThrow('amount_in_paise must be a positive integer');
	});

	it('parses an explicit budget treatment for manual fact recreation', () => {
		expect(parseCreateManualTransactionRequest({
			amount_in_paise: 100,
			type: 'expense',
			txn_date: '2026-08-04T10:00:00.000Z',
			budget_treatment: 'EXCLUDE',
		})).toMatchObject({ budget_treatment: 'EXCLUDE' });
	});

	it('enforces the immutable direction/type matrix', () => {
		expect(isTransactionTypeAllowedForDirection('expense', 'debit')).toBe(true);
		expect(isTransactionTypeAllowedForDirection('transfer', 'debit')).toBe(true);
		expect(isTransactionTypeAllowedForDirection('income', 'debit')).toBe(false);
		expect(isTransactionTypeAllowedForDirection('income', 'credit')).toBe(true);
		expect(isTransactionTypeAllowedForDirection('transfer', 'credit')).toBe(true);
		expect(isTransactionTypeAllowedForDirection('expense', 'credit')).toBe(false);
		expect(resolveManualTransactionDirection('transfer', 'credit')).toBe('credit');
		expect(() => resolveManualTransactionDirection('expense', 'credit')).toThrow(
			'credit transactions may only be income or transfer',
		);
	});

	it('rejects update payload when empty', () => {
		expect(() => parseUpdateTransactionRequest({})).toThrow(
			'At least one mutable transaction field must be provided for update',
		);
	});

	it('rejects AI confidence score outside [0, 1]', () => {
		expect(() =>
			parseUpdateTransactionRequest({
				ai_confidence_score: 1.2,
			}),
		).toThrow('ai_confidence_score must be between 0 and 1');
	});

	it('rejects mixed alpha page query values', () => {
		expect(() =>
			parseListTransactionsQuery({
				page: '1abc',
			}),
		).toThrow('page must be a positive integer');
	});

	it('parses review payload fields', () => {
		expect(
			parseReviewTransactionRequest({
				type: 'transfer',
				category_id: '00000000-0000-4000-8000-000000000101',
				merchant_id: null,
				user_note: 'looks correct',
				apply_rule: true,
				rule_search_key: 'zomato',
			}),
		).toEqual({
			type: 'transfer',
			category_id: '00000000-0000-4000-8000-000000000101',
			merchant_id: null,
			user_note: 'looks correct',
			apply_rule: true,
			rule_search_key: 'zomato',
		});
	});

	it('rejects non-boolean review apply_rule', () => {
		expect(() =>
			parseReviewTransactionRequest({
				apply_rule: 'true',
			}),
		).toThrow('apply_rule must be a boolean');
	});

	it('auto-aligns account_id when credit_card_id is updated', () => {
		const relationUpdate = prepareTransactionRelationUpdate({
			currentCreditCardId: '11111111-1111-4111-8111-111111111111',
			currentCreditCardAccountId: '22222222-2222-4222-8222-222222222222',
			requestedAccountId: undefined,
			requestedCreditCardId: '33333333-3333-4333-8333-333333333333',
			targetCreditCardAccountId: '44444444-4444-4444-8444-444444444444',
		});

		expect(relationUpdate).toEqual({
			shouldUpdateAccount: true,
			accountId: '44444444-4444-4444-8444-444444444444',
			shouldUpdateCreditCard: true,
			creditCardId: '33333333-3333-4333-8333-333333333333',
		});
	});

	it('rejects account_id mismatch when credit_card_id remains unchanged', () => {
		expect(() =>
			prepareTransactionRelationUpdate({
				currentCreditCardId: '11111111-1111-4111-8111-111111111111',
				currentCreditCardAccountId: '22222222-2222-4222-8222-222222222222',
				requestedAccountId: '99999999-9999-4999-8999-999999999999',
				requestedCreditCardId: undefined,
			}),
		).toThrow('account_id must match the linked credit card account');
	});

	it('rejects explicit account/credit-card mismatch in same update', () => {
		expect(() =>
			prepareTransactionRelationUpdate({
				currentCreditCardId: null,
				currentCreditCardAccountId: null,
				requestedAccountId: '99999999-9999-4999-8999-999999999999',
				requestedCreditCardId: '33333333-3333-4333-8333-333333333333',
				targetCreditCardAccountId: '44444444-4444-4444-8444-444444444444',
			}),
		).toThrow('account_id must match credit_card_id account');
	});
});
