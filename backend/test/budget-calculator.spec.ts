import { describe, expect, it } from 'vitest';

import {
	calculateBudget,
	type BudgetCalculationTransaction,
} from '../src/services/budget-calculator';

function transaction(
	id: string,
	overrides: Partial<BudgetCalculationTransaction> = {},
): BudgetCalculationTransaction {
	return {
		id,
		amountInPaise: 100_00,
		type: 'expense',
		direction: 'debit',
		treatment: 'AUTO',
		categoryTreatment: null,
		recurringTreatment: null,
		...overrides,
	};
}

describe('calculateBudget', () => {
	it('counts expenses by default but ignores ordinary income and transfers', () => {
		const result = calculateBudget([
			transaction('expense', { amountInPaise: 500_00 }),
			transaction('income', { type: 'income', direction: 'credit', amountInPaise: 2_000_00 }),
			transaction('transfer', { type: 'transfer', amountInPaise: 300_00 }),
		], []);

		expect(result.countedSpendingInPaise).toBe(500_00);
		expect(result.adjustmentsInPaise).toBe(0);
		expect(result.usedInPaise).toBe(500_00);
	});

	it('uses override, recurring, category, then default precedence', () => {
		const result = calculateBudget([
			transaction('override', {
				treatment: 'EXCLUDE',
				recurringTreatment: 'INCLUDE',
				categoryTreatment: 'INCLUDE',
			}),
			transaction('recurring', {
				recurringTreatment: 'EXCLUDE',
				categoryTreatment: 'INCLUDE',
			}),
			transaction('category', { categoryTreatment: 'EXCLUDE' }),
			transaction('default'),
		], []);

		expect(result.transactions.get('override')?.treatmentSource).toBe('TRANSACTION_OVERRIDE');
		expect(result.transactions.get('recurring')?.treatmentSource).toBe('RECURRING_RULE');
		expect(result.transactions.get('category')?.treatmentSource).toBe('CATEGORY_RULE');
		expect(result.transactions.get('default')?.treatmentSource).toBe('SYSTEM_DEFAULT');
		expect(result.countedSpendingInPaise).toBe(100_00);
		expect(result.outsidePlanInPaise).toBe(300_00);
	});

	it('nets multiple partial refunds and caps them at the purchase amount', () => {
		const result = calculateBudget([
			transaction('purchase', { amountInPaise: 6_000_00 }),
			transaction('refund-1', { type: 'income', direction: 'credit', amountInPaise: 2_000_00 }),
			transaction('refund-2', { type: 'income', direction: 'credit', amountInPaise: 5_000_00 }),
		], [
			{ sourceTransactionId: 'refund-1', targetTransactionId: 'purchase' },
			{ sourceTransactionId: 'refund-2', targetTransactionId: 'purchase' },
		]);

		expect(result.countedSpendingInPaise).toBe(6_000_00);
		expect(result.adjustmentsInPaise).toBe(6_000_00);
		expect(result.transactions.get('refund-2')?.effectInPaise).toBe(4_000_00);
		expect(result.usedInPaise).toBe(0);
	});

	it('gives a linked refund no effect when its purchase is outside the plan', () => {
		const result = calculateBudget([
			transaction('purchase', { treatment: 'EXCLUDE', amountInPaise: 6_000_00 }),
			transaction('refund', { type: 'income', direction: 'credit', amountInPaise: 2_000_00 }),
		], [{ sourceTransactionId: 'refund', targetTransactionId: 'purchase' }]);

		expect(result.adjustmentsInPaise).toBe(0);
		expect(result.transactions.get('refund')?.treatmentSource).toBe('REFUND_RELATIONSHIP');
		expect(result.transactions.get('refund')?.effectiveTreatment).toBe('EXCLUDE');
	});

	it('lets a source override win over a refund relationship', () => {
		const result = calculateBudget([
			transaction('purchase'),
			transaction('refund', {
				type: 'income',
				direction: 'credit',
				treatment: 'EXCLUDE',
			}),
		], [{ sourceTransactionId: 'refund', targetTransactionId: 'purchase' }]);

		expect(result.transactions.get('refund')?.treatmentSource).toBe('TRANSACTION_OVERRIDE');
		expect(result.adjustmentsInPaise).toBe(0);
	});

	it('does not let a standalone included credit create negative used spend', () => {
		const result = calculateBudget([
			transaction('cash-reimbursement', {
				type: 'income',
				direction: 'credit',
				treatment: 'INCLUDE',
			}),
		], []);

		expect(result.adjustmentsInPaise).toBe(100_00);
		expect(result.usedInPaise).toBe(0);
	});
});
