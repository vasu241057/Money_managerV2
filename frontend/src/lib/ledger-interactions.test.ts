import { describe, expect, it } from 'vitest';
import type { Transaction } from '../types/domain';
import { matchesLedgerFilter, resolveSwipeOutcome } from './ledger-interactions';

function transaction(overrides: Partial<Transaction> = {}): Transaction {
	return {
		id: 'txn-1',
		amount: 2_000,
		type: 'expense',
		category: 'Shopping',
		date: new Date('2026-08-04T10:00:00.000Z'),
		status: 'VERIFIED',
		effectiveBudgetTreatment: 'INCLUDE',
		budgetEffect: 'SPEND',
		...overrides,
	};
}

describe('Ledger swipe decisions', () => {
	it('keeps a short deliberate drag closed', () => {
		expect(resolveSwipeOutcome(18, 0.1, 360)).toBe('CLOSE');
	});

	it('opens either action with distance or a comfortable flick', () => {
		expect(resolveSwipeOutcome(50, 0.1, 360)).toBe('OPEN_RIGHT');
		expect(resolveSwipeOutcome(-24, -0.7, 360)).toBe('OPEN_LEFT');
	});

	it('commits the budget action only after dragging right across roughly half the row', () => {
		expect(resolveSwipeOutcome(188, 0, 360)).toBe('COMMIT_RIGHT');
		expect(resolveSwipeOutcome(180, 4, 360)).toBe('OPEN_RIGHT');
	});

	it('never commits a left drag, regardless of distance or speed', () => {
		expect(resolveSwipeOutcome(-188, 0, 360)).toBe('OPEN_LEFT');
		expect(resolveSwipeOutcome(-350, -4, 360)).toBe('OPEN_LEFT');
	});
});

describe('Ledger budget filters', () => {
	it('classifies review-required records by the same rules the monthly plan uses', () => {
		// The plan counts unreviewed spending, so the ledger has to show it —
		// otherwise an over-budget month cannot be explained from the ledger.
		const uncertainSpend = transaction({ status: 'NEEDS_REVIEW' });
		expect(matchesLedgerFilter(uncertainSpend, 'review')).toBe(true);
		expect(matchesLedgerFilter(uncertainSpend, 'in-budget')).toBe(true);

		const uncertainOutside = transaction({
			status: 'NEEDS_REVIEW',
			effectiveBudgetTreatment: 'EXCLUDE',
			budgetEffect: 'NONE',
		});
		expect(matchesLedgerFilter(uncertainOutside, 'outside-budget')).toBe(true);
		expect(matchesLedgerFilter(uncertainOutside, 'in-budget')).toBe(false);
	});

	it('shows verified records in their effective budget filter', () => {
		expect(matchesLedgerFilter(transaction(), 'in-budget')).toBe(true);
		expect(matchesLedgerFilter(transaction({
			effectiveBudgetTreatment: 'EXCLUDE',
			budgetEffect: 'NONE',
		}), 'outside-budget')).toBe(true);
	});
});
