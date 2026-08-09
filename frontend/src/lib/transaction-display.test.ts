import { describe, expect, it } from 'vitest';
import type { Transaction } from '../types/domain';
import {
	isIncomingTransaction,
	personInitials,
	personTint,
	transactionMetaLabel,
	transactionPrimaryLabel,
} from './transaction-display';

function transaction(overrides: Partial<Transaction> = {}): Transaction {
	return {
		id: 'transaction-1',
		amount: 100,
		type: 'expense',
		category: 'Food',
		date: new Date('2026-07-25T12:00:00.000Z'),
		...overrides,
	};
}

describe('transaction display labels', () => {
	it('promotes a user note and keeps the normalized merchant in metadata', () => {
		const item = transaction({
			description: 'Dinner with the team',
			merchantName: 'Bistro',
			accountName: 'HDFC',
		});

		expect(transactionPrimaryLabel(item)).toBe('Dinner with the team');
		expect(transactionMetaLabel(item, 'daily')).toBe('Bistro · HDFC · Food');
	});

	it('uses the normalized merchant when no note exists', () => {
		expect(transactionPrimaryLabel(transaction({
			merchantName: 'Blinkit',
			counterpartyRaw: 'BLINKIT PAYMENTS',
		}))).toBe('Blinkit');
	});

	it('falls back to the detected payee for an unmapped P2P transaction', () => {
		expect(transactionPrimaryLabel(transaction({
			merchantType: 'P2P',
			counterpartyRaw: 'Vasu Khandelwal',
		}))).toBe('Vasu Khandelwal');
	});
});

describe('person transfer artwork', () => {
	it('takes the first and last initials of a person name', () => {
		expect(personInitials('Vasu Khandelwal')).toBe('VK');
		expect(personInitials('  arjun   singh  rathore ')).toBe('AR');
		expect(personInitials('Meera')).toBe('M');
	});

	it('returns no initials when the payee carries no letters', () => {
		expect(personInitials('9876543210')).toBeNull();
		expect(personInitials('')).toBeNull();
		expect(personInitials(null)).toBeNull();
	});

	it('gives the same person the same tint every render', () => {
		expect(personTint('Vasu Khandelwal')).toBe(personTint('Vasu Khandelwal'));
		expect(personTint('')).toMatch(/^var\(--ledger-/);
	});

	it('reads direction from the bank marker before the transaction type', () => {
		expect(isIncomingTransaction(transaction({ type: 'transfer', direction: 'credit' }))).toBe(true);
		expect(isIncomingTransaction(transaction({ type: 'transfer', direction: 'debit' }))).toBe(false);
		expect(isIncomingTransaction(transaction({ type: 'income' }))).toBe(true);
		expect(isIncomingTransaction(transaction({ type: 'expense' }))).toBe(false);
	});
});
