import { describe, expect, it } from 'vitest';
import type { Transaction } from '../types/domain';
import {
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
