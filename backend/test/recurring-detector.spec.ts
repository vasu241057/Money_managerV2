import { describe, expect, it } from 'vitest';

import { detectRecurringPatterns, type RecurringOccurrence } from '../src/services/recurring-detector';

function occurrence(id: string, date: string, amountInPaise = 499_00): RecurringOccurrence {
	return {
		id,
		amountInPaise,
		date,
		direction: 'debit',
		merchantId: 'merchant-netflix',
		searchKey: 'NETFLIX',
		categoryId: 'subscriptions',
		label: 'Netflix',
		categoryTreatment: 'EXCLUDE',
	};
}

describe('detectRecurringPatterns', () => {
	it('suggests a monthly pattern only after three consistent occurrences', () => {
		expect(detectRecurringPatterns([
			occurrence('one', '2026-06-05T00:00:00.000Z'),
			occurrence('two', '2026-07-05T00:00:00.000Z'),
		])).toEqual([]);
		const patterns = detectRecurringPatterns([
			occurrence('one', '2026-06-05T00:00:00.000Z'),
			occurrence('two', '2026-07-05T00:00:00.000Z'),
			occurrence('three', '2026-08-05T00:00:00.000Z'),
		]);
		expect(patterns[0]).toMatchObject({ cadence: 'MONTHLY', expectedDay: 5, suggestedTreatment: 'EXCLUDE' });
	});

	it('detects a weekly cadence', () => {
		const patterns = detectRecurringPatterns([
			occurrence('one', '2026-07-07T00:00:00.000Z'),
			occurrence('two', '2026-07-14T00:00:00.000Z'),
			occurrence('three', '2026-07-21T00:00:00.000Z'),
		]);
		expect(patterns[0]?.cadence).toBe('WEEKLY');
	});

	it('uses the India calendar day for early-morning transactions', () => {
		const patterns = detectRecurringPatterns([
			occurrence('one', '2026-06-04T19:30:00.000Z'),
			occurrence('two', '2026-07-04T19:30:00.000Z'),
			occurrence('three', '2026-08-04T19:30:00.000Z'),
		]);
		expect(patterns[0]?.expectedDay).toBe(5);
	});

	it('rejects inconsistent timing or amount changes over fifteen percent', () => {
		expect(detectRecurringPatterns([
			occurrence('one', '2026-05-01T00:00:00.000Z'),
			occurrence('two', '2026-06-18T00:00:00.000Z'),
			occurrence('three', '2026-08-01T00:00:00.000Z'),
		])).toEqual([]);
		expect(detectRecurringPatterns([
			occurrence('one', '2026-06-05T00:00:00.000Z', 400_00),
			occurrence('two', '2026-07-05T00:00:00.000Z', 600_00),
			occurrence('three', '2026-08-05T00:00:00.000Z', 400_00),
		])).toEqual([]);
	});
});
