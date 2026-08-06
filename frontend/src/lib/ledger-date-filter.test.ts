import { describe, expect, it } from 'vitest';

import {
	buildLedgerDateQuery,
	formatLedgerDateFilterLabel,
	monthDateInputBounds,
	toLocalDateInputValue,
} from './ledger-date-filter';

describe('Ledger date filters', () => {
	it('formats a Date for a native date input using local calendar parts', () => {
		expect(toLocalDateInputValue(new Date(2026, 7, 4, 18, 30))).toBe('2026-08-04');
	});

	it('turns a single date into inclusive local-day boundaries', () => {
		const query = buildLedgerDateQuery('single', '2026-08-04', '');
		const from = new Date(query.from ?? '');
		const to = new Date(query.to ?? '');

		expect([
			from.getFullYear(), from.getMonth(), from.getDate(),
			from.getHours(), from.getMinutes(), from.getSeconds(), from.getMilliseconds(),
		]).toEqual([2026, 7, 4, 0, 0, 0, 0]);
		expect([
			to.getFullYear(), to.getMonth(), to.getDate(),
			to.getHours(), to.getMinutes(), to.getSeconds(), to.getMilliseconds(),
		]).toEqual([2026, 7, 4, 23, 59, 59, 999]);
	});

	it('requires both dates for a range and normalizes reversed selections', () => {
		expect(buildLedgerDateQuery('range', '2026-08-04', '')).toEqual({});

		const query = buildLedgerDateQuery('range', '2026-08-04', '2026-08-01');
		expect(new Date(query.from ?? '').getDate()).toBe(1);
		expect(new Date(query.to ?? '').getDate()).toBe(4);
	});

	it('expands a budget month into its first and last calendar day', () => {
		expect(monthDateInputBounds('2026-08')).toEqual({ from: '2026-08-01', to: '2026-08-31' });
		expect(monthDateInputBounds('2026-02')).toEqual({ from: '2026-02-01', to: '2026-02-28' });
		// Leap year, so February has to stretch to the 29th.
		expect(monthDateInputBounds('2028-02')).toEqual({ from: '2028-02-01', to: '2028-02-29' });
	});

	it('rejects month values a deep link could not have produced', () => {
		expect(monthDateInputBounds('2026-13')).toBeNull();
		expect(monthDateInputBounds('2026-00')).toBeNull();
		expect(monthDateInputBounds('2026-8')).toBeNull();
		expect(monthDateInputBounds('')).toBeNull();
	});

	it('creates concise labels for a day and a range', () => {
		expect(formatLedgerDateFilterLabel('single', '2026-08-04', '')).toBe('4 Aug 2026');
		expect(formatLedgerDateFilterLabel('range', '2026-08-01', '2026-08-04')).toBe('1–4 Aug 2026');
	});
});
