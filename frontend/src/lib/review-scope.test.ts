import { describe, expect, it } from 'vitest';
import { isInLastThreeCalendarDaysIST } from './review-scope';

describe('isInLastThreeCalendarDaysIST', () => {
	const now = new Date('2026-07-24T19:00:00.000Z'); // 25 Jul, 00:30 IST

	it('includes today and the previous two IST calendar dates', () => {
		expect(isInLastThreeCalendarDaysIST(
			new Date('2026-07-22T18:30:00.000Z'), // 23 Jul, 00:00 IST
			now,
		)).toBe(true);
	});

	it('excludes transactions immediately before the three-day IST boundary', () => {
		expect(isInLastThreeCalendarDaysIST(
			new Date('2026-07-22T18:29:59.999Z'), // 22 Jul, 23:59:59.999 IST
			now,
		)).toBe(false);
	});
});
