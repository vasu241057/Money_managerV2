import { describe, expect, it } from 'vitest';

import { maskNumericTokens, parseDateTextToIso, resolveMaskedDateText } from '../src/lib/masking';

describe('parseDateTextToIso', () => {
	it('parses the classic Indian formats (midnight IST when no time)', () => {
		expect(parseDateTextToIso('05-07-26')).toBe('2026-07-04T18:30:00.000Z');
		expect(parseDateTextToIso('04-Mar-2026')).toBe('2026-03-03T18:30:00.000Z');
		expect(parseDateTextToIso('Mar 04, 2026')).toBe('2026-03-03T18:30:00.000Z');
		expect(parseDateTextToIso('2026-03-04')).toBe('2026-03-03T18:30:00.000Z');
		expect(parseDateTextToIso('05-07-26 14:32')).toBe('2026-07-05T09:02:00.000Z');
	});

	it('parses month names with ordinal day suffixes', () => {
		expect(parseDateTextToIso('May 18, 2026')).toBe('2026-05-17T18:30:00.000Z');
		expect(parseDateTextToIso('May 18th, 2026')).toBe('2026-05-17T18:30:00.000Z');
		expect(parseDateTextToIso('18th May 2026')).toBe('2026-05-17T18:30:00.000Z');
	});

	it('handles AM/PM times', () => {
		expect(parseDateTextToIso('05-07-26 02:45 PM')).toBe('2026-07-05T09:15:00.000Z');
		expect(parseDateTextToIso('05-07-26 02:45 AM')).toBe('2026-07-04T21:15:00.000Z');
		expect(parseDateTextToIso('05-07-26 12:10 AM')).toBe('2026-07-04T18:40:00.000Z');
		expect(parseDateTextToIso('05-07-26 12:10 PM')).toBe('2026-07-05T06:40:00.000Z');
	});

	it('parses dot separators and ISO timestamps with a time part', () => {
		expect(parseDateTextToIso('18.05.2026')).toBe('2026-05-17T18:30:00.000Z');
		expect(parseDateTextToIso('2026-05-18T14:30:00')).toBe('2026-05-18T09:00:00.000Z');
	});

	it('reads an unambiguous MDY date correctly', () => {
		// There is no month 18, so only the month-first reading is valid.
		expect(parseDateTextToIso('05-18-26')).toBe('2026-05-17T18:30:00.000Z');
	});

	it('disambiguates DMY vs MDY using the email arrival date', () => {
		// 06-05-26 is June 5 (MDY) or May 6 (DMY); the reading closer to the
		// email's own arrival wins.
		expect(parseDateTextToIso('06-05-26', '2026-06-04T10:00:00.000Z')).toBe(
			'2026-06-04T18:30:00.000Z',
		);
		expect(parseDateTextToIso('06-05-26', '2026-05-07T10:00:00.000Z')).toBe(
			'2026-05-05T18:30:00.000Z',
		);
		// No reference: day-first, the Indian convention.
		expect(parseDateTextToIso('06-05-26')).toBe('2026-05-05T18:30:00.000Z');
	});

	it('keeps day-first when the reference is too far from either reading to carry signal', () => {
		// Reference is years away from both June 7 and July 6, 2026: the
		// marginally-closer month-first reading must NOT flip the convention.
		expect(parseDateTextToIso('06-07-26', '2023-11-14T22:20:00.000Z')).toBe(
			'2026-07-05T18:30:00.000Z',
		);
	});
});

describe('resolveMaskedDateText', () => {
	it('resolves token refs then disambiguates against the fallback date', () => {
		const masked = maskNumericTokens('Payment on 06-05-26 confirmed');
		expect(
			resolveMaskedDateText('<N1>-<N2>-<N3>', masked.tokens, '2026-06-04T10:00:00.000Z'),
		).toBe('2026-06-04T18:30:00.000Z');
	});
});
