import { describe, expect, it } from 'vitest';

import { rankRefundMatches, type RefundTargetCandidate } from '../src/services/refund-matcher';

const source = {
	id: 'refund',
	amountInPaise: 2_000_00,
	date: '2026-08-04T10:00:00.000Z',
	cardId: 'card-1',
	merchantId: 'merchant-amazon',
	searchKey: 'AMAZON',
};

function target(id: string, overrides: Partial<RefundTargetCandidate> = {}): RefundTargetCandidate {
	return {
		id,
		amountInPaise: 6_000_00,
		remainingInPaise: 6_000_00,
		date: '2026-07-29T10:00:00.000Z',
		cardId: 'card-1',
		merchantId: 'merchant-amazon',
		searchKey: 'AMAZON',
		label: 'Amazon',
		...overrides,
	};
}

describe('rankRefundMatches', () => {
	it('auto-links a unique capable same-card and same-merchant partial refund', () => {
		const matches = rankRefundMatches(source, [target('purchase')]);
		expect(matches).toHaveLength(1);
		expect(matches[0]?.canAutoLink).toBe(true);
		expect(matches[0]?.confidence).toBeGreaterThanOrEqual(0.94);
	});

	it('never auto-links when more than one purchase can absorb the refund', () => {
		const matches = rankRefundMatches(source, [target('one'), target('two')]);
		expect(matches).toHaveLength(2);
		expect(matches.every(match => !match.canAutoLink)).toBe(true);
	});

	it('rejects a refund larger than the remaining purchase amount', () => {
		expect(rankRefundMatches(source, [target('purchase', { remainingInPaise: 1_000_00 })])).toEqual([]);
	});

	it('keeps a non-card exact merchant match as a suggestion only', () => {
		const matches = rankRefundMatches(
			{ ...source, cardId: null },
			[target('purchase', { cardId: null, amountInPaise: 2_000_00, remainingInPaise: 2_000_00 })],
		);
		expect(matches[0]?.canAutoLink).toBe(false);
		expect(matches[0]?.confidence).toBeGreaterThanOrEqual(0.8);
	});

	it('still matches by search key while merchant enrichment is incomplete on one side', () => {
		const matches = rankRefundMatches(
			{ ...source, merchantId: 'merchant-amazon' },
			[target('purchase', { merchantId: null })],
		);
		expect(matches).toHaveLength(1);
		expect(matches[0]?.canAutoLink).toBe(true);
	});

	it('ignores purchases after the credit or older than 120 days', () => {
		expect(rankRefundMatches(source, [target('future', { date: '2026-08-05T10:00:00.000Z' })])).toEqual([]);
		expect(rankRefundMatches(source, [target('old', { date: '2026-01-01T10:00:00.000Z' })])).toEqual([]);
	});
});
