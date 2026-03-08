import { describe, expect, it } from 'vitest';

import {
	parseCreateManualTransactionRequest,
	parseListTransactionsQuery,
	parseUpdateTransactionRequest,
	prepareTransactionRelationUpdate,
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

	it('rejects create payload with non-positive amount', () => {
		expect(() =>
			parseCreateManualTransactionRequest({
				amount_in_paise: 0,
				type: 'expense',
				txn_date: new Date().toISOString(),
			}),
		).toThrow('amount_in_paise must be a positive integer');
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
