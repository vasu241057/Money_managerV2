import { describe, expect, it } from 'vitest';

import type {
	InstrumentAccountCandidate,
	InstrumentCardCandidate,
} from '../src/lib/identity';
import { resolveInstrumentLink } from '../src/lib/identity';

const HDFC_CARD: InstrumentCardCandidate = {
	id: 'card-hdfc',
	account_id: 'acct-hdfc-card',
	last4: '9876',
	issuer_name: 'HDFC Bank',
};

const ICICI_CARD: InstrumentCardCandidate = {
	id: 'card-icici',
	account_id: 'acct-icici-card',
	last4: '9876',
	issuer_name: 'ICICI Bank',
};

const BANK_ACCOUNT: InstrumentAccountCandidate = {
	id: 'acct-bank',
	type: 'bank',
	last4: '9876',
};

const CARD_ACCOUNT: InstrumentAccountCandidate = {
	id: 'acct-card',
	type: 'card',
	last4: '4321',
};

describe('resolveInstrumentLink', () => {
	it('links a credit-card alert to the unique card with that last4', () => {
		const link = resolveInstrumentLink({
			payment_method: 'credit_card',
			instrument_last4: '9876',
			from_address: 'alerts@hdfcbank.net',
			accounts: [BANK_ACCOUNT],
			credit_cards: [HDFC_CARD],
		});
		expect(link).toEqual({ credit_card_id: 'card-hdfc', account_id: 'acct-hdfc-card' });
	});

	it('never links a credit-card alert to a bank account with colliding last4', () => {
		// No card registered: the bank account shares the digits but a bank
		// a/c must not own a credit-card transaction.
		const link = resolveInstrumentLink({
			payment_method: 'credit_card',
			instrument_last4: '9876',
			from_address: 'alerts@hdfcbank.net',
			accounts: [BANK_ACCOUNT],
			credit_cards: [],
		});
		expect(link).toEqual({ credit_card_id: null, account_id: null });
	});

	it('breaks a same-last4 tie between two cards using the sender issuer', () => {
		const link = resolveInstrumentLink({
			payment_method: 'credit_card',
			instrument_last4: '9876',
			from_address: 'credit_cards@icicibank.com',
			accounts: [],
			credit_cards: [HDFC_CARD, ICICI_CARD],
		});
		expect(link).toEqual({ credit_card_id: 'card-icici', account_id: 'acct-icici-card' });
	});

	it('leaves an unresolvable same-last4 tie unlinked instead of guessing', () => {
		const link = resolveInstrumentLink({
			payment_method: 'credit_card',
			instrument_last4: '9876',
			from_address: 'noreply@someotherbank.example',
			accounts: [BANK_ACCOUNT],
			credit_cards: [HDFC_CARD, ICICI_CARD],
		});
		expect(link).toEqual({ credit_card_id: null, account_id: null });
	});

	it('links a upi/debit alert to a bank account, never to a card account', () => {
		const upiLink = resolveInstrumentLink({
			payment_method: 'upi',
			instrument_last4: '9876',
			from_address: 'alerts@hdfcbank.net',
			accounts: [BANK_ACCOUNT, CARD_ACCOUNT],
			credit_cards: [HDFC_CARD],
		});
		expect(upiLink).toEqual({ credit_card_id: null, account_id: 'acct-bank' });

		const cardCollision = resolveInstrumentLink({
			payment_method: 'debit_card',
			instrument_last4: '4321',
			from_address: 'alerts@hdfcbank.net',
			accounts: [CARD_ACCOUNT],
			credit_cards: [],
		});
		expect(cardCollision).toEqual({ credit_card_id: null, account_id: null });
	});

	it('requires uniqueness among account matches', () => {
		const link = resolveInstrumentLink({
			payment_method: 'upi',
			instrument_last4: '9876',
			from_address: null,
			accounts: [BANK_ACCOUNT, { id: 'acct-bank-2', type: 'bank', last4: '9876' }],
			credit_cards: [],
		});
		expect(link).toEqual({ credit_card_id: null, account_id: null });
	});

	it('does nothing without a last4', () => {
		const link = resolveInstrumentLink({
			payment_method: 'credit_card',
			instrument_last4: null,
			from_address: 'alerts@hdfcbank.net',
			accounts: [BANK_ACCOUNT],
			credit_cards: [HDFC_CARD],
		});
		expect(link).toEqual({ credit_card_id: null, account_id: null });
	});

	it('falls back to a card-type account when no card row matches', () => {
		const link = resolveInstrumentLink({
			payment_method: 'credit_card',
			instrument_last4: '4321',
			from_address: 'alerts@hdfcbank.net',
			accounts: [BANK_ACCOUNT, CARD_ACCOUNT],
			credit_cards: [HDFC_CARD],
		});
		expect(link).toEqual({ credit_card_id: null, account_id: 'acct-card' });
	});
});
