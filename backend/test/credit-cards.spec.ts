import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	createUserCreditCard,
	parseCreateUserCreditCardRequest,
	parseUpdateUserCreditCardRequest,
	relinkTransactionsForCreditCard,
} from '../src/services/credit-cards.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlMock(handlers: QueryHandler[]): SqlClient {
	let cursor = 0;

	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		const handler = handlers[cursor];
		cursor += 1;

		if (!handler) {
			throw new Error(`Unexpected query execution: ${query}`);
		}

		return handler(query, values);
	};

	return tag as unknown as SqlClient;
}

const USER_ID = '11111111-1111-4111-8111-111111111111';
const ACCOUNT_ID = '22222222-2222-4222-8222-222222222222';
const CARD_ID = '33333333-3333-4333-8333-333333333333';

const CARD_ROW = {
	id: CARD_ID,
	user_id: USER_ID,
	account_id: ACCOUNT_ID,
	card_label: 'HDFC Regalia',
	issuer_name: 'HDFC Bank',
	network: 'visa',
	first4: '4321',
	last4: '9876',
	is_active: true,
	created_at: '2026-07-11T10:00:00.000Z',
	updated_at: '2026-07-11T10:00:00.000Z',
};

describe('credit card request parsing', () => {
	it('parses a valid create payload', () => {
		const parsed = parseCreateUserCreditCardRequest({
			account_id: ACCOUNT_ID,
			card_label: 'HDFC Regalia',
			issuer_name: 'HDFC Bank',
			network: 'visa',
			first4: '4321',
			last4: '9876',
		});
		expect(parsed.last4).toBe('9876');
		expect(parsed.is_active).toBe(true);
	});

	it('rejects malformed last4, network, and account ids', () => {
		const base = {
			account_id: ACCOUNT_ID,
			card_label: 'Card',
			first4: '4321',
			last4: '9876',
		};
		expect(() =>
			parseCreateUserCreditCardRequest({ ...base, last4: '98x6' }),
		).toThrowError(/4-digit/);
		expect(() =>
			parseCreateUserCreditCardRequest({ ...base, network: 'plutonium' }),
		).toThrowError(/network/);
		expect(() =>
			parseCreateUserCreditCardRequest({ ...base, account_id: 'not-a-uuid' }),
		).toThrowError(/account_id/);
	});

	it('rejects an empty update payload', () => {
		expect(() => parseUpdateUserCreditCardRequest({})).toThrowError(/At least one/);
	});
});

describe('createUserCreditCard', () => {
	it('rejects a backing account that is not of type card', async () => {
		const sql = createSqlMock([() => [{ id: ACCOUNT_ID, type: 'bank' }]]);

		await expect(
			createUserCreditCard(sql, USER_ID, {
				account_id: ACCOUNT_ID,
				card_label: 'HDFC Regalia',
				first4: '4321',
				last4: '9876',
			}),
		).rejects.toMatchObject({ statusCode: 400, errorCode: 'ACCOUNT_NOT_CARD' });
	});

	it('creates the card and retroactively links matching transactions', async () => {
		const queries: string[] = [];
		const sql = createSqlMock([
			query => {
				queries.push(query);
				return [{ id: ACCOUNT_ID, type: 'card' }];
			},
			query => {
				queries.push(query);
				return [CARD_ROW];
			},
			// relink: card lookup, ambiguity count, then the update.
			query => {
				queries.push(query);
				return [{ id: CARD_ID, account_id: ACCOUNT_ID, last4: '9876' }];
			},
			query => {
				queries.push(query);
				return [{ count: 1 }];
			},
			query => {
				queries.push(query);
				return [{ id: 'txn-1' }, { id: 'txn-2' }];
			},
		]);

		const { card, relinked_transaction_count } = await createUserCreditCard(sql, USER_ID, {
			account_id: ACCOUNT_ID,
			card_label: 'HDFC Regalia',
			issuer_name: 'HDFC Bank',
			network: 'visa',
			first4: '4321',
			last4: '9876',
		});

		expect(card.id).toBe(CARD_ID);
		expect(relinked_transaction_count).toBe(2);

		const relinkUpdate = queries[4] ?? '';
		// The relink must only touch fully unlinked credit-card rows (manual
		// review sets account_id, so user-edited rows are excluded by the null
		// guards). It must NOT filter on classification_source: engine rows
		// classified via a user merchant rule carry source USER yet still
		// deserve the link, same as at insert time.
		expect(relinkUpdate).not.toContain('classification_source');
		expect(relinkUpdate).toContain('credit_card_id is null');
		expect(relinkUpdate).toContain('account_id is null');
		expect(relinkUpdate).toContain("payment_method = 'credit_card'");
	});
});

describe('relinkTransactionsForCreditCard', () => {
	it('skips relinking entirely when the last4 is ambiguous among active cards', async () => {
		const sql = createSqlMock([
			() => [{ id: CARD_ID, account_id: ACCOUNT_ID, last4: '9876' }],
			() => [{ count: 2 }],
		]);

		const relinked = await relinkTransactionsForCreditCard(sql, USER_ID, CARD_ID);
		expect(relinked).toBe(0);
	});

	it('does nothing for an inactive or missing card', async () => {
		const sql = createSqlMock([() => []]);
		const relinked = await relinkTransactionsForCreditCard(sql, USER_ID, CARD_ID);
		expect(relinked).toBe(0);
	});
});
