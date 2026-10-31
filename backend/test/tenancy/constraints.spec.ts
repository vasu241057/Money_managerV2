// Proves the database itself rejects cross-tenant references, independent of
// any service code. Each offending statement runs inside a transaction that is
// always rolled back: a rejected insert aborts it, an accepted one hits the
// sentinel throw. The last four cases were known gaps until Task 2's
// migration added the card_statements composite FK and the category scope
// triggers.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { insertLooseEvent, seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

const REJECTION = /foreign key|violates|must belong|must match|must reference|cannot change|not present|constraint/i;
const SENTINEL = 'TENANCY_ROLLBACK_SENTINEL';

type Attempt = (tx: SqlClient) => Promise<unknown>;

describe('tenancy: database backstops reject cross-user references', () => {
	let tenancy: TenancyDb;
	let fx: TwoTenantFixture;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		fx = await seedTwoTenants(tenancy.sql);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	/** Runs the statement in a transaction that never commits; resolves to the rejection error message. */
	async function attempt(statement: Attempt): Promise<string> {
		try {
			await tenancy.sql.begin(async (tx) => {
				await statement(tx as unknown as SqlClient);
				throw new Error(SENTINEL);
			});
		} catch (error) {
			return error instanceof Error ? error.message : String(error);
		}
		throw new Error('transaction unexpectedly committed');
	}

	function expectRejected(message: string): void {
		expect(message, 'statement was accepted by the database').not.toBe(SENTINEL);
		expect(message).toMatch(REJECTION);
	}

	it('transaction cannot point at another user’s financial event', async () => {
		const { a, b } = fx;
		const eventId = await insertLooseEvent(tenancy.sql, b.userId, null, 91);
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.transactions (user_id, financial_event_id, amount_in_paise, type, txn_date)
					values (${a.userId}, ${eventId}, ${12345}, ${'expense'}, ${'2026-09-07T10:00:00Z'})
				`,
			),
		);
	});

	it('transaction cannot use another user’s account', async () => {
		const { a, b } = fx;
		const eventId = await insertLooseEvent(tenancy.sql, a.userId, null, 92);
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.transactions (user_id, financial_event_id, account_id, amount_in_paise, type, txn_date)
					values (${a.userId}, ${eventId}, ${b.bankAccountId}, ${12345}, ${'expense'}, ${'2026-09-07T10:00:00Z'})
				`,
			),
		);
	});

	it('transaction cannot use another user’s credit card', async () => {
		const { a, b } = fx;
		const eventId = await insertLooseEvent(tenancy.sql, a.userId, null, 93);
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.transactions (user_id, financial_event_id, credit_card_id, amount_in_paise, type, txn_date)
					values (${a.userId}, ${eventId}, ${b.creditCardId}, ${12345}, ${'expense'}, ${'2026-09-07T10:00:00Z'})
				`,
			),
		);
	});

	it('transaction cannot use another user’s category', async () => {
		const { a, b } = fx;
		const eventId = await insertLooseEvent(tenancy.sql, a.userId, null, 94);
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.transactions (user_id, financial_event_id, category_id, amount_in_paise, type, txn_date)
					values (${a.userId}, ${eventId}, ${b.expenseCategoryId}, ${12345}, ${'expense'}, ${'2026-09-07T10:00:00Z'})
				`,
			),
		);
	});

	it('transaction cannot join another user’s recurring series', async () => {
		const { a, b } = fx;
		const eventId = await insertLooseEvent(tenancy.sql, a.userId, null, 95);
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.transactions (user_id, financial_event_id, recurring_series_id, amount_in_paise, type, txn_date)
					values (${a.userId}, ${eventId}, ${b.seriesId}, ${12345}, ${'expense'}, ${'2026-09-07T10:00:00Z'})
				`,
			),
		);
	});

	it('financial event cannot point at another user’s raw email', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.financial_events (
						user_id, raw_email_id, extraction_index, direction, amount_in_paise, txn_timestamp, payment_method
					)
					values (${a.userId}, ${b.rawEmailIds[0] ?? null}, ${77}, ${'debit'}, ${100}, ${'2026-09-07T10:00:00Z'}, ${'upi'})
				`,
			),
		);
	});

	it('raw email cannot point at another user’s mailbox connection', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.raw_emails (user_id, oauth_connection_id, source_id, internal_date, clean_text)
					values (${a.userId}, ${b.connectionId}, ${'cross-msg'}, ${'2026-09-07T10:00:00Z'}, ${'body'})
				`,
			),
		);
	});

	it('merchant rule cannot use another user’s category', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.user_merchant_rules (user_id, search_key, custom_category_id)
					values (${a.userId}, ${'CROSSRULE'}, ${b.expenseCategoryId})
				`,
			),
		);
	});

	it('merchant rule category cannot use another user’s category', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.user_merchant_rule_categories (user_merchant_rule_id, transaction_type, category_id)
					values (${a.ruleId}, ${'income'}, ${b.incomeCategoryId})
				`,
			),
		);
	});

	it('merchant category rule cannot use another user’s category', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.user_merchant_category_rules (user_id, merchant_id, transaction_type, category_id)
					values (${a.userId}, ${a.p2pMerchantId}, ${'expense'}, ${b.expenseCategoryId})
				`,
			),
		);
	});

	it('relationship cannot target another user’s transaction', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.transaction_relationships (
						user_id, source_transaction_id, target_transaction_id, kind, created_by, confidence
					)
					values (${a.userId}, ${a.needsReviewTxnId}, ${b.expenseTxnId}, ${'REFUND'}, ${'USER'}, ${0.5})
				`,
			),
		);
	});

	it('credit card cannot be backed by another user’s account', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.user_credit_cards (user_id, account_id, card_label, first4, last4)
					values (${a.userId}, ${b.cardAccountId}, ${'cross card'}, ${'4111'}, ${'9999'})
				`,
			),
		);
	});

	// ---- Backstops added by Task 4a (20260918120000_owned_merchants) ----

	it('transaction cannot point at another user’s private payee', async () => {
		const { a, b } = fx;
		const eventId = await insertLooseEvent(tenancy.sql, a.userId, null, 96);
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.transactions (user_id, financial_event_id, merchant_id, amount_in_paise, type, txn_date)
					values (${a.userId}, ${eventId}, ${b.p2pMerchantId}, ${12345}, ${'expense'}, ${'2026-09-07T10:00:00Z'})
				`,
			),
		);
	});

	it('merchant rule cannot point at another user’s private payee', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.user_merchant_rules (user_id, search_key, merchant_id)
					values (${a.userId}, ${'CROSSPAYEE'}, ${b.p2pMerchantId})
				`,
			),
		);
	});

	it('merchant category rule cannot point at another user’s private payee', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.user_merchant_category_rules (user_id, merchant_id, transaction_type, category_id)
					values (${a.userId}, ${b.p2pMerchantId}, ${'expense'}, ${a.expenseCategoryId})
				`,
			),
		);
	});

	it('recurring series cannot point at another user’s private payee', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.recurring_series (
						user_id, fingerprint, merchant_id, direction, cadence, expected_day,
						amount_min_in_paise, amount_max_in_paise
					)
					values (${a.userId}, ${'cross-payee-series'}, ${b.p2pMerchantId}, ${'debit'}, ${'MONTHLY'}, ${1}, ${100}, ${100})
				`,
			),
		);
	});

	it('a confirmation on a private payee cannot come from another user', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.merchant_confirmations (merchant_id, user_id, verdict)
					values (${a.p2pMerchantId}, ${b.userId}, ${'CONFIRM'})
				`,
			),
		);
	});

	it('an alias cannot carry a different owner than its merchant', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.global_merchant_aliases (search_key, merchant_id, owner_user_id)
					values (${'MISOWNED'}, ${a.p2pMerchantId}, ${b.userId})
				`,
			),
		);
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.global_merchant_aliases (search_key, merchant_id, owner_user_id)
					values (${'MISOWNED'}, ${fx.sharedMerchantId}, ${a.userId})
				`,
			),
		);
	});

	it('a merchant cannot be handed to a user while another user’s rows reference it', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					update public.global_merchants set owner_user_id = ${b.userId} where id = ${a.p2pMerchantId}
				`,
			),
		);
	});

	it('shared brands and the user’s own payee are accepted everywhere', async () => {
		const { a } = fx;
		const eventId = await insertLooseEvent(tenancy.sql, a.userId, null, 97);
		const outcome = await attempt(async (tx) => {
			await tx`
				insert into public.transactions (user_id, financial_event_id, merchant_id, amount_in_paise, type, txn_date)
				values (${a.userId}, ${eventId}, ${a.p2pMerchantId}, ${12345}, ${'expense'}, ${'2026-09-07T10:00:00Z'})
			`;
			await tx`
				insert into public.user_merchant_rules (user_id, search_key, merchant_id)
				values (${a.userId}, ${'OWNPAYEE'}, ${a.p2pMerchantId})
			`;
			await tx`
				insert into public.user_merchant_rules (user_id, search_key, merchant_id)
				values (${a.userId}, ${'SHAREDBRAND2'}, ${fx.sharedMerchantId})
			`;
		});
		expect(outcome).toBe(SENTINEL);
	});

	// ---- Backstops added by Task 2 (20260918090000_multitenant_identity_hardening) ----

	it('card statement cannot point at another user’s credit card', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.card_statements (user_id, credit_card_id, card_last4, due_date, source)
					values (${a.userId}, ${b.creditCardId}, ${'2222'}, ${'2026-10-31'}, ${'USER'})
				`,
			),
		);
	});

	it('budget category rule cannot use another user’s category', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.user_category_budget_rules (user_id, category_id, treatment)
					values (${a.userId}, ${b.expenseCategoryId}, ${'EXCLUDE'})
				`,
			),
		);
	});

	it('hidden-category row cannot name another user’s category', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.user_hidden_categories (user_id, category_id)
					values (${a.userId}, ${b.expenseCategoryId})
				`,
			),
		);
	});

	it('recurring series cannot use another user’s category', async () => {
		const { a, b } = fx;
		expectRejected(
			await attempt(
				(tx) => tx`
					insert into public.recurring_series (
						user_id, fingerprint, category_id, direction, cadence, expected_day,
						amount_min_in_paise, amount_max_in_paise
					)
					values (${a.userId}, ${'cross-series'}, ${b.expenseCategoryId}, ${'debit'}, ${'MONTHLY'}, ${1}, ${100}, ${100})
				`,
			),
		);
	});
});
