// Per-user voting on AI merchant mappings, on the real schema through the
// real review service: one user's review is one verdict, the merchant's
// confirmed flag is derived from every user's verdict against the threshold,
// a user changing their mind replaces their own vote, and a private payee's
// verdict can only come from its owner.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { recordMerchantConfirmation } from '../../src/services/merchants.service';
import { reviewTransaction } from '../../src/services/transactions.service';
import { insertLooseEvent, seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

interface IdRow {
	id: string;
}

interface MerchantState {
	confirmed: boolean;
	dissent_count: number;
}

describe('tenancy: merchant confirmations (voting)', () => {
	let tenancy: TenancyDb;
	let fx: TwoTenantFixture;
	let aiBrand: string;

	async function state(merchantId: string): Promise<MerchantState> {
		const rows = await tenancy.sql<MerchantState[]>`
			select confirmed, dissent_count from public.global_merchants where id = ${merchantId}
		`;
		return rows[0] as MerchantState;
	}

	/** A NEEDS_REVIEW transaction for `userId` whose event key hits the AI brand's alias. */
	async function reviewableTransaction(userId: string, extractionIndex: number): Promise<string> {
		const eventId = await insertLooseEvent(tenancy.sql, userId, null, extractionIndex);
		const rows = await tenancy.sql<IdRow[]>`
			insert into public.transactions (user_id, financial_event_id, merchant_id, amount_in_paise, type, txn_date, status, classification_source)
			values (${userId}, ${eventId}, ${aiBrand}, ${12_345}, ${'expense'}, ${'2026-09-07T10:00:00Z'}, ${'NEEDS_REVIEW'}, ${'AI'})
			returning id
		`;
		return rows[0]?.id as string;
	}

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		fx = await seedTwoTenants(tenancy.sql);
		const rows = await tenancy.sql<IdRow[]>`
			insert into public.global_merchants (canonical_name, type, source, confirmed)
			values (${'Loose Payee Co'}, ${'MERCHANT'}, ${'AI'}, ${false})
			returning id
		`;
		aiBrand = rows[0]?.id as string;
		await tenancy.sql`insert into public.global_merchant_aliases (search_key, merchant_id) values (${'LOOSEPAYEE'}, ${aiBrand})`;
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	it('threshold 1: one confirming review confirms, one dissenting review demotes', async () => {
		const txnA = await reviewableTransaction(fx.a.userId, 301);
		await reviewTransaction(tenancy.sql, fx.a.userId, txnA, {}, { merchantConfirmationThreshold: 1 });
		expect(await state(aiBrand)).toEqual({ confirmed: true, dissent_count: 0 });

		const txnB = await reviewableTransaction(fx.b.userId, 302);
		await reviewTransaction(
			tenancy.sql,
			fx.b.userId,
			txnB,
			{ merchant_id: fx.sharedMerchantId },
			{ merchantConfirmationThreshold: 1 },
		);
		expect(await state(aiBrand)).toEqual({ confirmed: false, dissent_count: 1 });

		const verdicts = await tenancy.sql<Array<{ user_id: string; verdict: string }>>`
			select user_id, verdict from public.merchant_confirmations where merchant_id = ${aiBrand} order by verdict
		`;
		expect(verdicts).toEqual([
			{ user_id: fx.a.userId, verdict: 'CONFIRM' },
			{ user_id: fx.b.userId, verdict: 'DISSENT' },
		]);
	});

	it('a user changing their mind replaces their own vote instead of adding one', async () => {
		const result = await recordMerchantConfirmation(tenancy.sql, fx.b.userId, aiBrand, 'CONFIRM', 1);
		expect(result).toEqual({ confirmed: true, confirm_count: 2, dissent_count: 0 });
		expect(await state(aiBrand)).toEqual({ confirmed: true, dissent_count: 0 });
	});

	it('threshold 2: a single confirmation is not enough, a single dissent does not demote', async () => {
		await recordMerchantConfirmation(tenancy.sql, fx.b.userId, aiBrand, 'DISSENT', 2);
		// A confirmed, B dissented: 1 confirm < 2, so not confirmed.
		expect(await state(aiBrand)).toEqual({ confirmed: false, dissent_count: 1 });
		const back = await recordMerchantConfirmation(tenancy.sql, fx.b.userId, aiBrand, 'CONFIRM', 2);
		expect(back).toEqual({ confirmed: true, confirm_count: 2, dissent_count: 0 });
	});

	it('a private payee accepts its owner’s verdict and rejects anyone else’s', async () => {
		const own = await recordMerchantConfirmation(tenancy.sql, fx.a.userId, fx.a.p2pMerchantId, 'CONFIRM', 1);
		expect(own.confirmed).toBe(true);
		await expect(
			recordMerchantConfirmation(tenancy.sql, fx.b.userId, fx.a.p2pMerchantId, 'CONFIRM', 1),
		).rejects.toThrow(/must belong to the same user or be a shared merchant/);
	});
});
