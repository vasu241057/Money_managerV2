// Owner-scoped merchant reads on the real schema: the alias lookup every
// extraction path uses resolves the SAME search key to each user's own payee
// (and to nothing for a stranger), an owned alias outranks a shared one, and
// the merchant search returns shared brands plus the caller's payees only.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { loadGlobalAliasesForLookup } from '../../src/lib/identity';
import { listGlobalMerchants } from '../../src/services/merchants.service';
import { seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

const STRANGER = '00000000-0000-4000-8000-00000000dead';
const SENTINEL = 'TENANCY_ROLLBACK_SENTINEL';

describe('tenancy: merchant ownership reads', () => {
	let tenancy: TenancyDb;
	let fx: TwoTenantFixture;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		fx = await seedTwoTenants(tenancy.sql);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	it('resolves the same alias key to each user’s own payee, and to nothing for a stranger', async () => {
		const keys = [fx.a.p2pAliasKey, fx.sharedAliasKey];
		const forA = await loadGlobalAliasesForLookup(tenancy.sql, keys, fx.a.userId);
		const forB = await loadGlobalAliasesForLookup(tenancy.sql, keys, fx.b.userId);
		const forStranger = await loadGlobalAliasesForLookup(tenancy.sql, keys, STRANGER);

		expect(forA.get(fx.a.p2pAliasKey)?.merchant_id).toBe(fx.a.p2pMerchantId);
		expect(forB.get(fx.b.p2pAliasKey)?.merchant_id).toBe(fx.b.p2pMerchantId);
		expect(forStranger.has(fx.a.p2pAliasKey)).toBe(false);
		for (const lookup of [forA, forB, forStranger]) {
			expect(lookup.get(fx.sharedAliasKey)?.merchant_id).toBe(fx.sharedMerchantId);
			expect(lookup.get(fx.sharedAliasKey)?.merchant_type).toBe('MERCHANT');
		}
		expect(forA.get(fx.a.p2pAliasKey)?.merchant_type).toBe('P2P');
	});

	it('lets an owned alias outrank a shared alias on the same key', async () => {
		let outcome = '';
		try {
			await tenancy.sql.begin(async (tx) => {
				const txSql = tx as unknown as SqlClient;
				const brand = await txSql<Array<{ id: string }>>`
					insert into public.global_merchants (canonical_name, type, source, confirmed)
					values (${'Fixture Person Brand'}, ${'MERCHANT'}, ${'AI'}, ${true})
					returning id
				`;
				await txSql`
					insert into public.global_merchant_aliases (search_key, merchant_id)
					values (${fx.a.p2pAliasKey}, ${brand[0]?.id as string})
				`;
				const forA = await loadGlobalAliasesForLookup(txSql, [fx.a.p2pAliasKey], fx.a.userId);
				const forStranger = await loadGlobalAliasesForLookup(txSql, [fx.a.p2pAliasKey], STRANGER);
				expect(forA.get(fx.a.p2pAliasKey)?.merchant_id).toBe(fx.a.p2pMerchantId);
				expect(forStranger.get(fx.a.p2pAliasKey)?.merchant_id).toBe(brand[0]?.id);
				throw new Error(SENTINEL);
			});
		} catch (error) {
			outcome = error instanceof Error ? error.message : String(error);
		}
		expect(outcome).toBe(SENTINEL);
	});

	it('merchant search returns shared brands plus the caller’s own payees only', async () => {
		const forA = await listGlobalMerchants(tenancy.sql, fx.a.userId, { q: 'Fixture', limit: 100 });
		const idsForA = forA.map((row) => row.id);
		expect(idsForA).toContain(fx.sharedMerchantId);
		expect(idsForA).toContain(fx.a.p2pMerchantId);
		expect(idsForA).not.toContain(fx.b.p2pMerchantId);

		const forB = await listGlobalMerchants(tenancy.sql, fx.b.userId, { q: 'Person', limit: 100 });
		expect(forB.map((row) => row.id)).toEqual([fx.b.p2pMerchantId]);
	});
});
