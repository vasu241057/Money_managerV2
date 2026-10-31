// The mapper's per-user persistence on the real schema: a person answer
// becomes one private payee per user who pays that key (never a shared,
// name-bearing row), a brand stays one shared row, relinking gives each user
// their own merchant, a key already resolved as a person fans out to a new
// payer without a model call, and a shared alias stops the fan-out.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { loadGlobalAliasesForLookup } from '../../src/lib/identity';
import { fanOutKnownPersons, persistMappings, relinkTransactions } from '../../src/workers/merchant-mapper';
import { seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

interface IdRow {
	id: string;
}

interface MerchantRow {
	id: string;
	canonical_name: string;
	type: string;
	owner_user_id: string | null;
}

async function insertKeyedTransaction(sql: SqlClient, userId: string, searchKey: string, index: number): Promise<string> {
	const events = await sql<IdRow[]>`
		insert into public.financial_events (user_id, raw_email_id, extraction_index, direction, amount_in_paise, txn_timestamp, payment_method, counterparty_raw, search_key)
		values (${userId}, null, ${index}, ${'debit'}, ${2_000}, ${'2026-09-10T10:00:00Z'}, ${'upi'}, ${searchKey}, ${searchKey})
		returning id
	`;
	const rows = await sql<IdRow[]>`
		insert into public.transactions (user_id, financial_event_id, amount_in_paise, type, txn_date)
		values (${userId}, ${events[0]?.id as string}, ${2_000}, ${'expense'}, ${'2026-09-10T10:00:00Z'})
		returning id
	`;
	return rows[0]?.id as string;
}

describe('tenancy: mapper fan-out of private payees', () => {
	let tenancy: TenancyDb;
	let fx: TwoTenantFixture;
	let txnA: string;
	let txnB: string;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		fx = await seedTwoTenants(tenancy.sql);
		txnA = await insertKeyedTransaction(tenancy.sql, fx.a.userId, 'NEWPERSON', 401);
		txnB = await insertKeyedTransaction(tenancy.sql, fx.b.userId, 'NEWPERSON', 402);
		await insertKeyedTransaction(tenancy.sql, fx.a.userId, 'NEWBRAND', 403);
		await insertKeyedTransaction(tenancy.sql, fx.b.userId, 'NEWBRAND', 404);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	async function merchantsNamed(name: string): Promise<MerchantRow[]> {
		return tenancy.sql<MerchantRow[]>`
			select id, canonical_name, type, owner_user_id from public.global_merchants
			where canonical_name = ${name} order by owner_user_id nulls first
		`;
	}

	it('writes a person once per paying user and a brand once, shared', async () => {
		const usersByKey = new Map<string, readonly string[]>([
			['NEWPERSON', [fx.a.userId, fx.b.userId]],
			['NEWBRAND', [fx.a.userId, fx.b.userId]],
		]);
		const persisted = await persistMappings(
			tenancy.sql,
			[
				{ search_key: 'NEWPERSON', canonical_name: 'New Person', merchant_type: 'P2P', default_category_id: null },
				{ search_key: 'NEWBRAND', canonical_name: 'New Brand', merchant_type: 'MERCHANT', default_category_id: null },
			],
			usersByKey,
		);
		expect(persisted).toEqual({ aliasCount: 3, ownedAliasCount: 2, mappedKeys: ['NEWPERSON', 'NEWBRAND'] });

		const people = await merchantsNamed('New Person');
		expect(people.map((row) => row.owner_user_id).sort()).toEqual([fx.a.userId, fx.b.userId].sort());
		expect(people.every((row) => row.type === 'P2P')).toBe(true);
		const brands = await merchantsNamed('New Brand');
		expect(brands).toHaveLength(1);
		expect(brands[0]?.owner_user_id).toBeNull();

		const relinked = await relinkTransactions(tenancy.sql, persisted.mappedKeys);
		expect(relinked).toBe(4);
		const linked = await tenancy.sql<Array<{ id: string; merchant_id: string }>>`
			select t.id, t.merchant_id from public.transactions t where t.id in (${txnA}, ${txnB}) order by t.id
		`;
		const byTxn = new Map(linked.map((row) => [row.id, row.merchant_id]));
		const ownerOf = new Map(people.map((row) => [row.id, row.owner_user_id]));
		expect(ownerOf.get(byTxn.get(txnA) as string)).toBe(fx.a.userId);
		expect(ownerOf.get(byTxn.get(txnB) as string)).toBe(fx.b.userId);

		const forA = await loadGlobalAliasesForLookup(tenancy.sql, ['NEWPERSON', 'NEWBRAND'], fx.a.userId);
		expect(ownerOf.get(forA.get('NEWPERSON')?.merchant_id as string)).toBe(fx.a.userId);
		expect(forA.get('NEWBRAND')?.merchant_id).toBe(brands[0]?.id);
	});

	it('is a no-op for users who already have the payee', async () => {
		const again = await persistMappings(
			tenancy.sql,
			[{ search_key: 'NEWPERSON', canonical_name: 'New Person', merchant_type: 'P2P', default_category_id: null }],
			new Map([['NEWPERSON', [fx.a.userId, fx.b.userId]]]),
		);
		expect(again).toEqual({ aliasCount: 0, ownedAliasCount: 0, mappedKeys: [] });
	});

	it('fans a known person out to a new payer without a model call, then stops', async () => {
		const stranger = await tenancy.sql<IdRow[]>`
			insert into public.users (email, auth_provider, auth_subject) values (${'charlie@example.test'}, ${'clerk'}, ${'user_fixture_charlie'}) returning id
		`;
		const charlie = stranger[0]?.id as string;
		const txnC = await insertKeyedTransaction(tenancy.sql, charlie, 'NEWPERSON', 405);
		await tenancy.sql`
			insert into public.merchant_mapping_attempts (search_key, last_resolution, person_display_name)
			values (${'NEWPERSON'}, ${'P2P'}, ${'New Person'})
			on conflict (search_key) do update set last_resolution = excluded.last_resolution, person_display_name = excluded.person_display_name
		`;

		const fanout = await fanOutKnownPersons(tenancy.sql);
		expect(fanout).toEqual({ keyCount: 1, aliasCount: 1, mappedKeys: ['NEWPERSON'] });
		await relinkTransactions(tenancy.sql, fanout.mappedKeys);

		const people = await merchantsNamed('New Person');
		expect(people).toHaveLength(3);
		const charliesMerchant = people.find((row) => row.owner_user_id === charlie);
		expect(charliesMerchant).toBeDefined();
		const linked = await tenancy.sql<Array<{ merchant_id: string }>>`select merchant_id from public.transactions where id = ${txnC}`;
		expect(linked[0]?.merchant_id).toBe(charliesMerchant?.id);

		expect(await fanOutKnownPersons(tenancy.sql)).toEqual({ keyCount: 0, aliasCount: 0, mappedKeys: [] });
	});

	it('never fans out a key that has a shared alias', async () => {
		await tenancy.sql`
			insert into public.merchant_mapping_attempts (search_key, last_resolution, person_display_name)
			values (${'NEWBRAND'}, ${'P2P'}, ${'Mistaken Person'})
			on conflict (search_key) do update set last_resolution = excluded.last_resolution, person_display_name = excluded.person_display_name
		`;
		expect(await fanOutKnownPersons(tenancy.sql)).toEqual({ keyCount: 0, aliasCount: 0, mappedKeys: [] });
		expect(await merchantsNamed('Mistaken Person')).toEqual([]);
	});
});
