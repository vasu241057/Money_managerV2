// Rehearsal of the Task 4a data migration on the pre-ownership schema. Seeds
// the shape production could hold (a P2P merchant referenced by two users, one
// referenced by a single user, one referenced by nobody, a shared brand), then
// applies 20260918120000_owned_merchants.sql the way the runner does and
// checks every outcome the plan promises: ids kept, clone per extra user with
// references repointed (recurring_series first, transactions with the reactive
// triggers disabled), orphans deleted, log rows for reversal, triggers back on,
// idempotent re-run, and the backstop assert green.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { applyMigration, createTenancyDb, type TenancyDb } from './pglite-db';

const OWNERSHIP_MIGRATION = '20260918120000_owned_merchants.sql';
// Fixed ids so "lowest user id keeps the original row" is deterministic.
const USER_LOW = '00000000-0000-4000-8000-0000000000aa';
const USER_HIGH = '00000000-0000-4000-8000-0000000000bb';

interface IdRow {
	id: string;
}

interface MerchantRow {
	id: string;
	canonical_name: string;
	type: string;
	owner_user_id: string | null;
	source: string;
	confirmed: boolean;
}

interface LogRow {
	merchant_id: string;
	canonical_name: string;
	action: string;
	new_owner_user_id: string | null;
	cloned_from_merchant_id: string | null;
}

async function insertUser(sql: SqlClient, id: string, label: string): Promise<void> {
	await sql`insert into public.users (id, email) values (${id}, ${`${label}@example.test`})`;
}

async function insertMerchant(sql: SqlClient, name: string, type: string, aliasKey: string): Promise<string> {
	const rows = await sql<IdRow[]>`
		insert into public.global_merchants (canonical_name, type, source, confirmed)
		values (${name}, ${type}, ${'AI'}, ${false})
		returning id
	`;
	const id = rows[0]?.id as string;
	await sql`insert into public.global_merchant_aliases (search_key, merchant_id) values (${aliasKey}, ${id})`;
	return id;
}

async function insertTransaction(sql: SqlClient, userId: string, merchantId: string, index: number): Promise<string> {
	const events = await sql<IdRow[]>`
		insert into public.financial_events (
			user_id, raw_email_id, extraction_index, direction, amount_in_paise, txn_timestamp, payment_method, search_key
		)
		values (${userId}, null, ${index}, ${'debit'}, ${5_000}, ${'2026-09-01T10:00:00Z'}, ${'upi'}, ${'REHEARSALKEY'})
		returning id
	`;
	const rows = await sql<IdRow[]>`
		insert into public.transactions (user_id, financial_event_id, merchant_id, amount_in_paise, type, txn_date)
		values (${userId}, ${events[0]?.id as string}, ${merchantId}, ${5_000}, ${'expense'}, ${'2026-09-01T10:00:00Z'})
		returning id
	`;
	return rows[0]?.id as string;
}

describe('tenancy: Task 4a ownership migration rehearsal', () => {
	let tenancy: TenancyDb;
	let sharedPerson: string;
	let singlePerson: string;
	let orphanPerson: string;
	let brand: string;
	let lowTxn: string;
	let highTxn: string;
	let highSeries: string;
	let groceriesId: string;

	beforeAll(async () => {
		tenancy = await createTenancyDb({ stopBefore: OWNERSHIP_MIGRATION });
		const { sql } = tenancy;
		expect(tenancy.appliedMigrations).not.toContain(OWNERSHIP_MIGRATION);

		await insertUser(sql, USER_LOW, 'low');
		await insertUser(sql, USER_HIGH, 'high');
		const groceries = await sql<IdRow[]>`
			select id from public.categories where user_id is null and is_system and name = ${'Groceries'} and type = ${'expense'} limit 1
		`;
		groceriesId = groceries[0]?.id as string;

		sharedPerson = await insertMerchant(sql, 'Shared Person', 'P2P', 'SHAREDPERSON');
		singlePerson = await insertMerchant(sql, 'Single Person', 'P2P', 'SINGLEPERSON');
		orphanPerson = await insertMerchant(sql, 'Orphan Person', 'P2P', 'ORPHANPERSON');
		brand = await insertMerchant(sql, 'Rehearsal Brand', 'MERCHANT', 'REHEARSALBRAND');

		// Both users reference the shared person through every referencing table.
		lowTxn = await insertTransaction(sql, USER_LOW, sharedPerson, 0);
		highTxn = await insertTransaction(sql, USER_HIGH, sharedPerson, 1);
		await sql`
			insert into public.user_merchant_rules (user_id, search_key, merchant_id)
			values (${USER_LOW}, ${'SHAREDPERSON'}, ${sharedPerson}), (${USER_HIGH}, ${'SHAREDPERSON'}, ${sharedPerson})
		`;
		await sql`
			insert into public.user_merchant_category_rules (user_id, merchant_id, transaction_type, category_id)
			values (${USER_HIGH}, ${sharedPerson}, ${'expense'}, ${groceriesId})
		`;
		const series = await sql<IdRow[]>`
			insert into public.recurring_series (
				user_id, fingerprint, merchant_id, direction, cadence, expected_day, amount_min_in_paise, amount_max_in_paise
			)
			values (${USER_HIGH}, ${'high-shared'}, ${sharedPerson}, ${'debit'}, ${'MONTHLY'}, ${1}, ${5_000}, ${5_000})
			returning id
		`;
		highSeries = series[0]?.id as string;

		// Only the low user references the single person; nobody references the orphan.
		await insertTransaction(sql, USER_LOW, singlePerson, 2);
		// Both reference the brand; it must stay shared.
		await insertTransaction(sql, USER_LOW, brand, 3);
		await insertTransaction(sql, USER_HIGH, brand, 4);
		// Logo research bookkeeping for a person must be dropped.
		await sql`insert into public.merchant_logo_lookup_attempts (merchant_id) values (${singlePerson}), (${brand})`;

		await applyMigration(tenancy.db, OWNERSHIP_MIGRATION);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	async function merchant(id: string): Promise<MerchantRow | undefined> {
		const rows = await tenancy.sql<MerchantRow[]>`
			select id, canonical_name, type, owner_user_id, source, confirmed from public.global_merchants where id = ${id}
		`;
		return rows[0];
	}

	it('keeps the original row for the lowest user id and clones it for the other', async () => {
		const original = await merchant(sharedPerson);
		expect(original?.owner_user_id).toBe(USER_LOW);

		const highRows = await tenancy.sql<Array<{ merchant_id: string }>>`
			select merchant_id from public.transactions where id = ${highTxn}
		`;
		const cloneId = highRows[0]?.merchant_id as string;
		expect(cloneId).not.toBe(sharedPerson);
		const clone = await merchant(cloneId);
		expect(clone).toMatchObject({
			canonical_name: 'Shared Person',
			type: 'P2P',
			owner_user_id: USER_HIGH,
			source: 'AI',
			confirmed: false,
		});

		const lowRows = await tenancy.sql<Array<{ merchant_id: string }>>`
			select merchant_id from public.transactions where id = ${lowTxn}
		`;
		expect(lowRows[0]?.merchant_id).toBe(sharedPerson);

		const aliases = await tenancy.sql<Array<{ merchant_id: string; owner_user_id: string | null }>>`
			select merchant_id, owner_user_id from public.global_merchant_aliases where search_key = ${'SHAREDPERSON'} order by owner_user_id
		`;
		expect(aliases).toEqual([
			{ merchant_id: sharedPerson, owner_user_id: USER_LOW },
			{ merchant_id: cloneId, owner_user_id: USER_HIGH },
		]);

		const highRefs = await tenancy.sql<Array<{ rule: string; category_rule: string; series: string }>>`
			select
				(select merchant_id from public.user_merchant_rules where user_id = ${USER_HIGH} and search_key = ${'SHAREDPERSON'}) as rule,
				(select merchant_id from public.user_merchant_category_rules where user_id = ${USER_HIGH} and transaction_type = ${'expense'}) as category_rule,
				(select merchant_id from public.recurring_series where id = ${highSeries}) as series
		`;
		expect(highRefs[0]).toEqual({ rule: cloneId, category_rule: cloneId, series: cloneId });
		const lowRule = await tenancy.sql<Array<{ merchant_id: string }>>`
			select merchant_id from public.user_merchant_rules where user_id = ${USER_LOW} and search_key = ${'SHAREDPERSON'}
		`;
		expect(lowRule[0]?.merchant_id).toBe(sharedPerson);
	});

	it('assigns a single-user payee to that user and deletes an unreferenced one', async () => {
		expect((await merchant(singlePerson))?.owner_user_id).toBe(USER_LOW);
		expect(await merchant(orphanPerson)).toBeUndefined();
		const orphanAliases = await tenancy.sql<Array<{ count: string }>>`
			select count(*) from public.global_merchant_aliases where search_key = ${'ORPHANPERSON'}
		`;
		expect(orphanAliases[0]?.count).toBe('0');
	});

	it('leaves brands shared and drops logo lookups only for people', async () => {
		expect((await merchant(brand))?.owner_user_id).toBeNull();
		const lookups = await tenancy.sql<Array<{ merchant_id: string }>>`
			select merchant_id from public.merchant_logo_lookup_attempts order by merchant_id
		`;
		expect(lookups).toEqual([{ merchant_id: brand }]);
	});

	it('records every move in the reversal log', async () => {
		const rows = await tenancy.sql<LogRow[]>`
			select merchant_id, canonical_name, action, new_owner_user_id, cloned_from_merchant_id
			from public.merchant_ownership_migration_log
			order by canonical_name, action
		`;
		const highRows = await tenancy.sql<Array<{ merchant_id: string }>>`
			select merchant_id from public.transactions where id = ${highTxn}
		`;
		const cloneId = highRows[0]?.merchant_id as string;
		expect(rows).toEqual([
			{ merchant_id: orphanPerson, canonical_name: 'Orphan Person', action: 'DELETED', new_owner_user_id: null, cloned_from_merchant_id: null },
			{ merchant_id: cloneId, canonical_name: 'Shared Person', action: 'CLONED', new_owner_user_id: USER_HIGH, cloned_from_merchant_id: sharedPerson },
			{ merchant_id: sharedPerson, canonical_name: 'Shared Person', action: 'OWNED', new_owner_user_id: USER_LOW, cloned_from_merchant_id: null },
			{ merchant_id: singlePerson, canonical_name: 'Single Person', action: 'OWNED', new_owner_user_id: USER_LOW, cloned_from_merchant_id: null },
		]);
	});

	it('re-enables the reactive transactions triggers and passes the backstop assert', async () => {
		const triggers = await tenancy.sql<Array<{ tgname: string; tgenabled: string }>>`
			select tgname, tgenabled::text as tgenabled from pg_trigger
			where tgrelid = 'public.transactions'::regclass
				and tgname in ('auto_link_unambiguous_card_refund', 'match_confirmed_recurring_series')
			order by tgname
		`;
		expect(triggers).toEqual([
			{ tgname: 'auto_link_unambiguous_card_refund', tgenabled: 'O' },
			{ tgname: 'match_confirmed_recurring_series', tgenabled: 'O' },
		]);
		await tenancy.db.query('select public.assert_tenancy_backstops()');
	});

	it('is idempotent: applying the file again changes nothing', async () => {
		const before = await tenancy.sql<Array<{ merchants: string; aliases: string; log: string }>>`
			select
				(select count(*) from public.global_merchants) as merchants,
				(select count(*) from public.global_merchant_aliases) as aliases,
				(select count(*) from public.merchant_ownership_migration_log) as log
		`;
		await applyMigration(tenancy.db, OWNERSHIP_MIGRATION);
		const after = await tenancy.sql<Array<{ merchants: string; aliases: string; log: string }>>`
			select
				(select count(*) from public.global_merchants) as merchants,
				(select count(*) from public.global_merchant_aliases) as aliases,
				(select count(*) from public.merchant_ownership_migration_log) as log
		`;
		expect(after).toEqual(before);
		expect((await merchant(sharedPerson))?.owner_user_id).toBe(USER_LOW);
	});

	it('breaks the pre-4a mapper write, as the runbook warns', async () => {
		await expect(
			tenancy.sql`
				insert into public.global_merchants (canonical_name, type, source, confirmed)
				values (${'Old Code Brand'}, ${'MERCHANT'}, ${'AI'}, ${false})
				on conflict (canonical_name) do nothing
			`,
		).rejects.toThrow(/no unique or exclusion constraint matching the ON CONFLICT specification/);
	});
});
