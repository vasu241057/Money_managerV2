// Rehearsal of the Task 4b backfill on the pre-confirmations schema: legacy
// confirmed / dissent_count caches become one verdict row for the user who
// could have reviewed the merchant (its owner, else its busiest user), and
// every owned P2P alias is remembered in merchant_mapping_attempts as a
// person so the mapper can fan it out without a model call.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { applyMigration, createTenancyDb, type TenancyDb } from './pglite-db';

const CONFIRMATIONS_MIGRATION = '20260918150000_merchant_confirmations.sql';
const USER_A = '00000000-0000-4000-8000-0000000000a1';
const USER_B = '00000000-0000-4000-8000-0000000000b1';

interface IdRow {
	id: string;
}

async function insertMerchant(
	sql: SqlClient,
	input: { name: string; type: string; source: string; confirmed: boolean; dissent: number; owner: string | null; alias: string },
): Promise<string> {
	const rows = await sql<IdRow[]>`
		insert into public.global_merchants (canonical_name, type, source, confirmed, dissent_count, owner_user_id)
		values (${input.name}, ${input.type}, ${input.source}, ${input.confirmed}, ${input.dissent}, ${input.owner})
		returning id
	`;
	const id = rows[0]?.id as string;
	await sql`
		insert into public.global_merchant_aliases (search_key, merchant_id, owner_user_id)
		values (${input.alias}, ${id}, ${input.owner})
	`;
	return id;
}

async function insertTransactions(sql: SqlClient, userId: string, merchantId: string, count: number, base: number): Promise<void> {
	for (let index = 0; index < count; index += 1) {
		const events = await sql<IdRow[]>`
			insert into public.financial_events (user_id, raw_email_id, extraction_index, direction, amount_in_paise, txn_timestamp, payment_method)
			values (${userId}, null, ${base + index}, ${'debit'}, ${1_000}, ${'2026-09-01T10:00:00Z'}, ${'upi'})
			returning id
		`;
		await sql`
			insert into public.transactions (user_id, financial_event_id, merchant_id, amount_in_paise, type, txn_date)
			values (${userId}, ${events[0]?.id as string}, ${merchantId}, ${1_000}, ${'expense'}, ${'2026-09-01T10:00:00Z'})
		`;
	}
}

describe('tenancy: Task 4b confirmations backfill rehearsal', () => {
	let tenancy: TenancyDb;
	let confirmedBrand: string;
	let dissentedBrand: string;
	let untouchedBrand: string;
	let seededBrand: string;
	let orphanBrand: string;
	let ownedPerson: string;
	let confirmedPerson: string;

	beforeAll(async () => {
		tenancy = await createTenancyDb({ stopBefore: CONFIRMATIONS_MIGRATION });
		const { sql } = tenancy;
		await sql`insert into public.users (id, email) values (${USER_A}, ${'a@example.test'}), (${USER_B}, ${'b@example.test'})`;

		confirmedBrand = await insertMerchant(sql, { name: 'Confirmed Brand', type: 'MERCHANT', source: 'AI', confirmed: true, dissent: 0, owner: null, alias: 'CONFIRMEDBRAND' });
		dissentedBrand = await insertMerchant(sql, { name: 'Dissented Brand', type: 'MERCHANT', source: 'AI', confirmed: false, dissent: 2, owner: null, alias: 'DISSENTEDBRAND' });
		untouchedBrand = await insertMerchant(sql, { name: 'Untouched Brand', type: 'MERCHANT', source: 'AI', confirmed: false, dissent: 0, owner: null, alias: 'UNTOUCHEDBRAND' });
		seededBrand = await insertMerchant(sql, { name: 'Seeded Brand', type: 'MERCHANT', source: 'SEED', confirmed: true, dissent: 0, owner: null, alias: 'SEEDEDBRAND' });
		orphanBrand = await insertMerchant(sql, { name: 'Orphan Brand', type: 'MERCHANT', source: 'AI', confirmed: true, dissent: 0, owner: null, alias: 'ORPHANBRAND' });
		ownedPerson = await insertMerchant(sql, { name: 'Owned Person', type: 'P2P', source: 'AI', confirmed: false, dissent: 0, owner: USER_A, alias: 'OWNEDPERSON' });
		confirmedPerson = await insertMerchant(sql, { name: 'Confirmed Person', type: 'P2P', source: 'AI', confirmed: true, dissent: 0, owner: USER_B, alias: 'CONFIRMEDPERSON' });

		// B references the confirmed brand more than A: B is the voter.
		await insertTransactions(sql, USER_A, confirmedBrand, 1, 0);
		await insertTransactions(sql, USER_B, confirmedBrand, 2, 10);
		await insertTransactions(sql, USER_A, dissentedBrand, 1, 20);
		await insertTransactions(sql, USER_A, untouchedBrand, 1, 30);
		await insertTransactions(sql, USER_A, seededBrand, 1, 40);
		// A legacy attempt row for the owned person, without a resolution.
		await sql`insert into public.merchant_mapping_attempts (search_key, attempt_count) values (${'OWNEDPERSON'}, ${3})`;

		await applyMigration(tenancy.db, CONFIRMATIONS_MIGRATION);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	it('derives one verdict per AI merchant that had a cache value, for the user who could have reviewed it', async () => {
		const rows = await tenancy.sql<Array<{ merchant_id: string; user_id: string; verdict: string }>>`
			select merchant_id, user_id, verdict from public.merchant_confirmations order by merchant_id, user_id
		`;
		const expected = [
			{ merchant_id: confirmedBrand, user_id: USER_B, verdict: 'CONFIRM' },
			{ merchant_id: dissentedBrand, user_id: USER_A, verdict: 'DISSENT' },
			{ merchant_id: confirmedPerson, user_id: USER_B, verdict: 'CONFIRM' },
		].sort((left, right) => (left.merchant_id + left.user_id).localeCompare(right.merchant_id + right.user_id));
		expect(rows).toEqual(expected);
		// Untouched, seeded and unreferenced merchants get nothing.
		expect(rows.map((row) => row.merchant_id)).not.toContain(untouchedBrand);
		expect(rows.map((row) => row.merchant_id)).not.toContain(seededBrand);
		expect(rows.map((row) => row.merchant_id)).not.toContain(orphanBrand);
		expect(rows.map((row) => row.merchant_id)).not.toContain(ownedPerson);
	});

	it('remembers every owned P2P alias as a resolved person and marks brand keys', async () => {
		const rows = await tenancy.sql<Array<{ search_key: string; attempt_count: number; last_resolution: string | null; person_display_name: string | null }>>`
			select search_key, attempt_count, last_resolution, person_display_name
			from public.merchant_mapping_attempts order by search_key
		`;
		expect(rows).toEqual([
			{ search_key: 'CONFIRMEDPERSON', attempt_count: 1, last_resolution: 'P2P', person_display_name: 'Confirmed Person' },
			{ search_key: 'OWNEDPERSON', attempt_count: 3, last_resolution: 'P2P', person_display_name: 'Owned Person' },
		]);
	});

	it('is idempotent and leaves the backstops green', async () => {
		await applyMigration(tenancy.db, CONFIRMATIONS_MIGRATION);
		const counts = await tenancy.sql<Array<{ confirmations: string; attempts: string }>>`
			select (select count(*) from public.merchant_confirmations) as confirmations,
				(select count(*) from public.merchant_mapping_attempts) as attempts
		`;
		expect(counts[0]).toEqual({ confirmations: '3', attempts: '2' });
		await tenancy.db.query('select public.assert_tenancy_backstops()');
	});
});
