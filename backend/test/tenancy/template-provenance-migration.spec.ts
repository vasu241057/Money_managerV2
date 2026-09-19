// Rehearsal of the Task 3 backfill on the pre-provenance schema, in the three
// shapes production or a restore could hold: templates whose source email
// still exists are attributed to that email's user; ownerless rows go to the
// admin user when present, else to the sole user, else are deleted.

import { afterAll, describe, expect, it } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { applyMigration, createTenancyDb, type TenancyDb } from './pglite-db';

const PROVENANCE_MIGRATION = '20260918170000_email_templates_provenance.sql';
const ADMIN = 'a661a921-cdb0-503b-a810-4b25e4f15ca0';
const USER_A = '00000000-0000-4000-8000-0000000000a3';
const USER_B = '00000000-0000-4000-8000-0000000000b3';

interface IdRow {
	id: string;
}

interface TemplateRow {
	skeleton: string;
	scope: string;
	learned_from_user_id: string | null;
}

async function seedUserWithEmail(sql: SqlClient, userId: string, label: string): Promise<string> {
	await sql`insert into public.users (id, email) values (${userId}, ${`${label}@example.test`})`;
	const connection = await sql<IdRow[]>`
		insert into public.oauth_connections (user_id, provider, email_address, sync_status)
		values (${userId}, ${'google'}, ${`${label}@gmail.test`}, ${'ACTIVE'})
		returning id
	`;
	const email = await sql<IdRow[]>`
		insert into public.raw_emails (user_id, oauth_connection_id, source_id, internal_date, clean_text, from_address, status)
		values (${userId}, ${connection[0]?.id as string}, ${`${label}-msg`}, ${'2026-09-01T10:00:00Z'}, ${'body'}, ${'alerts@bank.test'}, ${'PROCESSED'})
		returning id
	`;
	return email[0]?.id as string;
}

async function seedLegacyTemplate(sql: SqlClient, skeleton: string, rawEmailId: string | null): Promise<void> {
	await sql`
		insert into public.email_templates (from_address, skeleton_hash, skeleton, slot_map, learned_from_raw_email_id)
		values (${'alerts@bank.test'}, ${skeleton}, ${skeleton}, ${'{"version":1,"kind":"IGNORE"}'}::jsonb, ${rawEmailId})
	`;
}

async function templates(sql: SqlClient): Promise<TemplateRow[]> {
	return sql<TemplateRow[]>`
		select skeleton, scope, learned_from_user_id from public.email_templates order by skeleton
	`;
}

describe('tenancy: Task 3 template provenance backfill rehearsal', () => {
	const opened: TenancyDb[] = [];

	afterAll(async () => {
		await Promise.all(opened.map((tenancy) => tenancy.close()));
	});

	async function preMigrationDb(): Promise<TenancyDb> {
		const tenancy = await createTenancyDb({ stopBefore: PROVENANCE_MIGRATION });
		opened.push(tenancy);
		return tenancy;
	}

	it('attributes rows to their source email’s user and ownerless rows to the admin user', async () => {
		const tenancy = await preMigrationDb();
		const { sql } = tenancy;
		const emailA = await seedUserWithEmail(sql, USER_A, 'a');
		await seedUserWithEmail(sql, USER_B, 'b');
		await sql`insert into public.users (id, email) values (${ADMIN}, ${'admin@example.test'})`;
		await seedLegacyTemplate(sql, 'from-a', emailA);
		await seedLegacyTemplate(sql, 'orphan', null);

		await applyMigration(tenancy.db, PROVENANCE_MIGRATION);

		expect(await templates(sql)).toEqual([
			{ skeleton: 'from-a', scope: 'PRIVATE', learned_from_user_id: USER_A },
			{ skeleton: 'orphan', scope: 'PRIVATE', learned_from_user_id: ADMIN },
		]);

		const constraints = await sql<Array<{ conname: string }>>`
			select conname from pg_constraint where conrelid = 'public.email_templates'::regclass order by conname
		`;
		const names = constraints.map((row) => row.conname);
		expect(names).toContain('email_templates_scope_owner_check');
		expect(names).not.toContain('email_templates_sender_skeleton_unique');
		const indexes = await sql<Array<{ indexname: string }>>`
			select indexname from pg_indexes where tablename = 'email_templates' order by indexname
		`;
		expect(indexes.map((row) => row.indexname)).toEqual(
			expect.arrayContaining([
				'email_templates_private_skeleton_uidx',
				'email_templates_shared_skeleton_uidx',
				'email_templates_scope_lookup_idx',
			]),
		);

		// Idempotent, and the backstops stay green.
		await applyMigration(tenancy.db, PROVENANCE_MIGRATION);
		expect(await templates(sql)).toHaveLength(2);
		await tenancy.db.query('select public.assert_tenancy_backstops()');
	}, 120_000);

	it('gives ownerless rows to the sole user when there is exactly one', async () => {
		const tenancy = await preMigrationDb();
		const { sql } = tenancy;
		await seedUserWithEmail(sql, USER_A, 'solo');
		await seedLegacyTemplate(sql, 'orphan', null);

		await applyMigration(tenancy.db, PROVENANCE_MIGRATION);

		expect(await templates(sql)).toEqual([
			{ skeleton: 'orphan', scope: 'PRIVATE', learned_from_user_id: USER_A },
		]);
	}, 120_000);

	it('deletes ownerless rows when several users exist and none is the admin', async () => {
		const tenancy = await preMigrationDb();
		const { sql } = tenancy;
		const emailA = await seedUserWithEmail(sql, USER_A, 'a');
		await seedUserWithEmail(sql, USER_B, 'b');
		await seedLegacyTemplate(sql, 'from-a', emailA);
		await seedLegacyTemplate(sql, 'orphan', null);

		await applyMigration(tenancy.db, PROVENANCE_MIGRATION);

		expect(await templates(sql)).toEqual([
			{ skeleton: 'from-a', scope: 'PRIVATE', learned_from_user_id: USER_A },
		]);
	}, 120_000);
});
