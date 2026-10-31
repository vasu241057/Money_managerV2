import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { createTenancyDb, listMigrationFiles, type TenancyDb } from './pglite-db';

describe('tenancy harness: migrations load on PGlite', () => {
	let tenancy: TenancyDb;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	it('applies every checked-in migration in order', () => {
		expect(tenancy.appliedMigrations).toEqual(listMigrationFiles());
		expect(tenancy.appliedMigrations.length).toBeGreaterThan(30);
	});

	it('builds the expected public schema', async () => {
		const rows = await tenancy.sql<Array<{ table_name: string }>>`
			select table_name
			from information_schema.tables
			where table_schema = 'public' and table_type = 'BASE TABLE'
			order by table_name
		`;
		const names = rows.map((row) => row.table_name);
		for (const required of [
			'users',
			'oauth_connections',
			'accounts',
			'categories',
			'user_credit_cards',
			'raw_emails',
			'financial_events',
			'transactions',
			'global_merchants',
			'global_merchant_aliases',
			'user_merchant_rules',
			'email_templates',
			'sender_rules',
			'card_statements',
			'recurring_series',
			'transaction_relationships',
			'job_schedules',
		]) {
			expect(names, `missing table ${required}`).toContain(required);
		}
	});

	it('seeds the system taxonomy and shared merchants', async () => {
		const [categories] = await tenancy.sql<Array<{ count: string }>>`
			select count(*)::bigint as count
			from public.categories
			where user_id is null and is_system = true
		`;
		expect(Number(categories?.count)).toBeGreaterThan(50);

		const [merchants] = await tenancy.sql<Array<{ count: string }>>`
			select count(*)::bigint as count from public.global_merchants
		`;
		expect(Number(merchants?.count)).toBeGreaterThan(0);
	});

	it('round-trips uuid, array and boolean parameters through the adapter', async () => {
		const [userRow] = await tenancy.sql<Array<{ id: string }>>`
			insert into public.users (email) values (${'adapter@example.com'}) returning id
		`;
		const id = userRow?.id as string;
		const found = await tenancy.sql<Array<{ id: string }>>`
			select u.id from public.users as u
			where u.id = ${id}
				and u.id = any(${[id]}::uuid[])
				and (not ${false} or u.email = ${'never'})
			limit ${1}
		`;
		expect(found).toHaveLength(1);

		const [counted] = await tenancy.sql<Array<{ n: unknown }>>`
			select count(*)::bigint as n from public.users where id = any(${[id]}::uuid[])
		`;
		expect(typeof counted?.n).toBe('string');
	});
});
