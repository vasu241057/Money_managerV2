// Rehearsal of the Task 6 migration on the pre-quota schema: the two counter
// tables land hardened like every other table, the parked-page-token columns
// arrive with their pairing constraint, the stale-PENDING index exists, no
// row is touched, and a second application is a no-op.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { applyMigration, createTenancyDb, type TenancyDb } from './pglite-db';

const QUOTA_MIGRATION = '20260918220000_user_quotas.sql';

interface CountRow {
	count: string;
}

async function counts(sql: SqlClient): Promise<Record<string, number>> {
	const [users] = await sql<CountRow[]>`select count(*)::bigint as count from public.users`;
	const [rawEmails] = await sql<CountRow[]>`select count(*)::bigint as count from public.raw_emails`;
	const [connections] = await sql<CountRow[]>`select count(*)::bigint as count from public.oauth_connections`;
	return {
		users: Number(users?.count),
		raw_emails: Number(rawEmails?.count),
		oauth_connections: Number(connections?.count),
	};
}

describe('tenancy: Task 6 migration rehearsal', () => {
	let tenancy: TenancyDb;
	let sql: SqlClient;
	let fx: TwoTenantFixture;
	let before: Record<string, number>;

	beforeAll(async () => {
		tenancy = await createTenancyDb({ stopBefore: QUOTA_MIGRATION });
		sql = tenancy.sql;
		fx = await seedTwoTenants(sql);
		before = await counts(sql);
		await applyMigration(tenancy.db, QUOTA_MIGRATION);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	it('changes no existing row and seeds nothing', async () => {
		expect(await counts(sql)).toEqual(before);
		const [usage] = await sql<CountRow[]>`select count(*)::bigint as count from public.user_usage_daily`;
		const [limits] = await sql<CountRow[]>`select count(*)::bigint as count from public.user_limits`;
		expect(Number(usage?.count)).toBe(0);
		expect(Number(limits?.count)).toBe(0);
	});

	it('creates both tables hardened with RLS and the app_backend policy, cascading from users', async () => {
		const hardening = await sql<Array<{ relname: string; rls: boolean; policies: string }>>`
			select c.relname,
				c.relrowsecurity as rls,
				(select count(*)::bigint from pg_policies p where p.schemaname = 'public' and p.tablename = c.relname and p.policyname = 'app_backend_all') as policies
			from pg_class c
			join pg_namespace n on n.oid = c.relnamespace
			where n.nspname = 'public' and c.relname in ('user_usage_daily', 'user_limits')
			order by c.relname
		`;
		expect(hardening.map(row => [row.relname, row.rls, Number(row.policies)])).toEqual([
			['user_limits', true, 1],
			['user_usage_daily', true, 1],
		]);

		const cascades = await sql<Array<{ conrelid: string; confdeltype: string }>>`
			select conrelid::regclass::text as conrelid, confdeltype
			from pg_constraint
			where contype = 'f'
				and conrelid in ('public.user_usage_daily'::regclass, 'public.user_limits'::regclass)
			order by 1
		`;
		expect(cascades).toEqual([
			{ conrelid: 'user_limits', confdeltype: 'c' },
			{ conrelid: 'user_usage_daily', confdeltype: 'c' },
		]);
	});

	it('rejects negative counters and non-positive caps', async () => {
		await expect(
			sql`insert into public.user_usage_daily (user_id, day, bodies_fetched) values (${fx.a.userId}, ${'2026-09-19'}, ${-1})`,
		).rejects.toThrow(/check/i);
		await expect(
			sql`insert into public.user_limits (user_id, daily_ai_call_cap) values (${fx.a.userId}, ${0})`,
		).rejects.toThrow(/check/i);
	});

	it('adds the parked page token columns and insists they travel together', async () => {
		const columns = await sql<Array<{ column_name: string; data_type: string }>>`
			select column_name, data_type
			from information_schema.columns
			where table_schema = 'public' and table_name = 'oauth_connections'
				and column_name like 'email_sync_continuation_%'
			order by column_name
		`;
		expect(columns).toEqual([
			{ column_name: 'email_sync_continuation_after_seconds', data_type: 'bigint' },
			{ column_name: 'email_sync_continuation_max_seen', data_type: 'bigint' },
			{ column_name: 'email_sync_continuation_token', data_type: 'text' },
		]);

		// A token without its query window is meaningless: refused.
		await expect(
			sql`update public.oauth_connections set email_sync_continuation_token = ${'p1'} where id = ${fx.a.connectionId}`,
		).rejects.toThrow(/chk_oauth_connections_email_sync_continuation_pair/);
		// Token + window (+ optional watermark) is accepted, and clearing works.
		await sql`
			update public.oauth_connections
			set email_sync_continuation_token = ${'p1'}, email_sync_continuation_after_seconds = ${1_690_000_000}
			where id = ${fx.a.connectionId}
		`;
		await sql`
			update public.oauth_connections
			set email_sync_continuation_token = null, email_sync_continuation_after_seconds = null, email_sync_continuation_max_seen = null
			where id = ${fx.a.connectionId}
		`;
	});

	it('adds the per-user stale-PENDING partial index', async () => {
		const indexes = await sql<Array<{ indexdef: string }>>`
			select indexdef from pg_indexes
			where schemaname = 'public' and indexname = 'idx_raw_emails_pending_by_user'
		`;
		expect(indexes).toHaveLength(1);
		expect(indexes[0]?.indexdef).toMatch(/\(user_id, created_at\)/);
		expect(indexes[0]?.indexdef).toMatch(/WHERE \(status = 'PENDING_EXTRACTION'/);
	});

	it('applies a second time without changes', async () => {
		await applyMigration(tenancy.db, QUOTA_MIGRATION);
		expect(await counts(sql)).toEqual(before);
	});
});
