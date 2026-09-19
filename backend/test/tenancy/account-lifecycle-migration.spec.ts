// Rehearsal of the Task 5 migration on the pre-lifecycle schema: the purge
// marker and its scan index land, the purge job is seeded disabled at 03:30
// IST without disturbing the existing schedules, the tombstone table is
// hardened like every other table, and a second application is a no-op.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { applyMigration, createTenancyDb, type TenancyDb } from './pglite-db';

const LIFECYCLE_MIGRATION = '20260918200000_account_lifecycle_and_retention.sql';

interface ScheduleRow {
	job: string;
	enabled: boolean;
	run_at_local: string;
	timezone: string;
}

async function schedules(sql: SqlClient): Promise<ScheduleRow[]> {
	return sql<ScheduleRow[]>`
		select job, enabled, to_char(run_at_local, 'HH24:MI') as run_at_local, timezone
		from public.job_schedules
		order by job
	`;
}

describe('tenancy: Task 5 migration rehearsal', () => {
	let tenancy: TenancyDb;
	let sql: SqlClient;
	let before: ScheduleRow[];

	beforeAll(async () => {
		tenancy = await createTenancyDb({ stopBefore: LIFECYCLE_MIGRATION });
		sql = tenancy.sql;
		// Pre-state: the merchant mapper was switched on and moved, as in production.
		await sql`
			update public.job_schedules set enabled = true, run_at_local = '10:00' where job = 'merchant_mapping'
		`;
		before = await schedules(sql);
		await applyMigration(tenancy.db, LIFECYCLE_MIGRATION);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	it('adds the purge marker and its partial scan index', async () => {
		const columns = await sql<Array<{ column_name: string; data_type: string; is_nullable: string }>>`
			select column_name, data_type, is_nullable
			from information_schema.columns
			where table_schema = 'public' and table_name = 'raw_emails' and column_name = 'body_purged_at'
		`;
		expect(columns).toEqual([
			{ column_name: 'body_purged_at', data_type: 'timestamp with time zone', is_nullable: 'YES' },
		]);

		const indexes = await sql<Array<{ indexdef: string }>>`
			select indexdef from pg_indexes
			where schemaname = 'public' and indexname = 'idx_raw_emails_body_purge_scan'
		`;
		expect(indexes).toHaveLength(1);
		expect(indexes[0]?.indexdef).toMatch(/WHERE \(clean_text <> ''/);
		expect(indexes[0]?.indexdef).toMatch(/COALESCE\(processing_completed_at, created_at\)/);
	});

	it('seeds the purge job disabled at 03:30 IST and keeps the existing schedules as they were', async () => {
		const after = await schedules(sql);
		expect(after.filter((row) => row.job !== 'raw_email_body_purge')).toEqual(before);
		expect(after.find((row) => row.job === 'raw_email_body_purge')).toEqual({
			job: 'raw_email_body_purge',
			enabled: false,
			run_at_local: '03:30',
			timezone: 'Asia/Kolkata',
		});
	});

	it('accepts the new job name in the check constraint and still rejects unknown ones', async () => {
		await expect(
			sql`insert into public.job_schedules (job, run_at_local) values (${'bogus_job'}, ${'01:00'})`,
		).rejects.toThrow(/job_schedules_known_job/);
	});

	it('creates a hardened, PII-free tombstone table', async () => {
		const columns = await sql<Array<{ column_name: string }>>`
			select column_name from information_schema.columns
			where table_schema = 'public' and table_name = 'user_deletion_log'
			order by ordinal_position
		`;
		expect(columns.map((row) => row.column_name)).toEqual([
			'user_id',
			'deleted_at',
			'clerk_deleted',
			'google_revoked',
			'auth_provider',
			'auth_subject',
			'clerk_delete_attempts',
			'clerk_last_attempt_at',
			'clerk_last_error',
		]);

		const hardening = await sql<Array<{ rls: boolean; policies: string }>>`
			select c.relrowsecurity as rls,
				(select count(*)::bigint from pg_policies p where p.schemaname = 'public' and p.tablename = 'user_deletion_log' and p.policyname = 'app_backend_all') as policies
			from pg_class c
			join pg_namespace n on n.oid = c.relnamespace
			where n.nspname = 'public' and c.relname = 'user_deletion_log'
		`;
		expect(hardening[0]?.rls).toBe(true);
		expect(Number(hardening[0]?.policies)).toBe(1);

		// A resolved tombstone can never keep the provider subject around.
		await expect(
			sql`
				insert into public.user_deletion_log (user_id, clerk_deleted, auth_subject)
				values (${'00000000-0000-4000-8000-0000000000d5'}, ${true}, ${'user_should_be_cleared'})
			`,
		).rejects.toThrow(/user_deletion_log_subject_cleared_check/);
	});

	it('applies a second time without changes', async () => {
		await applyMigration(tenancy.db, LIFECYCLE_MIGRATION);
		const after = await schedules(sql);
		expect(after.filter((row) => row.job === 'raw_email_body_purge')).toHaveLength(1);
	});
});
