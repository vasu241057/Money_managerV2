// The stale-extraction alert against the real materialized view (Task 7).
//
// Every other test of this path mocks the SQL client, so the query itself
// was never executed: a wrong column name would pass the suite, throw in
// production, and be swallowed by the caller's own catch — leaving the alert
// permanently silent with only a CHECK_FAILED line to show for it. This runs
// the query the Worker actually sends, against the schema the migrations
// actually build.

import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { seedTenant } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

const harness: { sql: SqlClient | null } = { sql: null };
vi.mock('../../src/lib/db/client', async (importOriginal) => {
	const actual = await importOriginal<typeof import('../../src/lib/db/client')>();
	return {
		...actual,
		getSqlClient: () => {
			if (!harness.sql) throw new Error('harness sql not ready');
			return harness.sql;
		},
	};
});

const { reportStalePendingExtraction } = await import('../../src/workers/scheduled.worker');

let db: TenancyDb;
let sql: SqlClient;
let userId: string;
let connectionId: string;

const ENV = {} as unknown as Env;

async function refreshView(): Promise<void> {
	await sql`select public.refresh_mv_stale_pending_extraction()`;
}

async function insertPending(sourceId: string, ageMinutes: number): Promise<void> {
	await sql`
		insert into public.raw_emails (
			user_id, oauth_connection_id, source_id, internal_date, clean_text,
			from_address, subject, status, created_at
		)
		values (
			${userId}, ${connectionId}, ${sourceId}, ${'2026-09-01T10:00:00Z'},
			${'pending body'}, ${'alerts@bank.test'}, ${'Pending'}, ${'PENDING_EXTRACTION'},
			now() - (${ageMinutes}::int || ' minutes')::interval
		)
	`;
}

beforeAll(async () => {
	db = await createTenancyDb();
	sql = db.sql;
	harness.sql = sql;
	const merchant = await sql<{ id: string }[]>`
		insert into public.global_merchants (canonical_name, type, source, confirmed)
		values (${'Stale Fixture Brand'}, ${'MERCHANT'}, ${'SEED'}, ${true})
		returning id
	`;
	const tenant = await seedTenant(sql, 'stale', { sharedMerchantId: merchant[0].id }, 'Pets');
	userId = tenant.userId;
	connectionId = tenant.connectionId;
	// The fixture's own rows are PROCESSED, so the view starts empty.
	await refreshView();
}, 120_000);

afterAll(async () => {
	harness.sql = null;
	await db?.close();
});

describe('tenancy: stale extraction alert', () => {
	it('stays silent while the view is empty', async () => {
		const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});

		await reportStalePendingExtraction(ENV);

		expect(errorLog).not.toHaveBeenCalled();
		errorLog.mockRestore();
	});

	it('ignores pending mail that is not yet stale', async () => {
		await insertPending('stale-fresh-1', 5);
		await refreshView();
		const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});

		await reportStalePendingExtraction(ENV);

		// The view's own 30-minute threshold has not passed.
		expect(errorLog).not.toHaveBeenCalled();
		errorLog.mockRestore();
	});

	it('reports the real counts once mail has been pending too long', async () => {
		await insertPending('stale-old-1', 90);
		await insertPending('stale-old-2', 120);
		await refreshView();
		const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});

		await reportStalePendingExtraction(ENV);

		const alert = errorLog.mock.calls.find((call) => call[0] === 'STALE_PENDING_EXTRACTION');
		expect(alert, 'the query must run against the real view').toBeDefined();
		expect(alert?.[1]).toMatchObject({ userCount: 1, staleCount: 2 });
		// Freshly refreshed, so the snapshot is seconds old, not frozen.
		const payload = alert?.[1] as { snapshotAgeSeconds: number };
		expect(payload.snapshotAgeSeconds).toBeLessThan(60);
		errorLog.mockRestore();
	});

	it('goes quiet again once the backlog clears', async () => {
		await sql`
			update public.raw_emails set status = ${'PROCESSED'}
			where user_id = ${userId} and status = ${'PENDING_EXTRACTION'}
		`;
		await refreshView();
		const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});

		await reportStalePendingExtraction(ENV);

		expect(errorLog).not.toHaveBeenCalled();
		errorLog.mockRestore();
	});
});
