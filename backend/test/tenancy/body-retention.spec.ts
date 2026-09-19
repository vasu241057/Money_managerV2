// The retention purge on the real schema (Task 5): only judged rows past the
// window lose their body, FAILED and PENDING rows keep theirs, a gated row
// (never fetched) is not marked, and a purged IGNORED row — which now has the
// same shape as a gated one — is excluded from the queries that re-download
// or re-run gated mail.

import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { purgeExpiredRawEmailBodies } from '../../src/workers/raw-email-body-purge';
import { seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

// The fetcher's ops entry points build their own client from the env; route
// them to the in-memory database instead.
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

const { runSenderBackfillJob, runStatementHarvest } = await import('../../src/workers/email-sync-fetcher');

interface IdRow {
	id: string;
}

interface RawEmailState {
	id: string;
	status: string;
	clean_text: string;
	body_purged_at: Date | null;
}

interface SeedSpec {
	label: string;
	status: string;
	body: string;
	completedDaysAgo: number | null;
	createdDaysAgo: number;
	subject?: string;
}

async function seedRawEmail(sql: SqlClient, fx: TwoTenantFixture, spec: SeedSpec): Promise<string> {
	const rows = await sql<IdRow[]>`
		insert into public.raw_emails (
			user_id, oauth_connection_id, source_id, internal_date, clean_text, from_address, subject, status,
			processing_completed_at, created_at
		)
		values (
			${fx.a.userId}, ${fx.a.connectionId}, ${`retention-${spec.label}`}, ${'2026-06-01T10:00:00Z'},
			${spec.body}, ${'alerts@bank.test'}, ${spec.subject ?? `Alert ${spec.label}`}, ${spec.status},
			case
				when ${spec.completedDaysAgo}::int is null then null
				else now() - (${spec.completedDaysAgo}::int || ' days')::interval
			end,
			now() - (${spec.createdDaysAgo}::int || ' days')::interval
		)
		returning id
	`;
	return rows[0]?.id as string;
}

describe('tenancy: raw email body retention', () => {
	let tenancy: TenancyDb;
	let sql: SqlClient;
	let fx: TwoTenantFixture;
	const ids: Record<string, string> = {};

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		sql = tenancy.sql;
		harness.sql = sql;
		fx = await seedTwoTenants(sql);

		const specs: SeedSpec[] = [
			{ label: 'old-processed', status: 'PROCESSED', body: 'old processed body', completedDaysAgo: 120, createdDaysAgo: 121 },
			{ label: 'old-ignored-legacy', status: 'IGNORED', body: 'old ignored body', completedDaysAgo: null, createdDaysAgo: 120 },
			{ label: 'old-unrecognized', status: 'UNRECOGNIZED', body: 'old unrecognized body', completedDaysAgo: 100, createdDaysAgo: 101 },
			{ label: 'old-failed', status: 'FAILED', body: 'old failed body', completedDaysAgo: 200, createdDaysAgo: 201 },
			{ label: 'recent-processed', status: 'PROCESSED', body: 'recent processed body', completedDaysAgo: 10, createdDaysAgo: 11 },
			{ label: 'old-gated', status: 'IGNORED', body: '', completedDaysAgo: null, createdDaysAgo: 200 },
			{ label: 'old-pending', status: 'PENDING_EXTRACTION', body: 'stuck pending body', completedDaysAgo: null, createdDaysAgo: 200 },
			{ label: 'old-statement', status: 'PROCESSED', body: 'statement body', completedDaysAgo: 150, createdDaysAgo: 151, subject: 'Your card statement' },
			{ label: 'recent-statement', status: 'PROCESSED', body: 'statement body', completedDaysAgo: 5, createdDaysAgo: 6, subject: 'Your card statement' },
		];
		for (const spec of specs) {
			ids[spec.label] = await seedRawEmail(sql, fx, spec);
		}
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	async function stateOf(label: string): Promise<RawEmailState> {
		const rows = await sql<RawEmailState[]>`
			select id, status, clean_text, body_purged_at from public.raw_emails where id = ${ids[label] as string}
		`;
		return rows[0] as RawEmailState;
	}

	it('purges only judged rows past the window, in bounded batches', async () => {
		const outcome = await purgeExpiredRawEmailBodies(sql, 90, { batchSize: 2, maxBatches: 10 });

		// old-processed, old-ignored-legacy (created_at fallback), old-unrecognized,
		// old-statement: two full batches, then the empty one that ends the loop.
		expect(outcome).toEqual({ purged_count: 4, batch_count: 3, exhausted: false });

		for (const label of ['old-processed', 'old-ignored-legacy', 'old-unrecognized', 'old-statement']) {
			const row = await stateOf(label);
			expect(row.clean_text, label).toBe('');
			expect(row.body_purged_at, label).not.toBeNull();
		}
		for (const label of ['old-failed', 'recent-processed', 'old-pending', 'recent-statement']) {
			const row = await stateOf(label);
			expect(row.clean_text, label).not.toBe('');
			expect(row.body_purged_at, label).toBeNull();
		}
		// A gated row was never fetched: empty body, no purge mark.
		const gated = await stateOf('old-gated');
		expect(gated.clean_text).toBe('');
		expect(gated.body_purged_at).toBeNull();

		// The fixture's own rows (PROCESSED, created just now) are untouched.
		const fixtureRows = await sql<Array<{ count: string }>>`
			select count(*)::bigint as count from public.raw_emails
			where user_id = ${fx.b.userId} and clean_text <> '' and body_purged_at is null
		`;
		expect(Number(fixtureRows[0]?.count)).toBe(4);
	});

	it('is idempotent: a second run finds nothing', async () => {
		const outcome = await purgeExpiredRawEmailBodies(sql, 90);
		expect(outcome).toEqual({ purged_count: 0, batch_count: 1, exhausted: false });
	});

	it('the sender backfill never re-downloads a purged IGNORED row', async () => {
		const result = await runSenderBackfillJob(
			{
				job_type: 'SENDER_BACKFILL',
				user_id: fx.a.userId,
				raw_email_ids: [ids['old-ignored-legacy'] as string],
			},
			{} as Env,
		);
		// No candidate row, so the job returns before it needs Google at all.
		expect(result).toEqual({
			user_id: fx.a.userId,
			requested_count: 1,
			refetched_count: 0,
			skipped_count: 1,
			normalize_enqueued: false,
		});
	});

	it('the statement harvest skips purged rows', async () => {
		const result = await runStatementHarvest(100, true, {} as Env, null);
		expect(result.matched_count).toBe(1);
		expect(result.by_sender).toEqual({ 'alerts@bank.test': 1 });

		const [matched] = await sql<Array<{ count: string }>>`
			select count(*)::bigint as count from public.raw_emails
			where id = ${ids['recent-statement'] as string} and body_purged_at is null
		`;
		expect(Number(matched?.count)).toBe(1);
	});
});
