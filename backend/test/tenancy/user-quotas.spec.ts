// Fair-use caps on the real schema (Task 6): counters accumulate per user and
// IST day, AI calls come from the ledger, an override beats the default and
// disappears when cleared, the self-heal slot is claimed once an hour, only
// the owner's stale PENDING rows come back, the dispatcher's one-join skip
// leaves a capped user out, the admin rollup sees everyone against their own
// caps, the rows die with the user, and a fresh sync job on a real mailbox
// re-enqueues stale pending mail exactly once per hour.

import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import type { EmailSyncJobPayload } from '../../../shared/types';
import type { SqlClient } from '../../src/lib/db/client';
import {
	claimPendingRequeueSlot,
	listStalePendingRawEmailIds,
	listUserQuotaRollup,
	loadUserQuotaStatus,
	recordUserUsage,
	releasePendingRequeueSlot,
	upsertUserLimits,
} from '../../src/services/user-quotas.service';
import { dispatchEmailSyncUsers } from '../../src/workers/email-sync-dispatcher';
import { seedTenant, seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

// The fetcher's entry point builds its own client from the env; route it to
// the in-memory database instead.
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

const { runEmailSyncUserJob } = await import('../../src/workers/email-sync-fetcher');

const DEFAULTS = { daily_email_fetch_cap: 500, daily_ai_call_cap: 200 };

interface IdRow {
	id: string;
}

interface CountRow {
	count: string;
}

async function insertAiCall(sql: SqlClient, userId: string, daysAgo: number): Promise<void> {
	await sql`
		insert into public.ai_usage_events (user_id, provider, model, operation, status, latency_ms, created_at)
		values (
			${userId}, ${'opencode'}, ${'test-model'}, ${'EMAIL_NORMALIZATION'}, ${'SUCCESS'}, ${12},
			now() - (${daysAgo}::int || ' days')::interval
		)
	`;
}

async function insertPending(
	sql: SqlClient,
	userId: string,
	connectionId: string,
	label: string,
	minutesAgo: number,
	status = 'PENDING_EXTRACTION',
): Promise<string> {
	const rows = await sql<IdRow[]>`
		insert into public.raw_emails (
			user_id, oauth_connection_id, source_id, internal_date, clean_text, from_address, subject, status, created_at
		)
		values (
			${userId}, ${connectionId}, ${`quota-${label}`}, ${'2026-09-18T10:00:00Z'}, ${'body'},
			${'alerts@bank.test'}, ${label}, ${status}, now() - (${minutesAgo}::int || ' minutes')::interval
		)
		returning id
	`;
	return rows[0]?.id as string;
}

function createQueueMock() {
	const sendBatch = vi.fn().mockResolvedValue(undefined);
	const send = vi.fn().mockResolvedValue(undefined);
	return { queue: { sendBatch, send } as unknown as Queue<EmailSyncJobPayload>, sendBatch, send };
}

function enqueuedUserIds(sendBatch: ReturnType<typeof vi.fn>): string[] {
	return sendBatch.mock.calls.flatMap(call =>
		(call[0] as Array<{ body: { user_id: string } }>).map(entry => entry.body.user_id),
	);
}

describe('tenancy: fair-use caps', () => {
	let tenancy: TenancyDb;
	let sql: SqlClient;
	let fx: TwoTenantFixture;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		sql = tenancy.sql;
		harness.sql = sql;
		fx = await seedTwoTenants(sql);
		// The common fixture includes an AI event per user; isolate the events
		// this suite deliberately records from that unrelated fixture history.
		await sql`update public.ai_usage_events set created_at = '2020-01-02'::timestamptz`;
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	it('accumulates pages per user and day, and reads AI calls from the ledger for today only', async () => {
		await recordUserUsage(sql, fx.a.userId, { messages_listed: 25, bodies_fetched: 4 });
		await recordUserUsage(sql, fx.a.userId, { messages_listed: 25, bodies_fetched: 4 });
		await insertAiCall(sql, fx.a.userId, 0);
		await insertAiCall(sql, fx.a.userId, 0);
		await insertAiCall(sql, fx.a.userId, 2); // an earlier day: not today's
		await insertAiCall(sql, fx.b.userId, 0);

		const alpha = await loadUserQuotaStatus(sql, fx.a.userId, DEFAULTS);
		expect(alpha).toMatchObject({
			messages_listed: 50,
			bodies_fetched: 8,
			ai_calls: 2,
			daily_email_fetch_cap: 500,
			daily_ai_call_cap: 200,
			over_email_fetch_cap: false,
			over_ai_call_cap: false,
			email_fetch_remaining: 450,
			ai_call_remaining: 198,
		});
		expect(alpha.day).toMatch(/^\d{4}-\d{2}-\d{2}$/);

		const bravo = await loadUserQuotaStatus(sql, fx.b.userId, DEFAULTS);
		expect(bravo).toMatchObject({ messages_listed: 0, bodies_fetched: 0, ai_calls: 1 });
	});

	it('applies a per-user override and removes the row once both caps are cleared', async () => {
		expect(await upsertUserLimits(sql, fx.a.userId, { daily_ai_call_cap: 2 })).toBe(true);
		const capped = await loadUserQuotaStatus(sql, fx.a.userId, DEFAULTS);
		expect(capped).toMatchObject({
			daily_ai_call_cap: 2,
			ai_call_cap_overridden: true,
			over_ai_call_cap: true,
			ai_call_remaining: 0,
			daily_email_fetch_cap: 500,
			email_fetch_cap_overridden: false,
		});
		// Bravo is untouched by alpha's override.
		expect((await loadUserQuotaStatus(sql, fx.b.userId, DEFAULTS)).daily_ai_call_cap).toBe(200);

		// A second write keeps the field left out.
		await upsertUserLimits(sql, fx.a.userId, { daily_email_fetch_cap: 1000 });
		expect(await loadUserQuotaStatus(sql, fx.a.userId, DEFAULTS)).toMatchObject({
			daily_ai_call_cap: 2,
			daily_email_fetch_cap: 1000,
		});

		await upsertUserLimits(sql, fx.a.userId, { daily_ai_call_cap: null, daily_email_fetch_cap: null });
		const [rows] = await sql<CountRow[]>`
			select count(*)::bigint as count from public.user_limits where user_id = ${fx.a.userId}
		`;
		expect(Number(rows?.count)).toBe(0);
		expect((await loadUserQuotaStatus(sql, fx.a.userId, DEFAULTS)).daily_ai_call_cap).toBe(200);

		expect(await upsertUserLimits(sql, '00000000-0000-4000-8000-00000000dead', { daily_ai_call_cap: 1 })).toBe(false);
	});

	it('claims the self-heal slot at most once an hour, per user', async () => {
		expect(await claimPendingRequeueSlot(sql, fx.a.userId)).toBe(true);
		expect(await claimPendingRequeueSlot(sql, fx.a.userId)).toBe(false);
		// Another user's hour is their own.
		expect(await claimPendingRequeueSlot(sql, fx.b.userId)).toBe(true);

		await sql`
			update public.user_usage_daily
			set pending_requeue_at = now() - interval '61 minutes'
			where user_id = ${fx.a.userId}
		`;
		expect(await claimPendingRequeueSlot(sql, fx.a.userId)).toBe(true);
		expect(await claimPendingRequeueSlot(sql, fx.a.userId)).toBe(false);

		await releasePendingRequeueSlot(sql, fx.a.userId);
		expect(await claimPendingRequeueSlot(sql, fx.a.userId)).toBe(true);
	});

	it('lists only the owner’s stale PENDING rows, oldest first', async () => {
		const oldest = await insertPending(sql, fx.a.userId, fx.a.connectionId, 'a-oldest', 90);
		const stale = await insertPending(sql, fx.a.userId, fx.a.connectionId, 'a-stale', 40);
		await insertPending(sql, fx.a.userId, fx.a.connectionId, 'a-fresh', 5);
		await insertPending(sql, fx.a.userId, fx.a.connectionId, 'a-done', 90, 'PROCESSED');
		await insertPending(sql, fx.b.userId, fx.b.connectionId, 'b-stale', 40);

		expect(await listStalePendingRawEmailIds(sql, fx.a.userId, 30, 250)).toEqual([oldest, stale]);
		expect(await listStalePendingRawEmailIds(sql, fx.a.userId, 30, 1)).toEqual([oldest]);
	});

	it('does not reset the hourly self-heal allowance at the IST day boundary', async () => {
		const [user] = await sql<IdRow[]>`insert into public.users (email) values ('midnight@example.test') returning id`;
		const id = user!.id;
		await sql`
			insert into public.user_usage_daily (user_id, day, pending_requeue_at)
			values (${id}, (now() at time zone 'Asia/Kolkata')::date - 1, now() - interval '5 minutes')
		`;
		expect(await claimPendingRequeueSlot(sql, id)).toBe(false);
		await sql`update public.user_usage_daily set pending_requeue_at = now() - interval '61 minutes' where user_id = ${id}`;
		expect(await claimPendingRequeueSlot(sql, id)).toBe(true);
		await sql`delete from public.users where id = ${id}`;
	});

	it('the dispatcher leaves a user at their fetch cap out, by default or by override', async () => {
		// Alpha has 8 bodies so far today; push them to the default cap.
		await recordUserUsage(sql, fx.a.userId, { messages_listed: 0, bodies_fetched: 492 });

		const first = createQueueMock();
		const result = await dispatchEmailSyncUsers(sql, first.queue, Date.now());
		expect(result.quota_skipped_user_count).toBe(1);
		expect(enqueuedUserIds(first.sendBatch)).toEqual([fx.b.userId]);

		// A raised override lets alpha through again.
		await upsertUserLimits(sql, fx.a.userId, { daily_email_fetch_cap: 1000 });
		const second = createQueueMock();
		const raised = await dispatchEmailSyncUsers(sql, second.queue, Date.now());
		expect(raised.quota_skipped_user_count).toBe(0);
		expect(enqueuedUserIds(second.sendBatch).sort()).toEqual([fx.a.userId, fx.b.userId].sort());

		// A lower env default catches bravo too.
		await recordUserUsage(sql, fx.b.userId, { messages_listed: 1, bodies_fetched: 1 });
		const third = createQueueMock();
		const lowered = await dispatchEmailSyncUsers(sql, third.queue, Date.now(), { daily_email_fetch_cap: 1 });
		expect(lowered.quota_skipped_user_count).toBe(1);
		expect(enqueuedUserIds(third.sendBatch)).toEqual([fx.a.userId]);
		await sql`update public.user_usage_daily set messages_listed = 0, bodies_fetched = 0 where user_id = ${fx.b.userId}`;
	});

	it('the admin rollup shows every user against their own caps', async () => {
		const rollup = await listUserQuotaRollup(sql, null, DEFAULTS);
		const alpha = rollup.users.find(user => user.user_id === fx.a.userId);
		const bravo = rollup.users.find(user => user.user_id === fx.b.userId);
		expect(alpha).toMatchObject({
			email: 'alpha@example.test',
			bodies_fetched: 500,
			messages_listed: 50,
			ai_calls: 2,
			daily_email_fetch_cap: 1000,
			email_fetch_cap_override: 1000,
			over_email_fetch_cap: false,
			pending_extraction_count: 3,
			active_connection_count: 1,
			deferred_connection_count: 0,
		});
		expect(alpha?.last_pending_requeue_at).not.toBeNull();
		expect(bravo).toMatchObject({
			email: 'bravo@example.test',
			bodies_fetched: 0,
			ai_calls: 1,
			daily_email_fetch_cap: 500,
			email_fetch_cap_override: null,
			pending_extraction_count: 1,
		});
		// The most active user comes first.
		expect(rollup.users[0]?.user_id).toBe(fx.a.userId);

		// A parked page token shows up as a deferred mailbox.
		await sql`
			update public.oauth_connections
			set email_sync_continuation_token = ${'p9'}, email_sync_continuation_after_seconds = ${1_690_000_000}
			where id = ${fx.b.connectionId}
		`;
		const only = await listUserQuotaRollup(sql, null, DEFAULTS, 200, fx.b.userId);
		expect(only.users.map(user => user.user_id)).toEqual([fx.b.userId]);
		expect(only.users[0]?.deferred_connection_count).toBe(1);

		// Yesterday: nothing counted, caps still reported.
		const yesterday = await listUserQuotaRollup(sql, '2020-01-01', DEFAULTS);
		expect(yesterday.day).toBe('2020-01-01');
		expect(yesterday.users.every(user => user.bodies_fetched === 0 && user.ai_calls === 0)).toBe(true);
	});

	it('usage and overrides die with their user', async () => {
		const [user] = await sql<IdRow[]>`
			insert into public.users (email, auth_provider, auth_subject)
			values (${'delta@example.test'}, ${'clerk'}, ${'user_fixture_delta'})
			returning id
		`;
		const userId = user?.id as string;
		await recordUserUsage(sql, userId, { messages_listed: 1, bodies_fetched: 1 });
		await upsertUserLimits(sql, userId, { daily_ai_call_cap: 3 });

		await sql`delete from public.users where id = ${userId}`;

		const [usage] = await sql<CountRow[]>`select count(*)::bigint as count from public.user_usage_daily where user_id = ${userId}`;
		const [limits] = await sql<CountRow[]>`select count(*)::bigint as count from public.user_limits where user_id = ${userId}`;
		expect(Number(usage?.count)).toBe(0);
		expect(Number(limits?.count)).toBe(0);
	});

	it('a fresh sync job self-heals a real mailbox’s stale pending mail once per hour', async () => {
		const charlie = await seedTenant(sql, 'charlie', { sharedMerchantId: fx.sharedMerchantId }, 'Pets');
		const stale = await insertPending(sql, charlie.userId, charlie.connectionId, 'c-stale', 45);
		await insertPending(sql, charlie.userId, charlie.connectionId, 'c-fresh', 2);

		// No refresh token on the fixture connection: the sync marks it revoked
		// without touching Google, and the self-heal still runs afterwards.
		const send = vi.fn().mockResolvedValue(undefined);
		const env = {
			GOOGLE_CLIENT_ID: 'google-client-id',
			GOOGLE_CLIENT_SECRET: 'google-client-secret',
			GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY: Buffer.from(new Uint8Array(32).fill(9)).toString('base64url'),
			EMAIL_SYNC_QUEUE: { send },
		} as unknown as Env;
		const neverGoogle = async () => {
			throw new Error('Google must not be called');
		};

		const first = await runEmailSyncUserJob(
			{ job_type: 'EMAIL_SYNC_USER', user_id: charlie.userId, last_sync_timestamp: 0 },
			env,
			neverGoogle,
		);
		expect(first.revoked_connection_count).toBe(1);
		expect(first.pending_requeued_count).toBe(1);
		expect(send).toHaveBeenCalledTimes(1);
		expect(send).toHaveBeenCalledWith(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [stale] },
			{ contentType: 'json' },
		);

		// Within the hour: nothing more, even though the row is still pending.
		const second = await runEmailSyncUserJob(
			{ job_type: 'EMAIL_SYNC_USER', user_id: charlie.userId, last_sync_timestamp: 0 },
			env,
			neverGoogle,
		);
		expect(second.pending_requeued_count).toBe(0);
		expect(send).toHaveBeenCalledTimes(1);

		const status = await loadUserQuotaStatus(sql, charlie.userId, DEFAULTS);
		expect(status).toMatchObject({ messages_listed: 0, bodies_fetched: 0, ai_calls: 1 });
	});
});
