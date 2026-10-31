import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	claimPendingRequeueSlot,
	isValidQuotaDay,
	listStalePendingRawEmailIds,
	listUserQuotaRollup,
	loadUserQuotaStatus,
	parseUserLimitsUpdate,
	recordUserUsage,
	resolveDefaultQuotaCaps,
	upsertUserLimits,
} from '../src/services/user-quotas.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function createSqlMock(handler: QueryHandler): SqlClient & { queries: Array<{ query: string; values: unknown[] }> } {
	const queries: Array<{ query: string; values: unknown[] }> = [];
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		queries.push({ query, values });
		return handler(query, values);
	};
	const mock = tag as unknown as SqlClient & { queries: typeof queries };
	mock.queries = queries;
	return mock;
}

const USER_ID = '11111111-1111-4111-8111-111111111111';
const DEFAULTS = { daily_email_fetch_cap: 500, daily_ai_call_cap: 200 };

describe('resolveDefaultQuotaCaps', () => {
	it('reads the config and falls back to the shipped defaults on junk', () => {
		expect(resolveDefaultQuotaCaps({ userDailyEmailFetchCap: 40, userDailyAiCallCap: 7 })).toEqual({
			daily_email_fetch_cap: 40,
			daily_ai_call_cap: 7,
		});
		expect(resolveDefaultQuotaCaps({})).toEqual(DEFAULTS);
		expect(resolveDefaultQuotaCaps({ userDailyEmailFetchCap: 0, userDailyAiCallCap: Number.NaN })).toEqual(DEFAULTS);
	});
});

describe('loadUserQuotaStatus', () => {
	it('applies the env defaults when the user has no override row', async () => {
		const sql = createSqlMock(() => [{
			day: '2026-09-19',
			messages_listed: 12,
			bodies_fetched: 3,
			ai_calls: '2',
			email_fetch_cap_override: null,
			ai_call_cap_override: null,
		}]);

		const status = await loadUserQuotaStatus(sql, USER_ID, DEFAULTS);

		expect(status).toEqual({
			user_id: USER_ID,
			day: '2026-09-19',
			messages_listed: 12,
			bodies_fetched: 3,
			ai_calls: 2,
			daily_email_fetch_cap: 500,
			daily_ai_call_cap: 200,
			email_fetch_cap_overridden: false,
			ai_call_cap_overridden: false,
			over_email_fetch_cap: false,
			over_ai_call_cap: false,
			email_fetch_remaining: 488,
			ai_call_remaining: 198,
		});
		// The AI count comes from the ledger, keyed by the IST day (A.2 §9).
		expect(sql.queries[0]?.query).toContain('from public.ai_usage_events as aue');
		expect(sql.queries[0]?.query).toContain("to_char(today.day, 'YYYY-MM-DD')");
		expect(sql.queries[0]?.values).toContain('Asia/Kolkata');
	});

	it('prefers the per-user override and reports over-cap at the boundary', async () => {
		const sql = createSqlMock(() => [{
			day: '2026-09-19',
			messages_listed: 0,
			bodies_fetched: 10,
			ai_calls: '5',
			email_fetch_cap_override: 10,
			ai_call_cap_override: 6,
		}]);

		const status = await loadUserQuotaStatus(sql, USER_ID, DEFAULTS);

		expect(status.daily_email_fetch_cap).toBe(10);
		expect(status.daily_ai_call_cap).toBe(6);
		expect(status.email_fetch_cap_overridden).toBe(true);
		expect(status.over_email_fetch_cap).toBe(true);
		expect(status.email_fetch_remaining).toBe(0);
		expect(status.over_ai_call_cap).toBe(false);
		expect(status.ai_call_remaining).toBe(1);
	});
});

describe('recordUserUsage', () => {
	it('upserts today’s counters and skips an empty page', async () => {
		const sql = createSqlMock(() => []);

		await recordUserUsage(sql, USER_ID, { messages_listed: 0, bodies_fetched: 0 });
		expect(sql.queries).toHaveLength(0);

		await recordUserUsage(sql, USER_ID, { messages_listed: 25, bodies_fetched: 4 });
		expect(sql.queries[0]?.query).toContain('insert into public.user_usage_daily (user_id, day, messages_listed, bodies_fetched)');
		expect(sql.queries[0]?.query).toContain('messages_listed = user_usage_daily.messages_listed + excluded.messages_listed');
		expect(sql.queries[0]?.values).toEqual([USER_ID, 'Asia/Kolkata', 25, 4]);
	});
});

describe('claimPendingRequeueSlot', () => {
	it('reports whether the hourly slot was taken', async () => {
		const taken = createSqlMock(() => [{ user_id: USER_ID }]);
		expect(await claimPendingRequeueSlot(taken, USER_ID)).toBe(true);
		expect(taken.queries[0]?.query).toContain('pending_requeue_at <= now() - make_interval(mins =>');
		expect(taken.queries[0]?.values).toEqual([USER_ID, 'Asia/Kolkata', USER_ID, 60, 60]);

		const busy = createSqlMock(() => []);
		expect(await claimPendingRequeueSlot(busy, USER_ID, 30)).toBe(false);
		expect(busy.queries[0]?.values).toEqual([USER_ID, 'Asia/Kolkata', USER_ID, 30, 30]);
	});
});

describe('listStalePendingRawEmailIds', () => {
	it('asks for this user’s oldest pending rows past the staleness window', async () => {
		const sql = createSqlMock(() => [{ id: 'a' }, { id: 'b' }]);
		expect(await listStalePendingRawEmailIds(sql, USER_ID, 30, 250)).toEqual(['a', 'b']);
		expect(sql.queries[0]?.query).toContain("re.status = 'PENDING_EXTRACTION'");
		expect(sql.queries[0]?.query).toContain('order by re.created_at asc');
		expect(sql.queries[0]?.values).toEqual([USER_ID, 30, 250]);
	});
});

describe('parseUserLimitsUpdate', () => {
	it('accepts positive integers or null and refuses everything else', () => {
		expect(parseUserLimitsUpdate({ daily_email_fetch_cap: 50 })).toEqual({ daily_email_fetch_cap: 50 });
		expect(parseUserLimitsUpdate({ daily_ai_call_cap: null })).toEqual({ daily_ai_call_cap: null });
		expect(parseUserLimitsUpdate({ daily_email_fetch_cap: 1, daily_ai_call_cap: 2 })).toEqual({
			daily_email_fetch_cap: 1,
			daily_ai_call_cap: 2,
		});
		expect(() => parseUserLimitsUpdate({})).toThrow(/required/);
		expect(() => parseUserLimitsUpdate({ daily_email_fetch_cap: 0 })).toThrow(/between 1 and/);
		expect(() => parseUserLimitsUpdate({ daily_ai_call_cap: '10' })).toThrow(/between 1 and/);
		expect(() => parseUserLimitsUpdate({ daily_ai_call_cap: 1.5 })).toThrow(/between 1 and/);
		expect(() => parseUserLimitsUpdate({ daily_ai_call_cap: 5_000_000 })).toThrow(/between 1 and/);
		expect(() => parseUserLimitsUpdate(null)).toThrow(/JSON object/);
	});
});

describe('upsertUserLimits', () => {
	it('returns false for an unknown user without writing', async () => {
		const sql = createSqlMock(query => (query.includes('from public.users as u') ? [] : []));
		expect(await upsertUserLimits(sql, USER_ID, { daily_email_fetch_cap: 5 })).toBe(false);
		expect(sql.queries).toHaveLength(1);
	});

	it('writes the override, keeps the field left out, and prunes an all-null row', async () => {
		const sql = createSqlMock(query => (query.includes('from public.users as u') ? [{ id: USER_ID }] : []));
		expect(await upsertUserLimits(sql, USER_ID, { daily_ai_call_cap: 9 })).toBe(true);

		const upsert = sql.queries[1];
		expect(upsert?.query).toContain('insert into public.user_limits');
		// (user_id, email cap insert, ai cap insert, keep-email flag, email cap, keep-ai flag, ai cap)
		expect(upsert?.values).toEqual([USER_ID, null, 9, true, null, false, 9]);
		expect(sql.queries[2]?.query).toContain('delete from public.user_limits');
		expect(sql.queries[2]?.query).toContain('daily_email_fetch_cap is null and daily_ai_call_cap is null');
	});
});

describe('listUserQuotaRollup', () => {
	it('maps every user against the caps that apply to them', async () => {
		const sql = createSqlMock(() => [
			{
				user_id: USER_ID,
				email: 'one@example.test',
				day: '2026-09-19',
				messages_listed: 40,
				bodies_fetched: 12,
				pending_requeue_at: '2026-09-19T03:00:00.000Z',
				ai_calls: '200',
				ai_cost_usd: '0.0123',
				email_fetch_cap_override: null,
				ai_call_cap_override: null,
				pending_extraction_count: '7',
				active_connection_count: '1',
				deferred_connection_count: '0',
			},
			{
				user_id: '22222222-2222-4222-8222-222222222222',
				email: 'two@example.test',
				day: '2026-09-19',
				messages_listed: 0,
				bodies_fetched: 30,
				pending_requeue_at: null,
				ai_calls: '0',
				ai_cost_usd: '0',
				email_fetch_cap_override: 30,
				ai_call_cap_override: null,
				pending_extraction_count: '0',
				active_connection_count: '2',
				deferred_connection_count: '1',
			},
		]);

		const rollup = await listUserQuotaRollup(sql, '2026-09-19', DEFAULTS);

		expect(rollup.day).toBe('2026-09-19');
		expect(rollup.users).toHaveLength(2);
		expect(rollup.users[0]).toMatchObject({
			email: 'one@example.test',
			ai_calls: 200,
			ai_cost_usd: 0.0123,
			over_ai_call_cap: true,
			over_email_fetch_cap: false,
			daily_email_fetch_cap: 500,
			pending_extraction_count: 7,
			last_pending_requeue_at: '2026-09-19T03:00:00.000Z',
		});
		expect(rollup.users[1]).toMatchObject({
			email: 'two@example.test',
			daily_email_fetch_cap: 30,
			email_fetch_cap_override: 30,
			over_email_fetch_cap: true,
			deferred_connection_count: 1,
			last_pending_requeue_at: null,
		});
		expect(sql.queries[0]?.values[0]).toBe('2026-09-19');
	});

	it('resolves today when no day is given and nobody exists yet', async () => {
		const sql = createSqlMock(() => []);
		const rollup = await listUserQuotaRollup(sql, null, DEFAULTS);
		expect(rollup.users).toEqual([]);
		expect(rollup.day).toMatch(/^\d{4}-\d{2}-\d{2}$/);
		expect(sql.queries[0]?.values[0]).toBeNull();
	});
});

describe('isValidQuotaDay', () => {
	it('accepts bounded YYYY-MM-DD only', () => {
		expect(isValidQuotaDay('2026-09-19')).toBe(true);
		expect(isValidQuotaDay('2026-13-01')).toBe(false);
		expect(isValidQuotaDay('2026-9-1')).toBe(false);
		expect(isValidQuotaDay('1999-01-01')).toBe(false);
		expect(isValidQuotaDay('2026-02-29')).toBe(false);
		expect(isValidQuotaDay('2026-04-31')).toBe(false);
		expect(isValidQuotaDay('2028-02-29')).toBe(true);
	});
});
