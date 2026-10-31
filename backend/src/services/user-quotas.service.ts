// Per-user fair-use caps (Task 6). Two counters per IST day live in
// user_usage_daily (Gmail messages listed, bodies fetched); model calls are
// read from ai_usage_events, which already ledgers every provider attempt per
// user (A.2 §9 — never double-count). Caps come from the env defaults unless
// user_limits overrides them for that user.
//
// The same status query serves the fetcher (before each Gmail page), the
// normalizer (before the first model call of a user in a job) and the
// self-healing re-enqueue, so "over cap" means one thing everywhere.

import type { UUID, UserLimitsUpdateRequest, UserQuotaRollupRow } from '../../../shared/types';
import type { AppConfig } from '../lib/config';
import { DEFAULT_USER_DAILY_AI_CALL_CAP, DEFAULT_USER_DAILY_EMAIL_FETCH_CAP } from '../lib/quotas';
import type { SqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableNumber,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';
import { badRequest } from '../lib/http/errors';

/** Day boundaries for every per-user counter, like the other daily jobs. */
export const QUOTA_TIMEZONE = 'Asia/Kolkata';
/** A stale PENDING row is one the normalizer has not touched in this long. */
export const PENDING_REQUEUE_STALE_MINUTES = 30;
/** The self-heal runs at most this often per user (A.2 §8). */
export const PENDING_REQUEUE_MIN_INTERVAL_MINUTES = 60;
/** Rows the admin rollup returns, most active first. */
export const USER_QUOTA_ROLLUP_LIMIT = 200;
/** Upper bound for a per-user override: a typo like 5,000,000 is refused. */
const MAX_CAP_OVERRIDE = 1_000_000;

export interface UserQuotaCaps {
	daily_email_fetch_cap: number;
	daily_ai_call_cap: number;
}

function positiveOr(value: number | undefined, fallback: number): number {
	return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : fallback;
}

export function resolveDefaultQuotaCaps(
	config: Partial<Pick<AppConfig, 'userDailyEmailFetchCap' | 'userDailyAiCallCap'>>,
): UserQuotaCaps {
	return {
		daily_email_fetch_cap: positiveOr(config.userDailyEmailFetchCap, DEFAULT_USER_DAILY_EMAIL_FETCH_CAP),
		daily_ai_call_cap: positiveOr(config.userDailyAiCallCap, DEFAULT_USER_DAILY_AI_CALL_CAP),
	};
}

export interface UserQuotaStatus {
	user_id: UUID;
	/** YYYY-MM-DD in Asia/Kolkata. */
	day: string;
	messages_listed: number;
	bodies_fetched: number;
	ai_calls: number;
	daily_email_fetch_cap: number;
	daily_ai_call_cap: number;
	email_fetch_cap_overridden: boolean;
	ai_call_cap_overridden: boolean;
	over_email_fetch_cap: boolean;
	over_ai_call_cap: boolean;
	email_fetch_remaining: number;
	ai_call_remaining: number;
}

interface QuotaStatusRowRaw {
	day: unknown;
	messages_listed: unknown;
	bodies_fetched: unknown;
	ai_calls: unknown;
	email_fetch_cap_override: unknown;
	ai_call_cap_override: unknown;
}

function toOptionalPositiveInteger(value: unknown, fieldName: string): number | null {
	if (value === null || value === undefined) return null;
	const parsed = toSafeInteger(value, fieldName);
	return parsed > 0 ? parsed : null;
}

export async function loadUserQuotaStatus(
	sql: SqlClient,
	userId: UUID,
	defaults: UserQuotaCaps,
): Promise<UserQuotaStatus> {
	const rows = await sql<QuotaStatusRowRaw[]>`
		with today as (
			select (now() at time zone ${QUOTA_TIMEZONE})::date as day
		)
		select
			to_char(today.day, 'YYYY-MM-DD') as day,
			coalesce(uud.messages_listed, 0) as messages_listed,
			coalesce(uud.bodies_fetched, 0) as bodies_fetched,
			(
				select count(*)
				from public.ai_usage_events as aue
				where aue.user_id = ${userId}
					and aue.created_at >= (today.day::timestamp at time zone ${QUOTA_TIMEZONE})
					and aue.created_at < ((today.day + 1)::timestamp at time zone ${QUOTA_TIMEZONE})
			) as ai_calls,
			ul.daily_email_fetch_cap as email_fetch_cap_override,
			ul.daily_ai_call_cap as ai_call_cap_override
		from today
		left join public.user_usage_daily as uud
			on uud.user_id = ${userId} and uud.day = today.day
		left join public.user_limits as ul
			on ul.user_id = ${userId}
	`;
	const row = rows[0];
	if (!row) {
		throw new Error('quota status query returned no row');
	}

	const emailOverride = toOptionalPositiveInteger(
		row.email_fetch_cap_override,
		'user_limits.daily_email_fetch_cap',
	);
	const aiOverride = toOptionalPositiveInteger(
		row.ai_call_cap_override,
		'user_limits.daily_ai_call_cap',
	);
	const emailCap = emailOverride ?? defaults.daily_email_fetch_cap;
	const aiCap = aiOverride ?? defaults.daily_ai_call_cap;
	const bodiesFetched = toSafeInteger(row.bodies_fetched, 'user_usage_daily.bodies_fetched');
	const messagesListed = toSafeInteger(row.messages_listed, 'user_usage_daily.messages_listed');
	const aiCalls = toSafeInteger(row.ai_calls, 'ai_usage_events.count');

	return {
		user_id: userId,
		day: toRequiredString(row.day, 'user_usage_daily.day'),
		messages_listed: messagesListed,
		bodies_fetched: bodiesFetched,
		ai_calls: aiCalls,
		daily_email_fetch_cap: emailCap,
		daily_ai_call_cap: aiCap,
		email_fetch_cap_overridden: emailOverride !== null,
		ai_call_cap_overridden: aiOverride !== null,
		over_email_fetch_cap: bodiesFetched >= emailCap || messagesListed >= emailCap,
		over_ai_call_cap: aiCalls >= aiCap,
		email_fetch_remaining: Math.max(0, emailCap - Math.max(bodiesFetched, messagesListed)),
		ai_call_remaining: Math.max(0, aiCap - aiCalls),
	};
}

export interface UserUsageIncrement {
	messages_listed: number;
	bodies_fetched: number;
}

/** Adds one persisted Gmail page to today's counters (upsert, so the first page creates the row). */
export async function recordUserUsage(
	sql: SqlClient,
	userId: UUID,
	usage: UserUsageIncrement,
): Promise<void> {
	if (usage.messages_listed <= 0 && usage.bodies_fetched <= 0) return;
	await sql`
		insert into public.user_usage_daily (user_id, day, messages_listed, bodies_fetched)
		values (
			${userId},
			(now() at time zone ${QUOTA_TIMEZONE})::date,
			${Math.max(0, usage.messages_listed)},
			${Math.max(0, usage.bodies_fetched)}
		)
		on conflict (user_id, day) do update
		set
			messages_listed = user_usage_daily.messages_listed + excluded.messages_listed,
			bodies_fetched = user_usage_daily.bodies_fetched + excluded.bodies_fetched,
			updated_at = now()
	`;
}

/**
 * Atomically claims the hourly self-heal slot: the upsert only stamps
 * pending_requeue_at when it is null or older than the interval, so two jobs
 * for the same user in the same hour cannot both re-enqueue.
 */
export async function claimPendingRequeueSlot(
	sql: SqlClient,
	userId: UUID,
	minIntervalMinutes: number = PENDING_REQUEUE_MIN_INTERVAL_MINUTES,
): Promise<boolean> {
	const rows = await sql<Array<{ user_id: unknown }>>`
		insert into public.user_usage_daily (user_id, day, pending_requeue_at)
		select ${userId}, (now() at time zone ${QUOTA_TIMEZONE})::date, now()
		where not exists (
			select 1 from public.user_usage_daily as recent
			where recent.user_id = ${userId}
				and recent.pending_requeue_at > now() - make_interval(mins => ${minIntervalMinutes})
		)
		on conflict (user_id, day) do update
		set pending_requeue_at = now(), updated_at = now()
		where user_usage_daily.pending_requeue_at is null
			or user_usage_daily.pending_requeue_at <= now() - make_interval(mins => ${minIntervalMinutes})
		returning user_id
	`;
	return rows.length > 0;
}

/** Hands the slot back when the enqueue failed, so the next job retries instead of waiting an hour. */
export async function releasePendingRequeueSlot(sql: SqlClient, userId: UUID): Promise<void> {
	await sql`
		update public.user_usage_daily
		set pending_requeue_at = null, updated_at = now()
		where user_id = ${userId}
			and day = (now() at time zone ${QUOTA_TIMEZONE})::date
	`;
}

/** This user's PENDING_EXTRACTION rows nobody has touched for a while, oldest first. */
export async function listStalePendingRawEmailIds(
	sql: SqlClient,
	userId: UUID,
	olderThanMinutes: number,
	limit: number,
): Promise<UUID[]> {
	const rows = await sql<Array<{ id: unknown }>>`
		select re.id
		from public.raw_emails as re
		where re.user_id = ${userId}
			and re.status = 'PENDING_EXTRACTION'
			and re.created_at <= now() - make_interval(mins => ${olderThanMinutes})
		order by re.created_at asc
		limit ${limit}
	`;
	return rows.map(row => toRequiredString(row.id, 'raw_emails.id') as UUID);
}

// ---------------------------------------------------------------------------
// Admin: per-user rollup and overrides
// ---------------------------------------------------------------------------

export function isValidQuotaDay(value: string): boolean {
	if (!/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/.test(value)) return false;
	const year = Number(value.slice(0, 4));
	return year >= 2020 && year <= 2200
		&& new Date(`${value}T00:00:00Z`).toISOString().slice(0, 10) === value;
}

export interface ParsedUserLimitsUpdate {
	daily_email_fetch_cap?: number | null;
	daily_ai_call_cap?: number | null;
}

function parseCapField(
	payload: Record<string, unknown>,
	field: keyof UserLimitsUpdateRequest,
): number | null | undefined {
	if (!(field in payload)) return undefined;
	const value = payload[field];
	if (value === null) return null;
	if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1 || value > MAX_CAP_OVERRIDE) {
		throw badRequest(
			'INVALID_CAP',
			`${field} must be null or an integer between 1 and ${MAX_CAP_OVERRIDE}`,
		);
	}
	return value;
}

export function parseUserLimitsUpdate(payload: unknown): ParsedUserLimitsUpdate {
	if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
		throw badRequest('INVALID_BODY', 'Request body must be a JSON object');
	}
	const body = payload as Record<string, unknown>;
	const update: ParsedUserLimitsUpdate = {};
	const emailCap = parseCapField(body, 'daily_email_fetch_cap');
	if (emailCap !== undefined) update.daily_email_fetch_cap = emailCap;
	const aiCap = parseCapField(body, 'daily_ai_call_cap');
	if (aiCap !== undefined) update.daily_ai_call_cap = aiCap;
	if (emailCap === undefined && aiCap === undefined) {
		throw badRequest('EMPTY_UPDATE', 'daily_email_fetch_cap or daily_ai_call_cap is required');
	}
	return update;
}

/**
 * Writes the override row; fields left out keep their current value. A row
 * whose caps are both null is deleted rather than kept as noise. Returns false
 * when the user does not exist.
 */
export async function upsertUserLimits(
	sql: SqlClient,
	userId: UUID,
	update: ParsedUserLimitsUpdate,
): Promise<boolean> {
	const users = await sql<Array<{ id: unknown }>>`
		select u.id from public.users as u where u.id = ${userId}
	`;
	if (users.length === 0) return false;

	const emailCap = update.daily_email_fetch_cap;
	const aiCap = update.daily_ai_call_cap;
	await sql`
		insert into public.user_limits (user_id, daily_email_fetch_cap, daily_ai_call_cap)
		values (${userId}, ${emailCap ?? null}, ${aiCap ?? null})
		on conflict (user_id) do update
		set
			daily_email_fetch_cap = case
				when ${emailCap === undefined} then user_limits.daily_email_fetch_cap
				else ${emailCap ?? null}
			end,
			daily_ai_call_cap = case
				when ${aiCap === undefined} then user_limits.daily_ai_call_cap
				else ${aiCap ?? null}
			end,
			updated_at = now()
	`;
	await sql`
		delete from public.user_limits
		where user_id = ${userId}
			and daily_email_fetch_cap is null
			and daily_ai_call_cap is null
	`;
	return true;
}

interface RollupRowRaw {
	user_id: unknown;
	email: unknown;
	day: unknown;
	messages_listed: unknown;
	bodies_fetched: unknown;
	pending_requeue_at: unknown;
	ai_calls: unknown;
	ai_cost_usd: unknown;
	email_fetch_cap_override: unknown;
	ai_call_cap_override: unknown;
	pending_extraction_count: unknown;
	active_connection_count: unknown;
	deferred_connection_count: unknown;
}

function mapRollupRow(row: RollupRowRaw, defaults: UserQuotaCaps): UserQuotaRollupRow {
	const emailOverride = toOptionalPositiveInteger(
		row.email_fetch_cap_override,
		'user_limits.daily_email_fetch_cap',
	);
	const aiOverride = toOptionalPositiveInteger(
		row.ai_call_cap_override,
		'user_limits.daily_ai_call_cap',
	);
	const emailCap = emailOverride ?? defaults.daily_email_fetch_cap;
	const aiCap = aiOverride ?? defaults.daily_ai_call_cap;
	const bodiesFetched = toSafeInteger(row.bodies_fetched, 'user_usage_daily.bodies_fetched');
	const messagesListed = toSafeInteger(row.messages_listed, 'user_usage_daily.messages_listed');
	const aiCalls = toSafeInteger(row.ai_calls, 'ai_usage_events.count');
	return {
		user_id: toRequiredString(row.user_id, 'users.id') as UUID,
		email: toRequiredString(row.email, 'users.email'),
		day: toRequiredString(row.day, 'user_usage_daily.day'),
		messages_listed: messagesListed,
		bodies_fetched: bodiesFetched,
		ai_calls: aiCalls,
		ai_cost_usd: toNullableNumber(row.ai_cost_usd, 'ai_usage_events.cost_usd') ?? 0,
		pending_extraction_count: toSafeInteger(
			row.pending_extraction_count,
			'raw_emails.pending_extraction_count',
		),
		active_connection_count: toSafeInteger(
			row.active_connection_count,
			'oauth_connections.active_connection_count',
		),
		deferred_connection_count: toSafeInteger(
			row.deferred_connection_count,
			'oauth_connections.deferred_connection_count',
		),
		daily_email_fetch_cap: emailCap,
		daily_ai_call_cap: aiCap,
		email_fetch_cap_override: emailOverride,
		ai_call_cap_override: aiOverride,
		over_email_fetch_cap: bodiesFetched >= emailCap || messagesListed >= emailCap,
		over_ai_call_cap: aiCalls >= aiCap,
		last_pending_requeue_at:
			row.pending_requeue_at === null || row.pending_requeue_at === undefined
				? null
				: toIsoDateTime(row.pending_requeue_at, 'user_usage_daily.pending_requeue_at'),
	};
}

export interface UserQuotaRollup {
	day: string;
	users: UserQuotaRollupRow[];
}

/**
 * Every user's counters for one IST day (today when `day` is null), most
 * active first. Admin-only: it carries each user's email so the operator can
 * tell whose cap to change.
 */
export async function listUserQuotaRollup(
	sql: SqlClient,
	day: string | null,
	defaults: UserQuotaCaps,
	limit: number = USER_QUOTA_ROLLUP_LIMIT,
	onlyUserId: UUID | null = null,
): Promise<UserQuotaRollup> {
	const rows = await sql<RollupRowRaw[]>`
		with target as (
			select coalesce(${day}::date, (now() at time zone ${QUOTA_TIMEZONE})::date) as day
		),
		bounds as (
			select
				target.day,
				(target.day::timestamp at time zone ${QUOTA_TIMEZONE}) as start_at,
				((target.day + 1)::timestamp at time zone ${QUOTA_TIMEZONE}) as end_at
			from target
		)
		select
			u.id as user_id,
			u.email,
			to_char(b.day, 'YYYY-MM-DD') as day,
			coalesce(uud.messages_listed, 0) as messages_listed,
			coalesce(uud.bodies_fetched, 0) as bodies_fetched,
			uud.pending_requeue_at,
			ai.call_count as ai_calls,
			ai.cost_usd as ai_cost_usd,
			ul.daily_email_fetch_cap as email_fetch_cap_override,
			ul.daily_ai_call_cap as ai_call_cap_override,
			pend.pending_count as pending_extraction_count,
			conn.active_count as active_connection_count,
			conn.deferred_count as deferred_connection_count
		from public.users as u
		cross join bounds as b
		left join public.user_usage_daily as uud
			on uud.user_id = u.id and uud.day = b.day
		left join public.user_limits as ul
			on ul.user_id = u.id
		left join lateral (
			select count(*) as call_count, coalesce(sum(aue.cost_usd), 0) as cost_usd
			from public.ai_usage_events as aue
			where aue.user_id = u.id
				and aue.created_at >= b.start_at
				and aue.created_at < b.end_at
		) as ai on true
		left join lateral (
			select count(*) as pending_count
			from public.raw_emails as re
			where re.user_id = u.id and re.status = 'PENDING_EXTRACTION'
		) as pend on true
		left join lateral (
			select
				count(*) filter (where oc.sync_status in ('ACTIVE', 'ERROR_PAUSED')) as active_count,
				count(*) filter (where oc.email_sync_continuation_token is not null) as deferred_count
			from public.oauth_connections as oc
			where oc.user_id = u.id and oc.provider = 'google'
		) as conn on true
		where (${onlyUserId}::uuid is null or u.id = ${onlyUserId}::uuid)
		order by ai.call_count desc, coalesce(uud.bodies_fetched, 0) desc, u.created_at asc, u.id asc
		limit ${limit}
	`;
	const users = rows.map(row => mapRollupRow(row, defaults));
	const resolvedDay = users[0]?.day ?? day ?? new Intl.DateTimeFormat('en-CA', {
		timeZone: QUOTA_TIMEZONE,
		year: 'numeric',
		month: '2-digit',
		day: '2-digit',
	}).format(new Date());
	return { day: resolvedDay, users };
}
