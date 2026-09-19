import type {
	AiMerchantMapping,
	AiMerchantMappingDay,
	AiMerchantMappingSummary,
	AiTavilyKeyCredits,
	AiUsageDashboardResponse,
	AiUsageEvent,
	AiUsageModelBreakdown,
	AiUsageMonthSummary,
	AiUsageOperation,
	AiUsageOperationBreakdown,
	AiProcessingRun,
	AiProcessingSummary,
	AiUsageProvider,
	AiUsageStatus,
	AiUsageSummaryResponse,
	MerchantType,
} from '../../../shared/types';
import { isMerchantMappingKillSwitched } from '../lib/feature-flags';
import { tavilyCreditCounterName } from '../workers/merchant-mapper';
import { listJobSchedules } from './job-schedules.service';
import type { SqlClient } from '../lib/db/client';
import {
	toBoolean,
	toIsoDateTime,
	toNullableNumber,
	toNullableString,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';

const OPENROUTER_KEY_URL = 'https://openrouter.ai/api/v1/key';
const OPENROUTER_CREDITS_URL = 'https://openrouter.ai/api/v1/credits';
const OPENROUTER_KEY_TIMEOUT_MS = 5_000;
export const AI_USAGE_PAGE_SIZE = 30;
const TAVILY_USAGE_URL = 'https://api.tavily.com/usage';
const TAVILY_USAGE_TIMEOUT_MS = 5_000;
const MERCHANT_MAPPING_MAX_DAYS = 30;
// Published free-tier allowance, reset on the 1st. Used as the ceiling when
// Tavily's endpoint is unreachable so a derived balance still renders.
const TAVILY_FREE_PLAN_CREDITS = 1_000;

export interface AiUsageMetrics {
	promptTokens: number;
	completionTokens: number;
	reasoningTokens: number;
	cachedTokens: number;
	totalTokens: number;
	costUsd: number | null;
}

export interface RecordAiUsageInput extends AiUsageMetrics {
	userId?: string | null;
	callId?: string | null;
	attemptNumber?: number | null;
	provider: AiUsageProvider;
	model: string;
	operation: AiUsageOperation;
	status: AiUsageStatus;
	httpStatus: number | null;
	itemCount: number;
	latencyMs: number;
	errorCode?: string | null;
	generationId?: string | null;
	startedAtMs: number;
}

export interface CaptureAiUsageAttemptInput {
	sql: SqlClient;
	userId?: string | null;
	callId?: string | null;
	attemptNumber?: number | null;
	provider: AiUsageProvider;
	model: string;
	operation: AiUsageOperation;
	itemCount: number;
	startedAtMs: number;
	status: AiUsageStatus;
	httpStatus: number | null;
	payload?: unknown;
	errorCode?: string | null;
	generationId?: string | null;
}

interface UsageSummaryRowRaw {
	tracked_since: unknown;
	all_time_cost: unknown;
	month_cost: unknown;
	all_time_requests: unknown;
	month_requests: unknown;
}

interface MonthRowRaw {
	month: unknown;
	request_count: unknown;
	success_count: unknown;
	cost: unknown;
	item_count: unknown;
	average_latency_ms: unknown;
	total_tokens: unknown;
}

interface ModelRowRaw {
	provider: unknown;
	model: unknown;
	request_count: unknown;
	success_count: unknown;
	cost: unknown;
	prompt_tokens: unknown;
	completion_tokens: unknown;
	reasoning_tokens: unknown;
	cached_tokens: unknown;
	total_tokens: unknown;
	item_count: unknown;
	average_latency_ms: unknown;
}

interface EventRowRaw {
	id: unknown;
	call_id: unknown;
	attempt_number: unknown;
	provider: unknown;
	model: unknown;
	operation: unknown;
	status: unknown;
	http_status: unknown;
	item_count: unknown;
	prompt_tokens: unknown;
	completion_tokens: unknown;
	reasoning_tokens: unknown;
	cached_tokens: unknown;
	total_tokens: unknown;
	cost_usd: unknown;
	latency_ms: unknown;
	error_code: unknown;
	generation_id: unknown;
	created_at: unknown;
	total_event_count: unknown;
}

interface ProcessingSummaryRowRaw {
	emails_handled: unknown;
	template_hits: unknown;
	ai_routed_emails: unknown;
	email_final_failures: unknown;
	transactions_created: unknown;
	logical_calls: unknown;
	provider_attempts: unknown;
	first_attempt_successes: unknown;
	recovered_calls: unknown;
	final_failures: unknown;
	in_progress_calls: unknown;
}

interface ProcessingRunRowRaw {
	run_id: unknown;
	run_item_count: unknown;
	run_status: unknown;
	run_attempt_count: unknown;
	run_started_at: unknown;
	run_completed_at: unknown;
	attempt_id: unknown;
	attempt_number: unknown;
	model: unknown;
	attempt_status: unknown;
	http_status: unknown;
	error_code: unknown;
	cost_usd: unknown;
	latency_ms: unknown;
}

interface OperationRowRaw {
	operation: unknown;
	request_count: unknown;
	success_count: unknown;
	cost: unknown;
	total_tokens: unknown;
	item_count: unknown;
	average_latency_ms: unknown;
	background_only: unknown;
}

interface MappingDayRowRaw {
	day: unknown;
	tavily_searches: unknown;
	merchant_ai_calls: unknown;
	merchants_mapped: unknown;
	merchants_skipped_unsure: unknown;
	counters_available: unknown;
}

interface MappingRowRaw {
	day: unknown;
	search_key: unknown;
	raw_payee: unknown;
	canonical_name: unknown;
	merchant_type: unknown;
	owner_user_id: unknown;
	default_category_name: unknown;
	confirmed: unknown;
}

function finiteNonNegativeNumber(value: unknown): number | null {
	if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return null;
	return value;
}

function finiteNonNegativeInteger(value: unknown): number {
	const parsed = finiteNonNegativeNumber(value);
	return parsed !== null && Number.isSafeInteger(parsed) ? parsed : 0;
}

export function extractAiUsageMetrics(payload: unknown): AiUsageMetrics {
	if (typeof payload !== 'object' || payload === null) {
		return {
			promptTokens: 0,
			completionTokens: 0,
			reasoningTokens: 0,
			cachedTokens: 0,
			totalTokens: 0,
			costUsd: null,
		};
	}

	const usage = (payload as Record<string, unknown>).usage;
	if (typeof usage !== 'object' || usage === null) {
		return {
			promptTokens: 0,
			completionTokens: 0,
			reasoningTokens: 0,
			cachedTokens: 0,
			totalTokens: 0,
			costUsd: null,
		};
	}

	const record = usage as Record<string, unknown>;
	const completionDetails =
		typeof record.completion_tokens_details === 'object' && record.completion_tokens_details !== null
			? (record.completion_tokens_details as Record<string, unknown>)
			: {};
	const promptDetails =
		typeof record.prompt_tokens_details === 'object' && record.prompt_tokens_details !== null
			? (record.prompt_tokens_details as Record<string, unknown>)
			: {};

	return {
		promptTokens: finiteNonNegativeInteger(record.prompt_tokens),
		completionTokens: finiteNonNegativeInteger(record.completion_tokens),
		reasoningTokens: finiteNonNegativeInteger(completionDetails.reasoning_tokens),
		cachedTokens: finiteNonNegativeInteger(promptDetails.cached_tokens),
		totalTokens: finiteNonNegativeInteger(record.total_tokens),
		costUsd: finiteNonNegativeNumber(record.cost),
	};
}

// Usage logging must never turn a successful extraction into a failed email.
// A missing migration or temporary DB issue is visible in Worker logs and can
// be repaired without interrupting the transaction pipeline.
export async function recordAiUsageEvent(
	sql: SqlClient,
	input: RecordAiUsageInput,
): Promise<void> {
	try {
		await sql`
			insert into public.ai_usage_events (
				user_id, call_id, attempt_number,
				provider, model, operation, status, http_status, item_count,
				prompt_tokens, completion_tokens, reasoning_tokens, cached_tokens,
				total_tokens, cost_usd, latency_ms, error_code, generation_id,
				created_at
			)
			values (
				${input.userId ?? null}, ${input.callId ?? null}, ${input.attemptNumber ?? null},
				${input.provider}, ${input.model}, ${input.operation}, ${input.status},
				${input.httpStatus}, ${input.itemCount}, ${input.promptTokens},
				${input.completionTokens}, ${input.reasoningTokens}, ${input.cachedTokens},
				${input.totalTokens}, ${input.costUsd}, ${Math.max(0, input.latencyMs)},
				${input.errorCode ?? null}, ${input.generationId ?? null},
				${new Date(input.startedAtMs)}
			)
		`;
	} catch (error) {
		console.error('AI_USAGE_EVENT_PERSIST_FAILED', {
			provider: input.provider,
			model: input.model,
			operation: input.operation,
			message: error instanceof Error ? error.message : 'Unknown error',
		});
	}
}

export async function captureAiUsageAttempt(
	input: CaptureAiUsageAttemptInput,
): Promise<void> {
	const metrics = extractAiUsageMetrics(input.payload);
	await recordAiUsageEvent(input.sql, {
		...metrics,
		userId: input.userId,
		callId: input.callId,
		attemptNumber: input.attemptNumber,
		costUsd:
			input.status === 'ERROR' && input.httpStatus !== null && input.httpStatus >= 400
				? 0
				: metrics.costUsd,
		provider: input.provider,
		model: input.model,
		operation: input.operation,
		status: input.status,
		httpStatus: input.httpStatus,
		itemCount: input.itemCount,
		latencyMs: Date.now() - input.startedAtMs,
		errorCode: input.errorCode,
		generationId: input.generationId,
		startedAtMs: input.startedAtMs,
	});
}

export interface AiCallRunInput {
	sql: SqlClient;
	id: string;
	userId: string;
	operation: AiUsageOperation;
	itemCount: number;
	startedAtMs: number;
}

export async function startAiCallRun(input: AiCallRunInput): Promise<void> {
	try {
		await input.sql`
			insert into public.ai_call_runs (
				id, user_id, operation, item_count, status,
				attempt_count, started_at, completed_at
			)
			values (
				${input.id}, ${input.userId}, ${input.operation}, ${input.itemCount},
				'RUNNING', 0, ${new Date(input.startedAtMs)}, null
			)
			on conflict (id) do nothing
		`;
	} catch (error) {
		console.error('AI_CALL_RUN_START_FAILED', {
			callId: input.id,
			message: error instanceof Error ? error.message : 'Unknown error',
		});
	}
}

export async function finishAiCallRun(
	sql: SqlClient,
	callId: string,
	status: 'SUCCESS' | 'FAILED',
	attemptCount: number,
): Promise<void> {
	try {
		await sql`
			update public.ai_call_runs
			set status = ${status},
				attempt_count = ${Math.max(0, attemptCount)},
				completed_at = now()
			where id = ${callId}
				and status = 'RUNNING'
		`;
	} catch (error) {
		console.error('AI_CALL_RUN_FINISH_FAILED', {
			callId,
			message: error instanceof Error ? error.message : 'Unknown error',
		});
	}
}

export function isAiUsageAdmin(env: Env, userId: string): boolean {
	const configuredId = env.AI_USAGE_ADMIN_USER_ID?.trim();
	return Boolean(configuredId && configuredId === userId);
}

function currentMonthIst(): string {
	return new Intl.DateTimeFormat('en-CA', {
		timeZone: 'Asia/Kolkata',
		year: 'numeric',
		month: '2-digit',
	}).format(new Date());
}

export function isValidAiUsageMonth(value: string): boolean {
	if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(value)) return false;
	const year = Number(value.slice(0, 4));
	return year >= 2020 && year <= 2200;
}

interface OpenRouterKeyPayload {
	data?: {
		usage?: unknown;
		usage_monthly?: unknown;
		limit?: unknown;
		limit_remaining?: unknown;
	};
}

interface OpenRouterCreditsPayload {
	data?: {
		total_credits?: unknown;
		total_usage?: unknown;
	};
}

async function fetchOpenRouterJson(
	url: string,
	apiKey: string,
	fetcher: typeof fetch,
): Promise<unknown | null> {
	const controller = new AbortController();
	const timeoutId = setTimeout(() => controller.abort(), OPENROUTER_KEY_TIMEOUT_MS);
	try {
		const response = await fetcher(url, {
			headers: { Authorization: `Bearer ${apiKey}` },
			signal: controller.signal,
		});
		return response.ok ? await response.json() : null;
	} catch {
		return null;
	} finally {
		clearTimeout(timeoutId);
	}
}

export async function fetchOpenRouterKeyUsage(
	apiKey: string | null,
	fetcher: typeof fetch = fetch,
	managementApiKey: string | null = null,
): Promise<AiUsageSummaryResponse['account']> {
	if (!apiKey) return null;

	const [keyPayloadRaw, creditsPayloadRaw] = await Promise.all([
		fetchOpenRouterJson(OPENROUTER_KEY_URL, apiKey, fetcher),
		managementApiKey
			? fetchOpenRouterJson(OPENROUTER_CREDITS_URL, managementApiKey, fetcher)
			: Promise.resolve(null),
	]);
	const keyPayload = keyPayloadRaw as OpenRouterKeyPayload | null;
	const creditsPayload = creditsPayloadRaw as OpenRouterCreditsPayload | null;
	const keyAllTime = finiteNonNegativeNumber(keyPayload?.data?.usage);
	const currentMonth = finiteNonNegativeNumber(keyPayload?.data?.usage_monthly);
	if (keyAllTime === null || currentMonth === null) return null;

	const purchasedCredits = finiteNonNegativeNumber(creditsPayload?.data?.total_credits);
	const accountUsage = finiteNonNegativeNumber(creditsPayload?.data?.total_usage);
	return {
		provider: 'openrouter',
		all_time_cost_usd: accountUsage ?? keyAllTime,
		current_month_cost_usd: currentMonth,
		purchased_credits_usd: purchasedCredits,
		credits_remaining_usd:
			purchasedCredits !== null && accountUsage !== null
				? Math.max(0, purchasedCredits - accountUsage)
				: null,
		limit_usd: finiteNonNegativeNumber(keyPayload?.data?.limit),
		remaining_usd: finiteNonNegativeNumber(keyPayload?.data?.limit_remaining),
		fetched_at: new Date().toISOString(),
	};
}

interface TavilyUsagePayload {
	key?: {
		usage?: unknown;
		limit?: unknown;
	};
	account?: {
		current_plan?: unknown;
		plan_usage?: unknown;
		plan_limit?: unknown;
	};
}

/**
 * Live credit state for one Tavily key via GET /usage. Any failure (network,
 * timeout, revoked key, rate limit, malformed body) returns null so the
 * dashboard degrades to "unavailable" instead of breaking.
 */
export async function fetchTavilyKeyUsage(
	apiKey: string,
	fetcher: typeof fetch = fetch,
): Promise<TavilyUsagePayload | null> {
	const controller = new AbortController();
	const timeoutId = setTimeout(() => controller.abort(), TAVILY_USAGE_TIMEOUT_MS);
	try {
		const response = await fetcher(TAVILY_USAGE_URL, {
			headers: { Authorization: `Bearer ${apiKey}`, Accept: 'application/json' },
			signal: controller.signal,
		});
		if (!response.ok) return null;
		const parsed = (await response.json()) as TavilyUsagePayload;
		if (typeof parsed !== 'object' || parsed === null) return null;
		return parsed;
	} catch {
		return null;
	} finally {
		clearTimeout(timeoutId);
	}
}

/**
 * The mapper rotates through several comma-separated keys, starting each run
 * on the first. This reports each key's own credit state with a position
 * label only — the key material itself never leaves the server.
 */
/**
 * Credits this month, per key position, from our own ledger.
 *
 * Tavily's GET /usage reports zero on free-tier keys even immediately after a
 * billed search (verified against a live key), so it cannot be used as a
 * balance. Every search instead asks for `include_usage` and the billed figure
 * is banked in usage_counters, which makes this the authoritative number.
 * Allowances reset on the 1st, so the window is the calendar month.
 */
async function loadTavilyCreditsThisMonth(
	sql: SqlClient,
	keyCount: number,
): Promise<number[]> {
	const counters = Array.from({ length: keyCount }, (_unused, index) =>
		tavilyCreditCounterName(index),
	);
	const rows = await sql<Array<{ counter: unknown; credits: unknown }>>`
		select uc.counter, coalesce(sum(uc.count), 0) as credits
		from public.usage_counters as uc
		where uc.counter = any(${counters}::text[])
			and uc.day >= date_trunc('month', current_date)::date
		group by uc.counter
	`;

	const byCounter = new Map<string, number>();
	for (const row of rows) {
		byCounter.set(
			toRequiredString(row.counter, 'usage_counters.counter'),
			toSafeInteger(row.credits, 'usage_counters.count'),
		);
	}
	return counters.map(counter => byCounter.get(counter) ?? 0);
}

export async function fetchTavilyKeysUsage(
	sql: SqlClient,
	env: Env,
	fetcher: typeof fetch = fetch,
): Promise<AiTavilyKeyCredits[]> {
	const apiKeys = (env.TAVILY_API_KEY ?? '')
		.split(',')
		.map(key => key.trim())
		.filter(key => key.length > 0);
	if (apiKeys.length === 0) return [];

	const [settled, creditsByKey] = await Promise.all([
		Promise.allSettled(apiKeys.map(key => fetchTavilyKeyUsage(key, fetcher))),
		loadTavilyCreditsThisMonth(sql, apiKeys.length),
	]);

	const keys = apiKeys.map((_, index) => {
		const payload = settled[index].status === 'fulfilled' ? settled[index].value : null;
		const planLimit = finiteNonNegativeNumber(payload?.account?.plan_limit);
		// The plan ceiling is still worth reading from Tavily — only its usage
		// figures are untrustworthy. Free keys with an unreachable endpoint fall
		// back to the published free-tier allowance so a balance still shows.
		const limitCredits = planLimit ?? TAVILY_FREE_PLAN_CREDITS;
		const usedCredits = creditsByKey[index] ?? 0;

		return {
			label: `Key ${index + 1}`,
			is_live: false,
			used_credits: usedCredits,
			limit_credits: limitCredits,
			remaining_credits: Math.max(0, limitCredits - usedCredits),
			plan_name:
				payload === null
					? null
					: toNullableString(payload.account?.current_plan, 'tavily.account.current_plan'),
			reported_plan_usage: finiteNonNegativeNumber(payload?.account?.plan_usage),
			// Now strictly "could not reach Tavily" — the balance no longer
			// depends on that call succeeding.
			unavailable: payload === null,
			fetched_at: payload === null ? null : new Date().toISOString(),
		};
	});

	// The mapper starts each run on the first key and rotates forward only when
	// one is exhausted, so the key that will actually serve is the first with
	// credits left. Marking key 1 live regardless would hide an exhausted key —
	// the precise failure this panel exists to surface.
	const liveIndex = keys.findIndex(
		key => key.remaining_credits === null || key.remaining_credits > 0,
	);
	const live = keys[liveIndex === -1 ? 0 : liveIndex];
	if (live) live.is_live = true;
	return keys;
}

export async function getMerchantMappingSummary(
	sql: SqlClient,
	env: Env | undefined,
): Promise<AiMerchantMappingSummary> {
	// Days come from both sides of the ledger: operation counters (a run, or a
	// deliberate preview, really spent Tavily/AI credits) and aliases (the
	// mappings those calls produced).
	//
	// An alias day only counts as mapper activity if it has a counter row or at
	// least one AI-created merchant. Without that test the seed migration —
	// which inserts the baseline alias taxonomy (BHARATPE, CCAVENUE, CRED …) in
	// one burst — renders as a run reporting zero searches and zero mappings
	// while listing every seeded alias underneath.
	const dayRows = await sql<MappingDayRowRaw[]>`
		with day_counts as (
			select
				uc.day,
				coalesce(sum(uc.count) filter (where uc.counter = 'tavily_searches'), 0) as tavily_searches,
				coalesce(sum(uc.count) filter (where uc.counter = 'merchant_ai_calls'), 0) as merchant_ai_calls,
				coalesce(sum(uc.count) filter (where uc.counter = 'merchants_mapped'), 0) as merchants_mapped,
				coalesce(sum(uc.count) filter (where uc.counter = 'merchants_skipped_unsure'), 0) as merchants_skipped_unsure
			from public.usage_counters as uc
			group by uc.day
		),
		mapped_days as (
			select date(gma.created_at) as day
			from public.global_merchant_aliases as gma
			join public.global_merchants as gm on gm.id = gma.merchant_id
			where gm.source = 'AI'
			group by 1
		),
		days as (
			select day from day_counts
			union
			select day from mapped_days
		)
		select
			to_char(d.day, 'YYYY-MM-DD') as day,
			coalesce(dc.tavily_searches, 0) as tavily_searches,
			coalesce(dc.merchant_ai_calls, 0) as merchant_ai_calls,
			coalesce(dc.merchants_mapped, 0) as merchants_mapped,
			coalesce(dc.merchants_skipped_unsure, 0) as merchants_skipped_unsure,
			(dc.day is not null) as counters_available
		from days as d
		left join day_counts as dc on dc.day = d.day
		order by d.day desc
		limit ${MERCHANT_MAPPING_MAX_DAYS + 1}
	`;

	const truncated = dayRows.length > MERCHANT_MAPPING_MAX_DAYS;
	const visibleDayRows = truncated ? dayRows.slice(0, MERCHANT_MAPPING_MAX_DAYS) : dayRows;
	const visibleDays = visibleDayRows.map(row => toRequiredString(row.day, 'usage_counters.day'));

	// Mappings are fetched only for the days actually being shown; the full
	// alias table grows without bound and most of it is not mapper output.
	// A run attaches aliases to seeded and manual merchants too (July 2026:
	// 134 of 187 pointed at seeded brands), so the merchant's own source must
	// NOT filter this list — the day-level test above already excludes the
	// seed burst.
	const mappingRows = visibleDays.length === 0
		? []
		: await sql<MappingRowRaw[]>`
			select
				to_char(date(gma.created_at), 'YYYY-MM-DD') as day,
				gma.search_key,
				raw.counterparty_raw as raw_payee,
				gm.canonical_name,
				gm.type as merchant_type,
				gm.owner_user_id,
				c.name as default_category_name,
				gm.confirmed
			from public.global_merchant_aliases as gma
			join public.global_merchants as gm on gm.id = gma.merchant_id
			left join public.categories as c on c.id = gm.default_category_id
			left join lateral (
				select fe.counterparty_raw
				from public.financial_events as fe
				where fe.search_key = gma.search_key
					and fe.counterparty_raw is not null
				order by fe.created_at desc
				limit 1
			) as raw on true
			where to_char(date(gma.created_at), 'YYYY-MM-DD') = any(${visibleDays}::text[])
			order by date(gma.created_at) desc, gma.created_at desc
		`;

	const mappingsByDay = new Map<string, AiMerchantMapping[]>();
	for (const row of mappingRows) {
		const day = toRequiredString(row.day, 'global_merchant_aliases.created_at day');
		const mapping: AiMerchantMapping = {
			search_key: toRequiredString(row.search_key, 'global_merchant_aliases.search_key'),
			raw_payee: toNullableString(row.raw_payee, 'financial_events.counterparty_raw'),
			canonical_name: toRequiredString(row.canonical_name, 'global_merchants.canonical_name'),
			merchant_type: toRequiredString(
				row.merchant_type,
				'global_merchants.type',
			) as MerchantType,
			owner_user_id: toNullableString(row.owner_user_id, 'global_merchants.owner_user_id'),
			default_category: toNullableString(
				row.default_category_name,
				'categories.name',
			),
			confirmed: toBoolean(row.confirmed, 'global_merchants.confirmed'),
		};
		const list = mappingsByDay.get(day) ?? [];
		list.push(mapping);
		mappingsByDay.set(day, list);
	}

	const days: AiMerchantMappingDay[] = visibleDayRows.map(row => {
		const day = toRequiredString(row.day, 'usage_counters.day');
		return {
			day,
			tavily_searches: toSafeInteger(row.tavily_searches, 'usage_counters.tavily_searches'),
			merchant_ai_calls: toSafeInteger(
				row.merchant_ai_calls,
				'usage_counters.merchant_ai_calls',
			),
			merchants_mapped: toSafeInteger(row.merchants_mapped, 'usage_counters.merchants_mapped'),
			merchants_skipped_unsure: toSafeInteger(
				row.merchants_skipped_unsure,
				'usage_counters.merchants_skipped_unsure',
			),
			counters_available: toBoolean(row.counters_available, 'usage_counters.present'),
			mappings: mappingsByDay.get(day) ?? [],
		};
	});

	// Sourced from the schedule the dispatcher actually reads, so this banner
	// cannot claim the mapper is running while the claim query ignores it.
	const mapperEnabled =
		env === undefined
			? false
			: !isMerchantMappingKillSwitched(env) &&
				(await listJobSchedules(sql, env)).some(
					schedule => schedule.job === 'merchant_mapping' && schedule.enabled,
				);

	return {
		mapper_enabled: mapperEnabled,
		days,
		truncated,
	};
}

export async function getAiUsageSummary(
	sql: SqlClient,
	env: Env,
	userId: string,
	fetcher: typeof fetch = fetch,
): Promise<AiUsageSummaryResponse> {
	const [rows, account] = await Promise.all([
		sql<UsageSummaryRowRaw[]>`
			select
				min(aue.created_at) as tracked_since,
				coalesce(sum(aue.cost_usd), 0) as all_time_cost,
				coalesce(sum(aue.cost_usd) filter (
					where aue.created_at >= (
						date_trunc('month', now() at time zone 'Asia/Kolkata')
						at time zone 'Asia/Kolkata'
					)
				), 0) as month_cost,
				count(*) as all_time_requests,
				count(*) filter (
					where aue.created_at >= (
						date_trunc('month', now() at time zone 'Asia/Kolkata')
						at time zone 'Asia/Kolkata'
					)
				) as month_requests
			from public.ai_usage_events as aue
			-- Background jobs (sender classification, merchant mapping) record no
			-- user, so a plain equality filter hid their spend entirely. This is
			-- an admin-only view of a single-user deployment: system work counts.
			where (aue.user_id = ${userId} or aue.user_id is null)
		`,
		fetchOpenRouterKeyUsage(
			env.OPENROUTER_API_KEY?.trim() || null,
			fetcher,
			env.OPENROUTER_MANAGEMENT_API_KEY?.trim() || null,
		),
	]);

	const row = rows[0];
	return {
		account,
		tracked_since: row?.tracked_since
			? toIsoDateTime(row.tracked_since, 'ai_usage_events.tracked_since')
			: null,
		tracked_all_time_cost_usd:
			toNullableNumber(row?.all_time_cost ?? 0, 'ai_usage_events.all_time_cost') ?? 0,
		tracked_current_month_cost_usd:
			toNullableNumber(row?.month_cost ?? 0, 'ai_usage_events.month_cost') ?? 0,
		tracked_all_time_request_count: toSafeInteger(
			row?.all_time_requests ?? 0,
			'ai_usage_events.all_time_requests',
		),
		tracked_current_month_request_count: toSafeInteger(
			row?.month_requests ?? 0,
			'ai_usage_events.month_requests',
		),
	};
}

function mapMonth(row: MonthRowRaw): AiUsageMonthSummary {
	return {
		month: toRequiredString(row.month, 'ai_usage_events.month'),
		request_count: toSafeInteger(row.request_count, 'ai_usage_events.request_count'),
		success_count: toSafeInteger(row.success_count, 'ai_usage_events.success_count'),
		cost_usd: toNullableNumber(row.cost, 'ai_usage_events.cost') ?? 0,
		item_count: toSafeInteger(row.item_count, 'ai_usage_events.item_count'),
		average_latency_ms: toSafeInteger(
			row.average_latency_ms,
			'ai_usage_events.average_latency_ms',
		),
		total_tokens: toSafeInteger(row.total_tokens, 'ai_usage_events.total_tokens'),
	};
}

function mapOperation(row: OperationRowRaw): AiUsageOperationBreakdown {
	return {
		operation: toRequiredString(
			row.operation,
			'ai_usage_events.operation',
		) as AiUsageOperation,
		request_count: toSafeInteger(row.request_count, 'ai_usage_events.request_count'),
		success_count: toSafeInteger(row.success_count, 'ai_usage_events.success_count'),
		cost_usd: toNullableNumber(row.cost, 'ai_usage_events.cost') ?? 0,
		total_tokens: toSafeInteger(row.total_tokens, 'ai_usage_events.total_tokens'),
		item_count: toSafeInteger(row.item_count, 'ai_usage_events.item_count'),
		average_latency_ms: toSafeInteger(
			row.average_latency_ms,
			'ai_usage_events.average_latency_ms',
		),
		background_only: toBoolean(row.background_only, 'ai_usage_events.background_only'),
	};
}

function mapModel(row: ModelRowRaw): AiUsageModelBreakdown {
	return {
		provider: toRequiredString(row.provider, 'ai_usage_events.provider') as AiUsageProvider,
		model: toRequiredString(row.model, 'ai_usage_events.model'),
		request_count: toSafeInteger(row.request_count, 'ai_usage_events.request_count'),
		success_count: toSafeInteger(row.success_count, 'ai_usage_events.success_count'),
		cost_usd: toNullableNumber(row.cost, 'ai_usage_events.cost') ?? 0,
		prompt_tokens: toSafeInteger(row.prompt_tokens, 'ai_usage_events.prompt_tokens'),
		completion_tokens: toSafeInteger(
			row.completion_tokens,
			'ai_usage_events.completion_tokens',
		),
		reasoning_tokens: toSafeInteger(row.reasoning_tokens, 'ai_usage_events.reasoning_tokens'),
		cached_tokens: toSafeInteger(row.cached_tokens, 'ai_usage_events.cached_tokens'),
		total_tokens: toSafeInteger(row.total_tokens, 'ai_usage_events.total_tokens'),
		item_count: toSafeInteger(row.item_count, 'ai_usage_events.item_count'),
		average_latency_ms: toSafeInteger(
			row.average_latency_ms,
			'ai_usage_events.average_latency_ms',
		),
	};
}

function mapEvent(row: EventRowRaw): AiUsageEvent {
	return {
		id: toRequiredString(row.id, 'ai_usage_events.id'),
		call_id: toNullableString(row.call_id, 'ai_usage_events.call_id'),
		attempt_number:
			row.attempt_number === null
				? null
				: toSafeInteger(row.attempt_number, 'ai_usage_events.attempt_number'),
		provider: toRequiredString(row.provider, 'ai_usage_events.provider') as AiUsageProvider,
		model: toRequiredString(row.model, 'ai_usage_events.model'),
		operation: toRequiredString(row.operation, 'ai_usage_events.operation') as AiUsageOperation,
		status: toRequiredString(row.status, 'ai_usage_events.status') as AiUsageStatus,
		http_status:
			row.http_status === null
				? null
				: toSafeInteger(row.http_status, 'ai_usage_events.http_status'),
		item_count: toSafeInteger(row.item_count, 'ai_usage_events.item_count'),
		prompt_tokens: toSafeInteger(row.prompt_tokens, 'ai_usage_events.prompt_tokens'),
		completion_tokens: toSafeInteger(
			row.completion_tokens,
			'ai_usage_events.completion_tokens',
		),
		reasoning_tokens: toSafeInteger(row.reasoning_tokens, 'ai_usage_events.reasoning_tokens'),
		cached_tokens: toSafeInteger(row.cached_tokens, 'ai_usage_events.cached_tokens'),
		total_tokens: toSafeInteger(row.total_tokens, 'ai_usage_events.total_tokens'),
		cost_usd: toNullableNumber(row.cost_usd, 'ai_usage_events.cost_usd'),
		latency_ms: toSafeInteger(row.latency_ms, 'ai_usage_events.latency_ms'),
		error_code: toNullableString(row.error_code, 'ai_usage_events.error_code'),
		generation_id: toNullableString(row.generation_id, 'ai_usage_events.generation_id'),
		created_at: toIsoDateTime(row.created_at, 'ai_usage_events.created_at'),
	};
}

function mapProcessingSummary(
	row: ProcessingSummaryRowRaw | undefined,
	recentRuns: AiProcessingRun[],
): AiProcessingSummary {
	const integer = (value: unknown, field: string) =>
		toSafeInteger(value ?? 0, `ai_processing.${field}`);
	return {
		emails_handled: integer(row?.emails_handled, 'emails_handled'),
		template_hits: integer(row?.template_hits, 'template_hits'),
		ai_routed_emails: integer(row?.ai_routed_emails, 'ai_routed_emails'),
		email_final_failures: integer(row?.email_final_failures, 'email_final_failures'),
		transactions_created: integer(row?.transactions_created, 'transactions_created'),
		logical_calls: integer(row?.logical_calls, 'logical_calls'),
		provider_attempts: integer(row?.provider_attempts, 'provider_attempts'),
		first_attempt_successes: integer(
			row?.first_attempt_successes,
			'first_attempt_successes',
		),
		recovered_calls: integer(row?.recovered_calls, 'recovered_calls'),
		final_failures: integer(row?.final_failures, 'final_failures'),
		in_progress_calls: integer(row?.in_progress_calls, 'in_progress_calls'),
		recent_runs: recentRuns,
	};
}

function mapProcessingRuns(rows: ProcessingRunRowRaw[]): AiProcessingRun[] {
	const runs = new Map<string, AiProcessingRun>();
	for (const row of rows) {
		const id = toRequiredString(row.run_id, 'ai_call_runs.id');
		let run = runs.get(id);
		if (!run) {
			run = {
				id,
				item_count: toSafeInteger(row.run_item_count, 'ai_call_runs.item_count'),
				status: toRequiredString(row.run_status, 'ai_call_runs.status') as AiProcessingRun['status'],
				attempt_count: toSafeInteger(
					row.run_attempt_count,
					'ai_call_runs.attempt_count',
				),
				started_at: toIsoDateTime(row.run_started_at, 'ai_call_runs.started_at'),
				completed_at: row.run_completed_at
					? toIsoDateTime(row.run_completed_at, 'ai_call_runs.completed_at')
					: null,
				attempts: [],
			};
			runs.set(id, run);
		}

		if (row.attempt_id === null) continue;
		run.attempts.push({
			id: toRequiredString(row.attempt_id, 'ai_usage_events.id'),
			attempt_number: toSafeInteger(
				row.attempt_number,
				'ai_usage_events.attempt_number',
			),
			model: toRequiredString(row.model, 'ai_usage_events.model'),
			status: toRequiredString(
				row.attempt_status,
				'ai_usage_events.status',
			) as AiUsageStatus,
			http_status:
				row.http_status === null
					? null
					: toSafeInteger(row.http_status, 'ai_usage_events.http_status'),
			error_code: toNullableString(row.error_code, 'ai_usage_events.error_code'),
			cost_usd: toNullableNumber(row.cost_usd, 'ai_usage_events.cost_usd'),
			latency_ms: toSafeInteger(row.latency_ms, 'ai_usage_events.latency_ms'),
		});
	}
	return [...runs.values()];
}

export async function getAiUsageDashboard(
	sql: SqlClient,
	userId: string,
	selectedMonth = currentMonthIst(),
	page = 1,
	env?: Env,
): Promise<AiUsageDashboardResponse> {
	const offset = (page - 1) * AI_USAGE_PAGE_SIZE;
	const [
		monthRows,
		modelRows,
		operationRows,
		eventRows,
		processingRows,
		processingRunRows,
		merchantMapping,
	] =
		await Promise.all([
			sql<MonthRowRaw[]>`
			select
				to_char(aue.created_at at time zone 'Asia/Kolkata', 'YYYY-MM') as month,
				count(*) as request_count,
				count(*) filter (where aue.status = 'SUCCESS') as success_count,
				coalesce(sum(aue.cost_usd), 0) as cost,
				coalesce(sum(aue.item_count), 0) as item_count,
				coalesce(round(avg(aue.latency_ms)), 0) as average_latency_ms,
				coalesce(sum(aue.total_tokens), 0) as total_tokens
			from public.ai_usage_events as aue
			-- Background jobs (sender classification, merchant mapping) record no
			-- user, so a plain equality filter hid their spend entirely. This is
			-- an admin-only view of a single-user deployment: system work counts.
			where (aue.user_id = ${userId} or aue.user_id is null)
			group by 1
			order by 1 desc
		`,
		sql<ModelRowRaw[]>`
			select
				aue.provider,
				aue.model,
				count(*) as request_count,
				count(*) filter (where aue.status = 'SUCCESS') as success_count,
				coalesce(sum(aue.cost_usd), 0) as cost,
				coalesce(sum(aue.prompt_tokens), 0) as prompt_tokens,
				coalesce(sum(aue.completion_tokens), 0) as completion_tokens,
				coalesce(sum(aue.reasoning_tokens), 0) as reasoning_tokens,
				coalesce(sum(aue.cached_tokens), 0) as cached_tokens,
				coalesce(sum(aue.total_tokens), 0) as total_tokens,
				coalesce(sum(aue.item_count), 0) as item_count,
				coalesce(round(avg(aue.latency_ms)), 0) as average_latency_ms
			from public.ai_usage_events as aue
			-- Background jobs (sender classification, merchant mapping) record no
			-- user, so a plain equality filter hid their spend entirely. This is
			-- an admin-only view of a single-user deployment: system work counts.
			where (aue.user_id = ${userId} or aue.user_id is null)
			and aue.created_at >= (
				to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD')::timestamp
				at time zone 'Asia/Kolkata'
			)
			and aue.created_at < (
				(to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD') + interval '1 month')::timestamp
				at time zone 'Asia/Kolkata'
			)
			group by aue.provider, aue.model
			order by cost desc, request_count desc, aue.provider, aue.model
		`,
		sql<OperationRowRaw[]>`
			select
				aue.operation,
				count(*) as request_count,
				count(*) filter (where aue.status = 'SUCCESS') as success_count,
				coalesce(sum(aue.cost_usd), 0) as cost,
				coalesce(sum(aue.total_tokens), 0) as total_tokens,
				coalesce(sum(aue.item_count), 0) as item_count,
				coalesce(round(avg(aue.latency_ms)), 0) as average_latency_ms,
				bool_and(aue.user_id is null) as background_only
			from public.ai_usage_events as aue
			-- Background jobs (sender classification, merchant mapping) record no
			-- user, so a plain equality filter hid their spend entirely. This is
			-- an admin-only view of a single-user deployment: system work counts.
			where (aue.user_id = ${userId} or aue.user_id is null)
			and aue.created_at >= (
				to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD')::timestamp
				at time zone 'Asia/Kolkata'
			)
			and aue.created_at < (
				(to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD') + interval '1 month')::timestamp
				at time zone 'Asia/Kolkata'
			)
			group by aue.operation
			order by cost desc, request_count desc, aue.operation
		`,
		sql<EventRowRaw[]>`
			select
				aue.id, aue.call_id, aue.attempt_number,
				aue.provider, aue.model, aue.operation, aue.status,
				aue.http_status, aue.item_count, aue.prompt_tokens,
				aue.completion_tokens, aue.reasoning_tokens, aue.cached_tokens,
				aue.total_tokens, aue.cost_usd, aue.latency_ms, aue.error_code,
				aue.generation_id, aue.created_at,
				count(*) over () as total_event_count
			from public.ai_usage_events as aue
			-- Background jobs (sender classification, merchant mapping) record no
			-- user, so a plain equality filter hid their spend entirely. This is
			-- an admin-only view of a single-user deployment: system work counts.
			where (aue.user_id = ${userId} or aue.user_id is null)
			and aue.created_at >= (
				to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD')::timestamp
				at time zone 'Asia/Kolkata'
			)
			and aue.created_at < (
				(to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD') + interval '1 month')::timestamp
				at time zone 'Asia/Kolkata'
			)
			order by aue.created_at desc, aue.id desc
			limit ${AI_USAGE_PAGE_SIZE}
			offset ${offset}
		`,
		sql<ProcessingSummaryRowRaw[]>`
			with email_scope as (
				select re.id, re.processing_route, re.status
				from public.raw_emails as re
				where re.user_id = ${userId}
					and re.processing_completed_at >= (
						to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD')::timestamp
						at time zone 'Asia/Kolkata'
					)
					and re.processing_completed_at < (
						(to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD') + interval '1 month')::timestamp
						at time zone 'Asia/Kolkata'
					)
			),
			call_scope as (
				select acr.status, acr.attempt_count
				from public.ai_call_runs as acr
				where acr.user_id = ${userId}
					and acr.operation = 'EMAIL_NORMALIZATION'
					and acr.started_at >= (
						to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD')::timestamp
						at time zone 'Asia/Kolkata'
					)
					and acr.started_at < (
						(to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD') + interval '1 month')::timestamp
						at time zone 'Asia/Kolkata'
					)
			)
			select
				(select count(*) from email_scope) as emails_handled,
				(select count(*) from email_scope where processing_route = 'TEMPLATE') as template_hits,
				(select count(*) from email_scope where processing_route = 'AI') as ai_routed_emails,
				(select count(*) from email_scope where status = 'FAILED') as email_final_failures,
				(
					select count(*)
					from public.transactions as t
					join public.financial_events as fe
						on fe.id = t.financial_event_id and fe.user_id = t.user_id
					join email_scope as es on es.id = fe.raw_email_id
					where t.user_id = ${userId}
				) as transactions_created,
				(select count(*) from call_scope) as logical_calls,
				(select coalesce(sum(attempt_count), 0) from call_scope) as provider_attempts,
				(select count(*) from call_scope where status = 'SUCCESS' and attempt_count = 1) as first_attempt_successes,
				(select count(*) from call_scope where status = 'SUCCESS' and attempt_count > 1) as recovered_calls,
				(select count(*) from call_scope where status = 'FAILED') as final_failures,
				(select count(*) from call_scope where status = 'RUNNING') as in_progress_calls
		`,
		sql<ProcessingRunRowRaw[]>`
			with recent_calls as (
				select acr.id, acr.item_count, acr.status, acr.attempt_count,
					acr.started_at, acr.completed_at
				from public.ai_call_runs as acr
				where acr.user_id = ${userId}
					and acr.operation = 'EMAIL_NORMALIZATION'
					and acr.started_at >= (
						to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD')::timestamp
						at time zone 'Asia/Kolkata'
					)
					and acr.started_at < (
						(to_date(${`${selectedMonth}-01`}, 'YYYY-MM-DD') + interval '1 month')::timestamp
						at time zone 'Asia/Kolkata'
					)
				order by acr.started_at desc
				limit 12
			)
			select
				rc.id as run_id, rc.item_count as run_item_count,
				rc.status as run_status, rc.attempt_count as run_attempt_count,
				rc.started_at as run_started_at, rc.completed_at as run_completed_at,
				aue.id as attempt_id, aue.attempt_number, aue.model,
				aue.status as attempt_status, aue.http_status, aue.error_code,
				aue.cost_usd, aue.latency_ms
			from recent_calls as rc
			left join public.ai_usage_events as aue
				on aue.call_id = rc.id and aue.user_id = ${userId}
			order by rc.started_at desc, aue.attempt_number asc
		`,
		getMerchantMappingSummary(sql, env),
	]);

	const totalEventCount = eventRows[0]
		? toSafeInteger(eventRows[0].total_event_count, 'ai_usage_events.total_event_count')
		: modelRows.reduce(
				(sum, row) =>
					sum + toSafeInteger(row.request_count, 'ai_usage_events.request_count'),
				0,
			);

	return {
		selected_month: selectedMonth,
		page,
		page_size: AI_USAGE_PAGE_SIZE,
		total_event_count: totalEventCount,
		months: monthRows.map(mapMonth),
		models: modelRows.map(mapModel),
		operations: operationRows.map(mapOperation),
		events: eventRows.map(mapEvent),
		processing: mapProcessingSummary(
			processingRows[0],
			mapProcessingRuns(processingRunRows),
		),
		merchant_mapping: merchantMapping,
	};
}
