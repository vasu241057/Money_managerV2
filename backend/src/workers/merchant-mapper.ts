import type { MerchantMappingJobPayload, MerchantType } from '../../../shared/types';
import { resolveOpenCodeGoApiKey, resolveOpenCodeGoModel } from '../lib/ai';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import { toRequiredString } from '../lib/db/serialization';
import { TransientMessageError } from './queue.errors';

const OPENCODE_GO_CHAT_URL = 'https://opencode.ai/zen/go/v1/chat/completions';
const OPENCODE_GO_TIMEOUT_MS = 30_000;
const MAX_MERCHANTS_PER_RUN = 200;
const UNMAPPED_LOOKBACK_DAYS = 14;
const MAX_CANONICAL_NAME_LENGTH = 120;

const VALID_MERCHANT_TYPES = new Set<MerchantType>([
	'MERCHANT',
	'TRANSFER_INSTITUTION',
	'AGGREGATOR',
	'P2P',
]);

interface UnmappedCounterpartyRowRaw {
	search_key: unknown;
	counterparty_raw: unknown;
}

interface MerchantUpsertRowRaw {
	id: unknown;
}

interface MerchantMapping {
	search_key: string;
	canonical_name: string;
	merchant_type: MerchantType;
}

export interface MerchantMappingResult {
	candidate_count: number;
	mapped_count: number;
	alias_created_count: number;
	relinked_transaction_count: number;
	skipped_reason: string | null;
}

function createEmptyResult(): MerchantMappingResult {
	return {
		candidate_count: 0,
		mapped_count: 0,
		alias_created_count: 0,
		relinked_transaction_count: 0,
		skipped_reason: null,
	};
}

function buildMappingPrompt(
	candidates: Array<{ key: string; name: string }>,
): Array<{ role: string; content: string }> {
	return [
		{
			role: 'system',
			content: [
				'You canonicalize payee strings extracted from Indian banking and UPI alerts.',
				'Each input has a stable "key" and the raw payee "name" as it appeared in the alert.',
				'For each input, return the real-world brand or entity behind the raw string:',
				'- Corporate legal names map to their consumer brand ("FSN E-COMMERCE VENTURES" -> "Nykaa",',
				'  "BUNDL TECHNOLOGIES" -> "Swiggy", "ZEPTO MARKETPLACE PRIVATE LIMITED" -> "Zepto").',
				'- Strip legal suffixes (PVT LTD, LIMITED, INDIA) and normalize casing.',
				'- If the string is a person (UPI P2P), use the person name as-is with type "P2P".',
				'- Payment gateways/aggregators (Razorpay, BillDesk, PayU, CCAvenue, Cashfree) -> type "AGGREGATOR".',
				'- Banks/card issuers appearing as payees -> type "TRANSFER_INSTITUTION".',
				'- Everything else -> type "MERCHANT".',
				'- If unsure of the brand, return a cleaned title-cased version of the raw name.',
				'',
				'Return strict JSON only, no markdown, exactly:',
				'{ "merchants": [ { "key": "<input key>", "canonical_name": "<brand>", "type": "MERCHANT"|"TRANSFER_INSTITUTION"|"AGGREGATOR"|"P2P" } ] }',
				'Include every input key exactly once, unchanged.',
			].join('\n'),
		},
		{
			role: 'user',
			content: JSON.stringify({ merchants: candidates }),
		},
	];
}

async function callMerchantMappingModel(
	env: Env,
	apiKey: string,
	candidates: Array<{ key: string; name: string }>,
): Promise<MerchantMapping[] | null> {
	const controller = new AbortController();
	const timeoutId = setTimeout(() => controller.abort(), OPENCODE_GO_TIMEOUT_MS);

	let response: Response;
	try {
		response = await fetch(OPENCODE_GO_CHAT_URL, {
			method: 'POST',
			headers: {
				Authorization: `Bearer ${apiKey}`,
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({
				model: resolveOpenCodeGoModel(env),
				temperature: 0,
				messages: buildMappingPrompt(candidates),
				response_format: { type: 'json_object' },
			}),
			signal: controller.signal,
		});
	} catch (error) {
		clearTimeout(timeoutId);
		if (error instanceof Error && error.name === 'AbortError') {
			throw new TransientMessageError('Merchant mapping request timed out');
		}
		throw new TransientMessageError(
			`Merchant mapping network error: ${error instanceof Error ? error.message : 'Unknown'}`,
		);
	}
	clearTimeout(timeoutId);

	if (response.status === 429 || response.status === 408 || response.status >= 500) {
		throw new TransientMessageError(`Merchant mapping transient HTTP ${response.status}`);
	}
	if (!response.ok) {
		console.error('MERCHANT_MAPPER_HTTP_ERROR', { status: response.status });
		return null;
	}

	let content: string | undefined;
	try {
		const json = (await response.json()) as {
			choices?: Array<{ message?: { content?: string } }>;
		};
		content = json.choices?.[0]?.message?.content;
	} catch {
		content = undefined;
	}
	if (!content) {
		console.error('MERCHANT_MAPPER_EMPTY_RESPONSE');
		return null;
	}

	const cleaned = content
		.replace(/^```(?:json)?\s*/i, '')
		.replace(/\s*```\s*$/, '')
		.trim();

	let parsed: { merchants?: unknown };
	try {
		parsed = JSON.parse(cleaned) as { merchants?: unknown };
	} catch {
		console.error('MERCHANT_MAPPER_MALFORMED_JSON');
		return null;
	}
	if (!Array.isArray(parsed.merchants)) {
		console.error('MERCHANT_MAPPER_MISSING_MERCHANTS');
		return null;
	}

	// Keys must come from the input set — the model cannot invent aliases.
	const candidateKeys = new Set(candidates.map(candidate => candidate.key));
	const seen = new Set<string>();
	const mappings: MerchantMapping[] = [];
	for (const entry of parsed.merchants) {
		if (typeof entry !== 'object' || entry === null) continue;
		const record = entry as Record<string, unknown>;

		const key = typeof record.key === 'string' ? record.key.trim() : null;
		if (!key || !candidateKeys.has(key) || seen.has(key)) continue;

		const canonicalName =
			typeof record.canonical_name === 'string'
				? record.canonical_name.trim().slice(0, MAX_CANONICAL_NAME_LENGTH)
				: '';
		if (canonicalName.length === 0) continue;

		const rawType = typeof record.type === 'string' ? record.type.toUpperCase() : 'MERCHANT';
		const merchantType = VALID_MERCHANT_TYPES.has(rawType as MerchantType)
			? (rawType as MerchantType)
			: 'MERCHANT';

		seen.add(key);
		mappings.push({ search_key: key, canonical_name: canonicalName, merchant_type: merchantType });
	}
	return mappings;
}

async function loadUnmappedCounterparties(
	sql: SqlClient,
): Promise<Array<{ key: string; name: string }>> {
	const rows = await sql<UnmappedCounterpartyRowRaw[]>`
		select
			fe.search_key,
			max(fe.counterparty_raw) as counterparty_raw
		from public.financial_events as fe
		where fe.search_key is not null
			and fe.counterparty_raw is not null
			and fe.created_at > now() - (${UNMAPPED_LOOKBACK_DAYS} || ' days')::interval
			and not exists (
				select 1
				from public.global_merchant_aliases as gma
				where gma.search_key = fe.search_key
			)
		group by fe.search_key
		limit ${MAX_MERCHANTS_PER_RUN}
	`;

	const candidates: Array<{ key: string; name: string }> = [];
	for (const row of rows) {
		if (typeof row.search_key !== 'string' || typeof row.counterparty_raw !== 'string') {
			continue;
		}
		const key = row.search_key.trim();
		const name = row.counterparty_raw.trim();
		if (key.length === 0 || name.length === 0) continue;
		candidates.push({ key, name });
	}
	return candidates;
}

async function persistMappings(
	sql: SqlClient,
	mappings: MerchantMapping[],
): Promise<{ aliasCount: number; mappedKeys: string[] }> {
	let aliasCount = 0;
	const mappedKeys: string[] = [];

	for (const mapping of mappings) {
		const merchantRows = await sql<MerchantUpsertRowRaw[]>`
			insert into public.global_merchants (canonical_name, type)
			values (${mapping.canonical_name}, ${mapping.merchant_type})
			on conflict (canonical_name) do update
			set updated_at = now()
			returning id
		`;
		const merchantId = toRequiredString(merchantRows[0]?.id, 'global_merchants.id');

		const aliasRows = await sql<Array<{ search_key: unknown }>>`
			insert into public.global_merchant_aliases (search_key, merchant_id)
			values (${mapping.search_key}, ${merchantId})
			on conflict (search_key) do nothing
			returning search_key
		`;
		if (aliasRows.length > 0) {
			aliasCount += 1;
			mappedKeys.push(mapping.search_key);
		}
	}

	return { aliasCount, mappedKeys };
}

// Links recent merchant-less transactions to the freshly mapped merchants so
// yesterday's extractions get named too, not only tomorrow's. User-classified
// rows are never touched.
async function relinkTransactions(sql: SqlClient, mappedKeys: string[]): Promise<number> {
	if (mappedKeys.length === 0) {
		return 0;
	}

	const rows = await sql<Array<{ id: unknown }>>`
		update public.transactions as t
		set merchant_id = gma.merchant_id
		from public.financial_events as fe
		join public.global_merchant_aliases as gma
			on gma.search_key = fe.search_key
		where t.financial_event_id = fe.id
			and t.user_id = fe.user_id
			and t.merchant_id is null
			and t.classification_source <> 'USER'
			and fe.search_key = any(${mappedKeys}::text[])
		returning t.id
	`;
	return rows.length;
}

export async function runMerchantMappingJob(
	_job: MerchantMappingJobPayload,
	env: Env,
): Promise<MerchantMappingResult> {
	const result = createEmptyResult();

	const apiKey = resolveOpenCodeGoApiKey(env);
	if (!apiKey) {
		result.skipped_reason = 'OPENCODE_GO_API_KEY not set';
		return result;
	}

	const config = getAppConfig(env);
	const sql = getSqlClient(config);

	const candidates = await loadUnmappedCounterparties(sql);
	result.candidate_count = candidates.length;
	if (candidates.length === 0) {
		return result;
	}

	const mappings = await callMerchantMappingModel(env, apiKey, candidates);
	if (mappings === null) {
		result.skipped_reason = 'invalid model response';
		return result;
	}
	result.mapped_count = mappings.length;

	const persisted = await persistMappings(sql, mappings);
	result.alias_created_count = persisted.aliasCount;

	result.relinked_transaction_count = await relinkTransactions(sql, persisted.mappedKeys);

	return result;
}
