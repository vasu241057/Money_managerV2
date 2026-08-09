import type { MerchantMappingJobPayload, MerchantType, UUID } from '../../../shared/types';
import { probeAiProvider, resolveAiProviderConfig } from '../lib/ai';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import { toRequiredString } from '../lib/db/serialization';
import { loadSystemExpenseCategories } from '../lib/identity';
import { captureAiUsageAttempt } from '../services/ai-usage.service';
import { TransientMessageError } from './queue.errors';

// The provider routinely takes 25-45s on large prompts; queue consumers
// can afford a generous window.
const AI_CHAT_TIMEOUT_MS = 120_000;
// Candidates per AI call: one 200-entry answer exceeds sane response sizes
// (and slow-provider timeouts); small chunks keep each call fast and bounded.
const MAPPING_AI_CHUNK_SIZE = 40;
const MAX_MERCHANTS_PER_RUN = 200;
const MAX_CATEGORY_BACKFILL_PER_RUN = 100;
const UNMAPPED_LOOKBACK_DAYS = 14;
const MAX_CANONICAL_NAME_LENGTH = 120;
// Web-context searches per daily run: matches the mapper's candidate cap so
// every key the model sees gets context. Steady state is a few dozen keys a
// month; only a backlog day comes near this, and key rotation absorbs it.
const MAX_WEB_SEARCHES_PER_RUN = MAX_MERCHANTS_PER_RUN;
const WEB_SEARCH_TIMEOUT_MS = 10_000;
const MAX_WEB_SNIPPET_LENGTH = 240;
const WEB_SEARCH_MAX_RESULTS = 5;
// Unsure keys rest this long before the mapper asks the model again.
const MAPPING_RETRY_COOLDOWN_DAYS = 10;
// A person transfer is the one candidate class a web search can never help
// with: nobody publishes a page about a private individual, so every credit
// spent on one is wasted. Triage sorts the backlog before Tavily sees it.
// The bar is deliberately high and the error costs are asymmetric — a person
// mistaken for a business costs one credit, a business mistaken for a person
// is never identified at all — so anything short of near-certainty is searched.
const PERSON_TRIAGE_MIN_CONFIDENCE = 0.9;

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

export interface TavilySearchOutcome {
	context: Map<string, string>;
	searches_performed: number;
	/** Credits Tavily reported billing, indexed by key position. */
	credits_by_key: number[];
}

interface MerchantMapping {
	search_key: string;
	canonical_name: string;
	merchant_type: MerchantType;
	default_category_id: UUID | null;
}

export interface MerchantMappingResult {
	candidate_count: number;
	mapped_count: number;
	unsure_skipped_count: number;
	/** Candidates triage kept away from Tavily as private individuals. */
	person_skipped_count: number;
	person_triage_call_count: number;
	web_search_count: number;
	alias_created_count: number;
	category_backfill_candidate_count: number;
	category_backfilled_count: number;
	relinked_transaction_count: number;
	skipped_reason: string | null;
}

function createEmptyResult(): MerchantMappingResult {
	return {
		candidate_count: 0,
		mapped_count: 0,
		unsure_skipped_count: 0,
		person_skipped_count: 0,
		person_triage_call_count: 0,
		web_search_count: 0,
		alias_created_count: 0,
		category_backfill_candidate_count: 0,
		category_backfilled_count: 0,
		relinked_transaction_count: 0,
		skipped_reason: null,
	};
}

// Best-effort web context per unknown payee: legal-entity names ("BUNDL
// TECHNOLOGIES") rarely resemble the consumer brand (Swiggy), and search
// snippets close that gap. Queries carry only the payee string — never
// amounts, users, or email content. Every failure (no key, quota exhausted,
// network) degrades to the exact pre-search behavior.
export async function searchMerchantWebContext(
	env: Env,
	candidates: Array<{ key: string; name: string }>,
	fetcher: typeof fetch = fetch,
): Promise<TavilySearchOutcome> {
	const context = new Map<string, string>();
	// Comma-separated keys rotate on quota exhaustion (free-tier stacking).
	const apiKeys = (typeof env.TAVILY_API_KEY === 'string' ? env.TAVILY_API_KEY : '')
		.split(',')
		.map(key => key.trim())
		.filter(key => key.length > 0);
	let searchesPerformed = 0;
	// Credits billed per key position, so each free-tier account's own monthly
	// allowance can be tracked separately.
	const creditsByKey = new Array<number>(apiKeys.length).fill(0);
	if (apiKeys.length === 0 || candidates.length === 0) {
		return { context, searches_performed: 0, credits_by_key: creditsByKey };
	}

	let keyIndex = 0;
	for (const candidate of candidates.slice(0, MAX_WEB_SEARCHES_PER_RUN)) {
		let answered = false;
		while (!answered && keyIndex < apiKeys.length) {
			const controller = new AbortController();
			const timeoutId = setTimeout(() => controller.abort(), WEB_SEARCH_TIMEOUT_MS);
			try {
				const response = await fetcher('https://api.tavily.com/search', {
					method: 'POST',
					headers: {
						Authorization: `Bearer ${apiKeys[keyIndex]}`,
						'Content-Type': 'application/json',
					},
					body: JSON.stringify({
						query: `"${candidate.name}" India company brand`,
						search_depth: 'basic',
						max_results: WEB_SEARCH_MAX_RESULTS,
						// Tavily reports what it billed for this request but never a
						// remaining balance, so the ledger we keep from these numbers
						// is the only usable source of "credits left this month".
						include_usage: true,
					}),
					signal: controller.signal,
				});
				if (
					response.status === 429 ||
					response.status === 432 ||
					response.status === 402 ||
					response.status === 401
				) {
					// Key exhausted (or revoked): rotate and retry this candidate.
					console.warn('MERCHANT_MAPPER_WEB_SEARCH_KEY_EXHAUSTED', {
						status: response.status,
						keyIndex,
					});
					keyIndex += 1;
					continue;
				}
				answered = true;
				if (!response.ok) break;
				searchesPerformed += 1;
				const parsed = (await response.json()) as {
					results?: Array<{ title?: string; content?: string }>;
					usage?: { credits?: unknown };
				};
				// A basic search bills 1; trust the reported figure when present so
				// a future depth or plan change is recorded at its real cost.
				const billed = parsed.usage?.credits;
				creditsByKey[keyIndex] +=
					typeof billed === 'number' && Number.isFinite(billed) && billed >= 0 ? billed : 1;
				const snippets = (parsed.results ?? [])
					.map(result =>
						[result.title, result.content]
							.filter((part): part is string => typeof part === 'string' && part.length > 0)
							.join(': ')
							.slice(0, MAX_WEB_SNIPPET_LENGTH),
					)
					.filter(snippet => snippet.length > 0);
				if (snippets.length > 0) {
					context.set(candidate.key, snippets.join(' | '));
				}
			} catch {
				// Timeout/network: skip this candidate, keep the key.
				answered = true;
			} finally {
				clearTimeout(timeoutId);
			}
		}
		if (keyIndex >= apiKeys.length) break;
	}

	console.info('MERCHANT_MAPPER_WEB_CONTEXT', {
		requested: Math.min(candidates.length, MAX_WEB_SEARCHES_PER_RUN),
		resolved: context.size,
		searchesPerformed,
		creditsByKey,
		keysExhausted: keyIndex,
	});
	return { context, searches_performed: searchesPerformed, credits_by_key: creditsByKey };
}

/**
 * Counter names for the per-key credit ledger. Positional, matching the order
 * of the comma-separated TAVILY_API_KEY value — each free-tier key is its own
 * account with its own monthly allowance.
 */
export function tavilyCreditCounterName(keyIndex: number): string {
	return `tavily_credits_key_${keyIndex + 1}`;
}

function creditCounters(creditsByKey: number[]): Record<string, number> {
	const counters: Record<string, number> = {};
	let total = 0;
	creditsByKey.forEach((credits, index) => {
		if (credits <= 0) return;
		counters[tavilyCreditCounterName(index)] = credits;
		total += credits;
	});
	if (total > 0) counters.tavily_credits = total;
	return counters;
}

/**
 * Trading and legal markers that make a payee a business however much the words
 * around them read as somebody's name: "Ajay Singham Private Limited" and
 * "Verma Traders" are firms, not people. The model is told this too, but a
 * private individual is the one call we refuse to leave to model confidence
 * alone — this list can only ever push a candidate back towards being searched.
 */
const BUSINESS_NAME_MARKERS = new Set([
	'PVT', 'PRIVATE', 'LTD', 'LIMITED', 'LLP', 'LLC', 'INC', 'CORP', 'CORPORATION',
	'COMPANY', 'CO', 'FIRM', 'GROUP', 'HOLDINGS', 'VENTURES', 'ENTERPRISE',
	'ENTERPRISES', 'INDUSTRIES', 'TRADERS', 'TRADING', 'AGENCY', 'AGENCIES',
	'ASSOCIATES', 'SONS', 'BROTHERS', 'BROS', 'TECHNOLOGIES', 'TECHNOLOGY',
	'SYSTEMS', 'SOLUTIONS', 'SERVICES', 'RETAIL', 'MART', 'SUPERMARKET', 'STORE',
	'STORES', 'SHOP', 'SHOPPE', 'BAZAAR', 'COLLECTION', 'COLLECTIONS', 'GARMENTS',
	'JEWELLERS', 'JEWELLERY', 'ELECTRONICS', 'AUTOMOBILES', 'MOTORS', 'FURNITURE',
	'FOODS', 'RESTAURANT', 'HOTEL', 'CAFE', 'BAKERY', 'SWEETS', 'DHABA', 'CATERERS',
	'MEDICAL', 'MEDICALS', 'PHARMACY', 'CHEMIST', 'CLINIC', 'HOSPITAL', 'DIAGNOSTICS',
	'SALON', 'STUDIO', 'FITNESS', 'ACADEMY', 'INSTITUTE', 'SCHOOL', 'COLLEGE',
	'CENTRE', 'CENTER', 'LOGISTICS', 'TRANSPORT', 'TRAVELS', 'BUILDERS',
	'DEVELOPERS', 'CONSTRUCTION', 'HARDWARE', 'TEXTILES', 'TRADERSHOP',
]);

/**
 * True when a raw payee string carries a business marker as a whole word.
 * Deliberately token-based: a substring test would read "Cooperative" as "CO".
 */
export function hasBusinessNameMarker(rawName: string): boolean {
	return rawName
		.toUpperCase()
		.split(/[^A-Z0-9]+/)
		.some(token => token.length > 0 && BUSINESS_NAME_MARKERS.has(token));
}

function buildPersonTriagePrompt(
	candidates: Array<{ key: string; name: string }>,
): Array<{ role: string; content: string }> {
	return [
		{
			role: 'system',
			content: [
				'You sort raw payee strings from Indian bank and UPI alerts into two kinds.',
				'Each input has a stable "key" and the raw payee "name" as it appeared in the alert.',
				'',
				'"PERSON" — a private individual. A UPI transfer between two human beings.',
				'  Signals: the string is a personal name ("Ajay Kumar", "PRIYA S", "Rohit Verma"),',
				'  a UPI handle built from a personal name, or a name beside a UPI/VPA marker.',
				'"MERCHANT" — any business at all: brand, company, shop, firm, institution,',
				'  however small or local, and any payment gateway, bank or aggregator.',
				'',
				'CRITICAL: a trading or legal marker makes it a MERCHANT even when the words',
				'before it are somebody\'s name. "Ajay Singham Private Limited", "R K Sharma & Sons",',
				'"Verma Traders", "Patel Enterprises" and "Singh Medical Store" are all MERCHANT.',
				'A bare personal name with no such marker is PERSON.',
				'The word "UPI" alone means only that UPI moved the money — it says nothing',
				'about whether the counterparty is a person or a shop. Never decide on it alone.',
				'',
				'"confidence" is your honest probability from 0 to 1 that the payee is a private',
				`individual. Anything below ${PERSON_TRIAGE_MIN_CONFIDENCE} is treated as a business and researched anyway,`,
				'so there is no cost to admitting doubt — only say you are certain when you are.',
				'',
				'Return strict JSON only, no markdown, exactly:',
				'{ "assessments": [ { "key": "<input key>", "kind": "PERSON"|"MERCHANT", "confidence": 0.0 } ] }',
				'Include every input key exactly once, unchanged.',
			].join('\n'),
		},
		{
			role: 'user',
			content: JSON.stringify({ payees: candidates }),
		},
	];
}

function buildMappingPrompt(
	candidates: Array<{ key: string; name: string; web_context?: string }>,
	categoryNames: string[],
): Array<{ role: string; content: string }> {
	return [
		{
			role: 'system',
			content: [
				'You canonicalize payee strings extracted from Indian banking and UPI alerts.',
				'Each input has a stable "key" and the raw payee "name" as it appeared in the alert.',
				'Some inputs include "web_context": snippets from a web search about the raw name —',
				'trust them over guesswork when identifying the real brand behind a legal entity.',
				'For each input, return the real-world brand or entity behind the raw string:',
				'- Corporate legal names map to their consumer brand ("FSN E-COMMERCE VENTURES" -> "Nykaa",',
				'  "BUNDL TECHNOLOGIES" -> "Swiggy", "ZEPTO MARKETPLACE PRIVATE LIMITED" -> "Zepto").',
				'- Strip legal suffixes (PVT LTD, LIMITED, INDIA) and normalize casing.',
				'- If the string is a person (UPI P2P), use the person name as-is with type "P2P".',
				'- Payment gateways/aggregators (Razorpay, BillDesk, PayU, CCAvenue, Cashfree) -> type "AGGREGATOR".',
				'- Banks/card issuers appearing as payees -> type "TRANSFER_INSTITUTION".',
				'- Everything else -> type "MERCHANT".',
				'- Every entry carries "sure": true when you are certain of the identification, false otherwise.',
				'  A P2P person name used as-is and a cleaned title-cased raw name always count as sure.',
				'  "sure": false is ONLY for brand identifications you are not fully certain of',
				'  ("this legal entity is probably X") — those are skipped and retried later, so be honest.',
				'- If unsure of the brand, prefer "sure": false over returning a guess or a cleaned name.',
				'',
				'For type "MERCHANT" also pick the single best default spending category',
				'for the brand from this exact list (or null when none clearly fits):',
				JSON.stringify(categoryNames),
				'Aggregators, P2P persons, and transfer institutions always get category null',
				'(an aggregator masks the real merchant, so no category can be right).',
				'',
				'Return strict JSON only, no markdown, exactly:',
				'{ "merchants": [ { "key": "<input key>", "canonical_name": "<brand>", "type": "MERCHANT"|"TRANSFER_INSTITUTION"|"AGGREGATOR"|"P2P", "category": "<name from list>"|null, "sure": true|false } ] }',
				'Include every input key exactly once, unchanged.',
			].join('\n'),
		},
		{
			role: 'user',
			content: JSON.stringify({ merchants: candidates }),
		},
	];
}

// Case-insensitive exact-name lookup into the system expense taxonomy; any
// answer outside the list is dropped (hallucination guard).
function resolveCategoryName(
	raw: unknown,
	categoriesByName: Map<string, UUID>,
	categoriesByLowerName: Map<string, UUID>,
): UUID | null {
	if (typeof raw !== 'string') return null;
	const trimmed = raw.trim();
	if (trimmed.length === 0) return null;
	return categoriesByName.get(trimmed) ?? categoriesByLowerName.get(trimmed.toLowerCase()) ?? null;
}

export interface PersonTriageOutcome {
	/** Keys confidently identified as private individuals — never searched. */
	person_keys: Set<string>;
	call_count: number;
	/** Keys the model called a person that a business marker sent back. */
	guard_override_count: number;
	failed: boolean;
}

/**
 * Sorts candidates into people and businesses before a single Tavily credit is
 * spent. Every failure path — no answer, malformed JSON, HTTP error, timeout —
 * returns an empty person set, which searches everything exactly as before.
 * Triage is an optimisation, so it must never be able to cost the run a
 * merchant, and never throws the way the mapping call deliberately does.
 */
export async function triagePersonTransfers(
	sql: SqlClient,
	env: Env,
	apiKey: string,
	candidates: Array<{ key: string; name: string }>,
): Promise<PersonTriageOutcome> {
	const outcome: PersonTriageOutcome = {
		person_keys: new Set<string>(),
		call_count: 0,
		guard_override_count: 0,
		failed: false,
	};
	if (candidates.length === 0) return outcome;

	const providerConfig = resolveAiProviderConfig(env);
	const candidatesByKey = new Map(candidates.map(candidate => [candidate.key, candidate.name]));

	for (let i = 0; i < candidates.length; i += MAPPING_AI_CHUNK_SIZE) {
		const chunk = candidates.slice(i, i + MAPPING_AI_CHUNK_SIZE);
		const controller = new AbortController();
		const timeoutId = setTimeout(() => controller.abort(), AI_CHAT_TIMEOUT_MS);
		const startedAtMs = Date.now();
		outcome.call_count += 1;

		try {
			const response = await fetch(providerConfig.chatUrl, {
				method: 'POST',
				headers: {
					Authorization: `Bearer ${apiKey}`,
					'Content-Type': 'application/json',
				},
				body: JSON.stringify({
					model: providerConfig.dailyModel,
					temperature: 0,
					messages: buildPersonTriagePrompt(chunk),
					response_format: { type: 'json_object' },
				}),
				signal: controller.signal,
			});
			const generationId = response.headers.get('X-Generation-Id');
			// Triage rides under MERCHANT_MAPPING: it is part of that job's cost,
			// and ai_usage_events constrains the operation column to known names.
			const capture = (
				status: 'SUCCESS' | 'ERROR',
				payload?: unknown,
				errorCode?: string,
			) =>
				captureAiUsageAttempt({
					sql,
					provider: providerConfig.provider,
					model: providerConfig.dailyModel,
					operation: 'MERCHANT_MAPPING',
					itemCount: chunk.length,
					startedAtMs,
					status,
					httpStatus: response.status,
					payload,
					errorCode,
					generationId,
				});

			if (!response.ok) {
				await capture('ERROR', undefined, `HTTP_${response.status}`);
				outcome.failed = true;
				console.warn('MERCHANT_TRIAGE_HTTP_ERROR', { status: response.status });
				continue;
			}

			const payload = (await response.json()) as MerchantModelPayload;
			const content = payload.choices?.[0]?.message?.content;
			if (!content) {
				await capture('ERROR', payload, 'EMPTY_RESPONSE');
				outcome.failed = true;
				continue;
			}
			const cleaned = content
				.replace(/^```(?:json)?\s*/i, '')
				.replace(/\s*```\s*$/, '')
				.trim();
			const parsed = JSON.parse(cleaned) as { assessments?: unknown };
			if (!Array.isArray(parsed.assessments)) {
				await capture('ERROR', payload, 'MISSING_ASSESSMENTS');
				outcome.failed = true;
				continue;
			}
			await capture('SUCCESS', payload);

			for (const entry of parsed.assessments) {
				if (typeof entry !== 'object' || entry === null) continue;
				const record = entry as Record<string, unknown>;
				const key = typeof record.key === 'string' ? record.key.trim() : '';
				// The model cannot invent a key into the skip list: only strings
				// from this run's own candidate set are allowed to dodge a search.
				const rawName = candidatesByKey.get(key);
				if (rawName === undefined) continue;
				if (typeof record.kind !== 'string' || record.kind.toUpperCase() !== 'PERSON') continue;
				const confidence = typeof record.confidence === 'number' ? record.confidence : 0;
				if (!Number.isFinite(confidence) || confidence < PERSON_TRIAGE_MIN_CONFIDENCE) continue;
				if (hasBusinessNameMarker(rawName)) {
					outcome.guard_override_count += 1;
					continue;
				}
				outcome.person_keys.add(key);
			}
		} catch (error) {
			outcome.failed = true;
			console.warn('MERCHANT_TRIAGE_FAILED', {
				message: error instanceof Error ? error.message : 'unknown',
			});
		} finally {
			clearTimeout(timeoutId);
		}
	}

	console.info('MERCHANT_TRIAGE', {
		candidates: candidates.length,
		persons: outcome.person_keys.size,
		guardOverrides: outcome.guard_override_count,
		calls: outcome.call_count,
		failed: outcome.failed,
	});
	return outcome;
}

interface MappingCallOutcome {
	mappings: MerchantMapping[];
	unsure_count: number;
}

interface MerchantModelPayload {
	id?: string;
	choices?: Array<{ message?: { content?: string } }>;
	usage?: unknown;
}

async function callMerchantMappingModel(
	sql: SqlClient,
	env: Env,
	apiKey: string,
	candidates: Array<{ key: string; name: string; web_context?: string }>,
	categoriesByName: Map<string, UUID>,
): Promise<MappingCallOutcome | null> {
	const controller = new AbortController();
	const providerConfig = resolveAiProviderConfig(env);
	const timeoutId = setTimeout(() => controller.abort(), AI_CHAT_TIMEOUT_MS);
	const startedAtMs = Date.now();
	const capture = async (
		status: 'SUCCESS' | 'ERROR',
		httpStatus: number | null,
		payload?: unknown,
		errorCode?: string,
		generationId?: string | null,
	) =>
		captureAiUsageAttempt({
			sql,
			provider: providerConfig.provider,
			model: providerConfig.dailyModel,
			operation: 'MERCHANT_MAPPING',
			itemCount: candidates.length,
			startedAtMs,
			status,
			httpStatus,
			payload,
			errorCode,
			generationId,
		});

	let response: Response;
	try {
		response = await fetch(providerConfig.chatUrl, {
			method: 'POST',
			headers: {
				Authorization: `Bearer ${apiKey}`,
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({
				model: providerConfig.dailyModel,
				temperature: 0,
				messages: buildMappingPrompt(candidates, [...categoriesByName.keys()]),
				response_format: { type: 'json_object' },
			}),
			signal: controller.signal,
		});
	} catch (error) {
		clearTimeout(timeoutId);
		if (error instanceof Error && error.name === 'AbortError') {
			await capture('ERROR', null, undefined, 'TIMEOUT');
			throw new TransientMessageError('Merchant mapping request timed out');
		}
		await capture('ERROR', null, undefined, 'NETWORK_ERROR');
		throw new TransientMessageError(
			`Merchant mapping network error: ${error instanceof Error ? error.message : 'Unknown'}`,
		);
	}
	clearTimeout(timeoutId);
	const generationId = response.headers.get('X-Generation-Id');

	if (response.status === 429 || response.status === 408 || response.status >= 500) {
		await capture('ERROR', response.status, undefined, `HTTP_${response.status}`, generationId);
		throw new TransientMessageError(`Merchant mapping transient HTTP ${response.status}`);
	}
	if (!response.ok) {
		await capture('ERROR', response.status, undefined, `HTTP_${response.status}`, generationId);
		console.error('MERCHANT_MAPPER_HTTP_ERROR', { status: response.status });
		return null;
	}

	let content: string | undefined;
	let payload: MerchantModelPayload | null = null;
	try {
		payload = (await response.json()) as MerchantModelPayload;
		content = payload.choices?.[0]?.message?.content;
	} catch {
		await capture('ERROR', response.status, undefined, 'MALFORMED_RESPONSE', generationId);
		console.error('MERCHANT_MAPPER_MALFORMED_RESPONSE');
		return null;
	}
	if (!content) {
		await capture(
			'ERROR',
			response.status,
			payload,
			'EMPTY_RESPONSE',
			generationId ?? payload?.id ?? null,
		);
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
		await capture(
			'ERROR',
			response.status,
			payload,
			'MALFORMED_RESPONSE',
			generationId ?? payload?.id ?? null,
		);
		console.error('MERCHANT_MAPPER_MALFORMED_JSON');
		return null;
	}
	if (!Array.isArray(parsed.merchants)) {
		await capture(
			'ERROR',
			response.status,
			payload,
			'MISSING_MERCHANTS',
			generationId ?? payload?.id ?? null,
		);
		console.error('MERCHANT_MAPPER_MISSING_MERCHANTS');
		return null;
	}

	// Keys must come from the input set — the model cannot invent aliases.
	const candidateKeys = new Set(candidates.map(candidate => candidate.key));
	const lowerCategoriesByName = new Map(
		[...categoriesByName].map(([name, id]) => [name.toLowerCase(), id] as const),
	);
	const seen = new Set<string>();
	const mappings: MerchantMapping[] = [];
	let unsureCount = 0;
	for (const entry of parsed.merchants) {
		if (typeof entry !== 'object' || entry === null) continue;
		const record = entry as Record<string, unknown>;

		const key = typeof record.key === 'string' ? record.key.trim() : null;
		if (!key || !candidateKeys.has(key) || seen.has(key)) continue;

		// Sure-gate: an explicitly unsure identification is skipped rather
		// than persisted — the cooldown table re-asks it in a few days.
		if (record.sure === false) {
			seen.add(key);
			unsureCount += 1;
			continue;
		}

		const canonicalName =
			typeof record.canonical_name === 'string'
				? record.canonical_name.trim().slice(0, MAX_CANONICAL_NAME_LENGTH)
				: '';
		if (canonicalName.length === 0) continue;

		const rawType = typeof record.type === 'string' ? record.type.toUpperCase() : 'MERCHANT';
		const merchantType = VALID_MERCHANT_TYPES.has(rawType as MerchantType)
			? (rawType as MerchantType)
			: 'MERCHANT';

		// Only true merchants get a default category: an aggregator masks the
		// real payee and a person has no category, so a default would be wrong
		// for both (and would auto-verify miscategorized transactions).
		const defaultCategoryId =
			merchantType === 'MERCHANT'
				? resolveCategoryName(record.category, categoriesByName, lowerCategoriesByName)
				: null;

		seen.add(key);
		mappings.push({
			search_key: key,
			canonical_name: canonicalName,
			merchant_type: merchantType,
			default_category_id: defaultCategoryId,
		});
	}
	await capture(
		'SUCCESS',
		response.status,
		payload,
		undefined,
		generationId ?? payload?.id ?? null,
	);
	return { mappings, unsure_count: unsureCount };
}

// Day-bucketed operation counters (tavily_searches, merchant_ai_calls, ...)
// so usage stays queryable after provider keys rotate. Best-effort: a
// counter failure never fails the run.
async function incrementUsageCounters(
	sql: SqlClient,
	counters: Record<string, number>,
): Promise<void> {
	for (const [counter, count] of Object.entries(counters)) {
		if (count <= 0) continue;
		try {
			await sql`
				insert into public.usage_counters (day, counter, count)
				values (current_date, ${counter}, ${count})
				on conflict (day, counter) do update
				set count = public.usage_counters.count + excluded.count
			`;
		} catch (error) {
			console.warn('MERCHANT_MAPPER_COUNTER_FAILED', {
				counter,
				message: error instanceof Error ? error.message : 'unknown',
			});
		}
	}
}

// Marks every key that faced the model this run; unsure keys then rest for
// the cooldown window (mapped keys gain an alias and drop out anyway).
async function recordMappingAttempts(sql: SqlClient, keys: string[]): Promise<void> {
	if (keys.length === 0) return;
	try {
		await sql`
			insert into public.merchant_mapping_attempts (search_key)
			select unnest(${keys}::text[])
			on conflict (search_key) do update
			set attempt_count = public.merchant_mapping_attempts.attempt_count + 1,
				last_attempt_at = now()
		`;
	} catch (error) {
		console.warn('MERCHANT_MAPPER_ATTEMPTS_FAILED', {
			message: error instanceof Error ? error.message : 'unknown',
		});
	}
}

/**
 * Candidate keys for mapping. The daily job takes the defaults; the preview
 * endpoint widens them so a backlog older than the lookback window, or a
 * specific key resting on cooldown, can still be inspected without waiting.
 */
interface LoadCandidatesOptions {
	ignoreLookback?: boolean;
	ignoreCooldown?: boolean;
	searchKeys?: string[];
	limit?: number;
}

async function loadUnmappedCounterparties(
	sql: SqlClient,
	options: LoadCandidatesOptions = {},
): Promise<Array<{ key: string; name: string }>> {
	const applyLookback = options.ignoreLookback !== true;
	const applyCooldown = options.ignoreCooldown !== true;
	const explicitKeys = options.searchKeys ?? [];
	const hasExplicitKeys = explicitKeys.length > 0;
	const limit = options.limit ?? MAX_MERCHANTS_PER_RUN;
	const rows = await sql<UnmappedCounterpartyRowRaw[]>`
		select
			fe.search_key,
			max(fe.counterparty_raw) as counterparty_raw
		from public.financial_events as fe
		where fe.search_key is not null
			and fe.counterparty_raw is not null
			and (
				not ${applyLookback}
				or fe.created_at > now() - (${UNMAPPED_LOOKBACK_DAYS} || ' days')::interval
			)
			and (not ${hasExplicitKeys} or fe.search_key = any(${explicitKeys}::text[]))
			and not exists (
				select 1
				from public.global_merchant_aliases as gma
				where gma.search_key = fe.search_key
			)
			and (
				not ${applyCooldown}
				or not exists (
					select 1
					from public.merchant_mapping_attempts as mma
					where mma.search_key = fe.search_key
						and mma.last_attempt_at > now() - (${MAPPING_RETRY_COOLDOWN_DAYS} || ' days')::interval
				)
			)
		group by fe.search_key
		limit ${limit}
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
		// Seed/manual categories are never overwritten (coalesce keeps the
		// existing value); the AI only fills empty ones.
		const merchantRows = await sql<MerchantUpsertRowRaw[]>`
			insert into public.global_merchants (canonical_name, default_category_id, type, source, confirmed)
			values (${mapping.canonical_name}, ${mapping.default_category_id}, ${mapping.merchant_type}, 'AI', false)
			on conflict (canonical_name) do update
			set default_category_id = coalesce(global_merchants.default_category_id, excluded.default_category_id),
				updated_at = now()
			returning id
		`;
		const merchantId = toRequiredString(merchantRows[0]?.id, 'global_merchants.id');
		if (mapping.default_category_id) {
			await sql`
				insert into public.global_merchant_category_defaults (
					merchant_id,
					transaction_type,
					category_id
				)
				values (${merchantId}, 'expense', ${mapping.default_category_id})
				on conflict (merchant_id, transaction_type) do nothing
			`;
		}

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
// yesterday's extractions get named too, not only tomorrow's. Only the
// merchant link is stamped (never category or status), and only where
// merchant_id is still null — a merchant the user chose stays untouched.
// classification_source is deliberately NOT a filter: rows classified via a
// user merchant rule carry source USER yet still deserve the name.
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
			and fe.search_key = any(${mappedKeys}::text[])
		returning t.id
	`;
	return rows.length;
}

interface CategoryBackfillRowRaw {
	id: unknown;
	canonical_name: unknown;
}

// Categories for merchants mapped before categorization existed (or whose
// earlier answer was null). Same fixed-enum guardrails as the mapping call;
// shrinks to zero work once the backlog is drained.
async function backfillMerchantCategories(
	sql: SqlClient,
	env: Env,
	apiKey: string,
	categoriesByName: Map<string, UUID>,
): Promise<{ candidateCount: number; backfilledCount: number }> {
	const rows = await sql<CategoryBackfillRowRaw[]>`
		select gm.id, gm.canonical_name
		from public.global_merchants as gm
		where gm.type = 'MERCHANT'
			and gm.default_category_id is null
		order by gm.created_at asc
		limit ${MAX_CATEGORY_BACKFILL_PER_RUN}
	`;

	const candidates: Array<{ key: string; name: string }> = [];
	for (const row of rows) {
		if (typeof row.id !== 'string' || typeof row.canonical_name !== 'string') continue;
		const name = row.canonical_name.trim();
		if (name.length === 0) continue;
		candidates.push({ key: row.id, name });
	}
	if (candidates.length === 0) {
		return { candidateCount: 0, backfilledCount: 0 };
	}

	const outcome = await callMerchantMappingModel(sql, env, apiKey, candidates, categoriesByName);
	if (outcome === null) {
		return { candidateCount: candidates.length, backfilledCount: 0 };
	}

	let backfilled = 0;
	for (const mapping of outcome.mappings) {
		if (!mapping.default_category_id) continue;
		const updated = await sql<Array<{ id: unknown }>>`
			update public.global_merchants as gm
			set default_category_id = ${mapping.default_category_id},
				updated_at = now()
			where gm.id = ${mapping.search_key}
				and gm.type = 'MERCHANT'
				and gm.default_category_id is null
			returning gm.id
		`;
		if (updated.length > 0) {
			await sql`
				insert into public.global_merchant_category_defaults (
					merchant_id,
					transaction_type,
					category_id
				)
				values (${mapping.search_key}, 'expense', ${mapping.default_category_id})
				on conflict (merchant_id, transaction_type) do nothing
			`;
		}
		backfilled += updated.length;
	}
	return { candidateCount: candidates.length, backfilledCount: backfilled };
}

export interface MerchantMappingPreviewProposal {
	search_key: string;
	raw_name: string;
	/** Tavily snippet the model saw, or null when search returned nothing. */
	web_context: string | null;
	canonical_name: string;
	merchant_type: MerchantType;
	default_category: string | null;
}

export interface MerchantMappingSearchRecord {
	search_key: string;
	raw_name: string;
	/** Tavily snippet, or null when the search returned nothing usable. */
	web_context: string | null;
}

export interface MerchantMappingPreviewResult {
	candidate_count: number;
	web_search_count: number;
	ai_call_count: number;
	proposal_count: number;
	unsure_skipped_count: number;
	/** Candidates triage kept away from Tavily as private individuals. */
	person_skipped_count: number;
	person_triage_call_count: number;
	/** The raw names triage judged to be people, for eyeballing the calls. */
	person_skipped_names: string[];
	/** Candidates the model answered on but returned no usable mapping for. */
	unanswered_keys: string[];
	/**
	 * Every candidate searched and what the search found — returned whatever
	 * happens next, so a failed model call never discards context that real
	 * Tavily credits were spent on.
	 */
	searched: MerchantMappingSearchRecord[];
	proposals: MerchantMappingPreviewProposal[];
	skipped_reason: string | null;
}

export interface ApplyMappingProposal {
	search_key: string;
	canonical_name: string;
	merchant_type: string;
	/** Category name as the preview rendered it; null leaves the default unset. */
	default_category?: string | null;
}

export interface ApplyMappingsResult {
	requested_count: number;
	accepted_count: number;
	alias_created_count: number;
	relinked_transaction_count: number;
	rejected: Array<{ search_key: string; reason: string }>;
}

/**
 * Persists proposals a human has already read and approved, through the exact
 * same writes the daily job uses. Separate from the preview so approval is an
 * explicit act: the preview stays a pure dry run, and applying costs neither a
 * Tavily search nor a model call because the answers are supplied.
 */
export async function applyMerchantMappings(
	env: Env,
	proposals: ApplyMappingProposal[],
): Promise<ApplyMappingsResult> {
	const result: ApplyMappingsResult = {
		requested_count: proposals.length,
		accepted_count: 0,
		alias_created_count: 0,
		relinked_transaction_count: 0,
		rejected: [],
	};
	if (proposals.length === 0) return result;

	const sql = getSqlClient(getAppConfig(env));
	const categoriesByName = await loadSystemExpenseCategories(sql);
	const lowerCategoriesByName = new Map<string, UUID>();
	for (const [name, id] of categoriesByName) lowerCategoriesByName.set(name.toLowerCase(), id);

	const mappings: MerchantMapping[] = [];
	const seen = new Set<string>();
	for (const proposal of proposals) {
		const key = typeof proposal.search_key === 'string' ? proposal.search_key.trim() : '';
		const canonicalName =
			typeof proposal.canonical_name === 'string' ? proposal.canonical_name.trim() : '';
		const merchantType = proposal.merchant_type as MerchantType;

		if (key.length === 0 || canonicalName.length === 0) {
			result.rejected.push({ search_key: key, reason: 'missing search_key or canonical_name' });
			continue;
		}
		if (canonicalName.length > MAX_CANONICAL_NAME_LENGTH) {
			result.rejected.push({ search_key: key, reason: 'canonical_name too long' });
			continue;
		}
		if (!VALID_MERCHANT_TYPES.has(merchantType)) {
			result.rejected.push({ search_key: key, reason: `unknown merchant_type` });
			continue;
		}
		if (seen.has(key)) {
			result.rejected.push({ search_key: key, reason: 'duplicate search_key' });
			continue;
		}

		// Same rule as the model path: only real merchants carry a default
		// category, never aggregators, banks or people.
		const defaultCategoryId =
			merchantType === 'MERCHANT'
				? resolveCategoryName(
						proposal.default_category ?? null,
						categoriesByName,
						lowerCategoriesByName,
					)
				: null;

		seen.add(key);
		mappings.push({
			search_key: key,
			canonical_name: canonicalName,
			merchant_type: merchantType,
			default_category_id: defaultCategoryId,
		});
	}

	if (mappings.length === 0) return result;
	result.accepted_count = mappings.length;

	const persisted = await persistMappings(sql, mappings);
	result.alias_created_count = persisted.aliasCount;
	result.relinked_transaction_count = await relinkTransactions(sql, persisted.mappedKeys);
	await recordMappingAttempts(sql, mappings.map(mapping => mapping.search_key));

	console.info('MERCHANT_MAPPER_APPLIED', {
		requested: result.requested_count,
		accepted: result.accepted_count,
		aliases: result.alias_created_count,
		relinked: result.relinked_transaction_count,
		rejected: result.rejected.length,
	});
	return result;
}

export interface MerchantMappingPreviewOptions {
	ignoreLookback?: boolean;
	ignoreCooldown?: boolean;
	searchKeys?: string[];
	limit?: number;
}

/**
 * Runs the exact mapping pipeline the daily job runs — same candidate query,
 * same Tavily search, same prompt and model — and returns what it WOULD have
 * written, without writing any of it. No merchants, aliases, cooldown rows,
 * relinks or category backfill are touched, so this is safe to run repeatedly
 * while judging whether the search results are good enough to trust.
 *
 * The one thing it does record is cost: the AI call captures usage the same way
 * every other call does, and the Tavily/AI counters increment, because those
 * calls really were made and really consume quota.
 */
export async function previewMerchantMappings(
	env: Env,
	options: MerchantMappingPreviewOptions = {},
): Promise<MerchantMappingPreviewResult> {
	const result: MerchantMappingPreviewResult = {
		candidate_count: 0,
		web_search_count: 0,
		ai_call_count: 0,
		proposal_count: 0,
		unsure_skipped_count: 0,
		person_skipped_count: 0,
		person_triage_call_count: 0,
		person_skipped_names: [],
		unanswered_keys: [],
		searched: [],
		proposals: [],
		skipped_reason: null,
	};

	const providerConfig = resolveAiProviderConfig(env);
	const apiKey = providerConfig.apiKey;
	if (!apiKey) {
		result.skipped_reason = `${providerConfig.apiKeyEnvVar} not set`;
		return result;
	}

	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const categoriesByName = await loadSystemExpenseCategories(sql);
	const categoryNamesById = new Map<string, string>();
	for (const [name, id] of categoriesByName) categoryNamesById.set(id, name);

	const candidates = await loadUnmappedCounterparties(sql, {
		ignoreLookback: options.ignoreLookback ?? true,
		ignoreCooldown: options.ignoreCooldown ?? true,
		...(options.searchKeys === undefined ? {} : { searchKeys: options.searchKeys }),
		...(options.limit === undefined ? {} : { limit: options.limit }),
	});
	result.candidate_count = candidates.length;
	if (candidates.length === 0) return result;

	// Prove the model answers before buying anything. Searching first would
	// spend one Tavily credit per candidate and then discard the lot.
	const probe = await probeAiProvider(env, apiKey);
	if (!probe.ok) {
		result.skipped_reason = probe.detail;
		return result;
	}

	const triage = await triagePersonTransfers(sql, env, apiKey, candidates);
	result.person_skipped_count = triage.person_keys.size;
	result.person_triage_call_count = triage.call_count;
	result.person_skipped_names = candidates
		.filter(candidate => triage.person_keys.has(candidate.key))
		.map(candidate => candidate.name);
	const searchTargets = candidates.filter(candidate => !triage.person_keys.has(candidate.key));

	const webSearch = await searchMerchantWebContext(env, searchTargets);
	result.web_search_count = webSearch.searches_performed;
	result.searched = searchTargets.map(candidate => ({
		search_key: candidate.key,
		raw_name: candidate.name,
		web_context: webSearch.context.get(candidate.key) ?? null,
	}));
	const enriched = candidates.map(candidate => {
		const snippet = webSearch.context.get(candidate.key);
		return snippet ? { ...candidate, web_context: snippet } : candidate;
	});

	const rawNamesByKey = new Map(candidates.map(candidate => [candidate.key, candidate.name]));
	const answeredKeys = new Set<string>();
	let anyChunkSucceeded = false;
	for (let i = 0; i < enriched.length; i += MAPPING_AI_CHUNK_SIZE) {
		const chunk = enriched.slice(i, i + MAPPING_AI_CHUNK_SIZE);
		result.ai_call_count += 1;
		const outcome = await callMerchantMappingModel(sql, env, apiKey, chunk, categoriesByName);
		if (outcome === null) {
			result.skipped_reason = 'invalid model response';
			continue;
		}
		anyChunkSucceeded = true;
		result.unsure_skipped_count += outcome.unsure_count;
		for (const mapping of outcome.mappings) {
			answeredKeys.add(mapping.search_key);
			result.proposals.push({
				search_key: mapping.search_key,
				raw_name: rawNamesByKey.get(mapping.search_key) ?? mapping.search_key,
				web_context: webSearch.context.get(mapping.search_key) ?? null,
				canonical_name: mapping.canonical_name,
				merchant_type: mapping.merchant_type,
				default_category: mapping.default_category_id === null
					? null
					: categoryNamesById.get(mapping.default_category_id) ?? null,
			});
		}
	}

	if (!anyChunkSucceeded && result.skipped_reason === null) {
		result.skipped_reason = 'invalid model response';
	}
	result.proposal_count = result.proposals.length;
	result.unanswered_keys = candidates
		.map(candidate => candidate.key)
		.filter(key => !answeredKeys.has(key));

	await incrementUsageCounters(sql, {
		tavily_searches: result.web_search_count,
		merchant_ai_calls: result.ai_call_count + result.person_triage_call_count,
		person_searches_skipped: result.person_skipped_count,
		person_triage_calls: result.person_triage_call_count,
		...creditCounters(webSearch.credits_by_key),
	});

	return result;
}

export async function runMerchantMappingJob(
	_job: MerchantMappingJobPayload,
	env: Env,
): Promise<MerchantMappingResult> {
	const result = createEmptyResult();

	const providerConfig = resolveAiProviderConfig(env);
	const apiKey = providerConfig.apiKey;
	if (!apiKey) {
		result.skipped_reason = `${providerConfig.apiKeyEnvVar} not set`;
		return result;
	}

	const config = getAppConfig(env);
	const sql = getSqlClient(config);

	const categoriesByName = await loadSystemExpenseCategories(sql);

	const candidates = await loadUnmappedCounterparties(sql);
	result.candidate_count = candidates.length;

	let creditsByKey: number[] = [];
	if (candidates.length > 0) {
		// Same guard as the preview: never pay for search context the model is
		// not in a position to consume.
		const probe = await probeAiProvider(env, apiKey);
		if (!probe.ok) {
			result.skipped_reason = probe.detail;
			console.error('MERCHANT_MAPPER_PROVIDER_UNAVAILABLE', { detail: probe.detail });
			return result;
		}

		// Sort people out of the backlog first — they are mapped like everyone
		// else, they just never reach Tavily.
		const triage = await triagePersonTransfers(sql, env, apiKey, candidates);
		result.person_skipped_count = triage.person_keys.size;
		result.person_triage_call_count = triage.call_count;
		const searchTargets = candidates.filter(candidate => !triage.person_keys.has(candidate.key));

		const webSearch = await searchMerchantWebContext(env, searchTargets);
		result.web_search_count = webSearch.searches_performed;
		creditsByKey = webSearch.credits_by_key;
		const enriched = candidates.map(candidate => {
			const snippet = webSearch.context.get(candidate.key);
			return snippet ? { ...candidate, web_context: snippet } : candidate;
		});
		const allMappings: MerchantMapping[] = [];
		const attemptedKeys: string[] = [];
		let anyChunkSucceeded = false;
		for (let i = 0; i < enriched.length; i += MAPPING_AI_CHUNK_SIZE) {
			const chunk = enriched.slice(i, i + MAPPING_AI_CHUNK_SIZE);
			const outcome = await callMerchantMappingModel(sql, env, apiKey, chunk, categoriesByName);
			if (outcome === null) {
				// One malformed chunk answer must not discard the others.
				result.skipped_reason = 'invalid model response';
				continue;
			}
			anyChunkSucceeded = true;
			allMappings.push(...outcome.mappings);
			result.unsure_skipped_count += outcome.unsure_count;
			attemptedKeys.push(...chunk.map(candidate => candidate.key));
		}

		if (anyChunkSucceeded) {
			result.mapped_count = allMappings.length;

			const persisted = await persistMappings(sql, allMappings);
			result.alias_created_count = persisted.aliasCount;

			result.relinked_transaction_count = await relinkTransactions(sql, persisted.mappedKeys);

			// Cooldown bookkeeping only for keys whose chunk actually answered.
			await recordMappingAttempts(sql, attemptedKeys);
		} else if (result.skipped_reason === null) {
			result.skipped_reason = 'invalid model response';
		}
	}

	// Categorize merchants mapped before this feature existed (one extra
	// call per day at most, none once the backlog drains).
	const backfill = await backfillMerchantCategories(sql, env, apiKey, categoriesByName);
	result.category_backfill_candidate_count = backfill.candidateCount;
	result.category_backfilled_count = backfill.backfilledCount;

	await incrementUsageCounters(sql, {
		tavily_searches: result.web_search_count,
		merchant_ai_calls:
			Math.ceil(result.candidate_count / MAPPING_AI_CHUNK_SIZE) +
			result.person_triage_call_count +
			(backfill.candidateCount > 0 ? 1 : 0),
		merchants_mapped: result.alias_created_count,
		merchants_skipped_unsure: result.unsure_skipped_count,
		person_searches_skipped: result.person_skipped_count,
		person_triage_calls: result.person_triage_call_count,
		...creditCounters(creditsByKey),
	});

	return result;
}
