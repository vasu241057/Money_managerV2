import type {
	AiNormalizationResponse,
	AiNormalizedTransactionItem,
	ClassificationSource,
	NormalizeRawEmailsJobPayload,
	PaymentMethod,
	RawEmailStatus,
	TransactionStatus,
	TransactionType,
	UUID,
} from '../../../shared/types';
import { NORMALIZE_RAW_EMAILS_MAX_IDS } from '../../../shared/types';
import {
	detectAggregatorKey,
	detectVpaHandle,
	loadGlobalAliasesForLookup,
	loadSystemCategoryMap,
	loadUserRulesForLookup,
	parseAmountTokenToPaise,
	resolveIdentityForSearchKeys,
	toSearchKey,
} from './email-deterministic-normalizer';
import type { IdentityGraphLookup } from './email-deterministic-normalizer';
import {
	maskNumericTokens,
	resolveMaskedDateText,
	resolveTokenValue,
	unmaskText,
} from '../lib/masking';
import { hasLikelyDuplicateEvent, resolveDedupWindowMinutes } from '../lib/txn-dedup';
import {
	resolveNormalizerTierConfig,
	resolveOpenCodeGoApiKey,
	resolveAiQueueDelaySeconds,
} from '../lib/ai';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import { toIsoDateTime, toRequiredString } from '../lib/db/serialization';
import { PoisonMessageError, TransientMessageError } from './queue.errors';
import { buildAiClassificationJob } from './queue.messages';

const OPENCODE_GO_CHAT_URL = 'https://opencode.ai/zen/go/v1/chat/completions';

const VALID_PAYMENT_METHODS: readonly PaymentMethod[] = [
	'upi',
	'credit_card',
	'debit_card',
	'netbanking',
	'cash',
	'unknown',
];

const VALID_DIRECTIONS = ['debit', 'credit'] as const;

interface UserNormalizationContext {
	categories: Array<{ id: string; name: string; type: string; sub_categories: Array<{ id: string; name: string }> }>;
	accounts: Array<{ id: string; name: string; type: string; last4: string | null }>;
	misc_category_id: string | null;
}

function buildUserContextPrompt(ctx: UserNormalizationContext): string {
	const catList = ctx.categories.map(c => {
		const subList = c.sub_categories.map(s => `      { "id": "${s.id}", "name": "${s.name}" }`).join(',\n');
		return `    {
      "id": "${c.id}",
      "name": "${c.name}",
      "type": "${c.type}"${subList.length > 0 ? ',\n      "sub_categories": [\n' + subList + '\n      ]' : ''}
    }`;
	}).join(',\n');

	return [
		'',
		'USER CONTEXT — your category_id and sub_category_id MUST come from this list:',
		'',
		'Available Categories:',
		'[',
		catList,
		']',
		'',
		ctx.misc_category_id
			? `MISCELLANEOUS category ID: "${ctx.misc_category_id}" — use this when no other category fits.`
			: 'If no category fits, leave category_id as null.',
		'',
		'RULES for category matching:',
		'- Pick the best matching category from the list above based on the transaction content.',
		'- If a sub-category fits, set both category_id (parent) and sub_category_id.',
		'- If unsure, use the MISCELLANEOUS category ID. Do NOT invent category IDs.',
	].join('\n');
}

function buildSystemPrompt(ctx: UserNormalizationContext): string {
	return [
		'You are a financial transaction extractor for Indian banking and UPI SMS/email alerts.',
		'Extract ALL monetary transactions from the email text.',
		'',
		'Required JSON output format (strict — no other keys):',
		'{',
		'  "transactions": [',
		'    {',
		'      "direction": "debit" | "credit",',
		'      "amount_in_paise": number,',
		'      "txn_timestamp": "ISO-8601 string (YYYY-MM-DDTHH:mm:ss.sssZ)",',
		'      "payment_method": "upi" | "credit_card" | "debit_card" | "netbanking" | "cash" | "unknown",',
		'      "counterparty_raw": "string or null",',
		'      "instrument_last4": "string or null",',
		'      "category_guess": "string or null",',
		'      "category_id": "string or null",',
		'      "sub_category_id": "string or null",',
		'      "confidence": number',
		'    }',
		'  ]',
		'}',
		'',
		'CRITICAL RULES:',
		'- amount_in_paise: Convert rupees to paise. Rs.100.50 → 10050. NO decimals, NO commas.',
		'- txn_timestamp: Use the explicit date/time from email. Format: YYYY-MM-DDTHH:mm:ss.sssZ.',
		'- direction: "debit" = money going OUT. "credit" = money coming IN.',
		'- counterparty_raw: Extract merchant/person/bank name. null if unclear.',
		'- instrument_last4: Last 4 digits of card or account number seen in email. null if not present.',
		'- payment_method: "upi" if VPA/UPI, "credit_card"/"debit_card" if card, "netbanking" if bank transfer, "cash" if ATM, "unknown" otherwise.',
		'- category_id: Pick the best matching category ID from the user context. If none fits, use the MISCELLANEOUS category ID.',
		'- sub_category_id: Pick the best matching sub-category ID from the user context. null if none fits.',
		'- confidence: 0.95-1.0 when clear. 0.7-0.9 with inference. 0.4-0.7 when guessing. NEVER below 0.4.',
		'- If the email has NO financial transactions, return {"transactions": []}.',
		'- ALWAYS return valid JSON. No markdown, no code fences.',
		buildUserContextPrompt(ctx),
	].join('\n');
}

// Masked-mode prompt: the model sees <N#> placeholders instead of digits and
// must answer with token references; all numeric resolution happens locally.
function buildMaskedSystemPrompt(ctx: UserNormalizationContext): string {
	return [
		'You are a financial transaction extractor for Indian banking and UPI SMS/email alerts.',
		'All numbers in the email have been replaced with placeholders like <N1>, <N2>.',
		'You never see the digits — reference the placeholders instead.',
		'Extract ALL monetary transactions from the email text.',
		'',
		'Required JSON output format (strict — no other keys):',
		'{',
		'  "transactions": [',
		'    {',
		'      "direction": "debit" | "credit",',
		'      "amount_token": "N1",',
		'      "date_text": "string copied from the email containing the transaction date, placeholders included (e.g. \\"<N2>-<N3>-<N4>\\" or \\"Mar <N5>, <N6> at <N7>:<N8>\\") or null",',
		'      "payment_method": "upi" | "credit_card" | "debit_card" | "netbanking" | "cash" | "unknown",',
		'      "counterparty_raw": "string copied from the email (may include placeholders) or null",',
		'      "instrument_token": "N9 (the placeholder holding the card/account number) or null",',
		'      "category_id": "string or null",',
		'      "sub_category_id": "string or null",',
		'      "confidence": number',
		'    }',
		'  ]',
		'}',
		'',
		'CRITICAL RULES:',
		'- amount_token: the placeholder id of the TRANSACTION amount. Never pick balance/limit amounts',
		'  (they follow words like "Avl Bal", "Available Limit", "Total Due").',
		'- date_text: copy the date fragment exactly as it appears, placeholders included. null if absent.',
		'- direction: "debit" = money going OUT. "credit" = money coming IN.',
		'- counterparty_raw: merchant/person/bank name copied from the email. null if unclear.',
		'- instrument_token: placeholder holding the account/card digits mentioned for the instrument. null if none.',
		'- payment_method: "upi" if VPA/UPI, "credit_card"/"debit_card" if card, "netbanking" for NEFT/IMPS/RTGS, "cash" if ATM, "unknown" otherwise.',
		'- confidence: 0.95-1.0 when clear. 0.7-0.9 with inference. 0.4-0.7 when guessing. NEVER below 0.4.',
		'- If the email has NO financial transactions, return {"transactions": []}.',
		'- ALWAYS return valid JSON. No markdown, no code fences.',
		buildUserContextPrompt(ctx),
	].join('\n');
}

// Batch variant: several same-user emails in one call, answered per email.
function buildMaskedBatchSystemPrompt(ctx: UserNormalizationContext, emailCount: number): string {
	return [
		buildMaskedSystemPrompt(ctx),
		'',
		`BATCH MODE: the user message contains ${emailCount} emails, separated by "=== EMAIL k ===" headers.`,
		'Placeholder ids are unique across ALL emails — reference them exactly as printed.',
		'Return strict JSON with exactly this shape instead of the single-email shape:',
		'{ "emails": [ { "email": 1, "transactions": [ ...same transaction objects... ] }, ... ] }',
		`Include every email number from 1 to ${emailCount} exactly once, even when its transactions array is empty.`,
	].join('\n');
}

// Splits a batch response into per-email single-email-shaped responses,
// keyed by 1-based email number. Invalid or duplicate entries are dropped.
function splitBatchResponse(
	response: AiNormalizationResponse,
	emailCount: number,
): Map<number, AiNormalizationResponse> {
	const perEmail = new Map<number, AiNormalizationResponse>();
	const emails = (response as unknown as Record<string, unknown>).emails;
	if (!Array.isArray(emails)) {
		return perEmail;
	}

	for (const entry of emails) {
		if (typeof entry !== 'object' || entry === null) continue;
		const record = entry as Record<string, unknown>;
		const emailNumber = Number(record.email);
		if (!Number.isInteger(emailNumber) || emailNumber < 1 || emailNumber > emailCount) continue;
		if (perEmail.has(emailNumber)) continue;
		const transactions = Array.isArray(record.transactions) ? record.transactions : [];
		perEmail.set(emailNumber, { transactions } as AiNormalizationResponse);
	}
	return perEmail;
}

const KILL_SWITCH_ENV_KEYS = [
	'NORMALIZATION_KILL_SWITCH',
	'EMAIL_NORMALIZATION_KILL_SWITCH',
] as const;

interface RawEmailRecord {
	id: UUID;
	user_id: UUID;
	internal_date_iso: string;
	clean_text: string;
	status: RawEmailStatus;
}

interface RawEmailFetchRowRaw {
	id: unknown;
	user_id: unknown;
	internal_date: unknown;
	clean_text: unknown;
	status: unknown;
}

interface FinancialEventInsertRowRaw {
	id: unknown;
}

interface TransactionInsertRowRaw {
	id: unknown;
	status: unknown;
}

export interface NormalizeRawEmailsResult {
	requested_raw_email_count: number;
	loaded_raw_email_count: number;
	kill_switch_enabled: boolean;
	processed_raw_email_count: number;
	ignored_raw_email_count: number;
	unrecognized_raw_email_count: number;
	failed_raw_email_count: number;
	skipped_raw_email_count: number;
	extracted_fact_count: number;
	reconciled_fact_count: number;
	persisted_financial_event_count: number;
	created_transaction_count: number;
	needs_review_transaction_count: number;
	ai_enqueued_count: number;
	failed_raw_email_ids: UUID[];
	unrecognized_raw_email_ids: UUID[];
}

// Distinct subclass so classifyError can route 429s to the rate_limited
// branch (immediate tier cooldown + rotation) instead of generic transient.
class RateLimitedModelError extends TransientMessageError {
	constructor(message: string) {
		super(message);
		this.name = 'RateLimitedModelError';
	}
}

type ModelErrorKind = 'rate_limited' | 'transient' | 'permanent' | 'parse_error' | 'timeout';

interface ModelError {
	kind: ModelErrorKind;
	status?: number;
	message?: string;
	retryAfterMs?: number;
}

interface TierState {
	activeIndex: number;
	failureCountPerTier: Map<number, number>;
	cooldownUntil: Map<number, number>;
}

function normalizeBooleanEnvFlag(value: unknown): boolean {
	if (typeof value !== 'string') return false;
	switch (value.trim().toLowerCase()) {
		case '1':
		case 'true':
		case 'yes':
		case 'on':
			return true;
		default:
			return false;
	}
}

function isKillSwitchEnabled(env: Env): boolean {
	const envRecord = env as unknown as Record<string, unknown>;
	for (const key of KILL_SWITCH_ENV_KEYS) {
		if (normalizeBooleanEnvFlag(envRecord[key])) return true;
	}
	return false;
}

function directionToType(direction: 'debit' | 'credit'): TransactionType {
	return direction === 'credit' ? 'income' : 'expense';
}

// Same canonical search-key format as the deterministic engine and the
// review service (uppercase alphanumeric), so user_merchant_rules and
// global_merchant_aliases resolve identically regardless of which engine
// extracted the transaction.
function buildCanonicalSearchKeys(item: AiNormalizedTransactionItem): string[] {
	const counterparty = item.counterparty_raw ?? '';
	const vpaHandle = detectVpaHandle(counterparty);
	const candidates = [
		toSearchKey(counterparty),
		toSearchKey(vpaHandle),
		toSearchKey(vpaHandle?.split('@')[0] ?? null),
		toSearchKey(detectAggregatorKey(counterparty)),
	];

	const seen = new Set<string>();
	const keys: string[] = [];
	for (const candidate of candidates) {
		if (!candidate || seen.has(candidate)) continue;
		seen.add(candidate);
		keys.push(candidate);
	}
	return keys;
}

interface ResolvedAiItem {
	item: AiNormalizedTransactionItem;
	search_key: string | null;
	merchant_id: UUID | null;
	category_id: string | null;
	transaction_type: TransactionType;
	classification_source: ClassificationSource;
	status: TransactionStatus;
}

function resolveAiItemsWithIdentityGraph(
	userId: UUID,
	items: AiNormalizedTransactionItem[],
	lookup: IdentityGraphLookup,
): ResolvedAiItem[] {
	return items.map(item => {
		const searchKeys = buildCanonicalSearchKeys(item);
		const identity = resolveIdentityForSearchKeys(userId, searchKeys, lookup);

		const transferIntercepted = identity.merchant_type === 'TRANSFER_INSTITUTION';
		const transactionType: TransactionType = transferIntercepted
			? 'transfer'
			: directionToType(item.direction);

		// Category precedence: user rule > AI's per-email pick (user-context
		// aware) > global alias default > system default for the type.
		const aiCategoryId = item.sub_category_id || item.category_id || null;
		const categoryId =
			(identity.user_rule_hit ? identity.category_id : null) ??
			aiCategoryId ??
			identity.category_id ??
			lookup.system_categories[transactionType];

		// Auto-verify comes from identity signals only — a user rule or a
		// known merchant alias. Model self-reported confidence is stored for
		// review-inbox triage but never verifies on its own (it is not
		// calibrated).
		const identityVerified =
			(identity.user_rule_hit || identity.global_alias_hit) && categoryId !== null;
		const status: TransactionStatus = identityVerified ? 'VERIFIED' : 'NEEDS_REVIEW';

		const classificationSource: ClassificationSource = identity.user_rule_hit
			? 'USER'
			: 'AI';

		return {
			item,
			search_key: searchKeys[0] ?? null,
			merchant_id: identity.merchant_id,
			category_id: categoryId,
			transaction_type: transactionType,
			classification_source: classificationSource,
			status,
		};
	});
}

function dedupeUuidPreserveOrder(ids: UUID[]): UUID[] {
	const seen = new Set<string>();
	const deduped: UUID[] = [];
	for (const id of ids) {
		if (seen.has(id)) continue;
		seen.add(id);
		deduped.push(id);
	}
	return deduped;
}

function mapRawEmailRow(row: RawEmailFetchRowRaw): RawEmailRecord {
	return {
		id: toRequiredString(row.id, 'raw_emails.id') as UUID,
		user_id: toRequiredString(row.user_id, 'raw_emails.user_id') as UUID,
		internal_date_iso: toIsoDateTime(row.internal_date, 'raw_emails.internal_date'),
		clean_text: toRequiredString(row.clean_text, 'raw_emails.clean_text'),
		status: toRequiredString(row.status, 'raw_emails.status') as RawEmailStatus,
	};
}

async function loadRawEmailsByIds(
	sql: SqlClient,
	rawEmailIds: UUID[],
): Promise<RawEmailRecord[]> {
	const rows = await sql<RawEmailFetchRowRaw[]>`
		select re.id, re.user_id, re.internal_date, re.clean_text, re.status
		from public.raw_emails as re
		where re.id = any(${rawEmailIds}::uuid[])
		order by re.internal_date asc
	`;
	return rows.map(mapRawEmailRow);
}

interface CategoryRowRaw { id: unknown; name: unknown; type: unknown; parent_id: unknown; }

interface AccountRowRaw { id: unknown; name: unknown; type: unknown; instrument_last4: unknown; }

async function fetchUserNormalizationContext(
	sql: SqlClient,
	userId: UUID,
): Promise<UserNormalizationContext> {
	const catRows = await sql<CategoryRowRaw[]>`
		select c.id, c.name, c.type, c.parent_id
		from public.categories as c
		where c.user_id = ${userId} and c.is_system = false
		order by c.parent_id nulls first, c.name asc
	`;

	const parents = catRows.filter(r => !r.parent_id);
	const children = catRows.filter(r => r.parent_id);

	const categories = parents.map(p => ({
		id: toRequiredString(p.id, 'categories.id'),
		name: toRequiredString(p.name, 'categories.name'),
		type: toRequiredString(p.type, 'categories.type'),
		sub_categories: children
			.filter(c => toRequiredString(c.parent_id, 'categories.parent_id') === toRequiredString(p.id, 'categories.id'))
			.map(c => ({
				id: toRequiredString(c.id, 'categories.id'),
				name: toRequiredString(c.name, 'categories.name'),
			})),
	}));

	const miscCat = categories.find(c => c.name === 'Miscellaneous' && c.type === 'expense');

	const acctRows = await sql<AccountRowRaw[]>`
		select a.id, a.name, a.type, a.instrument_last4
		from public.accounts as a
		where a.user_id = ${userId}
		order by a.name asc
	`;

	const accounts = acctRows.map(a => ({
		id: toRequiredString(a.id, 'accounts.id'),
		name: toRequiredString(a.name, 'accounts.name'),
		type: toRequiredString(a.type, 'accounts.type'),
		last4: typeof a.instrument_last4 === 'string' ? a.instrument_last4 || null : null,
	}));

	return {
		categories,
		accounts,
		misc_category_id: miscCat?.id ?? null,
	};
}

function createEmptyResult(requestedCount: number): NormalizeRawEmailsResult {
	return {
		requested_raw_email_count: requestedCount,
		loaded_raw_email_count: 0,
		kill_switch_enabled: false,
		processed_raw_email_count: 0,
		ignored_raw_email_count: 0,
		unrecognized_raw_email_count: 0,
		failed_raw_email_count: 0,
		skipped_raw_email_count: 0,
		extracted_fact_count: 0,
		reconciled_fact_count: 0,
		persisted_financial_event_count: 0,
		created_transaction_count: 0,
		needs_review_transaction_count: 0,
		ai_enqueued_count: 0,
		failed_raw_email_ids: [],
		unrecognized_raw_email_ids: [],
	};
}

function classifyError(error: unknown): ModelError {
	if (error instanceof PoisonMessageError) {
		return { kind: 'permanent', message: error.message };
	}

	if (error instanceof RateLimitedModelError) {
		return { kind: 'rate_limited', message: error.message };
	}

	if (error instanceof TransientMessageError) {
		return { kind: 'transient', message: error.message };
	}

	if (error instanceof Error) {
		if (error.name === 'AbortError' || error.message.includes('timed out')) {
			return { kind: 'timeout', message: error.message };
		}

		if (error.message.includes('rate') || error.message.includes('429')) {
			return { kind: 'rate_limited', message: error.message };
		}

		if (error.message.includes('parse') || error.message.includes('JSON')) {
			return { kind: 'parse_error', message: error.message };
		}
	}

	const msg = error instanceof Error ? error.message : 'Unknown error';
	return { kind: 'transient', message: msg };
}

async function callModel(
	model: string,
	emailText: string,
	apiKey: string,
	timeoutMs: number,
	temperature: number,
	maxTokens: number,
	systemPrompt: string,
): Promise<AiNormalizationResponse> {
	const controller = new AbortController();
	const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

	const callStartMs = Date.now();
	let response: Response;
	try {
		const headers: Record<string, string> = {
			Authorization: `Bearer ${apiKey}`,
			'Content-Type': 'application/json',
		};

		const body: Record<string, unknown> = {
			model,
			temperature,
			messages: [
				{ role: 'system', content: systemPrompt },
				{
					role: 'user',
					content: `Analyze this email and extract all financial transactions:\n---\n${emailText}\n---`,
				},
			],
			response_format: { type: 'json_object' },
		};

		if (maxTokens > 0) {
			body.max_tokens = maxTokens;
		}

		response = await fetch(OPENCODE_GO_CHAT_URL, {
			method: 'POST',
			headers,
			body: JSON.stringify(body),
			signal: controller.signal,
		});
	} catch (error) {
		clearTimeout(timeoutId);
		if (error instanceof Error && error.name === 'AbortError') {
			throw new TransientMessageError('AI normalizer request timed out');
		}
		throw new TransientMessageError(
			`AI normalizer network error: ${error instanceof Error ? error.message : 'Unknown'}`,
		);
	}
	clearTimeout(timeoutId);

	if (response.status === 429) {
		const retryAfter = response.headers.get('Retry-After');
		throw new RateLimitedModelError(
			`AI normalizer rate limited (429)${retryAfter ? `, retry after ${retryAfter}s` : ''}`,
		);
	}

	if (response.status >= 500 || response.status === 408) {
		throw new TransientMessageError(
			`AI normalizer server error (${response.status})`,
		);
	}

	if (!response.ok) {
		throw new PoisonMessageError(
			`AI normalizer HTTP ${response.status}: model possibly unavailable`,
		);
	}

	const json = (await response.json()) as {
		choices?: Array<{ message?: { content?: string } }>;
	};

	const content = json.choices?.[0]?.message?.content;
	if (!content) {
		console.error('AI_NORMALIZER_EMPTY_RESPONSE', {
			status: response.status,
			model,
			responseKeys: Object.keys(json),
			choicesLength: json.choices?.length,
			hasMessage: Boolean(json.choices?.[0]?.message),
			messageKeys: json.choices?.[0]?.message ? Object.keys(json.choices[0].message!) : [],
		});
		throw new TransientMessageError('AI normalizer returned empty response');
	}

	const cleaned = content
		.replace(/^```(?:json)?\s*/i, '')
		.replace(/\s*```\s*$/, '')
		.trim();

	try {
		const parsed = JSON.parse(cleaned) as AiNormalizationResponse;
		const callElapsedMs = Date.now() - callStartMs;
		const usage = (json as Record<string, unknown>).usage as Record<string, number> | undefined;

		console.info('AI_NORMALIZER_CALL_COMPLETED', {
			model,
			elapsedMs: callElapsedMs,
			txnCount: Array.isArray(parsed.transactions) ? parsed.transactions.length : 0,
			promptTokens: usage?.prompt_tokens,
			completionTokens: usage?.completion_tokens,
			totalTokens: usage?.total_tokens,
		});

		// Batch responses carry `emails` instead of `transactions`; pass them
		// through untouched for splitBatchResponse.
		if (Array.isArray((parsed as unknown as Record<string, unknown>).emails)) {
			return parsed;
		}
		if (!parsed.transactions || !Array.isArray(parsed.transactions)) {
			return { transactions: [] };
		}
		return parsed;
	} catch {
		throw new TransientMessageError('AI normalizer returned malformed JSON');
	}
}

async function callWithTieredFallback(
	env: Env,
	emailText: string,
	apiKey: string,
	systemPrompt: string,
): Promise<AiNormalizationResponse> {
	const config = resolveNormalizerTierConfig(env);
	const state: TierState = {
		activeIndex: 0,
		failureCountPerTier: new Map(),
		cooldownUntil: new Map(),
	};

	let totalAttempts = 0;

	while (totalAttempts < config.maxRetries) {
		let tierIdx = state.activeIndex;
		while (tierIdx < config.models.length) {
			const cooldown = state.cooldownUntil.get(tierIdx) ?? 0;
			if (Date.now() < cooldown) {
				tierIdx++;
			} else {
				break;
			}
		}

		if (tierIdx >= config.models.length) {
			throw new PoisonMessageError('All AI model tiers exhausted');
		}

		const model = config.models[tierIdx];
		state.activeIndex = tierIdx;

		try {
			return await callModel(
				model,
				emailText,
				apiKey,
				config.timeoutMs,
				config.temperature,
				config.maxTokens,
				systemPrompt,
			);
		} catch (error) {
			const classified = classifyError(error);
			totalAttempts++;

			const tierFails = (state.failureCountPerTier.get(tierIdx) ?? 0) + 1;
			state.failureCountPerTier.set(tierIdx, tierFails);

			console.warn('AI_NORMALIZER_TIER_FAILURE', {
				tierIndex: tierIdx,
				model,
				attempt: totalAttempts,
				tierAttempts: tierFails,
				errorKind: classified.kind,
				message: classified.message ?? '',
			});

			if (classified.kind === 'rate_limited' || classified.kind === 'permanent') {
				state.cooldownUntil.set(tierIdx, Date.now() + config.cooldownMs);
				state.activeIndex = tierIdx + 1;
				continue;
			}

			if (tierFails >= config.rotateFailures) {
				state.cooldownUntil.set(tierIdx, Date.now() + config.cooldownMs);
				state.activeIndex = tierIdx + 1;
			}
		}
	}

	throw new PoisonMessageError(`All ${config.maxRetries} normalizer attempts exhausted`);
}

// Any id outside this set would violate the transactions.category_id FK
// and fail the whole email, so hallucinated ids must be dropped here.
function buildKnownCategoryIdSet(ctx: UserNormalizationContext): Set<string> {
	const knownCategoryIds = new Set<string>();
	for (const category of ctx.categories) {
		knownCategoryIds.add(category.id);
		for (const sub of category.sub_categories) {
			knownCategoryIds.add(sub.id);
		}
	}
	return knownCategoryIds;
}

function validateCategoryIds(
	record: Record<string, unknown>,
	knownCategoryIds: Set<string>,
	miscCategoryId: string | null,
): { categoryId: string | null; subCategoryId: string | null } {
	const rawCategoryId =
		typeof record.category_id === 'string' ? record.category_id || null : null;
	const categoryId =
		rawCategoryId && knownCategoryIds.has(rawCategoryId) ? rawCategoryId : miscCategoryId;

	const rawSubCategoryId =
		typeof record.sub_category_id === 'string' ? record.sub_category_id || null : null;
	const subCategoryId =
		rawSubCategoryId && knownCategoryIds.has(rawSubCategoryId) ? rawSubCategoryId : null;

	return { categoryId, subCategoryId };
}

// Masked mode: the model answers with placeholder references; every numeric
// field is resolved locally from the token map so digits never round-trip.
function validateMaskedResponse(
	response: AiNormalizationResponse,
	minConfidence: number,
	ctx: UserNormalizationContext,
	tokens: Map<string, string>,
	fallbackIso: string,
): AiNormalizedTransactionItem[] {
	const txnList = Array.isArray(response.transactions) ? response.transactions : [];
	if (txnList.length === 0) return [];

	const knownCategoryIds = buildKnownCategoryIdSet(ctx);
	const valid: AiNormalizedTransactionItem[] = [];

	for (const raw of txnList) {
		if (!raw || typeof raw !== 'object') continue;
		const record = raw as unknown as Record<string, unknown>;

		const direction = record.direction;
		if (direction !== 'debit' && direction !== 'credit') continue;

		const amountText = resolveTokenValue(record.amount_token as string | undefined, tokens);
		if (!amountText) continue;
		const amountInPaise = parseAmountTokenToPaise(amountText);
		if (!amountInPaise || amountInPaise > 1_000_000_00) continue;

		const confidence = Number(record.confidence);
		if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) continue;
		if (confidence < minConfidence) continue;

		const txnTimestamp = resolveMaskedDateText(
			typeof record.date_text === 'string' ? record.date_text : null,
			tokens,
			fallbackIso,
		);

		const paymentMethod: PaymentMethod = VALID_PAYMENT_METHODS.includes(
			record.payment_method as PaymentMethod,
		)
			? (record.payment_method as PaymentMethod)
			: 'unknown';

		const counterpartyUnmasked = unmaskText(
			typeof record.counterparty_raw === 'string' ? record.counterparty_raw : null,
			tokens,
		);
		const counterpartyRaw = counterpartyUnmasked
			? counterpartyUnmasked.trim().slice(0, 500) || null
			: null;

		const instrumentText = resolveTokenValue(
			record.instrument_token as string | undefined,
			tokens,
		);
		const instrumentLast4 = instrumentText
			? instrumentText.replace(/\D/g, '').slice(-4) || null
			: null;

		const { categoryId, subCategoryId } = validateCategoryIds(
			record,
			knownCategoryIds,
			ctx.misc_category_id,
		);

		valid.push({
			direction,
			amount_in_paise: amountInPaise,
			txn_timestamp: txnTimestamp,
			payment_method: paymentMethod,
			counterparty_raw: counterpartyRaw,
			instrument_last4: instrumentLast4,
			category_guess: null,
			category_id: categoryId,
			sub_category_id: subCategoryId,
			account_id: null,
			confidence,
		});
	}

	return valid;
}

function validateAndCleanResponse(
	response: AiNormalizationResponse,
	minConfidence: number,
	ctx: UserNormalizationContext,
	fallbackIso: string,
): AiNormalizedTransactionItem[] {
	const txnList = Array.isArray(response.transactions) ? response.transactions : [];
	if (txnList.length === 0) return [];

	const knownCategoryIds = buildKnownCategoryIdSet(ctx);

	const valid: AiNormalizedTransactionItem[] = [];

	for (const item of txnList) {
		if (!item || typeof item !== 'object') continue;

		const direction = item.direction as string;
		if (!VALID_DIRECTIONS.includes(direction as 'debit' | 'credit')) continue;

		const amountInPaise = Number(item.amount_in_paise);
		if (!Number.isSafeInteger(amountInPaise) || amountInPaise <= 0) continue;
		if (amountInPaise > 1_000_000_00) continue;

		const confidence = Number(item.confidence);
		if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) continue;

		if (confidence < minConfidence) continue;

		const txnTimestamp =
			typeof item.txn_timestamp === 'string' && item.txn_timestamp.length > 0
				? item.txn_timestamp
				: fallbackIso;

		const paymentMethod: PaymentMethod = VALID_PAYMENT_METHODS.includes(
			item.payment_method as PaymentMethod,
		)
			? (item.payment_method as PaymentMethod)
			: 'unknown';

		const counterpartyRaw =
			typeof item.counterparty_raw === 'string'
				? item.counterparty_raw.trim().slice(0, 500) || null
				: null;

		const instrumentLast4 =
			typeof item.instrument_last4 === 'string'
				? item.instrument_last4.trim().replace(/\D/g, '').slice(-4) || null
				: null;

		const categoryGuess =
			typeof item.category_guess === 'string'
				? item.category_guess.trim().toLowerCase() || null
				: null;

		const { categoryId, subCategoryId } = validateCategoryIds(
			item as unknown as Record<string, unknown>,
			knownCategoryIds,
			ctx.misc_category_id,
		);

		valid.push({
			direction: direction as 'debit' | 'credit',
			amount_in_paise: amountInPaise,
			txn_timestamp: txnTimestamp,
			payment_method: paymentMethod,
			counterparty_raw: counterpartyRaw,
			instrument_last4: instrumentLast4,
			category_guess: categoryGuess,
			category_id: categoryId,
			sub_category_id: subCategoryId,
			account_id: null,
			confidence,
		});
	}

	return valid;
}

async function persistAiTransactions(
	sql: SqlClient,
	rawEmail: RawEmailRecord,
	resolvedItems: ResolvedAiItem[],
	dedupWindowMinutes: number,
): Promise<{ transactionCount: number; needsReviewIds: UUID[] }> {
	return sql.begin(async (tx) => {
		const txSql = tx as unknown as SqlClient;

		const lockRows = await txSql<RawEmailFetchRowRaw[]>`
			select re.id, re.user_id, re.internal_date, re.clean_text, re.status
			from public.raw_emails as re
			where re.id = ${rawEmail.id}
			limit 1
			for update
		`;
		const lockedRow = lockRows[0] ? mapRawEmailRow(lockRows[0]) : null;
		if (!lockedRow || lockedRow.status !== 'PENDING_EXTRACTION') {
			return { transactionCount: 0, needsReviewIds: [] };
		}

		let persistedFinancialEvents = 0;
		let persistedTransactions = 0;
		const needsReviewIds: UUID[] = [];

		for (let idx = 0; idx < resolvedItems.length; idx++) {
			const resolved = resolvedItems[idx] as ResolvedAiItem;
			const item = resolved.item;

			const feRows = await txSql<FinancialEventInsertRowRaw[]>`
				insert into public.financial_events (
					user_id, raw_email_id, extraction_index,
					direction, amount_in_paise, currency,
					txn_timestamp, payment_method, instrument_id,
					counterparty_raw, search_key, status
				)
				values (
					${lockedRow.user_id}, ${lockedRow.id}, ${idx},
					${item.direction}, ${item.amount_in_paise}, 'INR',
					${item.txn_timestamp}, ${item.payment_method},
					${item.instrument_last4},
					${item.counterparty_raw},
					${resolved.search_key},
					'ACTIVE'
				)
				on conflict (raw_email_id, extraction_index) do nothing
				returning id
			`;

			let feId: string | null = null;
			if (feRows.length > 0 && feRows[0]?.id) {
				feId = toRequiredString(feRows[0].id, 'financial_events.id');
				persistedFinancialEvents++;
			}

			if (!feId) continue;

			// Cross-email dedup: the same payment often triggers two alerts
			// (bank + payment app). Never silently auto-verify the second one.
			let transactionStatus = resolved.status;
			if (transactionStatus === 'VERIFIED' && dedupWindowMinutes > 0) {
				const hasDuplicate = await hasLikelyDuplicateEvent(txSql, {
					user_id: lockedRow.user_id,
					raw_email_id: lockedRow.id,
					amount_in_paise: item.amount_in_paise,
					direction: item.direction,
					txn_timestamp: item.txn_timestamp,
					window_minutes: dedupWindowMinutes,
				});
				if (hasDuplicate) {
					transactionStatus = 'NEEDS_REVIEW';
				}
			}

			const resolvedAccountId = item.account_id || null;

			const txnRows = await txSql<TransactionInsertRowRaw[]>`
				insert into public.transactions (
					user_id, financial_event_id, account_id,
					category_id, merchant_id, credit_card_id,
					amount_in_paise, type, txn_date, user_note,
					status, classification_source, ai_confidence_score
				)
				values (
					${lockedRow.user_id}, ${feId}, ${resolvedAccountId},
					${resolved.category_id}, ${resolved.merchant_id}, null,
					${item.amount_in_paise},
					${resolved.transaction_type},
					${item.txn_timestamp}, null,
					${transactionStatus}, ${resolved.classification_source}, ${item.confidence}
				)
				on conflict (financial_event_id) do nothing
				returning id, status
			`;

			const insertedTxnId = txnRows[0]?.id
				? toRequiredString(txnRows[0].id, 'transactions.id')
				: null;
			if (insertedTxnId) {
				persistedTransactions++;
				const insertedStatus = toRequiredString(
					txnRows[0]?.status,
					'transactions.status',
				) as TransactionStatus;
				if (insertedStatus === 'NEEDS_REVIEW') {
					needsReviewIds.push(insertedTxnId as UUID);
				}
			}
		}

		await txSql`
			update public.raw_emails as re
			set status = 'PROCESSED'
			where re.id = ${lockedRow.id}
				and re.user_id = ${lockedRow.user_id}
				and re.status = 'PENDING_EXTRACTION'
		`;

		return {
			transactionCount: persistedTransactions,
			needsReviewIds,
		};
	});
}

async function markRawEmailFailed(
	sql: SqlClient,
	rawEmail: RawEmailRecord,
	error: unknown,
): Promise<void> {
	const reason =
		error instanceof Error
			? error.message.slice(0, 500)
			: 'Unknown normalizer error';
	try {
		await sql`
			update public.raw_emails as re
			set status = 'FAILED'
			where re.id = ${rawEmail.id}
				and re.user_id = ${rawEmail.user_id}
				and re.status = 'PENDING_EXTRACTION'
		`;
	} catch {
		// Same contract as the deterministic engine: if we can't record the
		// failure, retry the job rather than leaving the row stuck PENDING.
		throw new TransientMessageError(
			`Failed to mark raw email ${rawEmail.id} as FAILED after normalization error: ${reason}`,
		);
	}
}

async function markRawEmailIgnored(
	sql: SqlClient,
	rawEmail: RawEmailRecord,
): Promise<void> {
	await sql`
		update public.raw_emails as re
		set status = 'IGNORED'
		where re.id = ${rawEmail.id}
			and re.user_id = ${rawEmail.user_id}
			and re.status = 'PENDING_EXTRACTION'
	`;
}

async function enqueueAiClassificationJobs(
	queue: Queue,
	transactionIds: UUID[],
	delaySeconds: number,
): Promise<number> {
	const deduped = dedupeUuidPreserveOrder(transactionIds);
	let enqueued = 0;

	for (const id of deduped) {
		try {
			await queue.send(buildAiClassificationJob(id), {
				contentType: 'json',
				delaySeconds,
			});
			enqueued++;
		} catch (error) {
			console.warn('Failed to enqueue AI_CLASSIFICATION from AI normalizer', {
				transactionId: id,
				error: error instanceof Error ? error.message : 'Unknown',
			});
		}
	}

	return enqueued;
}

export async function runNormalizeRawEmailsJob(
	job: NormalizeRawEmailsJobPayload,
	env: Env,
): Promise<NormalizeRawEmailsResult> {
	if (job.raw_email_ids.length > NORMALIZE_RAW_EMAILS_MAX_IDS) {
		throw new PoisonMessageError(
			`NORMALIZE_RAW_EMAILS raw_email_ids exceeds max ${NORMALIZE_RAW_EMAILS_MAX_IDS}`,
		);
	}

	const requestedIds = dedupeUuidPreserveOrder(job.raw_email_ids);
	if (requestedIds.length > NORMALIZE_RAW_EMAILS_MAX_IDS) {
		throw new PoisonMessageError(
			`NORMALIZE_RAW_EMAILS raw_email_ids exceeds max ${NORMALIZE_RAW_EMAILS_MAX_IDS}`,
		);
	}

	const result = createEmptyResult(job.raw_email_ids.length);

	if (isKillSwitchEnabled(env)) {
		result.kill_switch_enabled = true;
		result.skipped_raw_email_count = job.raw_email_ids.length;
		return result;
	}

	const apiKey = resolveOpenCodeGoApiKey(env);
	if (!apiKey) {
		result.skipped_raw_email_count = job.raw_email_ids.length;
		console.error('AI normalizer: OPENCODE_GO_API_KEY is not set');
		return result;
	}

	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const normConfig = resolveNormalizerTierConfig(env);

	const rawEmailRows = await loadRawEmailsByIds(sql, requestedIds);
	result.loaded_raw_email_count = rawEmailRows.length;

	const rawEmailById = new Map<UUID, RawEmailRecord>();
	for (const row of rawEmailRows) {
		rawEmailById.set(row.id, row);
	}

	const allNeedsReviewIds: UUID[] = [];

	const userContextCache = new Map<UUID, UserNormalizationContext>();
	const getUserContext = async (userId: UUID): Promise<UserNormalizationContext> => {
		let ctx = userContextCache.get(userId);
		if (!ctx) {
			ctx = await fetchUserNormalizationContext(sql, userId);
			userContextCache.set(userId, ctx);
		}
		return ctx;
	};

	// Applies validated extractions to one email: account match, identity
	// graph, persistence, and result accounting.
	const finishEmail = async (
		rawEmail: RawEmailRecord,
		ctx: UserNormalizationContext,
		validated: AiNormalizedTransactionItem[],
		emailStartMs: number,
	): Promise<void> => {
		result.reconciled_fact_count += validated.length;

		if (validated.length === 0) {
			await markRawEmailIgnored(sql, rawEmail);
			result.ignored_raw_email_count++;
			console.info('AI_NORMALIZER_EMAIL_IGNORED', {
				rawEmailId: rawEmail.id,
				totalElapsedMs: Date.now() - emailStartMs,
				reason: 'no transactions extracted',
			});
			return;
		}

		for (const item of validated) {
			if (item.instrument_last4) {
				const matched = ctx.accounts.find(
					a => a.last4 && (a.last4 === item.instrument_last4),
				);
				if (matched) {
					item.account_id = matched.id;
				}
			}

			if (!item.account_id && item.payment_method === 'upi') {
				const emailLower = rawEmail.clean_text.toLowerCase();
				const counterpartyLower = (item.counterparty_raw || '').toLowerCase();
				const searchText = counterpartyLower + ' ' + emailLower;

				const matched = ctx.accounts.find(a => {
					const nameLower = a.name.toLowerCase();
					const keywords = nameLower.split(' ').filter(w => w.length > 1);
					return a.type === 'bank' && keywords.some(kw => searchText.includes(kw));
				});
				if (matched) {
					item.account_id = matched.id;
				}
			}
		}

		// Identity graph pass: user rules and global merchant aliases must
		// influence AI extractions too, so user reviews teach this path.
		const allSearchKeys = Array.from(new Set(validated.flatMap(buildCanonicalSearchKeys)));
		const [systemCategories, userRules, globalAliases] = await Promise.all([
			loadSystemCategoryMap(sql),
			loadUserRulesForLookup(sql, [rawEmail.user_id], allSearchKeys),
			loadGlobalAliasesForLookup(sql, allSearchKeys),
		]);
		const identityLookup: IdentityGraphLookup = {
			system_categories: systemCategories,
			user_rules: userRules,
			global_aliases: globalAliases,
		};
		const resolvedItems = resolveAiItemsWithIdentityGraph(
			rawEmail.user_id,
			validated,
			identityLookup,
		);

		const persistence = await persistAiTransactions(
			sql,
			rawEmail,
			resolvedItems,
			resolveDedupWindowMinutes(env),
		);

		result.persisted_financial_event_count += persistence.transactionCount;
		result.created_transaction_count += persistence.transactionCount;
		result.needs_review_transaction_count += persistence.needsReviewIds.length;
		allNeedsReviewIds.push(...persistence.needsReviewIds);
		result.processed_raw_email_count++;

		console.info('AI_NORMALIZER_EMAIL_PROCESSED', {
			rawEmailId: rawEmail.id,
			totalElapsedMs: Date.now() - emailStartMs,
			txnCount: persistence.transactionCount,
			needsReview: persistence.needsReviewIds.length,
		});
	};

	const failEmail = async (rawEmail: RawEmailRecord, error: unknown): Promise<void> => {
		await markRawEmailFailed(sql, rawEmail, error);
		result.failed_raw_email_count++;
		result.failed_raw_email_ids.push(rawEmail.id);
	};

	const processSingleEmail = async (rawEmail: RawEmailRecord): Promise<void> => {
		const ctx = await getUserContext(rawEmail.user_id);

		// Masked mode (default): the model only ever sees <N#> placeholders in
		// place of digits — no amounts, balances, or account numbers leave us.
		const masked = normConfig.maskNumbers ? maskNumericTokens(rawEmail.clean_text) : null;
		const promptText = masked ? masked.masked : rawEmail.clean_text;
		const systemPrompt = masked ? buildMaskedSystemPrompt(ctx) : buildSystemPrompt(ctx);
		const emailStartMs = Date.now();

		// Never log email content: bodies carry balances, account numbers,
		// and personal mail. Structural metadata only.
		console.info('AI_NORMALIZER_EMAIL_INPUT', {
			rawEmailId: rawEmail.id,
			textLength: rawEmail.clean_text.length,
			masked: masked !== null,
		});

		try {
			const aiResponse = await callWithTieredFallback(env, promptText, apiKey, systemPrompt);

			const validated = masked
				? validateMaskedResponse(
						aiResponse,
						normConfig.minConfidence,
						ctx,
						masked.tokens,
						rawEmail.internal_date_iso,
					)
				: validateAndCleanResponse(
						aiResponse,
						normConfig.minConfidence,
						ctx,
						rawEmail.internal_date_iso,
					);

			result.extracted_fact_count +=
				Array.isArray(aiResponse.transactions) ? aiResponse.transactions.length : 0;

			await finishEmail(rawEmail, ctx, validated, emailStartMs);
		} catch (error) {
			if (error instanceof TransientMessageError) {
				throw error;
			}
			await failEmail(rawEmail, error);
		}
	};

	// Batched masked mode: one model call covers several same-user emails.
	// Placeholder ids are globally unique across the batch, so a token
	// reference can never resolve against the wrong email.
	const processMaskedBatch = async (batch: RawEmailRecord[]): Promise<void> => {
		const firstEmail = batch[0] as RawEmailRecord;
		const ctx = await getUserContext(firstEmail.user_id);
		const batchStartMs = Date.now();

		const sections: string[] = [];
		const tokens = new Map<string, string>();
		let nextIndex = 1;
		for (let position = 0; position < batch.length; position++) {
			const rawEmail = batch[position] as RawEmailRecord;
			const masked = maskNumericTokens(rawEmail.clean_text, nextIndex);
			nextIndex = masked.nextIndex;
			for (const [id, value] of masked.tokens) {
				tokens.set(id, value);
			}
			sections.push(`=== EMAIL ${position + 1} ===\n${masked.masked}`);
		}

		console.info('AI_NORMALIZER_BATCH_INPUT', {
			rawEmailIds: batch.map(rawEmail => rawEmail.id),
			batchSize: batch.length,
			masked: true,
		});

		let perEmailResponses: Map<number, AiNormalizationResponse>;
		try {
			const aiResponse = await callWithTieredFallback(
				env,
				sections.join('\n\n'),
				apiKey,
				buildMaskedBatchSystemPrompt(ctx, batch.length),
			);
			perEmailResponses = splitBatchResponse(aiResponse, batch.length);
		} catch (error) {
			if (error instanceof TransientMessageError) {
				throw error;
			}
			// Permanent model failure: every email in the batch is marked
			// FAILED so nothing silently disappears.
			for (const rawEmail of batch) {
				await failEmail(rawEmail, error);
			}
			return;
		}

		for (let position = 0; position < batch.length; position++) {
			const rawEmail = batch[position] as RawEmailRecord;
			const emailResponse = perEmailResponses.get(position + 1);
			if (!emailResponse) {
				// Model omitted this email: fail it visibly rather than
				// treating "no answer" as "no transactions".
				await failEmail(
					rawEmail,
					new Error('Batch response missing entry for this email'),
				);
				continue;
			}

			try {
				const validated = validateMaskedResponse(
					emailResponse,
					normConfig.minConfidence,
					ctx,
					tokens,
					rawEmail.internal_date_iso,
				);
				result.extracted_fact_count += Array.isArray(emailResponse.transactions)
					? emailResponse.transactions.length
					: 0;

				await finishEmail(rawEmail, ctx, validated, batchStartMs);
			} catch (error) {
				if (error instanceof TransientMessageError) {
					throw error;
				}
				await failEmail(rawEmail, error);
			}
		}
	};

	// Build per-user batches preserving request order.
	const eligibleByUser = new Map<UUID, RawEmailRecord[]>();
	for (const rawEmailId of requestedIds) {
		const rawEmail = rawEmailById.get(rawEmailId);
		if (!rawEmail || rawEmail.status !== 'PENDING_EXTRACTION') {
			result.skipped_raw_email_count++;
			continue;
		}
		const list = eligibleByUser.get(rawEmail.user_id) ?? [];
		list.push(rawEmail);
		eligibleByUser.set(rawEmail.user_id, list);
	}

	const batchSize = normConfig.maskNumbers ? normConfig.batchSize : 1;
	for (const emails of eligibleByUser.values()) {
		for (let start = 0; start < emails.length; start += batchSize) {
			const batch = emails.slice(start, start + batchSize);
			if (batch.length === 1) {
				await processSingleEmail(batch[0] as RawEmailRecord);
			} else {
				await processMaskedBatch(batch);
			}
		}
	}

	result.ai_enqueued_count = await enqueueAiClassificationJobs(
		env.AI_CLASSIFICATION_QUEUE,
		allNeedsReviewIds,
		resolveAiQueueDelaySeconds(env),
	);

	return result;
}
