// The single normalization engine: template match → else AI → learn.
//
// Every pending email is first tried against the sender's learned templates
// (lib/email-templates.ts) — a hit is resolved locally with zero AI cost.
// Misses go to the model in masked, batched form, and each successful
// single-transaction answer is induced back into a template, so the next
// email of that format matches without a model call.

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
	resolveInstrumentLink,
	toSearchKey,
} from '../lib/identity';
import type { IdentityGraphLookup } from '../lib/identity';
import {
	applyTemplate,
	induceExtractTemplate,
	induceIgnoreTemplate,
	loadTemplatesForSenders,
	recordTemplateFailure,
	recordTemplateHit,
	saveTemplate,
	selfVerifyTemplate,
} from '../lib/email-templates';
import type {
	EmailTemplate,
	SelfVerifyExpectation,
	TemplateApplyResult,
	TemplateInductionResult,
} from '../lib/email-templates';
import {
	maskNumericTokens,
	resolveMaskedDateText,
	resolveTokenValue,
	unmaskText,
} from '../lib/masking';
import { hasLikelyDuplicateEvent, resolveDedupWindowMinutes } from '../lib/txn-dedup';
import {
	resolveAiProviderConfig,
	resolveEmailPipelineTraceEnabled,
	resolveEmailTemplatesEnabled,
	resolveNormalizerTierConfig,
} from '../lib/ai';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import { toIsoDateTime, toNullableString, toRequiredString } from '../lib/db/serialization';
import { PoisonMessageError, TransientMessageError } from './queue.errors';

const VALID_PAYMENT_METHODS: readonly PaymentMethod[] = [
	'upi',
	'credit_card',
	'debit_card',
	'netbanking',
	'cash',
	'unknown',
];

const VALID_DIRECTIONS = ['debit', 'credit'] as const;

// Above this (₹10,00,000) a transaction is never auto-verified — it is
// persisted and forced to NEEDS_REVIEW. It is NOT a validity cap: dropping
// large amounts would silently lose exactly the transactions that matter
// most.
const AMOUNT_REVIEW_THRESHOLD_IN_PAISE = 1_000_000_00;

interface UserNormalizationContext {
	categories: Array<{ id: string; name: string; type: string; sub_categories: Array<{ id: string; name: string }> }>;
	accounts: Array<{ id: string; name: string; type: string; last4: string | null }>;
	// Active user_credit_cards; never sent to the model — instrument linking
	// happens locally at persist time.
	credit_cards: Array<{ id: string; account_id: string; last4: string; issuer_name: string | null }>;
	misc_category_id: string | null;
	// System categories this user has hidden ("deleted"). Any resolved
	// category — rule, AI pick, or global-alias default — degrades to null
	// when hidden, so a deleted category never lands on their transactions.
	hidden_category_ids: Set<string>;
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
		'RULES for category matching:',
		'- Pick the best matching category from the list above based on the transaction content.',
		'- If a sub-category fits, set both category_id (parent) and sub_category_id.',
		'- The category type must match the money flow: debit → an "expense" (or "transfer") category; credit → an "income" (or "transfer") category. Never cross types.',
		'- If unsure or nothing fits, return null for category_id and sub_category_id. Never guess, never invent IDs.',
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
		'- counterparty_raw: Extract merchant/person/bank name. If BOTH a payment gateway/aggregator (Razorpay, PayU, BillDesk, Cashfree, CCAvenue) AND the actual merchant appear, use the actual merchant; if only the gateway is named, copy the gateway name as printed. null if unclear. Never invent a name.',
		'- instrument_last4: Last 4 digits of card or account number seen in email. null if not present.',
		'- payment_method: "upi" if VPA/UPI, "credit_card"/"debit_card" if card, "netbanking" if bank transfer, "cash" if ATM, "unknown" otherwise.',
		'- category_id / sub_category_id: pick from the user context per its rules; null when unsure.',
		'- Amounts must be INR. If the transaction amount is only in a foreign currency (USD, EUR, ...) with no INR amount, do NOT extract that transaction.',
		'- confidence: your honest certainty that this is a REAL transaction with the right values. 0.95-1.0 when explicit. 0.7-0.9 with inference. Below 0.4 when likely spurious — low-confidence items are discarded, so be honest.',
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
		'- date_text: copy the fragment holding the date the TRANSACTION occurred, exactly as it appears, placeholders included — never statement dates, due dates, or footer dates. null if absent.',
		'- direction: "debit" = money going OUT. "credit" = money coming IN.',
		'- counterparty_raw: merchant/person/bank name copied from the email. If BOTH a payment gateway/aggregator (Razorpay, PayU, BillDesk, Cashfree, CCAvenue) AND the actual merchant appear, use the actual merchant; if only the gateway is named, copy the gateway name as printed. null if unclear. Never invent a name.',
		'- instrument_token: placeholder holding the account/card digits mentioned for the instrument. null if none.',
		'- payment_method: "upi" if VPA/UPI, "credit_card"/"debit_card" if card, "netbanking" for NEFT/IMPS/RTGS, "cash" if ATM, "unknown" otherwise.',
		'- Amounts must be INR. If the transaction amount is only in a foreign currency (USD, EUR, ...) with no INR amount, do NOT extract that transaction.',
		'- confidence: your honest certainty that this is a REAL transaction with the right values. 0.95-1.0 when explicit. 0.7-0.9 with inference. Below 0.4 when likely spurious — low-confidence items are discarded, so be honest.',
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
	from_address: string | null;
	status: RawEmailStatus;
}

interface RawEmailFetchRowRaw {
	id: unknown;
	user_id: unknown;
	internal_date: unknown;
	clean_text: unknown;
	from_address?: unknown;
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
	template_matched_count: number;
	template_learned_count: number;
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
	hiddenCategoryIds: ReadonlySet<string> = new Set<string>(),
): ResolvedAiItem[] {
	// A category the user has hidden ("deleted") resolves as if it didn't
	// exist for them — regardless of whether a rule, the model, or a global
	// alias default proposed it. The chain then falls through (usually to the
	// fallback, which never auto-verifies), so the transaction reaches the
	// review inbox instead of silently wearing a deleted category.
	const unlessHidden = (categoryId: string | null): string | null =>
		categoryId && hiddenCategoryIds.has(categoryId) ? null : categoryId;

	return items.map(item => {
		const searchKeys = buildCanonicalSearchKeys(item);
		const identity = resolveIdentityForSearchKeys(userId, searchKeys, lookup);

		const transferIntercepted = identity.merchant_type === 'TRANSFER_INSTITUTION';
		const transactionType: TransactionType = transferIntercepted
			? 'transfer'
			: directionToType(item.direction);

		// Category precedence: user rule > AI's per-email pick (user-context
		// aware) > global alias default > system generic for the type
		// (Miscellaneous/Other — never a bare root). A model answer that IS
		// the system generic ("use Miscellaneous when unsure") means "don't
		// know" — it ranks and verifies like the fallback, not like a pick.
		const rawAiCategoryId = item.sub_category_id || item.category_id || null;
		const aiCategoryId = unlessHidden(
			rawAiCategoryId === lookup.system_categories[transactionType] ? null : rawAiCategoryId,
		);
		const resolvedCategoryId =
			unlessHidden(identity.rule_category_id) ??
			aiCategoryId ??
			unlessHidden(identity.alias_category_id) ??
			null;
		const categoryId = resolvedCategoryId ?? lookup.system_categories[transactionType];

		// Auto-verify needs BOTH an identity signal (user rule or known
		// merchant alias) AND a genuinely resolved category (rule, AI pick, or
		// alias default). The system fallback never verifies — otherwise every
		// mapper-created alias would silently verify junk-categorized
		// transactions and they would never reach the review inbox. Model
		// confidence is stored for triage but never verifies on its own.
		// An AGGREGATOR alias is not an identity signal: recognizing Razorpay
		// says nothing about the real merchant behind it, so only the user's
		// own rule may verify such a transaction.
		const aliasIdentitySignal =
			identity.global_alias_hit && identity.merchant_type !== 'AGGREGATOR';
		const identityVerified =
			(identity.user_rule_hit || aliasIdentitySignal) && resolvedCategoryId !== null;

		// Amounts above the plausibility threshold always face a human — a
		// mispicked token (account number as amount) or a genuinely huge
		// transfer both deserve review, and neither may be silently trusted.
		const overAmountThreshold = item.amount_in_paise > AMOUNT_REVIEW_THRESHOLD_IN_PAISE;
		const status: TransactionStatus =
			identityVerified && !overAmountThreshold ? 'VERIFIED' : 'NEEDS_REVIEW';

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
		from_address:
			row.from_address === undefined
				? null
				: toNullableString(row.from_address, 'raw_emails.from_address'),
		status: toRequiredString(row.status, 'raw_emails.status') as RawEmailStatus,
	};
}

async function loadRawEmailsByIds(
	sql: SqlClient,
	rawEmailIds: UUID[],
): Promise<RawEmailRecord[]> {
	const rows = await sql<RawEmailFetchRowRaw[]>`
		select re.id, re.user_id, re.internal_date, re.clean_text, re.from_address, re.status
		from public.raw_emails as re
		where re.id = any(${rawEmailIds}::uuid[])
		order by re.internal_date asc
	`;
	return rows.map(mapRawEmailRow);
}

interface CategoryRowRaw { id: unknown; name: unknown; type: unknown; parent_id: unknown; }

interface AccountRowRaw { id: unknown; name: unknown; type: unknown; instrument_last4: unknown; }

interface CreditCardRowRaw { id: unknown; account_id: unknown; last4: unknown; issuer_name: unknown; }

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

	// System subcategories join the prompt as flat top-level picks — roots
	// (Income/Expense/Transfer) are structural only and never offered, so the
	// model cannot assign a bare root. This is also what makes a fresh user
	// (no custom categories yet) classifiable at all. Categories the user has
	// hidden ("deleted" system rows) are excluded from their prompt.
	const systemCatRows = await sql<CategoryRowRaw[]>`
		select c.id, c.name, c.type, c.parent_id
		from public.categories as c
		where c.user_id is null
			and c.is_system = true
			and c.parent_id is not null
			and not exists (
				select 1
				from public.user_hidden_categories as uhc
				where uhc.user_id = ${userId}
					and uhc.category_id = c.id
			)
		order by c.type asc, c.name asc
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

	// Rows whose parent is not in the fetched set are parents (their parent
	// is a structural root, which is excluded); the rest are subcategories.
	const userCategoryNames = new Set(categories.map(c => `${c.name}:${c.type}`));
	const systemIds = new Set(
		systemCatRows.map(row => toRequiredString(row.id, 'categories.id')),
	);
	const systemParents = systemCatRows.filter(
		row => !systemIds.has(String(row.parent_id ?? '')),
	);
	const systemSubs = systemCatRows.filter(row => systemIds.has(String(row.parent_id ?? '')));
	for (const row of systemParents) {
		const id = toRequiredString(row.id, 'categories.id');
		const name = toRequiredString(row.name, 'categories.name');
		const type = toRequiredString(row.type, 'categories.type');
		// A user category with the same name shadows the system one, so the
		// prompt never offers confusing duplicates.
		if (userCategoryNames.has(`${name}:${type}`)) continue;
		categories.push({
			id,
			name,
			type,
			sub_categories: systemSubs
				.filter(sub => String(sub.parent_id ?? '') === id)
				.map(sub => ({
					id: toRequiredString(sub.id, 'categories.id'),
					name: toRequiredString(sub.name, 'categories.name'),
				})),
		});
	}

	const miscCat =
		categories.find(c => c.name === 'Miscellaneous' && c.type === 'expense') ?? null;

	// Hidden ids are needed at resolve time too: the alias-default path (and
	// a stale user rule) could otherwise assign a category the user deleted.
	const hiddenRows = await sql<Array<{ category_id: unknown }>>`
		select uhc.category_id
		from public.user_hidden_categories as uhc
		where uhc.user_id = ${userId}
	`;
	const hiddenCategoryIds = new Set<string>();
	for (const row of hiddenRows) {
		if (typeof row.category_id === 'string' && row.category_id.length > 0) {
			hiddenCategoryIds.add(row.category_id);
		}
	}

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

	const cardRows = await sql<CreditCardRowRaw[]>`
		select ucc.id, ucc.account_id, ucc.last4, ucc.issuer_name
		from public.user_credit_cards as ucc
		where ucc.user_id = ${userId}
			and ucc.is_active = true
		order by ucc.created_at asc
	`;

	const creditCards = cardRows.map(c => ({
		id: toRequiredString(c.id, 'user_credit_cards.id'),
		account_id: toRequiredString(c.account_id, 'user_credit_cards.account_id'),
		last4: toRequiredString(c.last4, 'user_credit_cards.last4'),
		issuer_name: toNullableString(c.issuer_name, 'user_credit_cards.issuer_name'),
	}));

	return {
		categories,
		accounts,
		credit_cards: creditCards,
		misc_category_id: miscCat?.id ?? null,
		hidden_category_ids: hiddenCategoryIds,
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
		template_matched_count: 0,
		template_learned_count: 0,
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

// Returns the last balanced top-level {...} block in the text, or null.
// String-aware enough for JSON: braces inside quoted strings are ignored.
function extractLastJsonObject(text: string): string | null {
	let lastComplete: string | null = null;
	let depth = 0;
	let start = -1;
	let inString = false;
	let escaped = false;
	for (let i = 0; i < text.length; i++) {
		const ch = text[i];
		if (inString) {
			if (escaped) {
				escaped = false;
			} else if (ch === '\\') {
				escaped = true;
			} else if (ch === '"') {
				inString = false;
			}
			continue;
		}
		if (ch === '"') {
			if (depth > 0) inString = true;
			continue;
		}
		if (ch === '{') {
			if (depth === 0) start = i;
			depth++;
		} else if (ch === '}') {
			if (depth > 0) {
				depth--;
				if (depth === 0 && start >= 0) {
					lastComplete = text.slice(start, i + 1);
					start = -1;
				}
			}
		}
	}
	return lastComplete;
}

async function callModel(
	chatUrl: string,
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

		response = await fetch(chatUrl, {
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
		choices?: Array<{ message?: { content?: string; reasoning_content?: string } }>;
	};

	const message = json.choices?.[0]?.message;
	let content = message?.content;
	// Reasoning-model responses (deepseek reasoner style) sometimes finish
	// with empty content and the final JSON at the end of reasoning_content.
	// Salvage the LAST balanced top-level object — earlier ones may be
	// abandoned drafts. Salvaged values still face full local validation and
	// token resolution, so a bad draft cannot inject data silently.
	if (!content && typeof message?.reasoning_content === 'string') {
		const salvaged = extractLastJsonObject(message.reasoning_content);
		if (salvaged) {
			console.warn('AI_NORMALIZER_REASONING_SALVAGE', {
				model,
				reasoningLength: message.reasoning_content.length,
				salvagedLength: salvaged.length,
			});
			content = salvaged;
		}
	}
	if (!content) {
		console.error('AI_NORMALIZER_EMPTY_RESPONSE', {
			status: response.status,
			model,
			responseKeys: Object.keys(json),
			choicesLength: json.choices?.length,
			hasMessage: Boolean(message),
			messageKeys: message ? Object.keys(message) : [],
			contentLength: message?.content?.length ?? 0,
			reasoningLength:
				typeof message?.reasoning_content === 'string' ? message.reasoning_content.length : 0,
			reasoningTail:
				typeof message?.reasoning_content === 'string'
					? message.reasoning_content.slice(-300)
					: null,
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
	chatUrl: string,
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
				chatUrl,
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

			if (tierFails >= (config.tierAttempts[tierIdx] ?? 1)) {
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
): { categoryId: string | null; subCategoryId: string | null } {
	// A hallucinated or missing category is NOT rewritten to Miscellaneous
	// here — that would masquerade as a deliberate model pick and auto-verify
	// alias hits. It stays null so the identity chain (alias default, then
	// the non-verifying system fallback) decides.
	const rawCategoryId =
		typeof record.category_id === 'string' ? record.category_id || null : null;
	const categoryId =
		rawCategoryId && knownCategoryIds.has(rawCategoryId) ? rawCategoryId : null;

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
		// No upper cap here: a large amount is NEVER dropped (silently losing
		// the biggest transactions is the worst failure for a money app) —
		// over-threshold amounts are forced to NEEDS_REVIEW at resolve time.
		if (!amountInPaise) continue;

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

		const { categoryId, subCategoryId } = validateCategoryIds(record, knownCategoryIds);

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
			credit_card_id: null,
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
		// See the masked validator: over-threshold amounts are kept and forced
		// to NEEDS_REVIEW at resolve time, never dropped.
		if (!Number.isSafeInteger(amountInPaise) || amountInPaise <= 0) continue;

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
			credit_card_id: null,
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
			// account_id comes from the same card row as credit_card_id, so the
			// DB consistency trigger always agrees.
			const resolvedCreditCardId = item.credit_card_id || null;

			const txnRows = await txSql<TransactionInsertRowRaw[]>`
				insert into public.transactions (
					user_id, financial_event_id, account_id,
					category_id, merchant_id, credit_card_id,
					amount_in_paise, type, txn_date, user_note,
					status, classification_source, ai_confidence_score
				)
				values (
					${lockedRow.user_id}, ${feId}, ${resolvedAccountId},
					${resolved.category_id}, ${resolved.merchant_id}, ${resolvedCreditCardId},
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

	// A missing key only disables the AI leg; template matches still resolve
	// locally and unmatched emails stay PENDING_EXTRACTION for a later run.
	const providerConfig = resolveAiProviderConfig(env);
	const apiKey = providerConfig.apiKey;
	if (!apiKey) {
		console.error(`AI normalizer: ${providerConfig.apiKeyEnvVar} is not set`);
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
			// Instrument linking (cards first, then type-compatible accounts;
			// ambiguity never guesses). Metadata only — auto-verify still runs
			// exclusively on identity-graph signals.
			const link = resolveInstrumentLink({
				payment_method: item.payment_method,
				instrument_last4: item.instrument_last4,
				from_address: rawEmail.from_address,
				accounts: ctx.accounts,
				credit_cards: ctx.credit_cards,
			});
			if (link.credit_card_id || link.account_id) {
				item.credit_card_id = link.credit_card_id;
				item.account_id = link.account_id;
			}
			// No guessy fallback beyond resolveInstrumentLink: digit-less or
			// unmatched instruments stay unlinked for review rather than being
			// keyword-matched to whichever bank account sorts first.
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
			ctx.hidden_category_ids,
		);

		if (resolveEmailPipelineTraceEnabled(env)) {
			// Content-bearing trace (counterparty names) — dev flag only.
			for (const resolved of resolvedItems) {
				console.info('PIPELINE_TRACE_RESOLVED', {
					rawEmailId: rawEmail.id,
					fromAddress: rawEmail.from_address,
					amountInPaise: resolved.item.amount_in_paise,
					txnTimestamp: resolved.item.txn_timestamp,
					direction: resolved.item.direction,
					paymentMethod: resolved.item.payment_method,
					instrumentLast4: resolved.item.instrument_last4,
					counterpartyRaw: resolved.item.counterparty_raw,
					searchKey: resolved.search_key,
					merchantId: resolved.merchant_id,
					categoryId: resolved.category_id,
					transactionType: resolved.transaction_type,
					classificationSource: resolved.classification_source,
					status: resolved.status,
					linkedAccountId: resolved.item.account_id ?? null,
					linkedCreditCardId: resolved.item.credit_card_id ?? null,
				});
			}
		}

		const persistence = await persistAiTransactions(
			sql,
			rawEmail,
			resolvedItems,
			resolveDedupWindowMinutes(env),
		);

		result.persisted_financial_event_count += persistence.transactionCount;
		result.created_transaction_count += persistence.transactionCount;
		result.needs_review_transaction_count += persistence.needsReviewIds.length;
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

	const templatesEnabled = resolveEmailTemplatesEnabled(env);

	// Turns one masked AI answer into a stored template. Only single-
	// transaction answers are learnable (multi-transaction emails stay
	// AI-only), and only templates that replay the AI's exact answer against
	// the source email survive self-verification. Learning failures are
	// logged and swallowed: the extraction is already persisted, and a
	// missed lesson just means the next email of this format pays for one
	// more AI call.
	const maybeLearnTemplate = async (
		rawEmail: RawEmailRecord,
		response: AiNormalizationResponse,
		validated: AiNormalizedTransactionItem[],
		maskedText: string,
		tokens: Map<string, string>,
	): Promise<void> => {
		if (!templatesEnabled || !rawEmail.from_address) return;

		try {
			const rawTransactions = Array.isArray(response.transactions)
				? response.transactions
				: null;
			let induced: TemplateInductionResult;
			let expectation: SelfVerifyExpectation | null = null;

			if (rawTransactions && rawTransactions.length === 0 && validated.length === 0) {
				// A well-formed "no transactions" answer can teach an IGNORE
				// template (recurring OTP/notification formats stop costing AI).
				induced = induceIgnoreTemplate(rawEmail.clean_text, maskedText);
			} else if (rawTransactions && rawTransactions.length === 1 && validated.length === 1) {
				const rawRecord = rawTransactions[0] as unknown as Record<string, unknown>;
				const item = validated[0] as AiNormalizedTransactionItem;
				induced = induceExtractTemplate({
					masked_text: maskedText,
					tokens,
					amount_token:
						typeof rawRecord.amount_token === 'string' ? rawRecord.amount_token : null,
					date_text: typeof rawRecord.date_text === 'string' ? rawRecord.date_text : null,
					instrument_token:
						typeof rawRecord.instrument_token === 'string'
							? rawRecord.instrument_token
							: null,
					counterparty_raw_masked:
						typeof rawRecord.counterparty_raw === 'string'
							? rawRecord.counterparty_raw
							: null,
					direction: item.direction,
					payment_method: item.payment_method,
					confidence: item.confidence,
				});
				expectation = {
					amount_in_paise: item.amount_in_paise,
					txn_timestamp: item.txn_timestamp,
					instrument_last4: item.instrument_last4,
					counterparty_raw: item.counterparty_raw,
				};
			} else {
				return;
			}

			if (!induced.ok) {
				console.info('AI_NORMALIZER_TEMPLATE_NOT_LEARNED', {
					rawEmailId: rawEmail.id,
					reason: induced.reason,
				});
				return;
			}

			const verified = selfVerifyTemplate(
				induced.skeleton,
				induced.slot_map,
				rawEmail.clean_text,
				rawEmail.internal_date_iso,
				expectation,
			);
			if (!verified) {
				console.warn('AI_NORMALIZER_TEMPLATE_SELF_VERIFY_FAILED', {
					rawEmailId: rawEmail.id,
					kind: induced.slot_map.kind,
				});
				return;
			}

			const saved = await saveTemplate(sql, {
				from_address: rawEmail.from_address,
				skeleton: induced.skeleton,
				slot_map: induced.slot_map,
				learned_from_raw_email_id: rawEmail.id,
			});
			if (saved) {
				result.template_learned_count++;
				console.info('AI_NORMALIZER_TEMPLATE_LEARNED', {
					rawEmailId: rawEmail.id,
					fromAddress: rawEmail.from_address,
					kind: induced.slot_map.kind,
				});
			}
		} catch (error) {
			console.warn('AI_NORMALIZER_TEMPLATE_LEARN_ERROR', {
				rawEmailId: rawEmail.id,
				message: error instanceof Error ? error.message : 'unknown',
			});
		}
	};

	const processSingleEmail = async (rawEmail: RawEmailRecord): Promise<void> => {
		if (!apiKey) return;
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
			const aiResponse = await callWithTieredFallback(
				env,
				promptText,
				apiKey,
				providerConfig.chatUrl,
				systemPrompt,
			);

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

			// Learning needs the model's token references, so masked mode only.
			if (masked) {
				await maybeLearnTemplate(rawEmail, aiResponse, validated, masked.masked, masked.tokens);
			}
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
		if (!apiKey) return;
		const firstEmail = batch[0] as RawEmailRecord;
		const ctx = await getUserContext(firstEmail.user_id);
		const batchStartMs = Date.now();

		const sections: string[] = [];
		const maskedTexts: string[] = [];
		const tokens = new Map<string, string>();
		let nextIndex = 1;
		for (let position = 0; position < batch.length; position++) {
			const rawEmail = batch[position] as RawEmailRecord;
			const masked = maskNumericTokens(rawEmail.clean_text, nextIndex);
			nextIndex = masked.nextIndex;
			for (const [id, value] of masked.tokens) {
				tokens.set(id, value);
			}
			maskedTexts.push(masked.masked);
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
				providerConfig.chatUrl,
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
				await maybeLearnTemplate(
					rawEmail,
					emailResponse,
					validated,
					maskedTexts[position] ?? '',
					tokens,
				);
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

	// Template pass: learned formats are resolved locally before anything is
	// sent to a model. A load failure fails open to the AI path — templates
	// are a cost optimization, never a correctness gate.
	let templatesBySender = new Map<string, EmailTemplate[]>();
	if (templatesEnabled) {
		const senders = Array.from(
			new Set(
				Array.from(eligibleByUser.values())
					.flat()
					.map(rawEmail => rawEmail.from_address)
					.filter((address): address is string => typeof address === 'string'),
			),
		);
		try {
			templatesBySender = await loadTemplatesForSenders(sql, senders);
		} catch (error) {
			console.warn('AI_NORMALIZER_TEMPLATE_LOAD_FAILED', {
				message: error instanceof Error ? error.message : 'unknown',
			});
		}
	}

	// Tries the sender's templates (most-hit first); returns true when the
	// email was fully handled locally. Structural misses cost nothing; a
	// match with garbage values penalizes that template and falls through.
	const applyTemplatesToEmail = async (rawEmail: RawEmailRecord): Promise<boolean> => {
		const templates = rawEmail.from_address
			? templatesBySender.get(rawEmail.from_address)
			: undefined;
		if (!templates || templates.length === 0) return false;

		const emailStartMs = Date.now();
		for (const template of templates) {
			let applied: TemplateApplyResult;
			try {
				applied = applyTemplate(template, rawEmail.clean_text, rawEmail.internal_date_iso);
			} catch (error) {
				applied = {
					outcome: 'matched_invalid',
					reason: error instanceof Error ? error.message : 'apply threw',
				};
			}

			if (applied.outcome === 'no_match') continue;

			if (applied.outcome === 'matched_invalid') {
				console.warn('AI_NORMALIZER_TEMPLATE_MATCH_INVALID', {
					rawEmailId: rawEmail.id,
					templateId: template.id,
					reason: applied.reason,
				});
				try {
					await recordTemplateFailure(sql, template.id);
				} catch (error) {
					console.warn('AI_NORMALIZER_TEMPLATE_FAILURE_RECORD_ERROR', {
						templateId: template.id,
						message: error instanceof Error ? error.message : 'unknown',
					});
				}
				continue;
			}

			if (applied.outcome === 'ignore') {
				await markRawEmailIgnored(sql, rawEmail);
				result.ignored_raw_email_count++;
				result.template_matched_count++;
			} else {
				const ctx = await getUserContext(rawEmail.user_id);
				const item: AiNormalizedTransactionItem = {
					direction: applied.extraction.direction,
					amount_in_paise: applied.extraction.amount_in_paise,
					txn_timestamp: applied.extraction.txn_timestamp,
					payment_method: applied.extraction.payment_method,
					counterparty_raw: applied.extraction.counterparty_raw,
					instrument_last4: applied.extraction.instrument_last4,
					category_guess: null,
					category_id: null,
					sub_category_id: null,
					account_id: null,
					credit_card_id: null,
					confidence: applied.extraction.confidence,
				};
				result.extracted_fact_count++;
				await finishEmail(rawEmail, ctx, [item], emailStartMs);
				result.template_matched_count++;
			}

			console.info('AI_NORMALIZER_TEMPLATE_MATCHED', {
				rawEmailId: rawEmail.id,
				templateId: template.id,
				outcome: applied.outcome,
			});
			try {
				await recordTemplateHit(sql, template.id);
			} catch (error) {
				console.warn('AI_NORMALIZER_TEMPLATE_HIT_RECORD_ERROR', {
					templateId: template.id,
					message: error instanceof Error ? error.message : 'unknown',
				});
			}
			return true;
		}
		return false;
	};

	const batchSize = normConfig.maskNumbers ? normConfig.batchSize : 1;
	for (const emails of eligibleByUser.values()) {
		const aiPending: RawEmailRecord[] = [];
		for (const rawEmail of emails) {
			const handledByTemplate = await applyTemplatesToEmail(rawEmail);
			if (!handledByTemplate) {
				aiPending.push(rawEmail);
			}
		}

		if (!apiKey) {
			result.skipped_raw_email_count += aiPending.length;
			continue;
		}

		for (let start = 0; start < aiPending.length; start += batchSize) {
			const batch = aiPending.slice(start, start + batchSize);
			if (batch.length === 1) {
				await processSingleEmail(batch[0] as RawEmailRecord);
			} else {
				await processMaskedBatch(batch);
			}
		}
	}

	return result;
}
