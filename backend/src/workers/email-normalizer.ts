import type {
	AiClassificationJobPayload,
	ClassificationSource,
	EventDirection,
	MerchantType,
	NormalizeRawEmailsJobPayload,
	PaymentMethod,
	RawEmailStatus,
	TransactionStatus,
	TransactionType,
	UUID,
} from '../../../shared/types';
import { NORMALIZE_RAW_EMAILS_MAX_IDS } from '../../../shared/types';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import { toIsoDateTime, toNullableString, toRequiredString } from '../lib/db/serialization';
import { PoisonMessageError, TransientMessageError } from './queue.errors';

const KILL_SWITCH_ENV_KEYS = [
	'NORMALIZATION_KILL_SWITCH',
	'EMAIL_NORMALIZATION_KILL_SWITCH',
] as const;
const AMOUNT_CANDIDATE_REGEX =
	/(?:^|[^A-Za-z0-9])(?:₹|inr\.?|rs\.?)\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)|(?:^|[^A-Za-z0-9])([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:₹|inr\.?|rs\.?)(?=$|[^A-Za-z0-9])/gi;
const DEBIT_PATTERNS: readonly RegExp[] = [
	/\bdebited\b/i,
	/\bspent\b/i,
	/\bpaid\b/i,
	/\bsent\b/i,
	/\bwithdrawn\b/i,
	/\bpurchase\b/i,
];
const CREDIT_PATTERNS: readonly RegExp[] = [
	/\bcredited\b/i,
	/\breceived\b/i,
	/\brefund\b/i,
	/\bdeposited\b/i,
	/\breversed\b/i,
];
const DIRECTION_CUE_PATTERNS = [...DEBIT_PATTERNS, ...CREDIT_PATTERNS] as const;
const REVERSAL_HINT_REGEX = /\b(reversal|reversed|refund|credited back|reinstated|chargeback)\b/i;
const TRANSFER_HINT_REGEX = /\b(card bill|credit card payment|wallet transfer|wallet topup|self transfer|to self|own account|a\/c transfer|account transfer|bank transfer)\b/i;
const TRANSACTION_AMOUNT_HINT_REGEX =
	/\b(debited|credited|spent|paid|sent|received|purchase|refund|reversal|transaction|txn|withdrawn|deposited)\b/i;
const BALANCE_AMOUNT_HINT_REGEX =
	/\b(balance|bal|avl|available|closing|opening|outstanding|limit|due|min(?:imum)?\s+due|total\s+due)\b/i;
const CASHBACK_HINT_REGEX = /\bcash[\s-]*back\b|\bcashback\b/i;
const NON_FINANCIAL_MARKERS = [
	'otp',
	'one time password',
	'statement',
	'newsletter',
	'offer',
	'promotion',
	'bill due reminder',
	'welcome',
] as const;

const AGGREGATOR_PATTERNS: ReadonlyArray<{ key: string; pattern: RegExp }> = [
	{ key: 'RAZORPAY', pattern: /\brazorpay\b/i },
	{ key: 'BHARATPE', pattern: /\bbharatpe\b/i },
	{ key: 'PAYU', pattern: /\bpayu\b/i },
	{ key: 'CCAVENUE', pattern: /\bccavenue\b/i },
	{ key: 'CASHFREE', pattern: /\bcashfree\b/i },
	{ key: 'BILLDESK', pattern: /\bbilldesk\b/i },
];
const GENERIC_INSTRUMENT_TOKENS = new Set([
	'UPI',
	'TXN',
	'TRANSACTION',
	'PAYMENT',
	'REF',
	'REFERENCE',
	'NUMBER',
	'NO',
]);

const SEARCH_KEY_MAX_LENGTH = 120;
const MAX_QUEUE_ENQUEUE_ATTEMPTS = 3;

interface RawEmailFetchRowRaw {
	id: unknown;
	user_id: unknown;
	internal_date: unknown;
	clean_text: unknown;
	status: unknown;
}

interface RawEmailRecord {
	id: UUID;
	user_id: UUID;
	internal_date_iso: string;
	clean_text: string;
	status: RawEmailStatus;
}

interface AmountCandidate {
	amount_in_paise: number;
	index: number;
	raw_token: string;
}

interface SystemCategoryRowRaw {
	id: unknown;
	type: unknown;
}

interface UserRuleLookupRowRaw {
	user_id: unknown;
	search_key: unknown;
	merchant_id: unknown;
	custom_category_id: unknown;
}

interface GlobalAliasLookupRowRaw {
	search_key: unknown;
	merchant_id: unknown;
	default_category_id: unknown;
	merchant_type: unknown;
}

interface FinancialEventInsertRowRaw {
	id: unknown;
}

interface FinancialEventExistingRowRaw {
	id: unknown;
}

interface TransactionInsertRowRaw {
	id: unknown;
	status: unknown;
}

interface ExtractedFactDraft {
	raw_email_id: UUID;
	user_id: UUID;
	line_index: number;
	raw_fragment: string;
	direction: EventDirection;
	amount_in_paise: number;
	txn_timestamp: string;
	payment_method: PaymentMethod;
	instrument_id: string | null;
	counterparty_raw: string | null;
}

interface FlaggedFact extends ExtractedFactDraft {
	vpa_handle: string | null;
	aggregator_key: string | null;
}

interface CanonicalFact extends FlaggedFact {
	canonical_counterparty: string | null;
	canonical_instrument: string | null;
	search_keys: string[];
	primary_search_key: string | null;
}

interface IdentityResolvedFact extends CanonicalFact {
	extraction_index: number;
	merchant_id: UUID | null;
	merchant_type: MerchantType | null;
	category_id: UUID | null;
	transaction_type: TransactionType;
	classification_source: ClassificationSource;
	transaction_status: TransactionStatus;
	transfer_intercepted: boolean;
}

interface PreparedRawEmailNormalization {
	raw_email: RawEmailRecord;
	extracted_facts: ExtractedFactDraft[];
	reconciled_facts: ExtractedFactDraft[];
	canonical_facts: CanonicalFact[];
}

interface SystemCategoryMap {
	income: UUID | null;
	expense: UUID | null;
	transfer: UUID | null;
}

interface UserRuleResolution {
	merchant_id: UUID | null;
	custom_category_id: UUID | null;
}

interface GlobalAliasResolution {
	merchant_id: UUID;
	default_category_id: UUID | null;
	merchant_type: MerchantType;
}

interface IdentityGraphLookup {
	system_categories: SystemCategoryMap;
	user_rules: Map<UUID, Map<string, UserRuleResolution>>;
	global_aliases: Map<string, GlobalAliasResolution>;
}

interface PersistenceOutcome {
	state: 'processed' | 'ignored' | 'unrecognized' | 'skipped';
	persisted_financial_event_count: number;
	persisted_transaction_count: number;
	created_needs_review_transaction_ids: UUID[];
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
}

function normalizeBooleanEnvFlag(value: unknown): boolean {
	if (typeof value !== 'string') {
		return false;
	}

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
		if (normalizeBooleanEnvFlag(envRecord[key])) {
			return true;
		}
	}
	return false;
}

function dedupeUuidPreserveOrder(ids: UUID[]): UUID[] {
	const seen = new Set<string>();
	const deduped: UUID[] = [];
	for (const id of ids) {
		if (seen.has(id)) {
			continue;
		}
		seen.add(id);
		deduped.push(id);
	}
	return deduped;
}

function normalizeWhitespace(value: string): string {
	return value.replace(/\r\n?/g, '\n').replace(/[ \t]{2,}/g, ' ').trim();
}

function toSearchKey(value: string | null): string | null {
	if (!value) {
		return null;
	}
	const normalized = value
		.toUpperCase()
		.replace(/[^A-Z0-9]/g, '')
		.slice(0, SEARCH_KEY_MAX_LENGTH);
	return normalized.length > 0 ? normalized : null;
}

function dedupeOrderedStrings(values: Array<string | null | undefined>): string[] {
	const seen = new Set<string>();
	const output: string[] = [];
	for (const value of values) {
		if (!value) {
			continue;
		}
		if (seen.has(value)) {
			continue;
		}
		seen.add(value);
		output.push(value);
	}
	return output;
}

function findFirstRegexIndex(value: string, patterns: readonly RegExp[]): number | null {
	let first: number | null = null;
	for (const pattern of patterns) {
		const match = pattern.exec(value);
		if (!match || typeof match.index !== 'number') {
			continue;
		}
		if (first === null || match.index < first) {
			first = match.index;
		}
	}
	return first;
}

function detectDirection(value: string): EventDirection | null {
	const debitIndex = findFirstRegexIndex(value, DEBIT_PATTERNS);
	const creditIndex = findFirstRegexIndex(value, CREDIT_PATTERNS);

	if (debitIndex === null && creditIndex === null) {
		return null;
	}
	if (debitIndex === null) {
		return 'credit';
	}
	if (creditIndex === null) {
		return 'debit';
	}
	return debitIndex <= creditIndex ? 'debit' : 'credit';
}

function shouldSuppressCashbackFragment(value: string): boolean {
	return CASHBACK_HINT_REGEX.test(value);
}

function detectNeighborDirection(lines: string[], index: number): EventDirection | null {
	const prevDirection = index > 0 ? detectDirection(lines[index - 1] ?? '') : null;
	const nextDirection = index + 1 < lines.length ? detectDirection(lines[index + 1] ?? '') : null;

	if (prevDirection && nextDirection && prevDirection !== nextDirection) {
		return null;
	}

	return prevDirection ?? nextDirection;
}

function parseAmountTokenToPaise(token: string): number | null {
	const normalized = token.replace(/,/g, '');
	const rupees = Number.parseFloat(normalized);
	if (!Number.isFinite(rupees) || rupees <= 0) {
		return null;
	}

	const paise = Math.round(rupees * 100);
	if (!Number.isSafeInteger(paise) || paise <= 0) {
		return null;
	}
	return paise;
}

function extractAmountCandidates(value: string): AmountCandidate[] {
	const candidates: AmountCandidate[] = [];
	const seen = new Set<string>();
	const regex = new RegExp(AMOUNT_CANDIDATE_REGEX.source, AMOUNT_CANDIDATE_REGEX.flags);
	for (const match of value.matchAll(regex)) {
		const token =
			typeof match[1] === 'string' && match[1].length > 0
				? match[1]
				: typeof match[2] === 'string' && match[2].length > 0
					? match[2]
					: null;
		if (!token || typeof match.index !== 'number') {
			continue;
		}
		const tokenOffset = match[0].indexOf(token);
		if (tokenOffset < 0) {
			continue;
		}
		const tokenIndex = match.index + tokenOffset;

		const amountInPaise = parseAmountTokenToPaise(token);
		if (!amountInPaise) {
			continue;
		}

		const dedupeKey = `${tokenIndex}:${amountInPaise}`;
		if (seen.has(dedupeKey)) {
			continue;
		}
		seen.add(dedupeKey);
		candidates.push({
			amount_in_paise: amountInPaise,
			index: tokenIndex,
			raw_token: token,
		});
	}
	return candidates;
}

function scoreAmountCandidate(
	fullText: string,
	candidate: AmountCandidate,
	transactionCueIndex: number | null,
): number {
	const contextStart = Math.max(0, candidate.index - 24);
	const contextEnd = Math.min(
		fullText.length,
		candidate.index + candidate.raw_token.length + 24,
	);
	const context = fullText.slice(contextStart, contextEnd);

	let score = 0;
	if (TRANSACTION_AMOUNT_HINT_REGEX.test(context)) {
		score += 6;
	}
	if (BALANCE_AMOUNT_HINT_REGEX.test(context)) {
		score -= 8;
	}
	if (transactionCueIndex !== null) {
		const distance = Math.abs(candidate.index - transactionCueIndex);
		if (distance <= 12) {
			score += 4;
		} else if (distance <= 24) {
			score += 2;
		} else if (distance <= 40) {
			score += 1;
		}
	}

	return score;
}

function extractAmountInPaise(value: string): number | null {
	const candidates = extractAmountCandidates(value);
	if (candidates.length === 0) {
		return null;
	}
	if (candidates.length === 1) {
		return candidates[0]?.amount_in_paise ?? null;
	}

	const transactionCueIndex = findFirstRegexIndex(value, DIRECTION_CUE_PATTERNS);
	let best = candidates[0] as AmountCandidate;
	let bestScore = scoreAmountCandidate(value, best, transactionCueIndex);

	for (let index = 1; index < candidates.length; index += 1) {
		const candidate = candidates[index] as AmountCandidate;
		const score = scoreAmountCandidate(value, candidate, transactionCueIndex);
		if (score > bestScore) {
			best = candidate;
			bestScore = score;
			continue;
		}

		if (score === bestScore && transactionCueIndex !== null) {
			const bestDistance = Math.abs(best.index - transactionCueIndex);
			const candidateDistance = Math.abs(candidate.index - transactionCueIndex);
			if (candidateDistance < bestDistance) {
				best = candidate;
			}
		}
	}

	return best.amount_in_paise;
}

function detectPaymentMethod(value: string): PaymentMethod {
	const normalized = value.toLowerCase();
	if (/\bupi\b|vpa|@ok|@ybl|@ibl|utr|upi ref/i.test(normalized)) {
		return 'upi';
	}
	if (/\bcredit\s*card\b|card ending|card xx/i.test(normalized)) {
		return 'credit_card';
	}
	if (/\bdebit\s*card\b/i.test(normalized)) {
		return 'debit_card';
	}
	if (/\bnetbanking\b|\bneft\b|\bimps\b|\brtgs\b/i.test(normalized)) {
		return 'netbanking';
	}
	if (/\bcash\b/i.test(normalized)) {
		return 'cash';
	}
	return 'unknown';
}

function extractInstrumentId(value: string): string | null {
	const patterns = [
		/\b(?:upi|utr|ref(?:erence)?|txn(?: id)?|transaction id)\s*(?:no\.?|number|#|:)?\s*([A-Za-z0-9-]{6,64})/i,
	];
	for (const pattern of patterns) {
		const match = value.match(pattern);
		if (!match || typeof match[1] !== 'string') {
			continue;
		}
		const normalized = match[1].trim().slice(0, 64);
		const normalizedUpper = normalized.toUpperCase();
		if (
			normalized.length < 6 ||
			!/[0-9]/.test(normalized) ||
			GENERIC_INSTRUMENT_TOKENS.has(normalizedUpper)
		) {
			continue;
		}
		if (normalized.length >= 6) {
			return normalized;
		}
	}
	return null;
}

function extractCounterparty(value: string, direction: EventDirection): string | null {
	const patterns =
		direction === 'debit'
			? [/\b(?:to|paid to|sent to|at)\s+([A-Za-z0-9@._& -]{2,120})/i]
			: [/\b(?:from|by)\s+([A-Za-z0-9@._& -]{2,120})/i];

	for (const pattern of patterns) {
		const match = value.match(pattern);
		if (!match || typeof match[1] !== 'string') {
			continue;
		}

		const trimmed = match[1]
			.split(/\b(?:via|using|for|on|ref|utr)\b/i)[0]
			?.split(/[\n,;|]/)[0]
			?.trim();
		if (!trimmed) {
			continue;
		}

		const normalized = normalizeWhitespace(trimmed).slice(0, 120);
		if (normalized.length > 1) {
			return normalized;
		}
	}

	return null;
}

function detectVpaHandle(value: string): string | null {
	const match = value.match(/\b[a-z0-9._-]{2,}@[a-z][a-z0-9.-]{1,}\b/i);
	if (!match || typeof match[0] !== 'string') {
		return null;
	}
	return match[0].toLowerCase();
}

function detectAggregatorKey(value: string): string | null {
	for (const entry of AGGREGATOR_PATTERNS) {
		if (entry.pattern.test(value)) {
			return entry.key;
		}
	}
	return null;
}

function resolveNoFactTerminalStatus(cleanText: string): 'IGNORED' | 'UNRECOGNIZED' {
	const normalized = cleanText.toLowerCase();
	if (normalized.trim().length === 0) {
		return 'IGNORED';
	}
	if (CASHBACK_HINT_REGEX.test(normalized)) {
		return 'IGNORED';
	}
	for (const marker of NON_FINANCIAL_MARKERS) {
		if (normalized.includes(marker)) {
			return 'IGNORED';
		}
	}
	return 'UNRECOGNIZED';
}

function mapRawEmailRow(row: RawEmailFetchRowRaw): RawEmailRecord {
	return {
		id: toRequiredString(row.id, 'raw_emails.id'),
		user_id: toRequiredString(row.user_id, 'raw_emails.user_id'),
		internal_date_iso: toIsoDateTime(row.internal_date, 'raw_emails.internal_date'),
		clean_text: toRequiredString(row.clean_text, 'raw_emails.clean_text'),
		status: toRequiredString(row.status, 'raw_emails.status') as RawEmailStatus,
	};
}

// Step 2: extractor
function extractDraftFacts(rawEmail: RawEmailRecord): ExtractedFactDraft[] {
	const normalizedText = normalizeWhitespace(rawEmail.clean_text);
	if (normalizedText.length === 0) {
		return [];
	}

	const globalDirection = detectDirection(normalizedText);
	const globalPaymentMethod = detectPaymentMethod(normalizedText);
	const globalInstrumentId = extractInstrumentId(normalizedText);
	const lines = normalizedText
		.split(/\n+/)
		.map(line => line.trim())
		.filter(line => line.length > 0);
	const isSingleLineEmail = lines.length === 1;

	const drafts: ExtractedFactDraft[] = [];
	for (let index = 0; index < lines.length; index += 1) {
		const line = lines[index] ?? '';
		const amountInPaise = extractAmountInPaise(line);
		if (amountInPaise === null) {
			continue;
		}

		if (shouldSuppressCashbackFragment(line)) {
			continue;
		}

		const lineDirection = detectDirection(line);
		if (
			lineDirection === null &&
			BALANCE_AMOUNT_HINT_REGEX.test(line) &&
			!TRANSACTION_AMOUNT_HINT_REGEX.test(line)
		) {
			continue;
		}

		const neighborDirection = detectNeighborDirection(lines, index);
		const direction =
			lineDirection ?? neighborDirection ?? (isSingleLineEmail ? globalDirection : null);
		if (!direction) {
			continue;
		}

		const linePaymentMethod = detectPaymentMethod(line);
		const paymentMethod =
			linePaymentMethod === 'unknown' && isSingleLineEmail ? globalPaymentMethod : linePaymentMethod;
		const lineInstrumentId = extractInstrumentId(line);
		const instrumentId = lineInstrumentId ?? (isSingleLineEmail ? globalInstrumentId : null);
		const counterparty =
			extractCounterparty(line, direction) ??
			(isSingleLineEmail ? extractCounterparty(normalizedText, direction) : null);

		drafts.push({
			raw_email_id: rawEmail.id,
			user_id: rawEmail.user_id,
			line_index: index,
			raw_fragment: line,
			direction,
			amount_in_paise: amountInPaise,
			txn_timestamp: rawEmail.internal_date_iso,
			payment_method: paymentMethod,
			instrument_id: instrumentId,
			counterparty_raw: counterparty,
		});
	}

	if (drafts.length > 0) {
		return drafts;
	}

	const fallbackAmount = extractAmountInPaise(normalizedText);
	const fallbackDirection = globalDirection;
	if (!fallbackAmount || !fallbackDirection) {
		return [];
	}
	if (shouldSuppressCashbackFragment(normalizedText)) {
		return [];
	}

	return [
		{
			raw_email_id: rawEmail.id,
			user_id: rawEmail.user_id,
			line_index: 0,
			raw_fragment: normalizedText,
			direction: fallbackDirection,
			amount_in_paise: fallbackAmount,
			txn_timestamp: rawEmail.internal_date_iso,
			payment_method: globalPaymentMethod,
			instrument_id: globalInstrumentId,
			counterparty_raw: extractCounterparty(normalizedText, fallbackDirection),
		},
	];
}

function buildDraftDedupKey(fact: ExtractedFactDraft): string {
	return [
		String(fact.line_index),
		fact.direction,
		String(fact.amount_in_paise),
		fact.instrument_id ?? '',
		fact.counterparty_raw ?? '',
		fact.payment_method,
	].join('|');
}

function normalizeReversalCounterpartyKey(value: string | null): string | null {
	if (!value) {
		return null;
	}

	const normalized = value
		.toUpperCase()
		.replace(/\b(REVERSAL|REFUND|CREDITED|BACK|REVERSED|REINSTATED|CHARGEBACK)\b/g, ' ')
		.replace(/\b(UPI|PAYMENT|TXN|TRANSACTION)\b/g, ' ');
	return toSearchKey(normalized);
}

function isLikelyReversalPair(debit: ExtractedFactDraft, credit: ExtractedFactDraft): boolean {
	if (debit.amount_in_paise !== credit.amount_in_paise) {
		return false;
	}

	const debitInstrument = normalizeInstrument(debit.instrument_id);
	const creditInstrument = normalizeInstrument(credit.instrument_id);
	if (debitInstrument && creditInstrument && debitInstrument !== creditInstrument) {
		return false;
	}

	const debitCounterparty = normalizeReversalCounterpartyKey(debit.counterparty_raw);
	const creditCounterparty = normalizeReversalCounterpartyKey(credit.counterparty_raw);
	if (debitCounterparty && creditCounterparty && debitCounterparty !== creditCounterparty) {
		return false;
	}

	const instrumentMatches =
		debitInstrument !== null &&
		creditInstrument !== null &&
		debitInstrument === creditInstrument;
	const counterpartyMatches =
		debitCounterparty !== null &&
		creditCounterparty !== null &&
		debitCounterparty === creditCounterparty;

	return instrumentMatches || counterpartyMatches;
}

function isReversalCreditFact(fact: ExtractedFactDraft): boolean {
	return fact.direction === 'credit' && REVERSAL_HINT_REGEX.test(fact.raw_fragment);
}

// Step 3: reconciler
function reconcileDraftFacts(rawText: string, extracted: ExtractedFactDraft[]): ExtractedFactDraft[] {
	const deduped = new Map<string, ExtractedFactDraft>();
	for (const fact of extracted) {
		const key = buildDraftDedupKey(fact);
		if (!deduped.has(key)) {
			deduped.set(key, fact);
		}
	}

	let reconciled = Array.from(deduped.values());
	if (REVERSAL_HINT_REGEX.test(rawText)) {
		const debitFacts = reconciled.filter(fact => fact.direction === 'debit');
		const creditFacts = reconciled.filter(fact => fact.direction === 'credit');
		const matchedDebits = new Set<ExtractedFactDraft>();
		const matchedCredits = new Set<ExtractedFactDraft>();

		for (const credit of creditFacts) {
			for (const debit of debitFacts) {
				if (matchedDebits.has(debit) || matchedCredits.has(credit)) {
					continue;
				}
				if (!isLikelyReversalPair(debit, credit)) {
					continue;
				}

				matchedDebits.add(debit);
				matchedCredits.add(credit);
				break;
			}
		}

		// Conservative fallback only for a single debit-credit pair. This
		// avoids over-matching in multi-event emails.
		if (
			reconciled.length === 2 &&
			debitFacts.length === 1 &&
			creditFacts.length === 1 &&
			matchedDebits.size === 0 &&
			matchedCredits.size === 0
		) {
			const debit = debitFacts[0] as ExtractedFactDraft;
			const credit = creditFacts[0] as ExtractedFactDraft;
			const debitCounterparty = normalizeReversalCounterpartyKey(debit.counterparty_raw);
			const creditCounterparty = normalizeReversalCounterpartyKey(credit.counterparty_raw);
			const hasCounterpartyConflict =
				debitCounterparty &&
				creditCounterparty &&
				debitCounterparty !== creditCounterparty;

			const debitInstrument = normalizeInstrument(debit.instrument_id);
			const creditInstrument = normalizeInstrument(credit.instrument_id);
			const hasInstrumentConflict =
				debitInstrument &&
				creditInstrument &&
				debitInstrument !== creditInstrument;

			const sparseOnBothSides =
				!debitCounterparty &&
				!creditCounterparty &&
				!debitInstrument &&
				!creditInstrument;
			const hasPaymentMethodConflict =
				debit.payment_method !== 'unknown' &&
				credit.payment_method !== 'unknown' &&
				debit.payment_method !== credit.payment_method;

			const withinReasonableLineDistance =
				credit.line_index >= debit.line_index &&
				credit.line_index - debit.line_index <= 4;
			if (
				!hasCounterpartyConflict &&
				!hasInstrumentConflict &&
				!hasPaymentMethodConflict &&
				sparseOnBothSides &&
				debit.amount_in_paise === credit.amount_in_paise &&
				isReversalCreditFact(credit) &&
				withinReasonableLineDistance
			) {
				matchedDebits.add(debit);
				matchedCredits.add(credit);
			}
		}

		if (matchedDebits.size > 0) {
			reconciled = reconciled.filter(
				fact => !(fact.direction === 'debit' && matchedDebits.has(fact)),
			);
		}
	}

	reconciled.sort((left, right) => {
		if (left.line_index !== right.line_index) {
			return left.line_index - right.line_index;
		}
		if (left.amount_in_paise !== right.amount_in_paise) {
			return left.amount_in_paise - right.amount_in_paise;
		}
		if (left.direction !== right.direction) {
			return left.direction === 'debit' ? -1 : 1;
		}
		return (left.instrument_id ?? '').localeCompare(right.instrument_id ?? '');
	});

	return reconciled;
}

// Step 4: VPA + aggregator flags
function addVpaAndAggregatorFlags(fact: ExtractedFactDraft): FlaggedFact {
	const vpaHandle =
		detectVpaHandle(fact.raw_fragment) ??
		detectVpaHandle(fact.counterparty_raw ?? '');
	const aggregatorKey = detectAggregatorKey(`${fact.raw_fragment} ${fact.counterparty_raw ?? ''}`);
	return {
		...fact,
		vpa_handle: vpaHandle,
		aggregator_key: aggregatorKey,
	};
}

function normalizeCounterparty(value: string | null): string | null {
	if (!value) {
		return null;
	}
	const normalized = normalizeWhitespace(value).toUpperCase().slice(0, 120);
	return normalized.length > 0 ? normalized : null;
}

function normalizeInstrument(value: string | null): string | null {
	if (!value) {
		return null;
	}
	const normalized = normalizeWhitespace(value).toUpperCase().slice(0, 64);
	return normalized.length >= 6 ? normalized : null;
}

// Step 5: canonicalization
function canonicalizeFlaggedFact(fact: FlaggedFact): CanonicalFact {
	const canonicalCounterparty = normalizeCounterparty(fact.counterparty_raw);
	const canonicalInstrument = normalizeInstrument(fact.instrument_id);
	const primaryCounterpartyKey = toSearchKey(canonicalCounterparty);
	const vpaKey = toSearchKey(fact.vpa_handle);
	const vpaUserKey = toSearchKey(fact.vpa_handle?.split('@')[0] ?? null);
	const aggregatorKey = toSearchKey(fact.aggregator_key);
	const instrumentKey = toSearchKey(canonicalInstrument);

	const searchKeys = dedupeOrderedStrings([
		primaryCounterpartyKey,
		vpaKey,
		vpaUserKey,
		aggregatorKey,
		instrumentKey,
	]);

	return {
		...fact,
		canonical_counterparty: canonicalCounterparty,
		canonical_instrument: canonicalInstrument,
		search_keys: searchKeys,
		primary_search_key: searchKeys[0] ?? null,
	};
}

async function loadSystemCategoryMap(sql: SqlClient): Promise<SystemCategoryMap> {
	const rows = await sql<SystemCategoryRowRaw[]>`
		select
			c.id,
			c.type
		from public.categories as c
		where c.user_id is null
			and c.is_system = true
			and c.parent_id is null
			and c.type in ('income', 'expense', 'transfer')
	`;

	const map: SystemCategoryMap = {
		income: null,
		expense: null,
		transfer: null,
	};
	for (const row of rows) {
		const type = toRequiredString(row.type, 'categories.type') as TransactionType;
		const id = toRequiredString(row.id, 'categories.id');
		if (type === 'income' || type === 'expense' || type === 'transfer') {
			map[type] = map[type] ?? id;
		}
	}
	return map;
}

async function loadUserRulesForLookup(
	sql: SqlClient,
	userIds: UUID[],
	searchKeys: string[],
): Promise<Map<UUID, Map<string, UserRuleResolution>>> {
	if (userIds.length === 0 || searchKeys.length === 0) {
		return new Map<UUID, Map<string, UserRuleResolution>>();
	}

	const rows = await sql<UserRuleLookupRowRaw[]>`
		select distinct on (umr.user_id, umr.search_key)
			umr.user_id,
			umr.search_key,
			umr.merchant_id,
			umr.custom_category_id
		from public.user_merchant_rules as umr
		where umr.user_id = any(${userIds}::uuid[])
			and umr.search_key = any(${searchKeys}::text[])
		order by umr.user_id, umr.search_key, umr.updated_at desc, umr.created_at desc, umr.id desc
	`;

	const lookup = new Map<UUID, Map<string, UserRuleResolution>>();
	for (const row of rows) {
		const userId = toRequiredString(row.user_id, 'user_merchant_rules.user_id');
		const searchKey = toRequiredString(row.search_key, 'user_merchant_rules.search_key');
		const userMap = lookup.get(userId) ?? new Map<string, UserRuleResolution>();
		userMap.set(searchKey, {
			merchant_id: toNullableString(row.merchant_id, 'user_merchant_rules.merchant_id'),
			custom_category_id: toNullableString(
				row.custom_category_id,
				'user_merchant_rules.custom_category_id',
			),
		});
		lookup.set(userId, userMap);
	}

	return lookup;
}

async function loadGlobalAliasesForLookup(
	sql: SqlClient,
	searchKeys: string[],
): Promise<Map<string, GlobalAliasResolution>> {
	if (searchKeys.length === 0) {
		return new Map<string, GlobalAliasResolution>();
	}

	const rows = await sql<GlobalAliasLookupRowRaw[]>`
		select
			gma.search_key,
			gm.id as merchant_id,
			gm.default_category_id,
			gm.type as merchant_type
		from public.global_merchant_aliases as gma
		join public.global_merchants as gm
			on gm.id = gma.merchant_id
		where gma.search_key = any(${searchKeys}::text[])
	`;

	const lookup = new Map<string, GlobalAliasResolution>();
	for (const row of rows) {
		const searchKey = toRequiredString(row.search_key, 'global_merchant_aliases.search_key');
		if (lookup.has(searchKey)) {
			continue;
		}
		lookup.set(searchKey, {
			merchant_id: toRequiredString(row.merchant_id, 'global_merchants.id'),
			default_category_id: toNullableString(
				row.default_category_id,
				'global_merchants.default_category_id',
			),
			merchant_type: toRequiredString(row.merchant_type, 'global_merchants.type') as MerchantType,
		});
	}

	return lookup;
}

// Step 6: batched identity graph lookup
async function loadIdentityGraphLookup(
	sql: SqlClient,
	preparedRows: PreparedRawEmailNormalization[],
): Promise<IdentityGraphLookup> {
	const systemCategories = await loadSystemCategoryMap(sql);

	const userIds = dedupeUuidPreserveOrder(preparedRows.map(row => row.raw_email.user_id));
	const allSearchKeys = dedupeOrderedStrings(
		preparedRows.flatMap(row => row.canonical_facts.flatMap(fact => fact.search_keys)),
	);

	const [userRules, globalAliases] = await Promise.all([
		loadUserRulesForLookup(sql, userIds, allSearchKeys),
		loadGlobalAliasesForLookup(sql, allSearchKeys),
	]);

	return {
		system_categories: systemCategories,
		user_rules: userRules,
		global_aliases: globalAliases,
	};
}

function shouldInterceptTransfer(
	fact: CanonicalFact,
	merchantType: MerchantType | null,
): boolean {
	if (merchantType === 'TRANSFER_INSTITUTION') {
		return true;
	}
	if (
		TRANSFER_HINT_REGEX.test(fact.raw_fragment) ||
		TRANSFER_HINT_REGEX.test(fact.canonical_counterparty ?? '')
	) {
		return true;
	}
	if (fact.vpa_handle && /\b(self|own)\b/i.test(fact.raw_fragment)) {
		return true;
	}
	return false;
}

interface IdentityResolution {
	merchant_id: UUID | null;
	merchant_type: MerchantType | null;
	category_id: UUID | null;
	user_rule_hit: boolean;
	global_alias_hit: boolean;
}

function resolveIdentityForSearchKeys(
	userId: UUID,
	searchKeys: string[],
	lookup: IdentityGraphLookup,
): IdentityResolution {
	let merchantId: UUID | null = null;
	let merchantType: MerchantType | null = null;
	let categoryId: UUID | null = null;
	let userRuleHit = false;
	let globalAliasHit = false;

	const userRules = lookup.user_rules.get(userId) ?? new Map<string, UserRuleResolution>();
	for (const key of searchKeys) {
		const userRule = userRules.get(key);
		if (!userRule) {
			continue;
		}
		userRuleHit = true;
		merchantId = merchantId ?? userRule.merchant_id;
		categoryId = categoryId ?? userRule.custom_category_id;
	}

	for (const key of searchKeys) {
		const alias = lookup.global_aliases.get(key);
		if (!alias) {
			continue;
		}
		globalAliasHit = true;
		merchantId = merchantId ?? alias.merchant_id;
		merchantType = merchantType ?? alias.merchant_type;
		categoryId = categoryId ?? alias.default_category_id;
		break;
	}

	return {
		merchant_id: merchantId,
		merchant_type: merchantType,
		category_id: categoryId,
		user_rule_hit: userRuleHit,
		global_alias_hit: globalAliasHit,
	};
}

// Step 7: transfer interceptor + final resolution
function resolveCanonicalFactsForPersistence(
	preparedRow: PreparedRawEmailNormalization,
	lookup: IdentityGraphLookup,
): IdentityResolvedFact[] {
	return preparedRow.canonical_facts.map((fact, index) => {
		const identity = resolveIdentityForSearchKeys(
			preparedRow.raw_email.user_id,
			fact.search_keys,
			lookup,
		);

		const transferIntercepted = shouldInterceptTransfer(fact, identity.merchant_type);
		const transactionType: TransactionType = transferIntercepted
			? 'transfer'
			: fact.direction === 'debit'
				? 'expense'
				: 'income';
		const categoryId = identity.category_id ?? lookup.system_categories[transactionType];

		const shouldReview =
			!categoryId ||
			(fact.payment_method === 'unknown' && !identity.merchant_id) ||
			(fact.aggregator_key !== null && !identity.merchant_id);
		const transactionStatus: TransactionStatus = shouldReview ? 'NEEDS_REVIEW' : 'VERIFIED';
		const classificationSource: ClassificationSource = identity.user_rule_hit
			? 'USER'
			: transferIntercepted || identity.global_alias_hit
				? 'HEURISTIC'
				: 'SYSTEM_DEFAULT';

		return {
			...fact,
			extraction_index: index,
			merchant_id: identity.merchant_id,
			merchant_type: identity.merchant_type,
			category_id: categoryId,
			transaction_type: transactionType,
			classification_source: classificationSource,
			transaction_status: transactionStatus,
			transfer_intercepted: transferIntercepted,
		};
	});
}

async function loadRawEmailsByIds(sql: SqlClient, rawEmailIds: UUID[]): Promise<RawEmailRecord[]> {
	if (rawEmailIds.length === 0) {
		return [];
	}

	const rows = await sql<RawEmailFetchRowRaw[]>`
		select
			re.id,
			re.user_id,
			re.internal_date,
			re.clean_text,
			re.status
		from public.raw_emails as re
		where re.id = any(${rawEmailIds}::uuid[])
	`;
	return rows.map(mapRawEmailRow);
}

async function resolveFinancialEventId(
	sql: SqlClient,
	rawEmailId: UUID,
	extractionIndex: number,
): Promise<UUID> {
	const rows = await sql<FinancialEventExistingRowRaw[]>`
		select fe.id
		from public.financial_events as fe
		where fe.raw_email_id = ${rawEmailId}
			and fe.extraction_index = ${extractionIndex}
		limit 1
	`;

	const id = toNullableString(rows[0]?.id, 'financial_events.id');
	if (!id) {
		throw new Error('Financial event conflict reconciliation failed');
	}
	return id;
}

// Step 8: persistence with ON CONFLICT DO NOTHING
async function persistRawEmailNormalization(
	sql: SqlClient,
	rawEmail: RawEmailRecord,
	resolvedFacts: IdentityResolvedFact[],
): Promise<PersistenceOutcome> {
	return sql.begin(async (tx) => {
		const txSql = tx as unknown as SqlClient;
		const lockRows = await txSql<RawEmailFetchRowRaw[]>`
			select
				re.id,
				re.user_id,
				re.internal_date,
				re.clean_text,
				re.status
			from public.raw_emails as re
			where re.id = ${rawEmail.id}
			limit 1
			for update
		`;
		const lockedRow = lockRows[0] ? mapRawEmailRow(lockRows[0]) : null;
		if (!lockedRow || lockedRow.status !== 'PENDING_EXTRACTION') {
			return {
				state: 'skipped',
				persisted_financial_event_count: 0,
				persisted_transaction_count: 0,
				created_needs_review_transaction_ids: [],
			};
		}

		if (resolvedFacts.length === 0) {
			const terminalStatus = resolveNoFactTerminalStatus(lockedRow.clean_text);
			await txSql`
				update public.raw_emails as re
				set status = ${terminalStatus}
				where re.id = ${lockedRow.id}
					and re.user_id = ${lockedRow.user_id}
					and re.status = 'PENDING_EXTRACTION'
			`;

			return {
				state: terminalStatus === 'IGNORED' ? 'ignored' : 'unrecognized',
				persisted_financial_event_count: 0,
				persisted_transaction_count: 0,
				created_needs_review_transaction_ids: [],
			};
		}

		let persistedFinancialEvents = 0;
		let persistedTransactions = 0;
		const needsReviewTransactionIds: UUID[] = [];

		for (const fact of resolvedFacts) {
			const insertFinancialEventRows = await txSql<FinancialEventInsertRowRaw[]>`
				insert into public.financial_events (
					user_id,
					raw_email_id,
					extraction_index,
					direction,
					amount_in_paise,
					currency,
					txn_timestamp,
					payment_method,
					instrument_id,
					counterparty_raw,
					search_key,
					status
				)
				values (
					${lockedRow.user_id},
					${lockedRow.id},
					${fact.extraction_index},
					${fact.direction},
					${fact.amount_in_paise},
					'INR',
					${fact.txn_timestamp},
					${fact.payment_method},
					${fact.canonical_instrument},
					${fact.canonical_counterparty},
					${fact.primary_search_key},
					'ACTIVE'
				)
				on conflict (raw_email_id, extraction_index) do nothing
				returning id
			`;

			let financialEventId = toNullableString(
				insertFinancialEventRows[0]?.id,
				'financial_events.id',
			);
			if (financialEventId) {
				persistedFinancialEvents += 1;
			} else {
				financialEventId = await resolveFinancialEventId(
					txSql,
					lockedRow.id,
					fact.extraction_index,
				);
			}

			const insertTransactionRows = await txSql<TransactionInsertRowRaw[]>`
				insert into public.transactions (
					user_id,
					financial_event_id,
					account_id,
					category_id,
					merchant_id,
					credit_card_id,
					amount_in_paise,
					type,
					txn_date,
					user_note,
					status,
					classification_source,
					ai_confidence_score
				)
				values (
					${lockedRow.user_id},
					${financialEventId},
					null,
					${fact.category_id},
					${fact.merchant_id},
					null,
					${fact.amount_in_paise},
					${fact.transaction_type},
					${fact.txn_timestamp},
					null,
					${fact.transaction_status},
					${fact.classification_source},
					null
				)
				on conflict (financial_event_id) do nothing
				returning id, status
			`;

			const insertedTransactionId = toNullableString(
				insertTransactionRows[0]?.id,
				'transactions.id',
			);
			if (insertedTransactionId) {
				persistedTransactions += 1;
				const insertedStatus = toRequiredString(
					insertTransactionRows[0]?.status,
					'transactions.status',
				) as TransactionStatus;
				if (insertedStatus === 'NEEDS_REVIEW') {
					needsReviewTransactionIds.push(insertedTransactionId);
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
			state: 'processed',
			persisted_financial_event_count: persistedFinancialEvents,
			persisted_transaction_count: persistedTransactions,
			created_needs_review_transaction_ids: needsReviewTransactionIds,
		};
	});
}

async function markRawEmailFailed(
	sql: SqlClient,
	rawEmail: RawEmailRecord,
	error: unknown,
): Promise<void> {
	try {
		await sql`
			update public.raw_emails as re
			set status = 'FAILED'
			where re.id = ${rawEmail.id}
				and re.user_id = ${rawEmail.user_id}
				and re.status = 'PENDING_EXTRACTION'
		`;
	} catch {
		throw new TransientMessageError(
			`Failed to mark raw email ${rawEmail.id} as FAILED after normalization error`,
		);
	}

	console.error('Marked raw email FAILED during normalization', {
		rawEmailId: rawEmail.id,
		userId: rawEmail.user_id,
		error: error instanceof Error ? error.message : 'Unknown normalization error',
	});
}

async function enqueueAiClassificationJobs(queue: Queue, transactionIds: UUID[]): Promise<number> {
	const deduped = dedupeUuidPreserveOrder(transactionIds);
	let enqueuedCount = 0;

	for (const transactionId of deduped) {
		const payload: AiClassificationJobPayload = {
			job_type: 'AI_CLASSIFICATION',
			transaction_id: transactionId,
			requested_at: new Date().toISOString(),
		};

		let sent = false;
		for (let attempt = 1; attempt <= MAX_QUEUE_ENQUEUE_ATTEMPTS; attempt += 1) {
			try {
				await queue.send(payload, { contentType: 'json' });
				sent = true;
				enqueuedCount += 1;
				break;
			} catch (error) {
				console.warn('Failed to enqueue AI_CLASSIFICATION from normalizer', {
					transactionId,
					attempt,
					attemptsMax: MAX_QUEUE_ENQUEUE_ATTEMPTS,
					error: error instanceof Error ? error.message : 'Unknown enqueue error',
				});
			}
		}

		if (!sent) {
			console.error('Dropping AI_CLASSIFICATION enqueue after retries', {
				transactionId,
			});
		}
	}

	return enqueuedCount;
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
	};
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

	const requestedRawEmailIds = dedupeUuidPreserveOrder(job.raw_email_ids);
	if (requestedRawEmailIds.length > NORMALIZE_RAW_EMAILS_MAX_IDS) {
		throw new PoisonMessageError(
			`NORMALIZE_RAW_EMAILS raw_email_ids exceeds max ${NORMALIZE_RAW_EMAILS_MAX_IDS}`,
		);
	}
	const result = createEmptyResult(job.raw_email_ids.length);

	if (isKillSwitchEnabled(env)) {
		result.kill_switch_enabled = true;
		result.skipped_raw_email_count = job.raw_email_ids.length;
		console.warn('Skipping NORMALIZE_RAW_EMAILS because kill switch is enabled', {
			requestedRawEmailCount: job.raw_email_ids.length,
		});
		return result;
	}

	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const rawEmailRows = await loadRawEmailsByIds(sql, requestedRawEmailIds);
	result.loaded_raw_email_count = rawEmailRows.length;

	const rawEmailById = new Map<UUID, RawEmailRecord>();
	for (const row of rawEmailRows) {
		rawEmailById.set(row.id, row);
	}

	const preparedRows: PreparedRawEmailNormalization[] = [];
	for (const rawEmailId of requestedRawEmailIds) {
		const rawEmail = rawEmailById.get(rawEmailId);
		if (!rawEmail || rawEmail.status !== 'PENDING_EXTRACTION') {
			result.skipped_raw_email_count += 1;
			continue;
		}

		const extracted = extractDraftFacts(rawEmail);
		result.extracted_fact_count += extracted.length;
		const reconciled = reconcileDraftFacts(rawEmail.clean_text, extracted);
		result.reconciled_fact_count += reconciled.length;
			const flagged = reconciled.map(addVpaAndAggregatorFlags);
		const canonical = flagged.map(canonicalizeFlaggedFact);

		preparedRows.push({
			raw_email: rawEmail,
			extracted_facts: extracted,
			reconciled_facts: reconciled,
			canonical_facts: canonical,
		});
	}

	if (preparedRows.length === 0) {
		return result;
	}

	const identityLookup = await loadIdentityGraphLookup(sql, preparedRows);
	const needsReviewTransactionIds: UUID[] = [];

	for (const preparedRow of preparedRows) {
		const resolvedFacts = resolveCanonicalFactsForPersistence(preparedRow, identityLookup);
		try {
			const persistence = await persistRawEmailNormalization(
				sql,
				preparedRow.raw_email,
				resolvedFacts,
			);

			switch (persistence.state) {
				case 'processed':
					result.processed_raw_email_count += 1;
					break;
				case 'ignored':
					result.ignored_raw_email_count += 1;
					break;
				case 'unrecognized':
					result.unrecognized_raw_email_count += 1;
					break;
				case 'skipped':
					result.skipped_raw_email_count += 1;
					break;
			}

			result.persisted_financial_event_count += persistence.persisted_financial_event_count;
			result.created_transaction_count += persistence.persisted_transaction_count;
			result.needs_review_transaction_count += persistence.created_needs_review_transaction_ids.length;
			needsReviewTransactionIds.push(...persistence.created_needs_review_transaction_ids);
		} catch (error) {
			await markRawEmailFailed(sql, preparedRow.raw_email, error);
			result.failed_raw_email_count += 1;
		}
	}

	result.ai_enqueued_count = await enqueueAiClassificationJobs(
		env.AI_CLASSIFICATION_QUEUE,
		needsReviewTransactionIds,
	);

	return result;
}
