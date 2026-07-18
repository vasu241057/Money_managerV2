// Self-learning email templates.
//
// Every successful AI extraction teaches a template: the masked email
// (digits already <N#> placeholders) is generalized into a skeleton keyed
// by the exact sender address, and the AI's token references become slots.
// The next structurally identical email is resolved locally from the
// skeleton — no model call. A template is only saved after replaying it
// against the very email it was learned from and reproducing the AI's own
// answer exactly (self-verification), so a template can never be worse
// than the extraction that taught it.
//
// Skeleton marker language (stored in email_templates.skeleton):
//   {{N}}  referenced number slot — captured; the AI referenced this token
//          (amount, date part, or instrument), ordinals assigned in order
//   {{n}}  anonymous number slot — matches any <N#> placeholder, not captured
//          (e.g. a time next to a referenced date)
//   {{W}}  word wildcard — a whole word containing unreferenced numbers
//          (balances, reference ids, phone numbers, footer dates)
//   {{M}}  merchant wildcard — captured; the variable merchant/VPA span
//   {{MON}} month slot — captured; the date's month word, so month-name
//          formats ("May 18, 2026") aren't locked to the learn-time month
//   {{mon}} anonymous month wildcard — a month word beside wildcarded
//          numbers (due dates, footer dates); matched, not captured
//
// Static text (bank boilerplate) stays literal, which is what anchors the
// match. Emails are whitespace-collapsed before both induction and
// matching so sanitizer drift can't break templates.

import type { PaymentMethod, UUID } from '../../../shared/types';
import type { SqlClient } from './db/client';
import { toNullableString, toRequiredString, toSafeInteger } from './db/serialization';
import { parseAmountTokenToPaise } from './identity';
import { maskNumericTokens, parseDateTextToIso, unmaskText } from './masking';

export const MAX_TEMPLATES_PER_SENDER = 40;
export const TEMPLATE_DISABLE_FAILURE_COUNT = 3;
// Below these, a skeleton is mostly wildcards and would over-match.
const MIN_STATIC_CHARS_EXTRACT = 40;
const MIN_STATIC_CHARS_IGNORE = 60;
// Longer emails (digests, statements) are not template material.
const MAX_SKELETON_LENGTH = 20_000;

const M_SENTINEL = '\u0000M\u0000';
const TOKEN_REF_GLOBAL_REGEX = /<(N\d+)>/g;
// Non-global twin for .test() (a /g regex is stateful across .test calls).
const TOKEN_REF_TEST_REGEX = /<N\d+>/;
const MARKER_SPLIT_REGEX = /(\{\{(?:MON|mon|[NnWM])\}\})/;
const MARKER_STRIP_REGEX = /\{\{(?:MON|mon|[NnWM]|\d+)\}\}/g;
// Month words near date slots are wildcarded so month-name formats don't
// lock a template to the month it was learned in. Exact-name alternation
// (not prefix matching), so e.g. merchant text "MAYUR" stays literal.
const MONTH_NAMES_PATTERN =
	'january|february|march|april|may|june|july|august|september|october|november|december|sept|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec';
const MONTH_WORD_GLOBAL_REGEX = new RegExp(
	`(?<![A-Za-z])(?:${MONTH_NAMES_PATTERN})(?![A-Za-z])`,
	'gi',
);
const MONTH_WORD_REGEX = new RegExp(`(?<![A-Za-z])(?:${MONTH_NAMES_PATTERN})(?![A-Za-z])`, 'i');
const MONTH_EXACT_REGEX = new RegExp(`^(?:${MONTH_NAMES_PATTERN})$`, 'i');
// Strong monetary markers: an email containing these must never be learned
// as an IGNORE template — a false ignore would silently drop transactions.
const MONETARY_MARKER_REGEX =
	/₹|\brs\.?(?=\s|\d|<)|\binr\b|\bdebit(?:ed)?\b|\bcredit(?:ed)?\b|\bwithdrawn\b|\btransferred\b/i;

export type TemplateKind = 'EXTRACT' | 'IGNORE';

export interface TemplateMerchantSlot {
	kind: 'WILDCARD' | 'FIXED' | 'NONE';
	name?: string;
}

export interface TemplateSlotMap {
	version: 1;
	kind: TemplateKind;
	direction?: 'debit' | 'credit';
	payment_method?: PaymentMethod;
	amount_ordinal?: number;
	// Model's date fragment with token refs replaced by slot ordinals, e.g.
	// '{{2}}-{{3}}-{{4}} at {{5}}:{{6}}'. Rebuilt with real token values at
	// apply time and parsed locally.
	date_pattern?: string | null;
	instrument_ordinal?: number | null;
	merchant?: TemplateMerchantSlot;
	// AI confidence at learn time; replayed onto template extractions so
	// review-inbox ordering behaves like the original extraction.
	confidence?: number;
}

export interface EmailTemplate {
	id: UUID;
	from_address: string;
	skeleton: string;
	slot_map: TemplateSlotMap;
	hit_count: number;
	failure_count: number;
	compiled?: CompiledSkeleton | null;
}

export interface TemplateExtraction {
	direction: 'debit' | 'credit';
	amount_in_paise: number;
	txn_timestamp: string;
	payment_method: PaymentMethod;
	counterparty_raw: string | null;
	instrument_last4: string | null;
	confidence: number;
}

export type TemplateApplyResult =
	| { outcome: 'no_match' }
	// Structurally matched but produced invalid values — counts as a
	// template failure (disabled after TEMPLATE_DISABLE_FAILURE_COUNT).
	| { outcome: 'matched_invalid'; reason: string }
	| { outcome: 'ignore' }
	| { outcome: 'extracted'; extraction: TemplateExtraction };

// Literal '{{' in an email could collide with skeleton markers, so it is
// broken up before markers are inserted; matching applies the same
// transform, keeping learn and match views identical.
export function normalizeForTemplate(masked: string): string {
	return masked.replace(/\{\{/g, '{ {').replace(/\s+/g, ' ').trim();
}

function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeTokenRef(ref: string | null | undefined): string | null {
	if (typeof ref !== 'string') return null;
	const normalized = ref.trim().replace(/^<|>$/g, '');
	return /^N\d+$/.test(normalized) ? normalized : null;
}

export interface CompiledSkeleton {
	regex: RegExp;
	// Capture-group kinds in group order: 'N' captures a token id, 'MON'
	// captures a month word, 'M' captures the merchant span.
	capture_kinds: Array<'N' | 'M' | 'MON'>;
}

export function compileSkeleton(skeleton: string): CompiledSkeleton {
	const parts = skeleton.split(MARKER_SPLIT_REGEX);
	const captureKinds: Array<'N' | 'M' | 'MON'> = [];
	let pattern = '^';
	for (const part of parts) {
		switch (part) {
			case '':
				break;
			case '{{N}}':
				pattern += '<(N\\d+)>';
				captureKinds.push('N');
				break;
			case '{{n}}':
				pattern += '<N\\d+>';
				break;
			case '{{W}}':
				pattern += '\\S+';
				break;
			case '{{M}}':
				pattern += '(.+?)';
				captureKinds.push('M');
				break;
			// Month slots compile to a generic word; apply validates the
			// captured word against the real month list (no_match otherwise),
			// keeping the compiled regex simple and case-proof.
			case '{{MON}}':
				pattern += '([A-Za-z]{3,9})';
				captureKinds.push('MON');
				break;
			case '{{mon}}':
				pattern += '[A-Za-z]{3,9}';
				break;
			default:
				pattern += escapeRegExp(part);
		}
	}
	pattern += '$';
	return { regex: new RegExp(pattern), capture_kinds: captureKinds };
}

export interface TemplateInductionInput {
	// Masked text of the email (any placeholder start index) + its token map
	// (a batch-wide superset map is fine).
	masked_text: string;
	tokens: Map<string, string>;
	// The model's raw token references for this email's single transaction.
	amount_token: string | null;
	date_text: string | null;
	instrument_token: string | null;
	// Model-copied counterparty, still containing <N#> refs if it had any.
	counterparty_raw_masked: string | null;
	// Canonical values from the validated item.
	direction: 'debit' | 'credit';
	payment_method: PaymentMethod;
	confidence: number;
}

export type TemplateInductionResult =
	| { ok: true; skeleton: string; slot_map: TemplateSlotMap }
	| { ok: false; reason: string };

interface MerchantSpanResolution {
	text: string;
	merchant: TemplateMerchantSlot;
}

// Wildcards the merchant span only when it occurs exactly once at word
// boundaries and doesn't cover a referenced token. Anything else falls back
// to a FIXED merchant name (the template remembers the AI's normalization),
// which is always safe: a future email with different text at that spot
// simply won't match and goes to the AI.
function resolveMerchantSpan(
	text: string,
	counterpartyMasked: string | null,
	tokens: Map<string, string>,
	referencedIds: ReadonlySet<string>,
): MerchantSpanResolution {
	if (!counterpartyMasked) {
		return { text, merchant: { kind: 'NONE' } };
	}

	const fixedName = (unmaskText(counterpartyMasked, tokens) ?? counterpartyMasked)
		.trim()
		.slice(0, 500);
	const fixed: MerchantSpanResolution = {
		text,
		merchant: fixedName ? { kind: 'FIXED', name: fixedName } : { kind: 'NONE' },
	};

	const needle = normalizeForTemplate(counterpartyMasked);
	if (needle.length < 2) {
		return fixed;
	}

	let occurrenceRegex: RegExp;
	try {
		occurrenceRegex = new RegExp(
			`(?<![A-Za-z0-9])${escapeRegExp(needle)}(?![A-Za-z0-9])`,
			'gi',
		);
	} catch {
		return fixed;
	}

	const occurrences = [...text.matchAll(occurrenceRegex)];
	if (occurrences.length !== 1) {
		return fixed;
	}

	const start = occurrences[0]?.index ?? -1;
	if (start < 0) return fixed;
	const spanText = text.slice(start, start + needle.length);
	for (const refMatch of spanText.matchAll(TOKEN_REF_GLOBAL_REGEX)) {
		if (referencedIds.has(refMatch[1] ?? '')) {
			return fixed;
		}
	}

	return {
		text: text.slice(0, start) + M_SENTINEL + text.slice(start + needle.length),
		merchant: { kind: 'WILDCARD' },
	};
}

interface SkeletonMonthContext {
	date_ref_ids: ReadonlySet<string>;
	want_month: boolean;
}

interface SkeletonBuildOutput {
	skeleton: string;
	ordinal_by_token_id: Map<string, number>;
	// Ordinal of the single captured {{MON}} slot, when one was created.
	month_ordinal: number | null;
}

function buildSkeleton(
	text: string,
	referencedIds: ReadonlySet<string>,
	monthContext?: SkeletonMonthContext,
): SkeletonBuildOutput {
	const ordinalByTokenId = new Map<string, number>();
	let ordinal = 0;
	let monthOrdinal: number | null = null;

	const words = text.split(' ');
	const refIdsByWord = words.map(word =>
		[...word.matchAll(TOKEN_REF_GLOBAL_REGEX)].map(match => match[1] ?? ''),
	);
	const hasDateRefAt = (index: number): boolean =>
		(refIdsByWord[index] ?? []).some(id => monthContext?.date_ref_ids.has(id) ?? false);
	const hasNumberAt = (index: number): boolean => (refIdsByWord[index]?.length ?? 0) > 0;

	// Rewrites one word's token refs (and, in date contexts, month names)
	// into markers strictly left to right, so slot ordinals always mirror
	// capture-group order in the compiled skeleton.
	const renderWord = (word: string, monthAction: 'capture' | 'anon' | 'none'): string => {
		interface Segment {
			start: number;
			end: number;
			kind: 'ref' | 'month';
			id: string;
		}
		const segments: Segment[] = [];
		for (const refMatch of word.matchAll(TOKEN_REF_GLOBAL_REGEX)) {
			const start = refMatch.index ?? -1;
			if (start < 0) continue;
			segments.push({
				start,
				end: start + refMatch[0].length,
				kind: 'ref',
				id: refMatch[1] ?? '',
			});
		}
		if (monthAction !== 'none') {
			for (const monthMatch of word.matchAll(MONTH_WORD_GLOBAL_REGEX)) {
				const start = monthMatch.index ?? -1;
				if (start < 0) continue;
				segments.push({ start, end: start + monthMatch[0].length, kind: 'month', id: '' });
			}
		}
		segments.sort((a, b) => a.start - b.start);

		let built = '';
		let cursor = 0;
		for (const segment of segments) {
			if (segment.start < cursor) continue;
			built += word.slice(cursor, segment.start);
			if (segment.kind === 'ref') {
				if (referencedIds.has(segment.id)) {
					ordinal += 1;
					ordinalByTokenId.set(segment.id, ordinal);
					built += '{{N}}';
				} else {
					built += '{{n}}';
				}
			} else if (monthAction === 'capture' && monthOrdinal === null) {
				ordinal += 1;
				monthOrdinal = ordinal;
				built += '{{MON}}';
			} else {
				built += '{{mon}}';
			}
			cursor = segment.end;
		}
		return built + word.slice(cursor);
	};

	const skeletonWords = words.map((word, index) => {
		const refIds = refIdsByWord[index] ?? [];
		const hasSentinel = word.includes(M_SENTINEL);
		const hasReferenced = refIds.some(id => referencedIds.has(id));

		const nearDateRef =
			hasDateRefAt(index) || hasDateRefAt(index - 1) || hasDateRefAt(index + 1);
		const nearNumber = refIds.length > 0 || hasNumberAt(index - 1) || hasNumberAt(index + 1);
		// A month word only becomes a slot when it rides next to numbers —
		// prose like "you may contact us" has none nearby and stays literal.
		const monthAction: 'capture' | 'anon' | 'none' =
			monthContext?.want_month && nearDateRef ? 'capture' : nearNumber ? 'anon' : 'none';

		if (refIds.length === 0 && !hasSentinel) {
			return monthAction === 'none' ? word : renderWord(word, monthAction);
		}

		if (!hasReferenced && !hasSentinel) {
			// Unreferenced numbers (refs, balances, phone numbers) vary in
			// token count between emails, so the whole word becomes a
			// wildcard rather than shifting slot ordinals.
			return '{{W}}';
		}

		let built = renderWord(word, monthAction);
		if (hasSentinel) {
			built = built.split(M_SENTINEL).join('{{M}}');
		}
		return built;
	});

	return {
		skeleton: skeletonWords.join(' '),
		ordinal_by_token_id: ordinalByTokenId,
		month_ordinal: monthOrdinal,
	};
}

function countStaticChars(skeleton: string): number {
	return skeleton.replace(MARKER_STRIP_REGEX, '').replace(/\s/g, '').length;
}

export function induceExtractTemplate(
	input: TemplateInductionInput,
): TemplateInductionResult {
	const text = normalizeForTemplate(input.masked_text);

	const amountId = normalizeTokenRef(input.amount_token);
	if (!amountId || !text.includes(`<${amountId}>`)) {
		return { ok: false, reason: 'amount token not present in email text' };
	}

	let instrumentId = normalizeTokenRef(input.instrument_token);
	if (instrumentId && !text.includes(`<${instrumentId}>`)) {
		instrumentId = null;
	}

	// A date fragment with any hallucinated ref is dropped entirely — the
	// apply-time fallback (email internal date) is safer than a partial date.
	let dateText =
		typeof input.date_text === 'string' && input.date_text.trim().length > 0
			? normalizeForTemplate(input.date_text)
			: null;
	let dateRefIds: string[] = [];
	if (dateText) {
		dateRefIds = [...dateText.matchAll(TOKEN_REF_GLOBAL_REGEX)].map(match => match[1] ?? '');
		if (dateRefIds.length === 0 || dateRefIds.some(id => !text.includes(`<${id}>`))) {
			dateText = null;
			dateRefIds = [];
		}
	}

	const referencedIds = new Set<string>([amountId, ...dateRefIds]);
	if (instrumentId) referencedIds.add(instrumentId);

	const { text: spannedText, merchant } = resolveMerchantSpan(
		text,
		input.counterparty_raw_masked,
		input.tokens,
		referencedIds,
	);

	// A FIXED merchant whose name still carries token refs would bake this
	// email's digits into every future match (e.g. "POS 4521 KIRANA STORE"
	// replayed on the 4522 email), silently going stale. Digits are only safe
	// in a WILDCARD merchant, where they resolve per-email; otherwise the
	// format stays AI-only.
	if (
		merchant.kind === 'FIXED' &&
		input.counterparty_raw_masked !== null &&
		TOKEN_REF_TEST_REGEX.test(input.counterparty_raw_masked)
	) {
		return { ok: false, reason: 'merchant name has volatile digits' };
	}

	const {
		skeleton,
		ordinal_by_token_id: ordinalByTokenId,
		month_ordinal: monthOrdinal,
	} = buildSkeleton(spannedText, referencedIds, {
		date_ref_ids: new Set(dateRefIds),
		want_month: dateText !== null && MONTH_WORD_REGEX.test(dateText),
	});

	if (skeleton.length > MAX_SKELETON_LENGTH) {
		return { ok: false, reason: 'email too long to template' };
	}
	if (countStaticChars(skeleton) < MIN_STATIC_CHARS_EXTRACT) {
		return { ok: false, reason: 'skeleton too generic (not enough static text)' };
	}

	const amountOrdinal = ordinalByTokenId.get(amountId);
	if (!amountOrdinal) {
		return { ok: false, reason: 'amount token lost during skeleton build' };
	}

	let datePattern: string | null = null;
	if (dateText) {
		let allDateOrdinalsFound = true;
		datePattern = dateText.replace(TOKEN_REF_GLOBAL_REGEX, (whole, id: string) => {
			const dateOrdinal = ordinalByTokenId.get(id);
			if (!dateOrdinal) {
				allDateOrdinalsFound = false;
				return whole;
			}
			return `{{${dateOrdinal}}}`;
		});
		if (!allDateOrdinalsFound) datePattern = null;
		if (datePattern !== null && monthOrdinal !== null) {
			datePattern = datePattern.replace(MONTH_WORD_REGEX, `{{${monthOrdinal}}}`);
		}
		// Anything left in the pattern besides slots and separators — a month
		// word that found no slot in the text, or digits (the model never sees
		// real ones, so any digit here is fabricated) — would stamp this
		// email's date parts onto every future match. The internal-date
		// fallback is safer.
		if (datePattern !== null) {
			const residual = datePattern.replace(/\{\{\d+\}\}/g, ' ');
			if (/\d/.test(residual) || MONTH_WORD_REGEX.test(residual)) {
				datePattern = null;
			}
		}
	}

	const instrumentOrdinal = instrumentId ? (ordinalByTokenId.get(instrumentId) ?? null) : null;

	return {
		ok: true,
		skeleton,
		slot_map: {
			version: 1,
			kind: 'EXTRACT',
			direction: input.direction,
			payment_method: input.payment_method,
			amount_ordinal: amountOrdinal,
			date_pattern: datePattern,
			instrument_ordinal: instrumentOrdinal,
			merchant,
			confidence: input.confidence,
		},
	};
}

// IGNORE templates remember "this format has no transactions" (OTP mails,
// login alerts) so recurring noise stops costing AI calls. Learned only
// when the email carries no monetary language at all: a false ignore would
// silently drop money, so the guard errs hard toward not learning.
export function induceIgnoreTemplate(
	cleanText: string,
	maskedText: string,
): TemplateInductionResult {
	if (MONETARY_MARKER_REGEX.test(cleanText)) {
		return { ok: false, reason: 'email mentions money; not safe to auto-ignore' };
	}

	const text = normalizeForTemplate(maskedText);
	const { skeleton } = buildSkeleton(text, new Set<string>());

	if (skeleton.length > MAX_SKELETON_LENGTH) {
		return { ok: false, reason: 'email too long to template' };
	}
	if (countStaticChars(skeleton) < MIN_STATIC_CHARS_IGNORE) {
		return { ok: false, reason: 'skeleton too generic (not enough static text)' };
	}

	return { ok: true, skeleton, slot_map: { version: 1, kind: 'IGNORE' } };
}

export function applyTemplate(
	template: EmailTemplate,
	cleanText: string,
	internalDateIso: string,
): TemplateApplyResult {
	if (template.compiled === undefined) {
		try {
			template.compiled = compileSkeleton(template.skeleton);
		} catch {
			template.compiled = null;
		}
	}
	if (template.compiled === null) {
		return { outcome: 'matched_invalid', reason: 'skeleton failed to compile' };
	}
	const compiled = template.compiled;

	const masked = maskNumericTokens(cleanText);
	const text = normalizeForTemplate(masked.masked);

	const match = compiled.regex.exec(text);
	if (!match) {
		return { outcome: 'no_match' };
	}

	if (template.slot_map.kind === 'IGNORE') {
		return { outcome: 'ignore' };
	}

	const valueByOrdinal: Array<string | null> = [];
	let merchantMaskedSpan: string | null = null;
	for (let groupIndex = 0; groupIndex < compiled.capture_kinds.length; groupIndex++) {
		const captured = match[groupIndex + 1] ?? '';
		const kind = compiled.capture_kinds[groupIndex];
		if (kind === 'N') {
			valueByOrdinal.push(captured ? (masked.tokens.get(captured) ?? null) : null);
		} else if (kind === 'MON') {
			// {{MON}} compiles to a generic word capture; a non-month word
			// there means this is structurally a different email.
			if (!MONTH_EXACT_REGEX.test(captured)) {
				return { outcome: 'no_match' };
			}
			valueByOrdinal.push(captured);
		} else {
			merchantMaskedSpan = captured;
		}
	}

	const tokenValueAtOrdinal = (ordinal: number | null | undefined): string | null => {
		if (!ordinal || ordinal < 1 || ordinal > valueByOrdinal.length) return null;
		return valueByOrdinal[ordinal - 1] ?? null;
	};

	const amountText = tokenValueAtOrdinal(template.slot_map.amount_ordinal);
	if (!amountText) {
		return { outcome: 'matched_invalid', reason: 'amount slot missing' };
	}
	// No upper cap: a big amount is a valid extraction (forced to
	// NEEDS_REVIEW downstream), not a template defect — capping here let
	// three large transactions disable a perfectly good template.
	const amountInPaise = parseAmountTokenToPaise(amountText);
	if (!amountInPaise) {
		return { outcome: 'matched_invalid', reason: 'amount slot not a valid amount' };
	}

	let txnTimestamp = internalDateIso;
	const datePattern = template.slot_map.date_pattern;
	if (datePattern) {
		let allDateValuesFound = true;
		const rebuiltDate = datePattern.replace(/\{\{(\d+)\}\}/g, (whole, ordinalText: string) => {
			const value = tokenValueAtOrdinal(Number.parseInt(ordinalText, 10));
			if (value === null) {
				allDateValuesFound = false;
				return whole;
			}
			return value;
		});
		if (allDateValuesFound) {
			txnTimestamp = parseDateTextToIso(rebuiltDate, internalDateIso) ?? internalDateIso;
		}
	}

	const instrumentText = tokenValueAtOrdinal(template.slot_map.instrument_ordinal);
	const instrumentLast4 = instrumentText
		? instrumentText.replace(/\D/g, '').slice(-4) || null
		: null;

	const merchantSlot = template.slot_map.merchant ?? { kind: 'NONE' };
	let counterpartyRaw: string | null = null;
	if (merchantSlot.kind === 'WILDCARD') {
		const unmasked = unmaskText(merchantMaskedSpan, masked.tokens);
		counterpartyRaw = unmasked ? unmasked.trim().slice(0, 500) || null : null;
		if (!counterpartyRaw) {
			return { outcome: 'matched_invalid', reason: 'merchant slot empty' };
		}
	} else if (merchantSlot.kind === 'FIXED') {
		counterpartyRaw = merchantSlot.name ?? null;
	}

	const direction = template.slot_map.direction;
	if (direction !== 'debit' && direction !== 'credit') {
		return { outcome: 'matched_invalid', reason: 'slot map missing direction' };
	}

	const confidence = template.slot_map.confidence;

	return {
		outcome: 'extracted',
		extraction: {
			direction,
			amount_in_paise: amountInPaise,
			txn_timestamp: txnTimestamp,
			payment_method: template.slot_map.payment_method ?? 'unknown',
			counterparty_raw: counterpartyRaw,
			instrument_last4: instrumentLast4,
			confidence:
				typeof confidence === 'number' && confidence >= 0 && confidence <= 1
					? confidence
					: 0.5,
		},
	};
}

export interface SelfVerifyExpectation {
	amount_in_paise: number;
	txn_timestamp: string;
	instrument_last4: string | null;
	counterparty_raw: string | null;
}

function normalizeComparable(value: string | null): string {
	return (value ?? '').trim().replace(/\s+/g, ' ').toLowerCase();
}

// Replays the freshly induced template against its own source email; only
// a template that reproduces the AI's exact answer may be saved.
export function selfVerifyTemplate(
	skeleton: string,
	slotMap: TemplateSlotMap,
	cleanText: string,
	internalDateIso: string,
	expected: SelfVerifyExpectation | null,
): boolean {
	const probe: EmailTemplate = {
		id: '00000000-0000-0000-0000-000000000000' as UUID,
		from_address: '',
		skeleton,
		slot_map: slotMap,
		hit_count: 0,
		failure_count: 0,
	};

	const applied = applyTemplate(probe, cleanText, internalDateIso);

	if (slotMap.kind === 'IGNORE') {
		return applied.outcome === 'ignore';
	}

	if (applied.outcome !== 'extracted' || !expected) {
		return false;
	}
	const extraction = applied.extraction;
	return (
		extraction.amount_in_paise === expected.amount_in_paise &&
		extraction.txn_timestamp === expected.txn_timestamp &&
		(extraction.instrument_last4 ?? null) === (expected.instrument_last4 ?? null) &&
		normalizeComparable(extraction.counterparty_raw) ===
			normalizeComparable(expected.counterparty_raw)
	);
}

// 64-bit FNV-1a over the skeleton; used only for the per-sender uniqueness
// constraint, never for security.
export function skeletonHash(skeleton: string): string {
	let hash = 0xcbf29ce484222325n;
	const prime = 0x100000001b3n;
	for (let index = 0; index < skeleton.length; index++) {
		hash ^= BigInt(skeleton.charCodeAt(index));
		hash = (hash * prime) & 0xffffffffffffffffn;
	}
	return hash.toString(16).padStart(16, '0');
}

interface EmailTemplateRowRaw {
	id: unknown;
	from_address: unknown;
	skeleton: unknown;
	slot_map: unknown;
	hit_count: unknown;
	failure_count: unknown;
}

function parseSlotMap(raw: unknown): TemplateSlotMap | null {
	const record =
		typeof raw === 'string'
			? (() => {
					try {
						return JSON.parse(raw) as unknown;
					} catch {
						return null;
					}
				})()
			: raw;
	if (typeof record !== 'object' || record === null) return null;
	const slotMap = record as TemplateSlotMap;
	if (slotMap.version !== 1) return null;
	if (slotMap.kind !== 'EXTRACT' && slotMap.kind !== 'IGNORE') return null;
	if (slotMap.kind === 'EXTRACT') {
		if (!Number.isInteger(slotMap.amount_ordinal) || (slotMap.amount_ordinal ?? 0) < 1) {
			return null;
		}
	}
	return slotMap;
}

export async function loadTemplatesForSenders(
	sql: SqlClient,
	fromAddresses: string[],
): Promise<Map<string, EmailTemplate[]>> {
	const bySender = new Map<string, EmailTemplate[]>();
	if (fromAddresses.length === 0) {
		return bySender;
	}

	const rows = await sql<EmailTemplateRowRaw[]>`
		select et.id, et.from_address, et.skeleton, et.slot_map, et.hit_count, et.failure_count
		from public.email_templates as et
		where et.from_address = any(${fromAddresses}::text[])
			and et.status = 'ACTIVE'
		order by et.hit_count desc, et.created_at asc
	`;

	for (const row of rows) {
		const slotMap = parseSlotMap(row.slot_map);
		if (!slotMap) {
			console.warn('EMAIL_TEMPLATE_INVALID_SLOT_MAP', {
				templateId: toNullableString(row.id, 'email_templates.id'),
			});
			continue;
		}
		const template: EmailTemplate = {
			id: toRequiredString(row.id, 'email_templates.id') as UUID,
			from_address: toRequiredString(row.from_address, 'email_templates.from_address'),
			skeleton: toRequiredString(row.skeleton, 'email_templates.skeleton'),
			slot_map: slotMap,
			hit_count: toSafeInteger(row.hit_count, 'email_templates.hit_count'),
			failure_count: toSafeInteger(row.failure_count, 'email_templates.failure_count'),
		};
		const list = bySender.get(template.from_address) ?? [];
		list.push(template);
		bySender.set(template.from_address, list);
	}

	return bySender;
}

export interface SaveTemplateParams {
	from_address: string;
	skeleton: string;
	slot_map: TemplateSlotMap;
	learned_from_raw_email_id: UUID | null;
}

interface InsertedTemplateRowRaw {
	id: unknown;
}

// Insert-or-skip on (from_address, skeleton_hash); concurrent learners of
// the same format collapse into one row. After a successful insert the
// sender's template set is pruned to the freshest MAX_TEMPLATES_PER_SENDER
// so a noisy sender can't grow the table unboundedly.
export async function saveTemplate(
	sql: SqlClient,
	params: SaveTemplateParams,
): Promise<boolean> {
	const hash = skeletonHash(params.skeleton);
	const slotMapJson = JSON.stringify(params.slot_map);

	const inserted = await sql<InsertedTemplateRowRaw[]>`
		insert into public.email_templates (
			from_address, skeleton_hash, skeleton, slot_map, learned_from_raw_email_id
		)
		values (
			${params.from_address}, ${hash}, ${params.skeleton},
			${slotMapJson}::jsonb, ${params.learned_from_raw_email_id}
		)
		on conflict (from_address, skeleton_hash) do nothing
		returning id
	`;

	if (inserted.length === 0) {
		return false;
	}

	await sql`
		delete from public.email_templates as et
		where et.from_address = ${params.from_address}
			and et.id in (
				select stale.id
				from public.email_templates as stale
				where stale.from_address = ${params.from_address}
				order by coalesce(stale.last_hit_at, stale.created_at) desc, stale.created_at desc
				offset ${MAX_TEMPLATES_PER_SENDER}
			)
	`;

	return true;
}

export async function recordTemplateHit(sql: SqlClient, templateId: UUID): Promise<void> {
	await sql`
		update public.email_templates as et
		set hit_count = et.hit_count + 1,
			failure_count = 0,
			last_hit_at = now(),
			updated_at = now()
		where et.id = ${templateId}
	`;
}

// A matched_invalid apply is a real defect in the template (the format
// matched but the values were garbage), so repeated failures disable it —
// the affected emails just fall through to the AI path.
export async function recordTemplateFailure(sql: SqlClient, templateId: UUID): Promise<void> {
	await sql`
		update public.email_templates as et
		set failure_count = et.failure_count + 1,
			status = case
				when et.failure_count + 1 >= ${TEMPLATE_DISABLE_FAILURE_COUNT} then 'DISABLED'
				else et.status
			end,
			updated_at = now()
		where et.id = ${templateId}
	`;
}
