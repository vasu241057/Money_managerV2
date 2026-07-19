// Statement-PDF field extraction — pure code, no AI, nothing persisted.
//
// A PDF is positioned text runs, not lines: the raw stream interleaves
// columns out of visual order (HDFC's boxed layout scrambles badly), so
// naive word matching breaks. Instead every run carries its x/y; a field's
// value is the nearest plausible run to the RIGHT of its label (same
// visual row), else the nearest run directly BELOW it (HDFC prints labels
// above values). All parsing reuses the engine's battle-tested amount and
// date parsers; contradictory values are nulled by the shared validator.

import { parseAmountTokenToPaise } from './identity';
import { parseDateTextToIso } from './masking';
import {
	parsePeriod,
	toDateOnly,
	validateResolvedStatement,
	type ResolvedCardStatement,
} from './statements';

export interface PdfTextItem {
	text: string;
	x: number;
	y: number;
	page: number;
}

export interface PdfStatementFields {
	card_last4: string | null;
	total_due_in_paise: number | null;
	min_due_in_paise: number | null;
	due_date: string | null;
	period_start: string | null;
	period_end: string | null;
}

const LABELS = {
	// "Total Amount Due", "Total Amount Due (TAD)", "TOTAL AMOUNT DUE"
	total: /total\s+amount\s+due/i,
	// "Minimum Amount Due", "Minimum Amount Due (MAD)", "MINIMUM DUE", "Min Due"
	min: /min(?:imum)?\s+(?:amount\s+)?due/i,
	// "Due Date", "Payment Due Date", Slice's "Due on 5 May"
	due: /(?:payment\s+)?due\s+date|due\s+on/i,
	// "Statement Period", "Billing Period", "Transactions Details from ..."
	period: /statement\s+period|billing\s+period|transactions?\s+details\s+from/i,
} as const;

// "Details of Total Amount Due" (HDFC/Kotak breakup tables) must not anchor
// the total field.
const LABEL_EXCLUDE = /details\s+of/i;

// A decimal amount anywhere, or a currency-marked whole amount (Slice
// prints "₹1,999" with the paise in a separate run) — never a bare integer,
// which would swallow dates and counts.
const AMOUNT_VALUE_REGEX =
	/(?:rs\.?|inr|₹)?\s*([\d,]+\.\d{2})(?!\d)|(?:rs\.?|inr|₹)\s*([\d,]+)(?!\d)/i;
const SAME_ROW_Y_TOLERANCE = 4;
const BELOW_MAX_DY = 40;
const BELOW_X_TOLERANCE = 80;

function parseAmountFrom(text: string): number | null {
	const match = AMOUNT_VALUE_REGEX.exec(text);
	const token = match?.[1] ?? match?.[2];
	return token ? parseAmountTokenToPaise(token) : null;
}

// Bank PDFs frequently print dates without a year ("Due on 5 May",
// "21 MAR - 20 APR"): retry with candidate years and let the plausibility
// validator reject wrong picks.
function parseDateWithYearFallback(
	text: string,
	referenceIso: string,
	candidateYears: number[],
): string | null {
	const direct = toDateOnly(parseDateTextToIso(text, referenceIso));
	if (direct) return direct;
	for (const year of candidateYears) {
		const withYear = toDateOnly(parseDateTextToIso(`${text} ${year}`, referenceIso));
		if (withYear) return withYear;
	}
	return null;
}

function parseDateFrom(text: string, referenceIso: string): string | null {
	// Strip label remnants ("Due Date: 13-Jul-2026" arrives as one run
	// sometimes) before parsing.
	const cleaned = text.replace(LABELS.due, '').replace(/^[:\s-]+/, '').trim();
	if (!cleaned) return null;
	const referenceYear = new Date(referenceIso).getUTCFullYear();
	// A due date sits at or after the statement email — same year, or the
	// next one across a December statement.
	return parseDateWithYearFallback(cleaned, referenceIso, [referenceYear, referenceYear + 1]);
}

// Label-less yearless period runs ("21 MAR - 20 APR" on Slice). The end
// half anchors the year (cycle closed near the statement email); the start
// half takes the end's year, stepping one back across a year boundary.
const BARE_PERIOD_REGEX =
	/^\s*(\d{1,2}\s+[A-Za-z]{3,9})\.?\s*[-–—]\s*(\d{1,2}\s+[A-Za-z]{3,9})\s*$/;

function parseBarePeriod(
	text: string,
	referenceIso: string,
): { start: string | null; end: string | null } {
	const none = { start: null, end: null };
	const match = BARE_PERIOD_REGEX.exec(text);
	if (!match || !match[1] || !match[2]) return none;
	const referenceYear = new Date(referenceIso).getUTCFullYear();

	const end = parseDateWithYearFallback(match[2], referenceIso, [
		referenceYear,
		referenceYear - 1,
	]);
	if (!end) return none;
	const endYear = Number.parseInt(end.slice(0, 4), 10);

	for (const year of [endYear, endYear - 1]) {
		const start = parseDateWithYearFallback(match[1], referenceIso, [year]);
		if (start && start < end) return { start, end };
	}
	return none;
}

// Candidate ranking: same visual row to the right first (nearest wins),
// then directly below within a column tolerance. PDF y grows upward, so
// "below" means smaller y.
function findValueNear(
	items: PdfTextItem[],
	label: PdfTextItem,
	accepts: (text: string) => boolean,
): PdfTextItem | null {
	let best: PdfTextItem | null = null;
	let bestScore = Number.POSITIVE_INFINITY;
	for (const item of items) {
		if (item.page !== label.page || item === label) continue;
		if (!accepts(item.text)) continue;

		const dy = label.y - item.y;
		let score: number | null = null;
		if (Math.abs(dy) <= SAME_ROW_Y_TOLERANCE && item.x > label.x) {
			score = item.x - label.x;
		} else if (
			dy > SAME_ROW_Y_TOLERANCE &&
			dy <= BELOW_MAX_DY &&
			Math.abs(item.x - label.x) <= BELOW_X_TOLERANCE
		) {
			// Below ranks after any same-row hit.
			score = 10_000 + dy * 100 + Math.abs(item.x - label.x);
		}
		if (score !== null && score < bestScore) {
			bestScore = score;
			best = item;
		}
	}
	return best;
}

function findLabel(items: PdfTextItem[], pattern: RegExp): PdfTextItem | null {
	// First page wins; banks print the summary box on page 1.
	let best: PdfTextItem | null = null;
	for (const item of items) {
		if (!pattern.test(item.text) || LABEL_EXCLUDE.test(item.text)) continue;
		if (!best || item.page < best.page || (item.page === best.page && item.y > best.y)) {
			best = item;
		}
	}
	return best;
}

// The card number in a statement is printed X-masked ("3561XXXXXXXX6279",
// "00361010XXXX9732", Kotak's terse "Zen Credit Card X6331"). A long mask
// is unambiguous anywhere; a short mask (1-3 X) only counts on a line that
// talks about a card/statement, so "Box 123"-style text can't false-match.
function findCardLast4(items: PdfTextItem[]): string | null {
	for (const item of items) {
		const compact = item.text.replace(/\s/g, '');
		const strong = /\d*[X*]{4,}(\d{3,4})(?!\d)/i.exec(compact);
		if (strong?.[1]) {
			return strong[1].slice(-4).padStart(4, '0');
		}
		if (/card|statement/i.test(item.text)) {
			const loose = /[X*]+(\d{4})(?!\d)/i.exec(compact);
			if (loose?.[1]) {
				return loose[1];
			}
		}
	}
	return null;
}

export function extractStatementFieldsFromItems(
	items: PdfTextItem[],
	referenceIso: string,
): PdfStatementFields {
	const empty: PdfStatementFields = {
		card_last4: null,
		total_due_in_paise: null,
		min_due_in_paise: null,
		due_date: null,
		period_start: null,
		period_end: null,
	};
	if (items.length === 0) return empty;

	const fields = { ...empty };
	fields.card_last4 = findCardLast4(items);

	const totalLabel = findLabel(items, LABELS.total);
	if (totalLabel) {
		// The value may live inside the label's own run ("Total Amount Due: Rs. 34,073.18").
		fields.total_due_in_paise = parseAmountFrom(totalLabel.text);
		if (fields.total_due_in_paise === null) {
			const value = findValueNear(items, totalLabel, text => parseAmountFrom(text) !== null);
			fields.total_due_in_paise = value ? parseAmountFrom(value.text) : null;
		}
	}

	const minLabel = findLabel(items, LABELS.min);
	if (minLabel) {
		fields.min_due_in_paise = parseAmountFrom(minLabel.text);
		if (fields.min_due_in_paise === null) {
			const value = findValueNear(items, minLabel, text => parseAmountFrom(text) !== null);
			fields.min_due_in_paise = value ? parseAmountFrom(value.text) : null;
		}
	}

	const dueLabel = findLabel(items, LABELS.due);
	if (dueLabel) {
		fields.due_date = parseDateFrom(dueLabel.text, referenceIso);
		if (fields.due_date === null) {
			const value = findValueNear(
				items,
				dueLabel,
				text => parseDateFrom(text, referenceIso) !== null,
			);
			fields.due_date = value ? parseDateFrom(value.text, referenceIso) : null;
		}
	}

	const periodLabel = findLabel(items, LABELS.period);
	if (periodLabel) {
		const tryPeriod = (text: string): { start: string | null; end: string | null } =>
			parsePeriod(text.replace(LABELS.period, '').replace(/^[:\s]+/, '').trim(), referenceIso);
		let period = tryPeriod(periodLabel.text);
		if (!period.end) {
			const value = findValueNear(items, periodLabel, text => tryPeriod(text).end !== null);
			if (value) period = tryPeriod(value.text);
		}
		fields.period_start = period.start;
		fields.period_end = period.end;
	}
	if (fields.period_end === null) {
		// No labeled period anywhere (Slice prints the bare cycle on the card
		// art): accept a standalone day-month range run.
		for (const item of items) {
			const period = parseBarePeriod(item.text, referenceIso);
			if (period.end) {
				fields.period_start = period.start;
				fields.period_end = period.end;
				break;
			}
		}
	}

	return fields;
}

// Applies the shared statement validator to PDF-extracted fields so both
// sources obey identical sanity rules.
export function validatePdfFields(
	fields: PdfStatementFields,
	internalDateIso: string,
): PdfStatementFields {
	const validated: ResolvedCardStatement = validateResolvedStatement(
		{ ...fields, confidence: 1 },
		internalDateIso,
	);
	return {
		card_last4: validated.card_last4,
		total_due_in_paise: validated.total_due_in_paise,
		min_due_in_paise: validated.min_due_in_paise,
		due_date: validated.due_date,
		period_start: validated.period_start,
		period_end: validated.period_end,
	};
}

// ---- Gmail attachment plumbing (pure helpers, testable) ----

interface GmailMessagePartBody {
	attachmentId?: string;
	data?: string;
	size?: number;
}

export interface GmailMessagePart {
	mimeType?: string;
	filename?: string;
	body?: GmailMessagePartBody;
	parts?: GmailMessagePart[];
}

// Walks the MIME tree for the statement PDF; largest PDF wins (marketing
// images and inline logos are also attachments).
export function pickPdfAttachment(
	root: GmailMessagePart | undefined,
): { attachmentId: string; filename: string } | null {
	const candidates: { attachmentId: string; filename: string; size: number }[] = [];
	const walk = (part: GmailMessagePart | undefined): void => {
		if (!part) return;
		const isPdf =
			part.mimeType === 'application/pdf' ||
			/\.pdf$/i.test(part.filename ?? '');
		const attachmentId = part.body?.attachmentId;
		if (isPdf && attachmentId) {
			candidates.push({
				attachmentId,
				filename: part.filename ?? '',
				size: part.body?.size ?? 0,
			});
		}
		for (const child of part.parts ?? []) walk(child);
	};
	walk(root);
	if (candidates.length === 0) return null;
	candidates.sort((a, b) => b.size - a.size);
	const best = candidates[0] as (typeof candidates)[number];
	return { attachmentId: best.attachmentId, filename: best.filename };
}

export function decodeBase64Url(data: string): Uint8Array {
	const base64 = data.replace(/-/g, '+').replace(/_/g, '/');
	const padded = base64 + '='.repeat((4 - (base64.length % 4)) % 4);
	const binary = atob(padded);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) {
		bytes[i] = binary.charCodeAt(i);
	}
	return bytes;
}
