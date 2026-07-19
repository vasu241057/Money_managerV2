// Statement-field resolution for the AI normalization path.
//
// The model flags a credit-card statement email and answers with token
// references (masked mode); every value is resolved locally from the token
// map — digits never round-trip through the provider. All parsing failures
// degrade field-by-field to null (the card surface shows nulls as "enter
// manually"), never to a guess.

import { parseAmountTokenToPaise } from './identity';
import { parseDateTextToIso, resolveTokenValue, unmaskText } from './masking';

export interface ResolvedCardStatement {
	card_last4: string | null;
	total_due_in_paise: number | null;
	min_due_in_paise: number | null;
	due_date: string | null; // YYYY-MM-DD
	period_start: string | null;
	period_end: string | null;
	confidence: number;
}

const DAY_MS = 24 * 60 * 60 * 1000;

// The date parser may construct its instant in the runtime's local zone
// (UTC on Workers, anything locally). Formatting in IST recovers the
// calendar date the bank printed in both cases — statement dates are
// always Indian banking dates.
export function toDateOnly(iso: string | null): string | null {
	if (!iso) return null;
	const ms = Date.parse(iso);
	if (!Number.isFinite(ms)) return null;
	return new Date(ms).toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
}

// Parses "13/06/2026 To 12/07/2026", "15 Jun, 2026 - 14 Jul, 2026",
// "26-May-2026 to 25-Jun-2026". Split on the word "to" first; a spaced
// dash only as fallback so dd-Mon-yyyy dates never split internally.
export function parsePeriod(
	text: string,
	referenceIso: string,
): { start: string | null; end: string | null } {
	const none = { start: null, end: null };
	let halves = text.split(/\s+to\s+/i);
	if (halves.length !== 2) {
		halves = text.split(/\s+[-–—]\s+/);
	}
	if (halves.length !== 2 || !halves[0] || !halves[1]) {
		return none;
	}
	const start = toDateOnly(parseDateTextToIso(halves[0].trim(), referenceIso));
	const end = toDateOnly(parseDateTextToIso(halves[1].trim(), referenceIso));
	if (!start || !end || start >= end) {
		return none;
	}
	return { start, end };
}

// Field-level sanity: values that contradict the email's own date are
// dropped (null → manual entry), never silently kept.
export function validateResolvedStatement(
	resolved: ResolvedCardStatement,
	internalDateIso: string,
): ResolvedCardStatement {
	const emailMs = Date.parse(internalDateIso);

	if (
		resolved.min_due_in_paise !== null &&
		resolved.total_due_in_paise !== null &&
		resolved.min_due_in_paise > resolved.total_due_in_paise
	) {
		resolved.min_due_in_paise = null;
	}

	if (resolved.due_date !== null && Number.isFinite(emailMs)) {
		const dueMs = Date.parse(resolved.due_date);
		// A due date sits between the statement email and ~90 days out.
		if (!Number.isFinite(dueMs) || dueMs < emailMs - 5 * DAY_MS || dueMs > emailMs + 90 * DAY_MS) {
			resolved.due_date = null;
		}
	}

	if (resolved.period_end !== null && Number.isFinite(emailMs)) {
		const endMs = Date.parse(resolved.period_end);
		// The cycle closed at or shortly before the statement email.
		if (!Number.isFinite(endMs) || endMs > emailMs + 5 * DAY_MS || endMs < emailMs - 60 * DAY_MS) {
			resolved.period_start = null;
			resolved.period_end = null;
		}
	}

	return resolved;
}

// Masked mode: `raw` is the model's statement object with token references.
// Returns null when the answer is not a usable statement (no field resolved).
export function resolveMaskedStatement(
	raw: unknown,
	tokens: Map<string, string>,
	internalDateIso: string,
	minConfidence: number,
): ResolvedCardStatement | null {
	if (!raw || typeof raw !== 'object') return null;
	const record = raw as Record<string, unknown>;

	const confidence = Number(record.confidence);
	if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) return null;
	if (confidence < minConfidence) return null;

	const last4Text = resolveTokenValue(record.card_last4_token as string | undefined, tokens);
	const cardLast4 = last4Text ? last4Text.replace(/\D/g, '').slice(-4) || null : null;

	const totalText = resolveTokenValue(record.total_due_token as string | undefined, tokens);
	const totalDue = totalText ? parseAmountTokenToPaise(totalText) : null;

	const minText = resolveTokenValue(record.min_due_token as string | undefined, tokens);
	const minDue = minText ? parseAmountTokenToPaise(minText) : null;

	// Due date and period parse WITHOUT an internal-date fallback: a bill's
	// due date defaulting to the email date would be confidently wrong.
	const dueDateText = unmaskText(
		typeof record.due_date_text === 'string' ? record.due_date_text : null,
		tokens,
	);
	const dueDate = dueDateText ? toDateOnly(parseDateTextToIso(dueDateText, internalDateIso)) : null;

	const periodText = unmaskText(
		typeof record.period_text === 'string' ? record.period_text : null,
		tokens,
	);
	const period = periodText
		? parsePeriod(periodText, internalDateIso)
		: { start: null, end: null };

	const resolved = validateResolvedStatement(
		{
			card_last4: cardLast4 && cardLast4.length === 4 ? cardLast4 : null,
			total_due_in_paise: totalDue,
			min_due_in_paise: minDue,
			due_date: dueDate,
			period_start: period.start,
			period_end: period.end,
			confidence,
		},
		internalDateIso,
	);

	const hasAnyField =
		resolved.total_due_in_paise !== null ||
		resolved.min_due_in_paise !== null ||
		resolved.due_date !== null ||
		resolved.period_end !== null;
	return hasAnyField ? resolved : null;
}
