// Indexed-placeholder masking for the AI normalization path.
//
// Every numeric token in the email is replaced with <N1>, <N2>, ... before
// the text is sent to the model, so no digits (amounts, balances, account
// numbers, OTPs, phone numbers) ever reach the provider — and every
// placeholder has the same width, so magnitudes are hidden too. The model
// answers with token references; amounts, dates, and last4 are resolved
// locally from the token map.

export interface MaskedText {
	masked: string;
	// 'N1' -> original token text, e.g. '1,67,450.00'
	tokens: Map<string, string>;
	// First unused index; pass as startIndex for the next text in a batch so
	// placeholder ids stay globally unique across batched emails.
	nextIndex: number;
}

const NUMERIC_TOKEN_REGEX = /\d[\d,]*(?:\.\d+)?/g;
const TOKEN_REF_REGEX = /<(N\d+)>/g;

export function maskNumericTokens(text: string, startIndex = 1): MaskedText {
	const tokens = new Map<string, string>();
	let index = startIndex - 1;
	const masked = text.replace(NUMERIC_TOKEN_REGEX, match => {
		index += 1;
		const id = `N${index}`;
		tokens.set(id, match);
		return `<${id}>`;
	});
	return { masked, tokens, nextIndex: index + 1 };
}

// Restores original tokens inside a model-returned string (counterparties,
// date snippets). Unknown references are left as-is.
export function unmaskText(
	value: string | null | undefined,
	tokens: Map<string, string>,
): string | null {
	if (typeof value !== 'string' || value.length === 0) {
		return value ?? null;
	}
	return value.replace(TOKEN_REF_REGEX, (whole, id: string) => tokens.get(id) ?? whole);
}

// Accepts 'N1' or '<N1>' and returns the original token text, or null.
export function resolveTokenValue(
	ref: string | null | undefined,
	tokens: Map<string, string>,
): string | null {
	if (typeof ref !== 'string') {
		return null;
	}
	const normalized = ref.trim().replace(/^<|>$/g, '');
	return tokens.get(normalized) ?? null;
}

const MONTHS: Record<string, number> = {
	jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
	jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
};

function normalizeYear(raw: string): number {
	const year = Number.parseInt(raw, 10);
	return raw.length <= 2 ? 2000 + year : year;
}

function buildIsoFromParts(
	year: number,
	month: number,
	day: number,
	timeText: string,
): string | null {
	if (!Number.isInteger(year) || year < 2000 || year > 2100) return null;
	if (!Number.isInteger(month) || month < 1 || month > 12) return null;
	if (!Number.isInteger(day) || day < 1 || day > 31) return null;

	const timeMatch = timeText.match(
		/(?<![\d:])(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?)?/,
	);
	let hours = timeMatch ? Number.parseInt(timeMatch[1] ?? '0', 10) : 0;
	const minutes = timeMatch ? Number.parseInt(timeMatch[2] ?? '0', 10) : 0;
	const seconds = timeMatch?.[3] ? Number.parseInt(timeMatch[3], 10) : 0;
	const meridiem = timeMatch?.[4]?.[0];
	if (meridiem === 'p' && hours < 12) hours += 12;
	if (meridiem === 'a' && hours === 12) hours = 0;
	if (hours > 23 || minutes > 59 || seconds > 59) return null;

	const pad = (value: number, width = 2): string => String(value).padStart(width, '0');
	// Bank alerts carry IST wall-clock times.
	const candidate = new Date(
		`${year}-${pad(month)}-${pad(day)}T${pad(hours)}:${pad(minutes)}:${pad(seconds)}+05:30`,
	);
	if (Number.isNaN(candidate.getTime())) return null;
	return candidate.toISOString();
}

// A transaction alert lands within hours of the transaction, so the
// month-first reading is only trusted when it falls near the email's own
// arrival date — not merely "less far" than day-first, which would let a
// reference with no real signal (e.g. a statement referencing old dates)
// flip the convention on a hair.
const MDY_PLAUSIBILITY_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;

// A numeric date like 06-05-26 is ambiguous (June 5 vs May 6). Day-first
// (the Indian convention) wins unless the month-first reading is both
// plausibly near the reference date and closer than day-first.
function pickNumericReading(
	dayFirst: string | null,
	monthFirst: string | null,
	referenceIso: string | undefined,
): string | null {
	if (!dayFirst || !monthFirst) return dayFirst ?? monthFirst;
	if (referenceIso) {
		const reference = new Date(referenceIso).getTime();
		if (!Number.isNaN(reference)) {
			const dayFirstGap = Math.abs(new Date(dayFirst).getTime() - reference);
			const monthFirstGap = Math.abs(new Date(monthFirst).getTime() - reference);
			if (monthFirstGap <= MDY_PLAUSIBILITY_WINDOW_MS && monthFirstGap < dayFirstGap) {
				return monthFirst;
			}
		}
	}
	return dayFirst;
}

// Parses the date formats Indian banks use ("03-03-26", "18.05.2026",
// "04-Mar-2026", "18th May 2026", "Mar 04, 2026", "2026-03-04[T14:30]",
// each optionally with HH:mm[:ss] and AM/PM). referenceIso (the email's
// internal date) disambiguates numeric day/month order.
export function parseDateTextToIso(text: string, referenceIso?: string): string | null {
	const lower = text.toLowerCase();

	let match = lower.match(/\b(\d{4})-(\d{1,2})-(\d{1,2})(?!\d)/);
	if (match) {
		return buildIsoFromParts(
			Number.parseInt(match[1] ?? '', 10),
			Number.parseInt(match[2] ?? '', 10),
			Number.parseInt(match[3] ?? '', 10),
			lower,
		);
	}

	match = lower.match(/\b(\d{1,2})(?:st|nd|rd|th)?[-/ .]+([a-z]{3,9})[-/ ,.]+(\d{2,4})\b/);
	if (match) {
		const month = MONTHS[(match[2] ?? '').slice(0, 3)];
		if (month) {
			return buildIsoFromParts(
				normalizeYear(match[3] ?? ''),
				month,
				Number.parseInt(match[1] ?? '', 10),
				lower,
			);
		}
	}

	match = lower.match(/\b([a-z]{3,9})[ -](\d{1,2})(?:st|nd|rd|th)?,?[ -]+(\d{2,4})\b/);
	if (match) {
		const month = MONTHS[(match[1] ?? '').slice(0, 3)];
		if (month) {
			return buildIsoFromParts(
				normalizeYear(match[3] ?? ''),
				month,
				Number.parseInt(match[2] ?? '', 10),
				lower,
			);
		}
	}

	// Numeric DD-MM-YY(YY) or MM-DD-YY(YY); both readings considered.
	match = lower.match(/\b(\d{1,2})[-/.](\d{1,2})[-/.](\d{2,4})\b/);
	if (match) {
		const first = Number.parseInt(match[1] ?? '', 10);
		const second = Number.parseInt(match[2] ?? '', 10);
		const year = normalizeYear(match[3] ?? '');
		const dayFirst = buildIsoFromParts(year, second, first, lower);
		const monthFirst = first === second ? null : buildIsoFromParts(year, first, second, lower);
		return pickNumericReading(dayFirst, monthFirst, referenceIso);
	}

	return null;
}

// Unmasks a model-returned date snippet (e.g. "<N2>-<N3>-<N4>" or
// "Mar <N5>, <N6>") and parses it; falls back to the email's internal date.
export function resolveMaskedDateText(
	dateText: string | null | undefined,
	tokens: Map<string, string>,
	fallbackIso: string,
): string {
	const unmasked = unmaskText(dateText ?? null, tokens);
	if (!unmasked) {
		return fallbackIso;
	}
	return parseDateTextToIso(unmasked, fallbackIso) ?? fallbackIso;
}
