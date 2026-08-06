export type LedgerDateFilterMode = 'single' | 'range';

export interface LedgerDateQuery {
	from?: string;
	to?: string;
}

function parseLocalDateInput(value: string): Date | null {
	const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
	if (!match) return null;

	const year = Number(match[1]);
	const month = Number(match[2]) - 1;
	const day = Number(match[3]);
	const date = new Date(year, month, day);
	if (
		date.getFullYear() !== year
		|| date.getMonth() !== month
		|| date.getDate() !== day
	) {
		return null;
	}
	return date;
}

export function toLocalDateInputValue(date: Date): string {
	const year = date.getFullYear();
	const month = String(date.getMonth() + 1).padStart(2, '0');
	const day = String(date.getDate()).padStart(2, '0');
	return `${year}-${month}-${day}`;
}

/**
 * Calendar bounds of a `YYYY-MM` month as date-input values, so a deep link
 * from the Budget screen can scope the ledger to the month being reviewed.
 */
export function monthDateInputBounds(month: string): { from: string; to: string } | null {
	const match = /^(\d{4})-(0[1-9]|1[0-2])$/.exec(month);
	if (!match) return null;

	const year = Number(match[1]);
	const monthIndex = Number(match[2]) - 1;
	return {
		from: toLocalDateInputValue(new Date(year, monthIndex, 1)),
		to: toLocalDateInputValue(new Date(year, monthIndex + 1, 0)),
	};
}

export function buildLedgerDateQuery(
	mode: LedgerDateFilterMode,
	fromValue: string,
	toValue: string,
): LedgerDateQuery {
	const fromDate = parseLocalDateInput(fromValue);
	if (mode === 'single') {
		if (!fromDate) return {};
		const from = new Date(fromDate);
		const to = new Date(fromDate);
		from.setHours(0, 0, 0, 0);
		to.setHours(23, 59, 59, 999);
		return { from: from.toISOString(), to: to.toISOString() };
	}

	const toDate = parseLocalDateInput(toValue);
	if (!fromDate || !toDate) return {};
	const [first, last] = fromDate.getTime() <= toDate.getTime()
		? [fromDate, toDate]
		: [toDate, fromDate];
	const from = new Date(first);
	const to = new Date(last);
	from.setHours(0, 0, 0, 0);
	to.setHours(23, 59, 59, 999);
	return { from: from.toISOString(), to: to.toISOString() };
}

function formatDate(date: Date, includeYear: boolean): string {
	return new Intl.DateTimeFormat('en-IN', {
		day: 'numeric',
		month: 'short',
		year: includeYear ? 'numeric' : undefined,
	}).format(date);
}

export function formatLedgerDateFilterLabel(
	mode: LedgerDateFilterMode,
	fromValue: string,
	toValue: string,
): string | null {
	const from = parseLocalDateInput(fromValue);
	if (!from) return null;
	if (mode === 'single') return formatDate(from, true);

	const to = parseLocalDateInput(toValue);
	if (!to) return null;
	const [first, last] = from.getTime() <= to.getTime() ? [from, to] : [to, from];
	if (first.getTime() === last.getTime()) return formatDate(first, true);

	const sameYear = first.getFullYear() === last.getFullYear();
	const sameMonth = sameYear && first.getMonth() === last.getMonth();
	if (sameMonth) {
		return `${first.getDate()}–${formatDate(last, true)}`;
	}
	return `${formatDate(first, !sameYear)}–${formatDate(last, true)}`;
}
