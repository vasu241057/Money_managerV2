import type { EffectiveBudgetTreatment, EventDirection } from '../../../shared/types';

export interface RecurringOccurrence {
	id: string;
	amountInPaise: number;
	date: string;
	direction: EventDirection;
	merchantId: string | null;
	searchKey: string | null;
	categoryId: string | null;
	label: string;
	categoryTreatment: EffectiveBudgetTreatment | null;
}

export interface DetectedRecurringPattern {
	fingerprint: string;
	label: string;
	occurrences: RecurringOccurrence[];
	cadence: 'WEEKLY' | 'MONTHLY';
	expectedDay: number;
	amountMinInPaise: number;
	amountMaxInPaise: number;
	confidence: number;
	suggestedTreatment: EffectiveBudgetTreatment;
}

function median(values: number[]): number {
	const sorted = [...values].sort((left, right) => left - right);
	return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

function dayGap(left: string, right: string): number {
	return Math.round((new Date(right).getTime() - new Date(left).getTime()) / 86_400_000);
}

function calendarPartsIST(value: string): { dayOfMonth: number; weekDay: number } {
	const date = new Date(value);
	const dayOfMonth = Number(new Intl.DateTimeFormat('en-CA', {
		timeZone: 'Asia/Kolkata',
		day: 'numeric',
	}).format(date));
	const shortWeekDay = new Intl.DateTimeFormat('en-US', {
		timeZone: 'Asia/Kolkata',
		weekday: 'short',
	}).format(date);
	const weekDay = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].indexOf(shortWeekDay);
	if (!Number.isInteger(dayOfMonth) || dayOfMonth < 1 || weekDay < 0) {
		throw new Error('Could not resolve recurring calendar day in Asia/Kolkata');
	}
	return { dayOfMonth, weekDay };
}

function detectGroup(occurrences: RecurringOccurrence[]): DetectedRecurringPattern | null {
	if (occurrences.length < 3) return null;
	const sorted = [...occurrences].sort((left, right) =>
		new Date(left.date).getTime() - new Date(right.date).getTime() || left.id.localeCompare(right.id),
	);
	const gaps = sorted.slice(1).map((item, index) => dayGap(sorted[index]?.date ?? item.date, item.date));
	const weekly = gaps.every(gap => gap >= 5 && gap <= 9);
	const monthly = gaps.every(gap => gap >= 24 && gap <= 38);
	if (!weekly && !monthly) return null;

	const amounts = sorted.map(item => item.amountInPaise);
	const amountMin = Math.min(...amounts);
	const amountMax = Math.max(...amounts);
	if (amountMax / amountMin > 1.15) return null;

	const cadence = weekly ? 'WEEKLY' : 'MONTHLY';
	const dates = sorted.map(item => calendarPartsIST(item.date));
	const expectedDay = cadence === 'WEEKLY'
		? Math.round(median(dates.map(date => date.weekDay)))
		: Math.round(median(dates.map(date => date.dayOfMonth)));
	const identity = sorted[0]?.merchantId ?? sorted[0]?.searchKey;
	if (!identity) return null;
	const categoryId = sorted[0]?.categoryId ?? 'uncategorized';
	const fingerprint = `recurring:${sorted[0]?.direction}:${identity}:${categoryId}:${cadence}`;
	const averageGap = gaps.reduce((total, gap) => total + gap, 0) / gaps.length;
	const cadenceTarget = cadence === 'WEEKLY' ? 7 : 30;
	const cadenceConsistency = Math.max(0, 1 - Math.abs(averageGap - cadenceTarget) / cadenceTarget);
	const amountConsistency = amountMin === amountMax ? 1 : 1 - (amountMax - amountMin) / amountMax;
	const confidence = Math.min(
		0.99,
		0.76 + Math.min(0.09, sorted.length * 0.02) + cadenceConsistency * 0.07 + amountConsistency * 0.07,
	);

	return {
		fingerprint,
		label: sorted[0]?.label ?? 'Recurring transaction',
		occurrences: sorted,
		cadence,
		expectedDay,
		amountMinInPaise: Math.floor(amountMin * 0.95),
		amountMaxInPaise: Math.ceil(amountMax * 1.05),
		confidence,
		suggestedTreatment: sorted[0]?.categoryTreatment ?? 'INCLUDE',
	};
}

export function detectRecurringPatterns(
	occurrences: RecurringOccurrence[],
): DetectedRecurringPattern[] {
	const groups = new Map<string, RecurringOccurrence[]>();
	for (const occurrence of occurrences) {
		const identity = occurrence.merchantId ?? occurrence.searchKey;
		if (!identity) continue;
		const key = `${occurrence.direction}:${identity}:${occurrence.categoryId ?? 'uncategorized'}`;
		groups.set(key, [...(groups.get(key) ?? []), occurrence]);
	}

	return [...groups.values()]
		.map(detectGroup)
		.filter((pattern): pattern is DetectedRecurringPattern => pattern !== null)
		.sort((left, right) => right.confidence - left.confidence || left.fingerprint.localeCompare(right.fingerprint));
}
