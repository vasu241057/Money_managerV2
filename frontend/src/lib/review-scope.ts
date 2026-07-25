const IST_CALENDAR_DATE = new Intl.DateTimeFormat('en-GB', {
	timeZone: 'Asia/Kolkata',
	year: 'numeric',
	month: '2-digit',
	day: '2-digit',
});

function istCalendarDayNumber(date: Date): number {
	const parts = IST_CALENDAR_DATE.formatToParts(date);
	const year = Number(parts.find((part) => part.type === 'year')?.value);
	const month = Number(parts.find((part) => part.type === 'month')?.value);
	const day = Number(parts.find((part) => part.type === 'day')?.value);
	return Math.floor(Date.UTC(year, month - 1, day) / 86_400_000);
}

// Mirrors the analytics summary query: today and the previous two calendar
// dates in India, rather than a rolling 72-hour window.
export function isInLastThreeCalendarDaysIST(date: Date, now = new Date()): boolean {
	return istCalendarDayNumber(date) >= istCalendarDayNumber(now) - 2;
}
