// Cross-email duplicate detection.
//
// One payment often produces two alerts (bank debit email + payment-app
// email, or SMS-gateway copies). Before auto-verifying a transaction, check
// whether another raw email already produced an ACTIVE financial event with
// the same user, amount, and direction inside the time window — if so, the
// new transaction is forced into NEEDS_REVIEW instead of silently
// double-counting spend.

import type { SqlClient } from './db/client';

const DEFAULT_DEDUP_WINDOW_MINUTES = 10;

// 0 disables the check.
export function resolveDedupWindowMinutes(env: Env): number {
	const raw = (env as unknown as Record<string, unknown>).TXN_DEDUP_WINDOW_MINUTES;
	if (typeof raw !== 'string' || raw.trim().length === 0) {
		return DEFAULT_DEDUP_WINDOW_MINUTES;
	}
	const parsed = Number.parseInt(raw.trim(), 10);
	if (!Number.isFinite(parsed) || parsed < 0) {
		return DEFAULT_DEDUP_WINDOW_MINUTES;
	}
	return parsed;
}

export interface DuplicateEventProbe {
	user_id: string;
	raw_email_id: string;
	amount_in_paise: number;
	direction: 'debit' | 'credit';
	txn_timestamp: string;
	window_minutes: number;
}

export async function hasLikelyDuplicateEvent(
	sql: SqlClient,
	probe: DuplicateEventProbe,
): Promise<boolean> {
	const rows = await sql<Array<{ id: unknown }>>`
		select fe.id
		from public.financial_events as fe
		where fe.user_id = ${probe.user_id}
			and fe.amount_in_paise = ${probe.amount_in_paise}
			and fe.direction = ${probe.direction}
			and fe.status = 'ACTIVE'
			and fe.raw_email_id is distinct from ${probe.raw_email_id}
			and fe.txn_timestamp between
				${probe.txn_timestamp}::timestamptz - (${probe.window_minutes} || ' minutes')::interval
				and ${probe.txn_timestamp}::timestamptz + (${probe.window_minutes} || ' minutes')::interval
		limit 1
	`;
	return rows.length > 0;
}
