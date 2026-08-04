// Cross-email duplicate detection.
//
// One payment often produces two alerts (bank debit email + payment-app
// receipt). Ordinary duplicates are forced into review. A narrower transfer
// shape, where exactly one copy is linked to the user's account, keeps that
// account-linked copy and reverses/skips only the weaker receipt projection.

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

export interface LikelyDuplicateTransfer {
	transaction_id: string;
	financial_event_id: string;
	account_id: string | null;
}

/**
 * Combines only the narrow bank-alert + payment-receipt shape: both rows are
 * transfer suggestions with the same amount/direction/time, and exactly one
 * side is linked to an account. Two independently account-linked movements
 * are never combined.
 */
export async function findLikelyDuplicateTransfer(
	sql: SqlClient,
	probe: DuplicateEventProbe & { account_id: string | null },
): Promise<LikelyDuplicateTransfer | null> {
	const rows = await sql<Array<{
		transaction_id: unknown;
		financial_event_id: unknown;
		account_id: unknown;
	}>>`
		select
			t.id as transaction_id,
			fe.id as financial_event_id,
			t.account_id
		from public.financial_events as fe
		join public.transactions as t
			on t.financial_event_id = fe.id
			and t.user_id = fe.user_id
		where fe.user_id = ${probe.user_id}
			and fe.amount_in_paise = ${probe.amount_in_paise}
			and fe.direction = ${probe.direction}
			and fe.status = 'ACTIVE'
			and fe.raw_email_id is distinct from ${probe.raw_email_id}
			and t.type = 'transfer'
			and (
				(t.account_id is null and ${probe.account_id}::uuid is not null)
				or (t.account_id is not null and ${probe.account_id}::uuid is null)
			)
			and fe.txn_timestamp between
				${probe.txn_timestamp}::timestamptz - (${probe.window_minutes} || ' minutes')::interval
				and ${probe.txn_timestamp}::timestamptz + (${probe.window_minutes} || ' minutes')::interval
		order by abs(extract(epoch from (fe.txn_timestamp - ${probe.txn_timestamp}::timestamptz)))
		limit 1
	`;

	const row = rows[0];
	if (!row || typeof row.transaction_id !== 'string' || typeof row.financial_event_id !== 'string') {
		return null;
	}

	return {
		transaction_id: row.transaction_id,
		financial_event_id: row.financial_event_id,
		account_id: typeof row.account_id === 'string' ? row.account_id : null,
	};
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
