import type { CardStatementRow, UUID } from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableString,
	toRequiredString,
} from '../lib/db/serialization';
import { badRequest, notFound } from '../lib/http/errors';
import { asRecord, parseUuid } from '../lib/http/validation';
import type { ResolvedCardStatement } from '../lib/statements';

interface CardCandidate {
	id: string;
	last4: string;
}

interface CardStatementRowRaw {
	id: unknown;
	user_id: unknown;
	credit_card_id: unknown;
	card_last4: unknown;
	period_start: unknown;
	period_end: unknown;
	statement_date: unknown;
	total_due_in_paise: unknown;
	min_due_in_paise: unknown;
	due_date: unknown;
	source: unknown;
	source_raw_email_id: unknown;
	created_at: unknown;
	updated_at: unknown;
}

function toDateOnlyOrNull(value: unknown): string | null {
	if (typeof value === 'string' && value.length >= 10) return value.slice(0, 10);
	if (value instanceof Date) return value.toISOString().slice(0, 10);
	return null;
}

function toPaiseOrNull(value: unknown): number | null {
	if (value === null || value === undefined) return null;
	const parsed = Number(value);
	return Number.isSafeInteger(parsed) ? parsed : null;
}

function mapCardStatementRow(row: CardStatementRowRaw): CardStatementRow {
	return {
		id: toRequiredString(row.id, 'card_statements.id'),
		user_id: toRequiredString(row.user_id, 'card_statements.user_id'),
		credit_card_id: toNullableString(row.credit_card_id, 'card_statements.credit_card_id'),
		card_last4: toNullableString(row.card_last4, 'card_statements.card_last4'),
		period_start: toDateOnlyOrNull(row.period_start),
		period_end: toDateOnlyOrNull(row.period_end),
		statement_date: toDateOnlyOrNull(row.statement_date),
		total_due_in_paise: toPaiseOrNull(row.total_due_in_paise),
		min_due_in_paise: toPaiseOrNull(row.min_due_in_paise),
		due_date: toDateOnlyOrNull(row.due_date),
		source: toRequiredString(row.source, 'card_statements.source') as CardStatementRow['source'],
		source_raw_email_id: toNullableString(
			row.source_raw_email_id,
			'card_statements.source_raw_email_id',
		),
		created_at: toIsoDateTime(row.created_at, 'card_statements.created_at'),
		updated_at: toIsoDateTime(row.updated_at, 'card_statements.updated_at'),
	};
}

export interface UpsertCardStatementInput {
	credit_card_id: UUID;
	total_due_in_paise: number | null;
	min_due_in_paise: number | null;
	due_date: string | null;
	period_start: string | null;
	period_end: string | null;
}

function parseOptionalPaise(value: unknown, fieldName: string): number | null {
	if (value === undefined || value === null) return null;
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed) || parsed < 0) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a non-negative integer (paise)`);
	}
	return parsed;
}

function parseOptionalDateOnly(value: unknown, fieldName: string): string | null {
	if (value === undefined || value === null) return null;
	if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a YYYY-MM-DD date`);
	}
	return value;
}

export function parseUpsertCardStatementRequest(payload: unknown): UpsertCardStatementInput {
	const body = asRecord(payload);
	const input: UpsertCardStatementInput = {
		credit_card_id: parseUuid(body.credit_card_id, 'credit_card_id'),
		total_due_in_paise: parseOptionalPaise(body.total_due_in_paise, 'total_due_in_paise'),
		min_due_in_paise: parseOptionalPaise(body.min_due_in_paise, 'min_due_in_paise'),
		due_date: parseOptionalDateOnly(body.due_date, 'due_date'),
		period_start: parseOptionalDateOnly(body.period_start, 'period_start'),
		period_end: parseOptionalDateOnly(body.period_end, 'period_end'),
	};
	if (
		input.total_due_in_paise === null &&
		input.min_due_in_paise === null &&
		input.due_date === null &&
		input.period_end === null
	) {
		throw badRequest('INVALID_PAYLOAD', 'At least one statement field must be provided');
	}
	if (
		input.min_due_in_paise !== null &&
		input.total_due_in_paise !== null &&
		input.min_due_in_paise > input.total_due_in_paise
	) {
		throw badRequest('INVALID_PAYLOAD', 'min_due_in_paise cannot exceed total_due_in_paise');
	}
	if ((input.period_start === null) !== (input.period_end === null)) {
		throw badRequest('INVALID_PAYLOAD', 'period_start and period_end must be set together');
	}
	if (input.period_start !== null && input.period_end !== null && input.period_start >= input.period_end) {
		throw badRequest('INVALID_PAYLOAD', 'period_start must be before period_end');
	}
	return input;
}

export async function listCardStatements(
	sql: SqlClient,
	userId: UUID,
): Promise<CardStatementRow[]> {
	const rows = await sql<CardStatementRowRaw[]>`
		select
			cs.id, cs.user_id, cs.credit_card_id, cs.card_last4, cs.period_start,
			cs.period_end, cs.statement_date, cs.total_due_in_paise,
			cs.min_due_in_paise, cs.due_date, cs.source, cs.source_raw_email_id,
			cs.created_at, cs.updated_at
		from public.card_statements as cs
		where cs.user_id = ${userId}
		order by cs.due_date desc nulls last, cs.created_at desc
	`;
	return rows.map(mapCardStatementRow);
}

// Manual bill entry / correction: writes a USER-source row for the card's
// current cycle (keyed by card_last4 + due_date when present). USER rows
// always win — email harvests never overwrite them, and a user save
// overwrites a previous EMAIL row for the same cycle.
export async function upsertUserCardStatement(
	sql: SqlClient,
	userId: UUID,
	input: UpsertCardStatementInput,
): Promise<CardStatementRow> {
	const cardRows = await sql<{ id: unknown; last4: unknown }[]>`
		select ucc.id, ucc.last4
		from public.user_credit_cards as ucc
		where ucc.id = ${input.credit_card_id}
			and ucc.user_id = ${userId}
		limit 1
	`;
	const card = cardRows[0];
	if (!card) {
		throw notFound('CREDIT_CARD_NOT_FOUND', 'Credit card not found');
	}
	const last4 = toRequiredString(card.last4, 'user_credit_cards.last4');

	// One statement per cycle: update the existing row for this card+due_date
	// (or the latest row when due_date is absent), else insert.
	const existingRows = await sql<{ id: unknown }[]>`
		select cs.id
		from public.card_statements as cs
		where cs.user_id = ${userId}
			and (cs.credit_card_id = ${input.credit_card_id} or cs.card_last4 = ${last4})
			and (
				(${input.due_date}::date is not null and cs.due_date = ${input.due_date})
				or (${input.due_date}::date is null)
			)
		order by cs.due_date desc nulls last, cs.created_at desc
		limit 1
	`;
	const existingId = existingRows[0]
		? toRequiredString(existingRows[0].id, 'card_statements.id')
		: null;

	if (existingId) {
		const rows = await sql<CardStatementRowRaw[]>`
			update public.card_statements as cs
			set
				credit_card_id = ${input.credit_card_id},
				card_last4 = ${last4},
				total_due_in_paise = coalesce(${input.total_due_in_paise}, cs.total_due_in_paise),
				min_due_in_paise = coalesce(${input.min_due_in_paise}, cs.min_due_in_paise),
				due_date = coalesce(${input.due_date}, cs.due_date),
				period_start = coalesce(${input.period_start}, cs.period_start),
				period_end = coalesce(${input.period_end}, cs.period_end),
				source = 'USER',
				updated_at = now()
			where cs.id = ${existingId}
				and cs.user_id = ${userId}
			returning
				id, user_id, credit_card_id, card_last4, period_start, period_end,
				statement_date, total_due_in_paise, min_due_in_paise, due_date,
				source, source_raw_email_id, created_at, updated_at
		`;
		const row = rows[0];
		if (!row) {
			throw notFound('CARD_STATEMENT_NOT_FOUND', 'Statement disappeared during update');
		}
		return mapCardStatementRow(row);
	}

	const rows = await sql<CardStatementRowRaw[]>`
		insert into public.card_statements (
			user_id, credit_card_id, card_last4,
			period_start, period_end,
			total_due_in_paise, min_due_in_paise, due_date,
			source
		)
		values (
			${userId}, ${input.credit_card_id}, ${last4},
			${input.period_start}, ${input.period_end},
			${input.total_due_in_paise}, ${input.min_due_in_paise}, ${input.due_date},
			'USER'
		)
		returning
			id, user_id, credit_card_id, card_last4, period_start, period_end,
			statement_date, total_due_in_paise, min_due_in_paise, due_date,
			source, source_raw_email_id, created_at, updated_at
	`;
	const row = rows[0];
	if (!row) {
		throw badRequest('CARD_STATEMENT_CREATE_FAILED', 'Failed to save statement');
	}
	return mapCardStatementRow(row);
}

function isUniqueViolation(error: unknown): boolean {
	return (
		typeof error === 'object' &&
		error !== null &&
		(error as { code?: unknown }).code === '23505'
	);
}

export interface PersistStatementOutcome {
	persisted: boolean;
	linked_credit_card_id: string | null;
	duplicate: boolean;
}

// Persists a statement harvested from an email. Card matching mirrors
// transaction linking: exactly one active card with the last4 links,
// anything else parks (credit_card_id null) for relink at registration.
// Re-processing the same email updates its row; a re-sent statement for an
// already-captured cycle is a duplicate and is skipped. USER-source rows
// are never overwritten by email harvests.
export async function persistCardStatement(
	sql: SqlClient,
	userId: UUID,
	rawEmailId: UUID,
	resolved: ResolvedCardStatement,
	activeCards: ReadonlyArray<CardCandidate>,
): Promise<PersistStatementOutcome> {
	const matches = resolved.card_last4
		? activeCards.filter(card => card.last4 === resolved.card_last4)
		: [];
	const creditCardId = matches.length === 1 && matches[0] ? matches[0].id : null;

	try {
		await sql`
			insert into public.card_statements (
				user_id, credit_card_id, card_last4,
				period_start, period_end,
				total_due_in_paise, min_due_in_paise, due_date,
				source, source_raw_email_id
			)
			values (
				${userId}, ${creditCardId}, ${resolved.card_last4},
				${resolved.period_start}, ${resolved.period_end},
				${resolved.total_due_in_paise}, ${resolved.min_due_in_paise}, ${resolved.due_date},
				'EMAIL', ${rawEmailId}
			)
			on conflict (source_raw_email_id) where source_raw_email_id is not null
			do update set
				credit_card_id = coalesce(excluded.credit_card_id, card_statements.credit_card_id),
				card_last4 = coalesce(excluded.card_last4, card_statements.card_last4),
				period_start = coalesce(excluded.period_start, card_statements.period_start),
				period_end = coalesce(excluded.period_end, card_statements.period_end),
				total_due_in_paise = coalesce(excluded.total_due_in_paise, card_statements.total_due_in_paise),
				min_due_in_paise = coalesce(excluded.min_due_in_paise, card_statements.min_due_in_paise),
				due_date = coalesce(excluded.due_date, card_statements.due_date),
				updated_at = now()
			where card_statements.source = 'EMAIL'
		`;
	} catch (error) {
		if (isUniqueViolation(error)) {
			// Same cycle already captured from a different email (CRED re-sends
			// the bank's bill): fill any fields the existing EMAIL row is
			// missing rather than discarding the better copy.
			await sql`
				update public.card_statements as cs
				set
					credit_card_id = coalesce(cs.credit_card_id, ${creditCardId}),
					period_start = coalesce(cs.period_start, ${resolved.period_start}),
					period_end = coalesce(cs.period_end, ${resolved.period_end}),
					total_due_in_paise = coalesce(cs.total_due_in_paise, ${resolved.total_due_in_paise}),
					min_due_in_paise = coalesce(cs.min_due_in_paise, ${resolved.min_due_in_paise}),
					updated_at = now()
				where cs.user_id = ${userId}
					and cs.card_last4 = ${resolved.card_last4}
					and cs.due_date = ${resolved.due_date}
					and cs.source = 'EMAIL'
			`;
			return { persisted: false, linked_credit_card_id: creditCardId, duplicate: true };
		}
		throw error;
	}

	if (creditCardId && resolved.period_end) {
		await deriveBillingCycleDay(sql, userId, creditCardId, resolved.period_end);
	}

	return { persisted: true, linked_credit_card_id: creditCardId, duplicate: false };
}

// The cycle day is the day-of-month the statement period closes. Derived
// once from real data; a value the user set manually is never overwritten.
async function deriveBillingCycleDay(
	sql: SqlClient,
	userId: UUID,
	creditCardId: string,
	periodEnd: string,
): Promise<void> {
	const day = Number.parseInt(periodEnd.slice(8, 10), 10);
	if (!Number.isInteger(day) || day < 1 || day > 31) return;
	await sql`
		update public.user_credit_cards as ucc
		set billing_cycle_day = ${day}, updated_at = now()
		where ucc.id = ${creditCardId}
			and ucc.user_id = ${userId}
			and ucc.billing_cycle_day is null
	`;
}

// Registration self-heal: parked statements whose last4 now belongs to
// exactly one active card get linked. The caller (card relink) has already
// enforced the twins guard, so linking here is unambiguous.
export async function relinkStatementsForCreditCard(
	sql: SqlClient,
	userId: UUID,
	creditCardId: string,
	last4: string,
): Promise<number> {
	const updated = await sql<{ id: unknown; period_end: unknown }[]>`
		update public.card_statements as cs
		set credit_card_id = ${creditCardId}, updated_at = now()
		where cs.user_id = ${userId}
			and cs.credit_card_id is null
			and cs.card_last4 = ${last4}
		returning cs.id, cs.period_end
	`;

	// date columns may surface as strings or Date objects depending on the
	// driver's parsers — normalize both to YYYY-MM-DD.
	const latestPeriodEnd = updated
		.map(row => {
			if (typeof row.period_end === 'string') return row.period_end.slice(0, 10);
			if (row.period_end instanceof Date) return row.period_end.toISOString().slice(0, 10);
			return null;
		})
		.filter((value): value is string => value !== null)
		.sort()
		.pop();
	if (latestPeriodEnd) {
		await deriveBillingCycleDay(sql, userId, creditCardId, latestPeriodEnd);
	}

	return updated.length;
}
