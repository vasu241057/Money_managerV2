import type {
	AccountRow,
	CreateAccountRequest,
	UpdateAccountRequest,
} from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableString,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';
import { badRequest, conflict, notFound } from '../lib/http/errors';
import {
	asRecord,
	parseLast4,
	parseOptionalNonNegativeInteger,
	parseRequiredString,
} from '../lib/http/validation';

const ACCOUNT_TYPES = new Set(['cash', 'bank', 'card', 'other'] as const);

interface AccountRowRaw {
	id: unknown;
	user_id: unknown;
	name: unknown;
	type: unknown;
	instrument_last4: unknown;
	initial_balance_in_paise: unknown;
	created_at: unknown;
	updated_at: unknown;
}

function mapAccountRow(row: AccountRowRaw): AccountRow {
	return {
		id: toRequiredString(row.id, 'accounts.id'),
		user_id: toRequiredString(row.user_id, 'accounts.user_id'),
		name: toRequiredString(row.name, 'accounts.name'),
		type: toRequiredString(row.type, 'accounts.type') as AccountRow['type'],
		instrument_last4: toNullableString(row.instrument_last4, 'accounts.instrument_last4'),
		initial_balance_in_paise: toSafeInteger(
			row.initial_balance_in_paise,
			'accounts.initial_balance_in_paise',
		),
		created_at: toIsoDateTime(row.created_at, 'accounts.created_at'),
		updated_at: toIsoDateTime(row.updated_at, 'accounts.updated_at'),
	};
}

function parseAccountType(value: unknown, fieldName: string): AccountRow['type'] {
	if (typeof value !== 'string' || !ACCOUNT_TYPES.has(value as AccountRow['type'])) {
		throw badRequest(
			'INVALID_PAYLOAD',
			`${fieldName} must be one of: cash, bank, card, other`,
		);
	}

	return value as AccountRow['type'];
}

export function parseCreateAccountRequest(payload: unknown): CreateAccountRequest {
	const body = asRecord(payload);

	return {
		name: parseRequiredString(body.name, 'name'),
		type: parseAccountType(body.type, 'type'),
		instrument_last4: parseLast4(body.instrument_last4, 'instrument_last4'),
		initial_balance_in_paise: parseOptionalNonNegativeInteger(
			body.initial_balance_in_paise,
			'initial_balance_in_paise',
		),
	};
}

export function parseUpdateAccountRequest(payload: unknown): UpdateAccountRequest {
	const body = asRecord(payload);
	const updateRequest: UpdateAccountRequest = {};

	if (body.name !== undefined) {
		updateRequest.name = parseRequiredString(body.name, 'name');
	}

	if (body.type !== undefined) {
		updateRequest.type = parseAccountType(body.type, 'type');
	}

	if (body.instrument_last4 !== undefined) {
		updateRequest.instrument_last4 = parseLast4(body.instrument_last4, 'instrument_last4');
	}

	if (body.initial_balance_in_paise !== undefined) {
		updateRequest.initial_balance_in_paise = parseOptionalNonNegativeInteger(
			body.initial_balance_in_paise,
			'initial_balance_in_paise',
		);
	}

	if (Object.keys(updateRequest).length === 0) {
		throw badRequest('INVALID_PAYLOAD', 'At least one account field must be provided for update');
	}

	return updateRequest;
}

export async function listAccounts(sql: SqlClient, userId: string): Promise<AccountRow[]> {
	const rows = await sql<AccountRowRaw[]>`
		select
			a.id,
			a.user_id,
			a.name,
			a.type,
			a.instrument_last4,
			a.initial_balance_in_paise,
			a.created_at,
			a.updated_at
		from public.accounts as a
		where a.user_id = ${userId}
		order by a.created_at asc, a.id asc
	`;

	return rows.map(mapAccountRow);
}

export async function createAccount(
	sql: SqlClient,
	userId: string,
	input: CreateAccountRequest,
): Promise<AccountRow> {
	const rows = await sql<AccountRowRaw[]>`
		insert into public.accounts (
			user_id,
			name,
			type,
			instrument_last4,
			initial_balance_in_paise
		)
		values (
			${userId},
			${input.name},
			${input.type},
			${input.instrument_last4 ?? null},
			${input.initial_balance_in_paise ?? 0}
		)
		returning
			id,
			user_id,
			name,
			type,
			instrument_last4,
			initial_balance_in_paise,
			created_at,
			updated_at
	`;

	if (rows.length === 0) {
		throw badRequest('ACCOUNT_CREATE_FAILED', 'Failed to create account');
	}

	return mapAccountRow(rows[0]);
}

export async function updateAccount(
	sql: SqlClient,
	userId: string,
	accountId: string,
	input: UpdateAccountRequest,
): Promise<AccountRow> {
	const hasNameUpdate = input.name !== undefined;
	const hasTypeUpdate = input.type !== undefined;
	const hasInstrumentLast4Update = input.instrument_last4 !== undefined;
	const hasInitialBalanceUpdate = input.initial_balance_in_paise !== undefined;

	if (hasTypeUpdate && input.type !== 'card') {
		const linkedCardRows = await sql<{ id: string }[]>`
			select ucc.id
			from public.user_credit_cards as ucc
			where ucc.user_id = ${userId}
				and ucc.account_id = ${accountId}
			limit 1
		`;

		if (linkedCardRows.length > 0) {
			throw conflict(
				'ACCOUNT_TYPE_CONFLICT',
				'Account type cannot be changed from card while linked credit cards exist',
			);
		}
	}

	const rows = await sql<AccountRowRaw[]>`
		update public.accounts as a
		set
			name = case when ${hasNameUpdate} then ${input.name ?? null} else a.name end,
			type = case when ${hasTypeUpdate} then ${input.type ?? null} else a.type end,
			instrument_last4 = case
				when ${hasInstrumentLast4Update} then ${input.instrument_last4 ?? null}
				else a.instrument_last4
			end,
			initial_balance_in_paise = case
				when ${hasInitialBalanceUpdate} then ${input.initial_balance_in_paise ?? null}
				else a.initial_balance_in_paise
			end
		where a.id = ${accountId}
			and a.user_id = ${userId}
		returning
			id,
			user_id,
			name,
			type,
			instrument_last4,
			initial_balance_in_paise,
			created_at,
			updated_at
	`;

	if (rows.length === 0) {
		throw notFound('ACCOUNT_NOT_FOUND', 'Account not found');
	}

	return mapAccountRow(rows[0]);
}

export async function deleteAccount(
	sql: SqlClient,
	userId: string,
	accountId: string,
): Promise<void> {
	const rows = await sql<{ id: string }[]>`
		delete from public.accounts as a
		where a.id = ${accountId}
			and a.user_id = ${userId}
		returning a.id
	`;

	if (rows.length === 0) {
		throw notFound('ACCOUNT_NOT_FOUND', 'Account not found');
	}
}
