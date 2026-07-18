import type {
	CardNetwork,
	CreateUserCreditCardRequest,
	UpdateUserCreditCardRequest,
	UserCreditCardRow,
} from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableString,
	toRequiredString,
} from '../lib/db/serialization';
import { badRequest, conflict, notFound } from '../lib/http/errors';
import { asRecord, parseLast4, parseRequiredString, parseUuid } from '../lib/http/validation';

const CARD_NETWORKS = new Set(['visa', 'mastercard', 'rupay', 'amex', 'diners', 'other'] as const);

interface UserCreditCardRowRaw {
	id: unknown;
	user_id: unknown;
	account_id: unknown;
	card_label: unknown;
	issuer_name: unknown;
	network: unknown;
	first4: unknown;
	last4: unknown;
	is_active: unknown;
	created_at: unknown;
	updated_at: unknown;
}

function mapUserCreditCardRow(row: UserCreditCardRowRaw): UserCreditCardRow {
	return {
		id: toRequiredString(row.id, 'user_credit_cards.id'),
		user_id: toRequiredString(row.user_id, 'user_credit_cards.user_id'),
		account_id: toRequiredString(row.account_id, 'user_credit_cards.account_id'),
		card_label: toRequiredString(row.card_label, 'user_credit_cards.card_label'),
		issuer_name: toNullableString(row.issuer_name, 'user_credit_cards.issuer_name'),
		network: toNullableString(
			row.network,
			'user_credit_cards.network',
		) as UserCreditCardRow['network'],
		first4: toNullableString(row.first4, 'user_credit_cards.first4'),
		last4: toRequiredString(row.last4, 'user_credit_cards.last4'),
		is_active: row.is_active === true,
		created_at: toIsoDateTime(row.created_at, 'user_credit_cards.created_at'),
		updated_at: toIsoDateTime(row.updated_at, 'user_credit_cards.updated_at'),
	};
}

function parseRequired4Digits(value: unknown, fieldName: string): string {
	const parsed = parseLast4(value, fieldName);
	if (parsed === undefined || parsed === null) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a 4-digit numeric string`);
	}
	return parsed;
}

function parseNetwork(value: unknown, fieldName: string): CardNetwork | null {
	if (value === undefined || value === null) {
		return null;
	}
	if (typeof value !== 'string' || !CARD_NETWORKS.has(value as CardNetwork)) {
		throw badRequest(
			'INVALID_PAYLOAD',
			`${fieldName} must be one of: visa, mastercard, rupay, amex, diners, other`,
		);
	}
	return value as CardNetwork;
}

function parseIssuerName(value: unknown, fieldName: string): string | null {
	if (value === undefined || value === null) {
		return null;
	}
	if (typeof value !== 'string' || value.trim().length === 0) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a non-empty string`);
	}
	return value.trim().slice(0, 200);
}

export function parseCreateUserCreditCardRequest(payload: unknown): CreateUserCreditCardRequest {
	const body = asRecord(payload);

	return {
		account_id:
			body.account_id === undefined || body.account_id === null
				? null
				: parseUuid(body.account_id, 'account_id'),
		card_label: parseRequiredString(body.card_label, 'card_label'),
		issuer_name: parseIssuerName(body.issuer_name, 'issuer_name'),
		network: parseNetwork(body.network, 'network'),
		first4: parseLast4(body.first4, 'first4') ?? null,
		last4: parseRequired4Digits(body.last4, 'last4'),
		is_active: body.is_active === undefined ? true : body.is_active === true,
	};
}

export function parseUpdateUserCreditCardRequest(payload: unknown): UpdateUserCreditCardRequest {
	const body = asRecord(payload);
	const updateRequest: UpdateUserCreditCardRequest = {};

	if (body.account_id !== undefined) {
		updateRequest.account_id = parseUuid(body.account_id, 'account_id');
	}
	if (body.card_label !== undefined) {
		updateRequest.card_label = parseRequiredString(body.card_label, 'card_label');
	}
	if (body.issuer_name !== undefined) {
		updateRequest.issuer_name = parseIssuerName(body.issuer_name, 'issuer_name');
	}
	if (body.network !== undefined) {
		updateRequest.network = parseNetwork(body.network, 'network');
	}
	if (body.first4 !== undefined) {
		updateRequest.first4 = parseLast4(body.first4, 'first4') ?? null;
	}
	if (body.last4 !== undefined) {
		updateRequest.last4 = parseRequired4Digits(body.last4, 'last4');
	}
	if (body.is_active !== undefined) {
		if (typeof body.is_active !== 'boolean') {
			throw badRequest('INVALID_PAYLOAD', 'is_active must be a boolean');
		}
		updateRequest.is_active = body.is_active;
	}

	if (Object.keys(updateRequest).length === 0) {
		throw badRequest('INVALID_PAYLOAD', 'At least one card field must be provided for update');
	}

	return updateRequest;
}

async function assertCardBackingAccount(
	sql: SqlClient,
	userId: string,
	accountId: string,
): Promise<void> {
	const rows = await sql<{ id: unknown; type: unknown }[]>`
		select a.id, a.type
		from public.accounts as a
		where a.id = ${accountId}
			and a.user_id = ${userId}
		limit 1
	`;
	const row = rows[0];
	if (!row) {
		throw notFound('ACCOUNT_NOT_FOUND', 'Backing account not found');
	}
	if (toRequiredString(row.type, 'accounts.type') !== 'card') {
		throw badRequest(
			'ACCOUNT_NOT_CARD',
			'A credit card must be backed by an account of type card',
		);
	}
}

// A card registered without an explicit backing account gets its own
// ledger bucket. An unregistered card-type account with the same last4 is
// adopted first — a manually created bucket whose transaction history must
// stay attached; two such buckets are ambiguous, so a fresh account is
// created rather than guessing.
async function resolveBackingAccountId(
	sql: SqlClient,
	userId: string,
	input: CreateUserCreditCardRequest,
): Promise<string> {
	const candidates = await sql<{ id: unknown }[]>`
		select a.id
		from public.accounts as a
		where a.user_id = ${userId}
			and a.type = 'card'
			and a.instrument_last4 = ${input.last4}
			and not exists (
				select 1
				from public.user_credit_cards as ucc
				where ucc.account_id = a.id
			)
	`;
	if (candidates.length === 1 && candidates[0]) {
		return toRequiredString(candidates[0].id, 'accounts.id');
	}

	const created = await sql<{ id: unknown }[]>`
		insert into public.accounts (
			user_id, name, type, instrument_last4, initial_balance_in_paise
		)
		values (${userId}, ${input.card_label}, 'card', ${input.last4}, 0)
		returning id
	`;
	const row = created[0];
	if (!row) {
		throw badRequest('ACCOUNT_CREATE_FAILED', 'Failed to create backing account for card');
	}
	return toRequiredString(row.id, 'accounts.id');
}

function isUniqueViolation(error: unknown): boolean {
	return (
		typeof error === 'object' &&
		error !== null &&
		(error as { code?: unknown }).code === '23505'
	);
}

// Retroactively links past transactions to a card by last4. Deliberately
// conservative: skipped entirely when the last4 is ambiguous among the
// user's active cards, and only fully unlinked rows (no account, no card)
// are touched — manual review sets account_id, so user-edited rows are
// naturally excluded. classification_source is NOT a filter here: engine
// rows classified via a user merchant rule carry source USER yet still
// deserve the link, matching what insert-time linking does. Status is
// untouched — linking is metadata, not verification.
export async function relinkTransactionsForCreditCard(
	sql: SqlClient,
	userId: string,
	creditCardId: string,
): Promise<number> {
	const cardRows = await sql<{ id: unknown; account_id: unknown; last4: unknown }[]>`
		select ucc.id, ucc.account_id, ucc.last4
		from public.user_credit_cards as ucc
		where ucc.id = ${creditCardId}
			and ucc.user_id = ${userId}
			and ucc.is_active = true
		limit 1
	`;
	const card = cardRows[0];
	if (!card) {
		return 0;
	}
	const last4 = toRequiredString(card.last4, 'user_credit_cards.last4');

	const twins = await sql<{ count: unknown }[]>`
		select count(*)::int as count
		from public.user_credit_cards as ucc
		where ucc.user_id = ${userId}
			and ucc.last4 = ${last4}
			and ucc.is_active = true
	`;
	if (Number(twins[0]?.count ?? 0) !== 1) {
		return 0;
	}

	const updated = await sql<{ id: unknown }[]>`
		update public.transactions as t
		set credit_card_id = ${creditCardId},
			account_id = ${toRequiredString(card.account_id, 'user_credit_cards.account_id')},
			updated_at = now()
		from public.financial_events as fe
		where fe.id = t.financial_event_id
			and fe.user_id = t.user_id
			and t.user_id = ${userId}
			and t.credit_card_id is null
			and t.account_id is null
			and fe.payment_method = 'credit_card'
			and fe.instrument_id = ${last4}
		returning t.id
	`;
	return updated.length;
}

export async function listUserCreditCards(
	sql: SqlClient,
	userId: string,
): Promise<UserCreditCardRow[]> {
	const rows = await sql<UserCreditCardRowRaw[]>`
		select
			ucc.id, ucc.user_id, ucc.account_id, ucc.card_label, ucc.issuer_name,
			ucc.network, ucc.first4, ucc.last4, ucc.is_active,
			ucc.created_at, ucc.updated_at
		from public.user_credit_cards as ucc
		where ucc.user_id = ${userId}
		order by ucc.created_at asc, ucc.id asc
	`;

	return rows.map(mapUserCreditCardRow);
}

export async function createUserCreditCard(
	sql: SqlClient,
	userId: string,
	input: CreateUserCreditCardRequest,
): Promise<{ card: UserCreditCardRow; relinked_transaction_count: number }> {
	let backingAccountId: string;
	if (input.account_id) {
		await assertCardBackingAccount(sql, userId, input.account_id);
		backingAccountId = input.account_id;
	} else {
		backingAccountId = await resolveBackingAccountId(sql, userId, input);
	}

	let rows: UserCreditCardRowRaw[];
	try {
		rows = await sql<UserCreditCardRowRaw[]>`
			insert into public.user_credit_cards (
				user_id, account_id, card_label, issuer_name, network,
				first4, last4, is_active
			)
			values (
				${userId}, ${backingAccountId}, ${input.card_label},
				${input.issuer_name ?? null}, ${input.network ?? null},
				${input.first4 ?? null}, ${input.last4}, ${input.is_active ?? true}
			)
			returning
				id, user_id, account_id, card_label, issuer_name,
				network, first4, last4, is_active, created_at, updated_at
		`;
	} catch (error) {
		if (isUniqueViolation(error)) {
			throw conflict(
				'CREDIT_CARD_DUPLICATE',
				'A card with this label or number already exists',
			);
		}
		throw error;
	}

	const row = rows[0];
	if (!row) {
		throw badRequest('CREDIT_CARD_CREATE_FAILED', 'Failed to create credit card');
	}
	const card = mapUserCreditCardRow(row);

	// Self-heal: transactions extracted before the card existed get linked
	// now, the same pattern the merchant mapper uses.
	const relinked = await relinkTransactionsForCreditCard(sql, userId, card.id);

	return { card, relinked_transaction_count: relinked };
}

export async function updateUserCreditCard(
	sql: SqlClient,
	userId: string,
	creditCardId: string,
	input: UpdateUserCreditCardRequest,
): Promise<{ card: UserCreditCardRow; relinked_transaction_count: number }> {
	if (input.account_id !== undefined) {
		await assertCardBackingAccount(sql, userId, input.account_id);
	}

	const hasAccountUpdate = input.account_id !== undefined;
	const hasLabelUpdate = input.card_label !== undefined;
	const hasIssuerUpdate = input.issuer_name !== undefined;
	const hasNetworkUpdate = input.network !== undefined;
	const hasFirst4Update = input.first4 !== undefined;
	const hasLast4Update = input.last4 !== undefined;
	const hasActiveUpdate = input.is_active !== undefined;

	let rows: UserCreditCardRowRaw[];
	try {
		rows = await sql<UserCreditCardRowRaw[]>`
			update public.user_credit_cards as ucc
			set
				account_id = case when ${hasAccountUpdate} then ${input.account_id ?? null} else ucc.account_id end,
				card_label = case when ${hasLabelUpdate} then ${input.card_label ?? null} else ucc.card_label end,
				issuer_name = case when ${hasIssuerUpdate} then ${input.issuer_name ?? null} else ucc.issuer_name end,
				network = case when ${hasNetworkUpdate} then ${input.network ?? null} else ucc.network end,
				first4 = case when ${hasFirst4Update} then ${input.first4 ?? null} else ucc.first4 end,
				last4 = case when ${hasLast4Update} then ${input.last4 ?? null} else ucc.last4 end,
				is_active = case when ${hasActiveUpdate} then ${input.is_active ?? true} else ucc.is_active end
			where ucc.id = ${creditCardId}
				and ucc.user_id = ${userId}
			returning
				id, user_id, account_id, card_label, issuer_name,
				network, first4, last4, is_active, created_at, updated_at
		`;
	} catch (error) {
		if (isUniqueViolation(error)) {
			throw conflict(
				'CREDIT_CARD_DUPLICATE',
				'A card with this label or number already exists',
			);
		}
		throw error;
	}

	const row = rows[0];
	if (!row) {
		throw notFound('CREDIT_CARD_NOT_FOUND', 'Credit card not found');
	}
	const card = mapUserCreditCardRow(row);

	// A changed last4 (reissued card) or reactivation may make old
	// transactions linkable; history the user set stays untouched.
	const shouldRelink = hasLast4Update || (hasActiveUpdate && input.is_active === true);
	const relinked = shouldRelink
		? await relinkTransactionsForCreditCard(sql, userId, card.id)
		: 0;

	return { card, relinked_transaction_count: relinked };
}

export async function deleteUserCreditCard(
	sql: SqlClient,
	userId: string,
	creditCardId: string,
): Promise<void> {
	const rows = await sql<{ id: unknown }[]>`
		delete from public.user_credit_cards as ucc
		where ucc.id = ${creditCardId}
			and ucc.user_id = ${userId}
		returning ucc.id
	`;

	if (rows.length === 0) {
		throw notFound('CREDIT_CARD_NOT_FOUND', 'Credit card not found');
	}
}
