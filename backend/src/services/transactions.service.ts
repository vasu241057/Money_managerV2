import type {
	AccountRow,
	CategoryRow,
	ClassificationSource,
	CreateManualTransactionRequest,
	FinancialEventRow,
	GlobalMerchantRow,
	PaginatedResponse,
	PaymentMethod,
	RawEmailRow,
	TransactionFeedItem,
	TransactionRow,
	TransactionStatus,
	UpdateTransactionRequest,
} from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableNumber,
	toNullableString,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';
import { badRequest, conflict, notFound } from '../lib/http/errors';
import {
	asRecord,
	parseIsoDateTime,
	parseLimit,
	parseNullableString,
	parseNullableUuid,
	parseOptionalFiniteNumber,
	parseOptionalIsoDateTime,
	parseOptionalUuid,
	parsePage,
	parsePositiveInteger,
	validateTransactionStatusTransition,
} from '../lib/http/validation';

const TRANSACTION_TYPES = new Set(['income', 'expense', 'transfer'] as const);
const TRANSACTION_STATUSES = new Set(['VERIFIED', 'NEEDS_REVIEW'] as const);
const CLASSIFICATION_SOURCES = new Set(['USER', 'SYSTEM_DEFAULT', 'HEURISTIC', 'AI'] as const);
const PAYMENT_METHODS = new Set([
	'upi',
	'credit_card',
	'debit_card',
	'netbanking',
	'cash',
	'unknown',
] as const);

interface TransactionFeedRowRaw {
	transaction_id: unknown;
	transaction_user_id: unknown;
	transaction_financial_event_id: unknown;
	transaction_account_id: unknown;
	transaction_category_id: unknown;
	transaction_merchant_id: unknown;
	transaction_credit_card_id: unknown;
	transaction_amount_in_paise: unknown;
	transaction_type: unknown;
	transaction_txn_date: unknown;
	transaction_user_note: unknown;
	transaction_status: unknown;
	transaction_classification_source: unknown;
	transaction_ai_confidence_score: unknown;
	transaction_created_at: unknown;
	transaction_updated_at: unknown;

	financial_event_id: unknown;
	financial_event_user_id: unknown;
	financial_event_raw_email_id: unknown;
	financial_event_extraction_index: unknown;
	financial_event_direction: unknown;
	financial_event_amount_in_paise: unknown;
	financial_event_currency: unknown;
	financial_event_txn_timestamp: unknown;
	financial_event_payment_method: unknown;
	financial_event_instrument_id: unknown;
	financial_event_counterparty_raw: unknown;
	financial_event_search_key: unknown;
	financial_event_status: unknown;
	financial_event_created_at: unknown;

	account_id: unknown;
	account_name: unknown;
	account_type: unknown;

	credit_card_id: unknown;
	credit_card_card_label: unknown;
	credit_card_first4: unknown;
	credit_card_last4: unknown;

	category_id: unknown;
	category_name: unknown;
	category_type: unknown;
	category_icon: unknown;

	merchant_id: unknown;
	merchant_canonical_name: unknown;
	merchant_type: unknown;

	raw_email_id: unknown;
	raw_email_source_id: unknown;
	raw_email_internal_date: unknown;
	raw_email_status: unknown;
}

interface TransactionStatusRow {
	transaction_id: string;
	transaction_status: TransactionStatus;
	financial_event_id: string;
	raw_email_id: string | null;
}

interface TransactionUpdateStateRow extends TransactionStatusRow {
	transaction_account_id: string | null;
	transaction_credit_card_id: string | null;
	current_credit_card_account_id: string | null;
}

interface PreparedTransactionRelationUpdate {
	shouldUpdateAccount: boolean;
	accountId: string | null;
	shouldUpdateCreditCard: boolean;
	creditCardId: string | null;
}

interface CountRow {
	total: number | string;
}

interface ParsedListTransactionsQuery {
	page: number;
	limit: number;
	from?: string;
	to?: string;
	status?: TransactionStatus;
	type?: TransactionRow['type'];
	account_id?: string;
	category_id?: string;
	credit_card_id?: string;
}

function parseTransactionType(value: unknown, fieldName: string): TransactionRow['type'] {
	if (typeof value !== 'string' || !TRANSACTION_TYPES.has(value as TransactionRow['type'])) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be one of: income, expense, transfer`);
	}

	return value as TransactionRow['type'];
}

function parsePaymentMethod(value: unknown, fieldName: string): PaymentMethod {
	if (typeof value !== 'string' || !PAYMENT_METHODS.has(value as PaymentMethod)) {
		throw badRequest(
			'INVALID_PAYLOAD',
			`${fieldName} must be one of: upi, credit_card, debit_card, netbanking, cash, unknown`,
		);
	}

	return value as PaymentMethod;
}

function parseTransactionStatus(value: unknown, fieldName: string): TransactionStatus {
	if (typeof value !== 'string' || !TRANSACTION_STATUSES.has(value as TransactionStatus)) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be VERIFIED or NEEDS_REVIEW`);
	}

	return value as TransactionStatus;
}

function parseClassificationSource(value: unknown, fieldName: string): ClassificationSource {
	if (
		typeof value !== 'string' ||
		!CLASSIFICATION_SOURCES.has(value as ClassificationSource)
	) {
		throw badRequest(
			'INVALID_PAYLOAD',
			`${fieldName} must be one of: USER, SYSTEM_DEFAULT, HEURISTIC, AI`,
		);
	}

	return value as ClassificationSource;
}

function parseOptionalAiConfidence(
	value: unknown,
	fieldName: string,
): number | null | undefined {
	if (value === undefined) {
		return undefined;
	}

	if (value === null) {
		return null;
	}

	const parsed = parseOptionalFiniteNumber(value, fieldName);
	if (parsed === undefined) {
		return undefined;
	}

	if (parsed < 0 || parsed > 1) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be between 0 and 1`);
	}

	return parsed;
}

function mapTransactionFeedRow(row: TransactionFeedRowRaw): TransactionFeedItem {
	const transaction: TransactionRow = {
		id: toRequiredString(row.transaction_id, 'transactions.id'),
		user_id: toRequiredString(row.transaction_user_id, 'transactions.user_id'),
		financial_event_id: toRequiredString(
			row.transaction_financial_event_id,
			'transactions.financial_event_id',
		),
		account_id: toNullableString(row.transaction_account_id, 'transactions.account_id'),
		category_id: toNullableString(row.transaction_category_id, 'transactions.category_id'),
		merchant_id: toNullableString(row.transaction_merchant_id, 'transactions.merchant_id'),
		credit_card_id: toNullableString(
			row.transaction_credit_card_id,
			'transactions.credit_card_id',
		),
		amount_in_paise: toSafeInteger(
			row.transaction_amount_in_paise,
			'transactions.amount_in_paise',
		),
		type: toRequiredString(row.transaction_type, 'transactions.type') as TransactionRow['type'],
		txn_date: toIsoDateTime(row.transaction_txn_date, 'transactions.txn_date'),
		user_note: toNullableString(row.transaction_user_note, 'transactions.user_note'),
		status: toRequiredString(
			row.transaction_status,
			'transactions.status',
		) as TransactionStatus,
		classification_source: toRequiredString(
			row.transaction_classification_source,
			'transactions.classification_source',
		) as ClassificationSource,
		ai_confidence_score: toNullableNumber(
			row.transaction_ai_confidence_score,
			'transactions.ai_confidence_score',
		),
		created_at: toIsoDateTime(row.transaction_created_at, 'transactions.created_at'),
		updated_at: toIsoDateTime(row.transaction_updated_at, 'transactions.updated_at'),
	};

	const financialEvent: FinancialEventRow = {
		id: toRequiredString(row.financial_event_id, 'financial_events.id'),
		user_id: toRequiredString(row.financial_event_user_id, 'financial_events.user_id'),
		raw_email_id: toNullableString(row.financial_event_raw_email_id, 'financial_events.raw_email_id'),
		extraction_index: toSafeInteger(
			row.financial_event_extraction_index,
			'financial_events.extraction_index',
		),
		direction: toRequiredString(
			row.financial_event_direction,
			'financial_events.direction',
		) as FinancialEventRow['direction'],
		amount_in_paise: toSafeInteger(
			row.financial_event_amount_in_paise,
			'financial_events.amount_in_paise',
		),
		currency: toRequiredString(row.financial_event_currency, 'financial_events.currency'),
		txn_timestamp: toIsoDateTime(
			row.financial_event_txn_timestamp,
			'financial_events.txn_timestamp',
		),
		payment_method: toRequiredString(
			row.financial_event_payment_method,
			'financial_events.payment_method',
		) as FinancialEventRow['payment_method'],
		instrument_id: toNullableString(
			row.financial_event_instrument_id,
			'financial_events.instrument_id',
		),
		counterparty_raw: toNullableString(
			row.financial_event_counterparty_raw,
			'financial_events.counterparty_raw',
		),
		search_key: toNullableString(row.financial_event_search_key, 'financial_events.search_key'),
		status: toRequiredString(
			row.financial_event_status,
			'financial_events.status',
		) as FinancialEventRow['status'],
		created_at: toIsoDateTime(row.financial_event_created_at, 'financial_events.created_at'),
	};

	const accountId = toNullableString(row.account_id, 'accounts.id');
	const categoryId = toNullableString(row.category_id, 'categories.id');
	const merchantId = toNullableString(row.merchant_id, 'global_merchants.id');
	const creditCardId = toNullableString(row.credit_card_id, 'user_credit_cards.id');
	const rawEmailId = toNullableString(row.raw_email_id, 'raw_emails.id');

	return {
		transaction,
		financial_event: financialEvent,
		account:
			accountId === null
				? null
				: {
						id: accountId,
						name: toRequiredString(row.account_name, 'accounts.name'),
						type: toRequiredString(row.account_type, 'accounts.type') as AccountRow['type'],
					},
		credit_card:
			creditCardId === null
				? null
				: {
						id: creditCardId,
						card_label: toRequiredString(
							row.credit_card_card_label,
							'user_credit_cards.card_label',
						),
						first4: toRequiredString(row.credit_card_first4, 'user_credit_cards.first4'),
						last4: toRequiredString(row.credit_card_last4, 'user_credit_cards.last4'),
					},
		category:
			categoryId === null
				? null
				: {
						id: categoryId,
						name: toRequiredString(row.category_name, 'categories.name'),
						type: toRequiredString(row.category_type, 'categories.type') as CategoryRow['type'],
						icon: toNullableString(row.category_icon, 'categories.icon'),
					},
		merchant:
			merchantId === null
				? null
				: {
						id: merchantId,
						canonical_name: toRequiredString(
							row.merchant_canonical_name,
							'global_merchants.canonical_name',
						),
						type: toRequiredString(
							row.merchant_type,
							'global_merchants.type',
						) as GlobalMerchantRow['type'],
					},
		raw_email:
			rawEmailId === null
				? null
				: {
						id: rawEmailId,
						source_id: toRequiredString(row.raw_email_source_id, 'raw_emails.source_id'),
						internal_date: toIsoDateTime(
							row.raw_email_internal_date,
							'raw_emails.internal_date',
						),
						status: toRequiredString(
							row.raw_email_status,
							'raw_emails.status',
						) as RawEmailRow['status'],
					},
	};
}

function getTransactionDirection(type: TransactionRow['type']): FinancialEventRow['direction'] {
	if (type === 'income') {
		return 'credit';
	}

	return 'debit';
}

async function getTransactionFeedItemById(
	sql: SqlClient,
	userId: string,
	transactionId: string,
): Promise<TransactionFeedItem | null> {
	const rows = await sql<TransactionFeedRowRaw[]>`
		select
			t.id as transaction_id,
			t.user_id as transaction_user_id,
			t.financial_event_id as transaction_financial_event_id,
			t.account_id as transaction_account_id,
			t.category_id as transaction_category_id,
			t.merchant_id as transaction_merchant_id,
			t.credit_card_id as transaction_credit_card_id,
			t.amount_in_paise as transaction_amount_in_paise,
			t.type as transaction_type,
			t.txn_date as transaction_txn_date,
			t.user_note as transaction_user_note,
			t.status as transaction_status,
			t.classification_source as transaction_classification_source,
			t.ai_confidence_score as transaction_ai_confidence_score,
			t.created_at as transaction_created_at,
			t.updated_at as transaction_updated_at,

			fe.id as financial_event_id,
			fe.user_id as financial_event_user_id,
			fe.raw_email_id as financial_event_raw_email_id,
			fe.extraction_index as financial_event_extraction_index,
			fe.direction as financial_event_direction,
			fe.amount_in_paise as financial_event_amount_in_paise,
			fe.currency as financial_event_currency,
			fe.txn_timestamp as financial_event_txn_timestamp,
			fe.payment_method as financial_event_payment_method,
			fe.instrument_id as financial_event_instrument_id,
			fe.counterparty_raw as financial_event_counterparty_raw,
			fe.search_key as financial_event_search_key,
			fe.status as financial_event_status,
			fe.created_at as financial_event_created_at,

			a.id as account_id,
			a.name as account_name,
			a.type as account_type,

			ucc.id as credit_card_id,
			ucc.card_label as credit_card_card_label,
			ucc.first4 as credit_card_first4,
			ucc.last4 as credit_card_last4,

			c.id as category_id,
			c.name as category_name,
			c.type as category_type,
			c.icon as category_icon,

			gm.id as merchant_id,
			gm.canonical_name as merchant_canonical_name,
			gm.type as merchant_type,

			re.id as raw_email_id,
			re.source_id as raw_email_source_id,
			re.internal_date as raw_email_internal_date,
			re.status as raw_email_status
		from public.transactions as t
		join public.financial_events as fe
			on fe.id = t.financial_event_id
			and fe.user_id = t.user_id
		left join public.accounts as a
			on a.id = t.account_id
			and a.user_id = t.user_id
		left join public.user_credit_cards as ucc
			on ucc.id = t.credit_card_id
			and ucc.user_id = t.user_id
		left join public.categories as c
			on c.id = t.category_id
		left join public.global_merchants as gm
			on gm.id = t.merchant_id
		left join public.raw_emails as re
			on re.id = fe.raw_email_id
			and re.user_id = fe.user_id
		where t.user_id = ${userId}
			and fe.status = 'ACTIVE'
			and t.id = ${transactionId}
		limit 1
	`;

	if (rows.length === 0) {
		return null;
	}

	return mapTransactionFeedRow(rows[0]);
}

export function parseListTransactionsQuery(query: Record<string, unknown>): ParsedListTransactionsQuery {
	const page = parsePage(query.page);
	const limit = parseLimit(query.limit);
	const from = parseOptionalIsoDateTime(query.from, 'from');
	const to = parseOptionalIsoDateTime(query.to, 'to');
	const status =
		query.status === undefined ? undefined : parseTransactionStatus(query.status, 'status');
	const type =
		query.type === undefined ? undefined : parseTransactionType(query.type, 'type');
	const accountId = parseOptionalUuid(query.account_id, 'account_id');
	const categoryId = parseOptionalUuid(query.category_id, 'category_id');
	const creditCardId = parseOptionalUuid(query.credit_card_id, 'credit_card_id');

	if (from && to && new Date(from).getTime() > new Date(to).getTime()) {
		throw badRequest('INVALID_QUERY', 'from must be earlier than or equal to to');
	}

	return {
		page,
		limit,
		from,
		to,
		status,
		type,
		account_id: accountId,
		category_id: categoryId,
		credit_card_id: creditCardId,
	};
}

export function parseCreateManualTransactionRequest(
	payload: unknown,
): CreateManualTransactionRequest {
	const body = asRecord(payload);

	return {
		amount_in_paise: parsePositiveInteger(body.amount_in_paise, 'amount_in_paise'),
		type: parseTransactionType(body.type, 'type'),
		txn_date: parseIsoDateTime(body.txn_date, 'txn_date'),
		account_id: parseNullableUuid(body.account_id, 'account_id'),
		category_id: parseNullableUuid(body.category_id, 'category_id'),
		merchant_id: parseNullableUuid(body.merchant_id, 'merchant_id'),
		user_note: parseNullableString(body.user_note, 'user_note'),
		payment_method:
			body.payment_method === undefined
				? undefined
				: parsePaymentMethod(body.payment_method, 'payment_method'),
		instrument_id: parseNullableString(body.instrument_id, 'instrument_id'),
		counterparty_raw: parseNullableString(body.counterparty_raw, 'counterparty_raw'),
	};
}

export function parseUpdateTransactionRequest(payload: unknown): UpdateTransactionRequest {
	const body = asRecord(payload);
	const updateRequest: UpdateTransactionRequest = {};

	if (body.account_id !== undefined) {
		updateRequest.account_id = parseNullableUuid(body.account_id, 'account_id');
	}

	if (body.category_id !== undefined) {
		updateRequest.category_id = parseNullableUuid(body.category_id, 'category_id');
	}

	if (body.merchant_id !== undefined) {
		updateRequest.merchant_id = parseNullableUuid(body.merchant_id, 'merchant_id');
	}

	if (body.credit_card_id !== undefined) {
		updateRequest.credit_card_id = parseNullableUuid(body.credit_card_id, 'credit_card_id');
	}

	if (body.user_note !== undefined) {
		updateRequest.user_note = parseNullableString(body.user_note, 'user_note');
	}

	if (body.status !== undefined) {
		updateRequest.status = parseTransactionStatus(body.status, 'status');
	}

	if (body.classification_source !== undefined) {
		updateRequest.classification_source = parseClassificationSource(
			body.classification_source,
			'classification_source',
		);
	}

	if (body.ai_confidence_score !== undefined) {
		updateRequest.ai_confidence_score = parseOptionalAiConfidence(
			body.ai_confidence_score,
			'ai_confidence_score',
		);
	}

	if (Object.keys(updateRequest).length === 0) {
		throw badRequest(
			'INVALID_PAYLOAD',
			'At least one mutable transaction field must be provided for update',
		);
	}

	return updateRequest;
}

export function prepareTransactionRelationUpdate(params: {
	currentCreditCardId: string | null;
	currentCreditCardAccountId: string | null;
	requestedAccountId: string | null | undefined;
	requestedCreditCardId: string | null | undefined;
	targetCreditCardAccountId?: string | null;
}): PreparedTransactionRelationUpdate {
	const hasAccountUpdate = params.requestedAccountId !== undefined;
	const hasCreditCardUpdate = params.requestedCreditCardId !== undefined;

	let shouldUpdateAccount = hasAccountUpdate;
	let accountId = params.requestedAccountId ?? null;

	if (!hasCreditCardUpdate && hasAccountUpdate && params.currentCreditCardId !== null) {
		if (params.requestedAccountId === null) {
			throw badRequest(
				'INVALID_PAYLOAD',
				'account_id cannot be null while credit_card_id is still set. Clear credit_card_id in the same request.',
			);
		}

		if (
			params.currentCreditCardAccountId === null ||
			params.requestedAccountId !== params.currentCreditCardAccountId
		) {
			throw badRequest(
				'INVALID_PAYLOAD',
				'account_id must match the linked credit card account unless credit_card_id is also updated.',
			);
		}
	}

	if (hasCreditCardUpdate && params.requestedCreditCardId !== null) {
		if (!params.targetCreditCardAccountId) {
			throw badRequest(
				'INVALID_PAYLOAD',
				'credit_card_id must reference a credit card owned by the authenticated user.',
			);
		}

		if (
			hasAccountUpdate &&
			params.requestedAccountId !== null &&
			params.requestedAccountId !== params.targetCreditCardAccountId
		) {
			throw badRequest(
				'INVALID_PAYLOAD',
				'account_id must match credit_card_id account when both are provided.',
			);
		}

		shouldUpdateAccount = true;
		accountId = params.targetCreditCardAccountId;
	}

	return {
		shouldUpdateAccount,
		accountId,
		shouldUpdateCreditCard: hasCreditCardUpdate,
		creditCardId: params.requestedCreditCardId ?? null,
	};
}

export async function listTransactions(
	sql: SqlClient,
	userId: string,
	input: ParsedListTransactionsQuery,
): Promise<PaginatedResponse<TransactionFeedItem>> {
	const hasFromFilter = Boolean(input.from);
	const hasToFilter = Boolean(input.to);
	const hasStatusFilter = Boolean(input.status);
	const hasTypeFilter = Boolean(input.type);
	const hasAccountFilter = Boolean(input.account_id);
	const hasCategoryFilter = Boolean(input.category_id);
	const hasCreditCardFilter = Boolean(input.credit_card_id);
	const offset = (input.page - 1) * input.limit;

	const countRows = await sql<CountRow[]>`
		select count(*)::bigint as total
		from public.transactions as t
		join public.financial_events as fe
			on fe.id = t.financial_event_id
			and fe.user_id = t.user_id
		where t.user_id = ${userId}
			and fe.status = 'ACTIVE'
			and (not ${hasFromFilter} or t.txn_date >= ${input.from ?? null})
			and (not ${hasToFilter} or t.txn_date <= ${input.to ?? null})
			and (not ${hasStatusFilter} or t.status = ${input.status ?? null})
			and (not ${hasTypeFilter} or t.type = ${input.type ?? null})
			and (not ${hasAccountFilter} or t.account_id = ${input.account_id ?? null})
			and (not ${hasCategoryFilter} or t.category_id = ${input.category_id ?? null})
			and (
				not ${hasCreditCardFilter}
				or t.credit_card_id = ${input.credit_card_id ?? null}
			)
	`;
	const total = toSafeInteger(countRows[0]?.total ?? 0, 'transactions.total');

	const rows = await sql<TransactionFeedRowRaw[]>`
		select
			t.id as transaction_id,
			t.user_id as transaction_user_id,
			t.financial_event_id as transaction_financial_event_id,
			t.account_id as transaction_account_id,
			t.category_id as transaction_category_id,
			t.merchant_id as transaction_merchant_id,
			t.credit_card_id as transaction_credit_card_id,
			t.amount_in_paise as transaction_amount_in_paise,
			t.type as transaction_type,
			t.txn_date as transaction_txn_date,
			t.user_note as transaction_user_note,
			t.status as transaction_status,
			t.classification_source as transaction_classification_source,
			t.ai_confidence_score as transaction_ai_confidence_score,
			t.created_at as transaction_created_at,
			t.updated_at as transaction_updated_at,

			fe.id as financial_event_id,
			fe.user_id as financial_event_user_id,
			fe.raw_email_id as financial_event_raw_email_id,
			fe.extraction_index as financial_event_extraction_index,
			fe.direction as financial_event_direction,
			fe.amount_in_paise as financial_event_amount_in_paise,
			fe.currency as financial_event_currency,
			fe.txn_timestamp as financial_event_txn_timestamp,
			fe.payment_method as financial_event_payment_method,
			fe.instrument_id as financial_event_instrument_id,
			fe.counterparty_raw as financial_event_counterparty_raw,
			fe.search_key as financial_event_search_key,
			fe.status as financial_event_status,
			fe.created_at as financial_event_created_at,

			a.id as account_id,
			a.name as account_name,
			a.type as account_type,

			ucc.id as credit_card_id,
			ucc.card_label as credit_card_card_label,
			ucc.first4 as credit_card_first4,
			ucc.last4 as credit_card_last4,

			c.id as category_id,
			c.name as category_name,
			c.type as category_type,
			c.icon as category_icon,

			gm.id as merchant_id,
			gm.canonical_name as merchant_canonical_name,
			gm.type as merchant_type,

			re.id as raw_email_id,
			re.source_id as raw_email_source_id,
			re.internal_date as raw_email_internal_date,
			re.status as raw_email_status
		from public.transactions as t
		join public.financial_events as fe
			on fe.id = t.financial_event_id
			and fe.user_id = t.user_id
		left join public.accounts as a
			on a.id = t.account_id
			and a.user_id = t.user_id
		left join public.user_credit_cards as ucc
			on ucc.id = t.credit_card_id
			and ucc.user_id = t.user_id
		left join public.categories as c
			on c.id = t.category_id
		left join public.global_merchants as gm
			on gm.id = t.merchant_id
		left join public.raw_emails as re
			on re.id = fe.raw_email_id
			and re.user_id = fe.user_id
		where t.user_id = ${userId}
			and fe.status = 'ACTIVE'
			and (not ${hasFromFilter} or t.txn_date >= ${input.from ?? null})
			and (not ${hasToFilter} or t.txn_date <= ${input.to ?? null})
			and (not ${hasStatusFilter} or t.status = ${input.status ?? null})
			and (not ${hasTypeFilter} or t.type = ${input.type ?? null})
			and (not ${hasAccountFilter} or t.account_id = ${input.account_id ?? null})
			and (not ${hasCategoryFilter} or t.category_id = ${input.category_id ?? null})
			and (
				not ${hasCreditCardFilter}
				or t.credit_card_id = ${input.credit_card_id ?? null}
			)
		order by t.txn_date desc, t.created_at desc, t.id desc
		limit ${input.limit}
		offset ${offset}
	`;

	const data = rows.map(mapTransactionFeedRow);
	return {
		data,
		total,
		page: input.page,
		limit: input.limit,
		has_more: input.page * input.limit < total,
	};
}

export async function createManualTransaction(
	sql: SqlClient,
	userId: string,
	input: CreateManualTransactionRequest,
): Promise<TransactionFeedItem> {
	return sql.begin(async (tx) => {
		const transactionSql = tx as unknown as SqlClient;

		const financialEventRows = await transactionSql<{ id: string }[]>`
			insert into public.financial_events (
				user_id,
				raw_email_id,
				extraction_index,
				direction,
				amount_in_paise,
				currency,
				txn_timestamp,
				payment_method,
				instrument_id,
				counterparty_raw,
				search_key,
				status
			)
			values (
				${userId},
				null,
				0,
				${getTransactionDirection(input.type)},
				${input.amount_in_paise},
				'INR',
				${input.txn_date},
				${input.payment_method ?? 'unknown'},
				${input.instrument_id ?? null},
				${input.counterparty_raw ?? null},
				null,
				'ACTIVE'
			)
			returning id
		`;

		const financialEventId = financialEventRows[0]?.id;
		if (!financialEventId) {
			throw badRequest('TRANSACTION_CREATE_FAILED', 'Failed to persist financial event');
		}

		const transactionRows = await transactionSql<{ id: string }[]>`
			insert into public.transactions (
				user_id,
				financial_event_id,
				account_id,
				category_id,
				merchant_id,
				credit_card_id,
				amount_in_paise,
				type,
				txn_date,
				user_note,
				status,
				classification_source,
				ai_confidence_score
			)
			values (
				${userId},
				${financialEventId},
				${input.account_id ?? null},
				${input.category_id ?? null},
				${input.merchant_id ?? null},
				null,
				${input.amount_in_paise},
				${input.type},
				${input.txn_date},
				${input.user_note ?? null},
				'VERIFIED',
				'USER',
				null
			)
			returning id
		`;

		const transactionId = transactionRows[0]?.id;
		if (!transactionId) {
			throw badRequest('TRANSACTION_CREATE_FAILED', 'Failed to persist transaction projection');
		}

		const createdTransaction = await getTransactionFeedItemById(
			transactionSql,
			userId,
			transactionId,
		);
		if (!createdTransaction) {
			throw badRequest('TRANSACTION_CREATE_FAILED', 'Failed to load created transaction');
		}

		return createdTransaction;
	});
}

export async function updateTransaction(
	sql: SqlClient,
	userId: string,
	transactionId: string,
	input: UpdateTransactionRequest,
): Promise<TransactionFeedItem> {
	const statusRows = await sql<TransactionUpdateStateRow[]>`
		select
			t.id as transaction_id,
			t.status as transaction_status,
			t.financial_event_id,
			fe.raw_email_id,
			t.account_id as transaction_account_id,
			t.credit_card_id as transaction_credit_card_id,
			ucc.account_id as current_credit_card_account_id
		from public.transactions as t
		join public.financial_events as fe
			on fe.id = t.financial_event_id
			and fe.user_id = t.user_id
			and fe.status = 'ACTIVE'
		left join public.user_credit_cards as ucc
			on ucc.id = t.credit_card_id
			and ucc.user_id = t.user_id
		where t.id = ${transactionId}
			and t.user_id = ${userId}
		limit 1
	`;

	if (statusRows.length === 0) {
		throw notFound('TRANSACTION_NOT_FOUND', 'Transaction not found');
	}

	const current = statusRows[0];
	if (input.status) {
		validateTransactionStatusTransition(current.transaction_status, input.status);
	}

	let targetCreditCardAccountId: string | null | undefined;
	if (input.credit_card_id !== undefined && input.credit_card_id !== null) {
		const cardRows = await sql<{ account_id: string }[]>`
			select ucc.account_id
			from public.user_credit_cards as ucc
			where ucc.id = ${input.credit_card_id}
				and ucc.user_id = ${userId}
			limit 1
		`;

		targetCreditCardAccountId = cardRows[0]?.account_id ?? null;
	}

	const relationUpdate = prepareTransactionRelationUpdate({
		currentCreditCardId: current.transaction_credit_card_id,
		currentCreditCardAccountId: current.current_credit_card_account_id,
		requestedAccountId: input.account_id,
		requestedCreditCardId: input.credit_card_id,
		targetCreditCardAccountId,
	});

	const hasAccountUpdate = relationUpdate.shouldUpdateAccount;
	const hasCategoryUpdate = input.category_id !== undefined;
	const hasMerchantUpdate = input.merchant_id !== undefined;
	const hasCreditCardUpdate = relationUpdate.shouldUpdateCreditCard;
	const hasUserNoteUpdate = input.user_note !== undefined;
	const hasStatusUpdate = input.status !== undefined;
	const hasClassificationSourceUpdate = input.classification_source !== undefined;
	const hasAiConfidenceScoreUpdate = input.ai_confidence_score !== undefined;

	await sql`
		update public.transactions as t
		set
			account_id = case when ${hasAccountUpdate} then ${relationUpdate.accountId} else t.account_id end,
			category_id = case when ${hasCategoryUpdate} then ${input.category_id ?? null} else t.category_id end,
			merchant_id = case when ${hasMerchantUpdate} then ${input.merchant_id ?? null} else t.merchant_id end,
			credit_card_id = case when ${hasCreditCardUpdate} then ${relationUpdate.creditCardId} else t.credit_card_id end,
			user_note = case when ${hasUserNoteUpdate} then ${input.user_note ?? null} else t.user_note end,
			status = case when ${hasStatusUpdate} then ${input.status ?? null} else t.status end,
			classification_source = case
				when ${hasClassificationSourceUpdate} then ${input.classification_source ?? null}
				else t.classification_source
			end,
			ai_confidence_score = case
				when ${hasAiConfidenceScoreUpdate} then ${input.ai_confidence_score ?? null}
				else t.ai_confidence_score
			end
		where t.id = ${transactionId}
			and t.user_id = ${userId}
	`;

	const updatedTransaction = await getTransactionFeedItemById(sql, userId, transactionId);
	if (!updatedTransaction) {
		throw notFound('TRANSACTION_NOT_FOUND', 'Transaction not found');
	}

	return updatedTransaction;
}

export async function deleteManualTransaction(
	sql: SqlClient,
	userId: string,
	transactionId: string,
): Promise<void> {
	await sql.begin(async (tx) => {
		const transactionSql = tx as unknown as SqlClient;

		const rows = await transactionSql<TransactionStatusRow[]>`
			select
				t.id as transaction_id,
				t.status as transaction_status,
				t.financial_event_id,
				fe.raw_email_id
			from public.transactions as t
			join public.financial_events as fe
				on fe.id = t.financial_event_id
				and fe.user_id = t.user_id
				and fe.status = 'ACTIVE'
			where t.id = ${transactionId}
				and t.user_id = ${userId}
			for update
		`;

		if (rows.length === 0) {
			throw notFound('TRANSACTION_NOT_FOUND', 'Transaction not found');
		}

		if (rows[0].raw_email_id !== null) {
			throw conflict(
				'TRANSACTION_NOT_MANUAL',
				'Only manual transactions can be deleted through this endpoint',
			);
		}

		await transactionSql`
			delete from public.transactions as t
			where t.id = ${transactionId}
				and t.user_id = ${userId}
		`;

		await transactionSql`
			update public.financial_events as fe
			set status = 'REVERSED'
			where fe.id = ${rows[0].financial_event_id}
				and fe.user_id = ${userId}
		`;
	});
}
