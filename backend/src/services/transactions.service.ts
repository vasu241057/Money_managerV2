import type {
	AccountRow,
	BudgetTreatment,
	CategoryRow,
	ClassificationSource,
	CreateManualTransactionRequest,
	EventDirection,
	FinancialEventRow,
	GlobalMerchantRow,
	PaginatedResponse,
	PaymentMethod,
	RawEmailRow,
	ReviewTransactionRequest,
	ReviewTransactionResponse,
	TransactionFeedItem,
	TransactionBudgetRelationship,
	TransactionRow,
	TransactionStatus,
	UpdateTransactionRequest,
} from '../../../shared/types';
import { calculateBudget, type BudgetCalculationTransaction } from './budget-calculator';
import { backfillMerchantCategory } from './merchants.service';
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
	parseBoolean,
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
const BUDGET_TREATMENTS = new Set(['AUTO', 'INCLUDE', 'EXCLUDE'] as const);
const SEARCH_KEY_MAX_LENGTH = 120;

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
	transaction_budget_treatment: unknown;
	transaction_recurring_series_id: unknown;
	transaction_user_deleted_at: unknown;
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
	merchant_logo_public_url: unknown;
	merchant_logo_brand_color: unknown;

	raw_email_id: unknown;
	raw_email_source_id: unknown;
	raw_email_internal_date: unknown;
	raw_email_status: unknown;

	category_budget_treatment: unknown;
	recurring_budget_treatment: unknown;
	recurring_status: unknown;

	budget_relationship_id: unknown;
	budget_relationship_kind: unknown;
	budget_relationship_role: unknown;
	budget_related_transaction_id: unknown;
	budget_related_amount_in_paise: unknown;
	budget_related_calculation_amount_in_paise: unknown;
	budget_related_txn_date: unknown;
	budget_related_label: unknown;
	budget_relationship_created_by: unknown;
	budget_relationship_confidence: unknown;
	budget_relationship_related_count: unknown;
	budget_related_type: unknown;
	budget_related_direction: unknown;
	budget_related_treatment: unknown;
	budget_related_category_treatment: unknown;
	budget_related_recurring_treatment: unknown;
	budget_related_recurring_status: unknown;
}

interface TransactionStatusRow {
	transaction_id: string;
	transaction_status: TransactionStatus;
	financial_event_id: string;
	raw_email_id: string | null;
}

interface ReviewContextRow {
	transaction_id: string;
	transaction_status: TransactionStatus;
	transaction_category_id: string | null;
	transaction_merchant_id: string | null;
	transaction_type: TransactionRow['type'];
	financial_event_direction: EventDirection;
	budget_relationship_as_source: unknown;
	budget_relationship_as_target: unknown;
	financial_event_search_key: string | null;
}

interface TransactionUpdateStateRow extends TransactionStatusRow {
	transaction_category_id: string | null;
	transaction_type: TransactionRow['type'];
	financial_event_direction: EventDirection;
	budget_relationship_as_source: unknown;
	budget_relationship_as_target: unknown;
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
	q: string | null;
	from?: string;
	to?: string;
	status?: TransactionStatus;
	type?: TransactionRow['type'];
	account_id?: string;
	category_id?: string;
	credit_card_id?: string;
}

const MAX_TRANSACTION_SEARCH_LENGTH = 120;

function parseOptionalTransactionSearch(value: unknown): string | null {
	if (value === undefined) return null;
	if (typeof value !== 'string') {
		throw badRequest('INVALID_QUERY', 'q must be a string');
	}

	const normalized = value.trim();
	if (!normalized) return null;
	if (normalized.length > MAX_TRANSACTION_SEARCH_LENGTH) {
		throw badRequest(
			'INVALID_QUERY',
			`q must be at most ${MAX_TRANSACTION_SEARCH_LENGTH} characters`,
		);
	}
	return normalized;
}

function escapeSqlLikePattern(value: string): string {
	return value.replace(/[\\%_]/g, match => `\\${match}`);
}

export function parseTransactionSearchAmountInPaise(value: string | null): number | null {
	const compact = normalizeTransactionSearchAmount(value);
	if (compact === null) return null;

	const [rupees = '0', paise = ''] = compact.split('.');
	const amount = Number(rupees) * 100 + Number(paise.padEnd(2, '0'));
	return Number.isSafeInteger(amount) && amount > 0 ? amount : null;
}

export function normalizeTransactionSearchAmount(value: string | null): string | null {
	if (value === null) return null;
	const compact = value.replace(/[₹,\s]/g, '');
	if (!/^\d+(?:\.\d{1,2})?$/.test(compact)) return null;
	return compact;
}

function parseTransactionType(value: unknown, fieldName: string): TransactionRow['type'] {
	if (typeof value !== 'string' || !TRANSACTION_TYPES.has(value as TransactionRow['type'])) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be one of: income, expense, transfer`);
	}

	return value as TransactionRow['type'];
}

function parseEventDirection(value: unknown, fieldName: string): EventDirection {
	if (value !== 'debit' && value !== 'credit') {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be debit or credit`);
	}

	return value;
}

export function isTransactionTypeAllowedForDirection(
	type: TransactionRow['type'],
	direction: EventDirection,
): boolean {
	return type === 'transfer' || (direction === 'debit' ? type === 'expense' : type === 'income');
}

function assertTransactionTypeMatchesDirection(
	type: TransactionRow['type'],
	direction: EventDirection,
): void {
	if (!isTransactionTypeAllowedForDirection(type, direction)) {
		throw badRequest(
			'TRANSACTION_TYPE_DIRECTION_MISMATCH',
			`${direction} transactions may only be ${direction === 'debit' ? 'expense or transfer' : 'income or transfer'}`,
		);
	}
}

function assertTransactionTypeMatchesBudgetRelationship(
	type: TransactionRow['type'],
	relationshipAsSource: unknown,
	relationshipAsTarget: unknown,
): void {
	if (relationshipAsSource === true && type !== 'income') {
		throw badRequest(
			'TRANSACTION_RELATIONSHIP_TYPE_MISMATCH',
			'Unlink the refund or reimbursement before changing this incoming credit type',
		);
	}
	if (relationshipAsTarget === true && type !== 'expense') {
		throw badRequest(
			'TRANSACTION_RELATIONSHIP_TYPE_MISMATCH',
			'Unlink its refunds or reimbursements before changing this expense type',
		);
	}
}

export function resolveManualTransactionDirection(
	type: TransactionRow['type'],
	direction?: EventDirection,
): EventDirection {
	const resolved = direction ?? (type === 'income' ? 'credit' : 'debit');
	assertTransactionTypeMatchesDirection(type, resolved);
	return resolved;
}

async function assertCategoryMatchesTransactionType(
	sql: SqlClient,
	userId: string,
	categoryId: string | null,
	type: TransactionRow['type'],
): Promise<void> {
	if (!categoryId) return;

	const rows = await sql<Array<{ type: unknown }>>`
		select c.type
		from public.categories as c
		where c.id = ${categoryId}
			and (c.user_id = ${userId} or (c.user_id is null and c.is_system = true))
		limit 1
	`;
	if (rows.length === 0) {
		throw badRequest(
			'CATEGORY_NOT_AVAILABLE',
			'category_id must reference an available system or user category',
		);
	}

	const categoryType = toRequiredString(rows[0]?.type, 'categories.type');
	if (categoryType !== type) {
		throw badRequest(
			'TRANSACTION_CATEGORY_TYPE_MISMATCH',
			`A ${type} transaction requires a ${type} category`,
		);
	}
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

function parseBudgetTreatment(value: unknown, fieldName: string): BudgetTreatment {
	if (typeof value !== 'string' || !BUDGET_TREATMENTS.has(value as BudgetTreatment)) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be AUTO, INCLUDE, or EXCLUDE`);
	}

	return value as BudgetTreatment;
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

function normalizeSearchKey(value: string | null | undefined): string | null {
	if (!value) {
		return null;
	}

	const normalized = value
		.toUpperCase()
		.replace(/[^A-Z0-9]/g, '')
		.slice(0, SEARCH_KEY_MAX_LENGTH);
	return normalized.length > 0 ? normalized : null;
}

function mapTransactionFeedRow(row: TransactionFeedRowRaw): TransactionFeedItem {
	const transactionBudgetTreatment = row.transaction_budget_treatment === undefined
		? 'AUTO'
		: parseBudgetTreatment(row.transaction_budget_treatment, 'transactions.budget_treatment');
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
		budget_treatment: transactionBudgetTreatment,
		recurring_series_id: toNullableString(
			row.transaction_recurring_series_id,
			'transactions.recurring_series_id',
		),
		user_deleted_at: row.transaction_user_deleted_at === undefined
			? null
			: toNullableString(row.transaction_user_deleted_at, 'transactions.user_deleted_at'),
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
	const merchantLogoPublicUrl = toNullableString(
		row.merchant_logo_public_url,
		'merchant_logos.public_url',
	);
	const creditCardId = toNullableString(row.credit_card_id, 'user_credit_cards.id');
	const rawEmailId = toNullableString(row.raw_email_id, 'raw_emails.id');
	const relationshipId = toNullableString(
		row.budget_relationship_id,
		'transaction_relationships.id',
	);
	const relationshipRole = row.budget_relationship_role === 'SOURCE' ? 'SOURCE' : 'TARGET';
	const relationship: TransactionBudgetRelationship | null = relationshipId === null
		? null
		: {
				id: relationshipId,
				kind: toRequiredString(
					row.budget_relationship_kind,
					'transaction_relationships.kind',
				) as TransactionBudgetRelationship['kind'],
				role: relationshipRole,
				related_transaction_id: toRequiredString(
					row.budget_related_transaction_id,
					'transaction_relationships.related_transaction_id',
				),
				related_amount_in_paise: toSafeInteger(
					row.budget_related_amount_in_paise,
					'transaction_relationships.related_amount_in_paise',
				),
				related_txn_date: toIsoDateTime(
					row.budget_related_txn_date,
					'transaction_relationships.related_txn_date',
				),
				related_label: toRequiredString(
					row.budget_related_label,
					'transaction_relationships.related_label',
				),
				created_by: toRequiredString(
					row.budget_relationship_created_by,
					'transaction_relationships.created_by',
				) as TransactionBudgetRelationship['created_by'],
				confidence: toNullableNumber(
					row.budget_relationship_confidence,
					'transaction_relationships.confidence',
				) ?? 0,
				related_count: toSafeInteger(
					row.budget_relationship_related_count ?? 1,
					'transaction_relationships.related_count',
				),
			};

	const currentCalculation: BudgetCalculationTransaction = {
		id: transaction.id,
		amountInPaise: transaction.amount_in_paise,
		type: transaction.type,
		direction: financialEvent.direction,
		treatment: transaction.budget_treatment,
		categoryTreatment:
			row.category_budget_treatment === 'INCLUDE' || row.category_budget_treatment === 'EXCLUDE'
				? row.category_budget_treatment
				: null,
		recurringTreatment:
			row.recurring_status === 'ACTIVE'
				&& (row.recurring_budget_treatment === 'AUTO'
					|| row.recurring_budget_treatment === 'INCLUDE'
					|| row.recurring_budget_treatment === 'EXCLUDE')
					? row.recurring_budget_treatment
					: null,
	};
	const calculationTransactions = [currentCalculation];
	const calculationRelationships: Array<{
		sourceTransactionId: string;
		targetTransactionId: string;
	}> = [];
	if (relationship) {
		const relatedType = toRequiredString(
			row.budget_related_type,
			'transactions.related_type',
		) as TransactionRow['type'];
		const relatedDirection = toRequiredString(
			row.budget_related_direction,
			'financial_events.related_direction',
		) as EventDirection;
		calculationTransactions.push({
			id: relationship.related_transaction_id,
			amountInPaise: toSafeInteger(
				row.budget_related_calculation_amount_in_paise
					?? relationship.related_amount_in_paise,
				'transaction_relationships.related_calculation_amount_in_paise',
			),
			type: relatedType,
			direction: relatedDirection,
			treatment: parseBudgetTreatment(
				row.budget_related_treatment,
				'transactions.related_budget_treatment',
			),
			categoryTreatment:
				row.budget_related_category_treatment === 'INCLUDE'
					|| row.budget_related_category_treatment === 'EXCLUDE'
					? row.budget_related_category_treatment
					: null,
			recurringTreatment:
				row.budget_related_recurring_status === 'ACTIVE'
					&& (row.budget_related_recurring_treatment === 'AUTO'
						|| row.budget_related_recurring_treatment === 'INCLUDE'
						|| row.budget_related_recurring_treatment === 'EXCLUDE')
						? row.budget_related_recurring_treatment
						: null,
		});
		calculationRelationships.push({
			sourceTransactionId: relationship.role === 'SOURCE'
				? transaction.id
				: relationship.related_transaction_id,
			targetTransactionId: relationship.role === 'TARGET'
				? transaction.id
				: relationship.related_transaction_id,
		});
	}
	const budgetResult = calculateBudget(calculationTransactions, calculationRelationships);
	const resolvedBudget = budgetResult.transactions.get(transaction.id);
	if (!resolvedBudget) throw new Error('Budget treatment could not be resolved');

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
						first4: toNullableString(row.credit_card_first4, 'user_credit_cards.first4'),
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
						logo:
							merchantLogoPublicUrl === null
								? null
								: {
										public_url: merchantLogoPublicUrl,
										brand_color: toNullableString(
											row.merchant_logo_brand_color,
											'merchant_logos.brand_color',
										),
									},
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
		budget: {
			treatment: transaction.budget_treatment,
			effective_treatment: resolvedBudget.effectiveTreatment,
			treatment_source: resolvedBudget.treatmentSource,
			effect: resolvedBudget.effect,
			effect_in_paise: resolvedBudget.effectInPaise,
			relationship,
		},
	};
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
			t.budget_treatment as transaction_budget_treatment,
			t.recurring_series_id as transaction_recurring_series_id,
			t.user_deleted_at as transaction_user_deleted_at,
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
			ml.public_url as merchant_logo_public_url,
			ml.brand_color as merchant_logo_brand_color,

			re.id as raw_email_id,
			re.source_id as raw_email_source_id,
			re.internal_date as raw_email_internal_date,
			re.status as raw_email_status,

			ucbr.treatment as category_budget_treatment,
			rs.budget_treatment as recurring_budget_treatment,
			rs.status as recurring_status,

			br.relationship_id as budget_relationship_id,
			br.kind as budget_relationship_kind,
			br.role as budget_relationship_role,
			br.related_transaction_id as budget_related_transaction_id,
			br.related_amount_in_paise as budget_related_amount_in_paise,
			br.related_calculation_amount_in_paise as budget_related_calculation_amount_in_paise,
			br.related_txn_date as budget_related_txn_date,
			br.related_label as budget_related_label,
			br.created_by as budget_relationship_created_by,
			br.confidence as budget_relationship_confidence,
			br.related_count as budget_relationship_related_count,
			br.related_type as budget_related_type,
			br.related_direction as budget_related_direction,
			br.related_treatment as budget_related_treatment,
			br.related_category_treatment as budget_related_category_treatment,
			br.related_recurring_treatment as budget_related_recurring_treatment,
			br.related_recurring_status as budget_related_recurring_status
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
		left join public.merchant_logo_aliases as mla
			on mla.search_key = public.normalize_merchant_logo_key(gm.canonical_name)
		left join public.merchant_logos as ml
			on ml.id = mla.merchant_logo_id
			and ml.is_active = true
			and ml.public_url is not null
		left join public.raw_emails as re
			on re.id = fe.raw_email_id
			and re.user_id = fe.user_id
		left join public.user_category_budget_rules as ucbr
			on ucbr.user_id = t.user_id
			and ucbr.category_id = t.category_id
		left join public.recurring_series as rs
			on rs.id = t.recurring_series_id
			and rs.user_id = t.user_id
		left join lateral (
			select
				tr.id as relationship_id,
				tr.kind,
				case when tr.source_transaction_id = t.id then 'SOURCE' else 'TARGET' end as role,
				rt.id as related_transaction_id,
				rt.amount_in_paise as related_amount_in_paise,
				case
					when tr.source_transaction_id = t.id then greatest(
						0,
						rt.amount_in_paise - coalesce((
							select sum(other_source.amount_in_paise)
							from public.transaction_relationships as other_rel
							join public.transactions as other_source
								on other_source.id = other_rel.source_transaction_id
								and other_source.user_id = other_rel.user_id
								and other_source.user_deleted_at is null
							where other_rel.user_id = tr.user_id
								and other_rel.target_transaction_id = rt.id
								and other_rel.source_transaction_id <> t.id
						), 0)
					)
					else rt.amount_in_paise
				end as related_calculation_amount_in_paise,
				rt.txn_date as related_txn_date,
				coalesce(rgm.canonical_name, rfe.counterparty_raw, rc.name, 'Transaction') as related_label,
				tr.created_by,
				tr.confidence,
				count(*) over ()::int as related_count,
				rt.type as related_type,
				rfe.direction as related_direction,
				rt.budget_treatment as related_treatment,
				rucbr.treatment as related_category_treatment,
				rrs.budget_treatment as related_recurring_treatment,
				rrs.status as related_recurring_status
			from public.transaction_relationships as tr
			join public.transactions as rt
				on rt.id = case
					when tr.source_transaction_id = t.id then tr.target_transaction_id
					else tr.source_transaction_id
				end
				and rt.user_id = tr.user_id
				and rt.user_deleted_at is null
			join public.financial_events as rfe
				on rfe.id = rt.financial_event_id
				and rfe.user_id = rt.user_id
			left join public.categories as rc on rc.id = rt.category_id
			left join public.global_merchants as rgm on rgm.id = rt.merchant_id
			left join public.user_category_budget_rules as rucbr
				on rucbr.user_id = rt.user_id
				and rucbr.category_id = rt.category_id
			left join public.recurring_series as rrs
				on rrs.id = rt.recurring_series_id
				and rrs.user_id = rt.user_id
			where tr.user_id = t.user_id
				and (tr.source_transaction_id = t.id or tr.target_transaction_id = t.id)
			order by
				case when tr.source_transaction_id = t.id then 0 else 1 end,
				tr.created_at desc
			limit 1
		) as br on true
		where t.user_id = ${userId}
			and fe.status = 'ACTIVE'
			and t.user_deleted_at is null
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
	const q = parseOptionalTransactionSearch(query.q);
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
		q,
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
		direction:
			body.direction === undefined
				? undefined
				: parseEventDirection(body.direction, 'direction'),
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
		budget_treatment:
			body.budget_treatment === undefined
				? undefined
				: parseBudgetTreatment(body.budget_treatment, 'budget_treatment'),
	};
}

export function parseUpdateTransactionRequest(payload: unknown): UpdateTransactionRequest {
	const body = asRecord(payload);
	const updateRequest: UpdateTransactionRequest = {};

	if (body.type !== undefined) {
		updateRequest.type = parseTransactionType(body.type, 'type');
	}

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

	if (body.budget_treatment !== undefined) {
		updateRequest.budget_treatment = parseBudgetTreatment(
			body.budget_treatment,
			'budget_treatment',
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

export function parseReviewTransactionRequest(payload: unknown): ReviewTransactionRequest {
	const body = asRecord(payload);
	const reviewRequest: ReviewTransactionRequest = {};

	if (body.type !== undefined) {
		reviewRequest.type = parseTransactionType(body.type, 'type');
	}

	if (body.category_id !== undefined) {
		reviewRequest.category_id = parseNullableUuid(body.category_id, 'category_id');
	}

	if (body.merchant_id !== undefined) {
		reviewRequest.merchant_id = parseNullableUuid(body.merchant_id, 'merchant_id');
	}

	if (body.user_note !== undefined) {
		reviewRequest.user_note = parseNullableString(body.user_note, 'user_note');
	}

	if (body.apply_rule !== undefined) {
		reviewRequest.apply_rule = parseBoolean(body.apply_rule, 'apply_rule');
	}

	if (body.rule_search_key !== undefined) {
		reviewRequest.rule_search_key = parseNullableString(body.rule_search_key, 'rule_search_key');
	}

	return reviewRequest;
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
	const hasSearch = input.q !== null;
	const searchPattern = input.q === null ? null : `%${escapeSqlLikePattern(input.q)}%`;
	const searchAmount = normalizeTransactionSearchAmount(input.q);
	const searchAmountPattern = searchAmount === null
		? null
		: `%${escapeSqlLikePattern(searchAmount)}%`;
	const searchAmountInPaise = parseTransactionSearchAmountInPaise(input.q);
	const hasAmountSearch = searchAmount !== null;
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
		left join public.global_merchants as gm
			on gm.id = t.merchant_id
		where t.user_id = ${userId}
			and fe.status = 'ACTIVE'
			and t.user_deleted_at is null
			and (
				not ${hasSearch}
				or t.user_note ilike ${searchPattern} escape '\\'
				or gm.canonical_name ilike ${searchPattern} escape '\\'
				or fe.counterparty_raw ilike ${searchPattern} escape '\\'
				or fe.search_key ilike ${searchPattern} escape '\\'
				or (${hasAmountSearch} and (
					t.amount_in_paise = ${searchAmountInPaise}
					or to_char(t.amount_in_paise / 100.0, 'FM999999999999990.00')
						ilike ${searchAmountPattern} escape '\\'
				))
			)
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
			t.budget_treatment as transaction_budget_treatment,
			t.recurring_series_id as transaction_recurring_series_id,
			t.user_deleted_at as transaction_user_deleted_at,
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
			ml.public_url as merchant_logo_public_url,
			ml.brand_color as merchant_logo_brand_color,

			re.id as raw_email_id,
			re.source_id as raw_email_source_id,
			re.internal_date as raw_email_internal_date,
			re.status as raw_email_status,

			ucbr.treatment as category_budget_treatment,
			rs.budget_treatment as recurring_budget_treatment,
			rs.status as recurring_status,

			br.relationship_id as budget_relationship_id,
			br.kind as budget_relationship_kind,
			br.role as budget_relationship_role,
			br.related_transaction_id as budget_related_transaction_id,
			br.related_amount_in_paise as budget_related_amount_in_paise,
			br.related_calculation_amount_in_paise as budget_related_calculation_amount_in_paise,
			br.related_txn_date as budget_related_txn_date,
			br.related_label as budget_related_label,
			br.created_by as budget_relationship_created_by,
			br.confidence as budget_relationship_confidence,
			br.related_count as budget_relationship_related_count,
			br.related_type as budget_related_type,
			br.related_direction as budget_related_direction,
			br.related_treatment as budget_related_treatment,
			br.related_category_treatment as budget_related_category_treatment,
			br.related_recurring_treatment as budget_related_recurring_treatment,
			br.related_recurring_status as budget_related_recurring_status
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
		left join public.merchant_logo_aliases as mla
			on mla.search_key = public.normalize_merchant_logo_key(gm.canonical_name)
		left join public.merchant_logos as ml
			on ml.id = mla.merchant_logo_id
			and ml.is_active = true
			and ml.public_url is not null
		left join public.raw_emails as re
			on re.id = fe.raw_email_id
			and re.user_id = fe.user_id
		left join public.user_category_budget_rules as ucbr
			on ucbr.user_id = t.user_id
			and ucbr.category_id = t.category_id
		left join public.recurring_series as rs
			on rs.id = t.recurring_series_id
			and rs.user_id = t.user_id
		left join lateral (
			select
				tr.id as relationship_id,
				tr.kind,
				case when tr.source_transaction_id = t.id then 'SOURCE' else 'TARGET' end as role,
				rt.id as related_transaction_id,
				rt.amount_in_paise as related_amount_in_paise,
				case
					when tr.source_transaction_id = t.id then greatest(
						0,
						rt.amount_in_paise - coalesce((
							select sum(other_source.amount_in_paise)
							from public.transaction_relationships as other_rel
							join public.transactions as other_source
								on other_source.id = other_rel.source_transaction_id
								and other_source.user_id = other_rel.user_id
								and other_source.user_deleted_at is null
							where other_rel.user_id = tr.user_id
								and other_rel.target_transaction_id = rt.id
								and other_rel.source_transaction_id <> t.id
						), 0)
					)
					else rt.amount_in_paise
				end as related_calculation_amount_in_paise,
				rt.txn_date as related_txn_date,
				coalesce(rgm.canonical_name, rfe.counterparty_raw, rc.name, 'Transaction') as related_label,
				tr.created_by,
				tr.confidence,
				count(*) over ()::int as related_count,
				rt.type as related_type,
				rfe.direction as related_direction,
				rt.budget_treatment as related_treatment,
				rucbr.treatment as related_category_treatment,
				rrs.budget_treatment as related_recurring_treatment,
				rrs.status as related_recurring_status
			from public.transaction_relationships as tr
			join public.transactions as rt
				on rt.id = case
					when tr.source_transaction_id = t.id then tr.target_transaction_id
					else tr.source_transaction_id
				end
				and rt.user_id = tr.user_id
				and rt.user_deleted_at is null
			join public.financial_events as rfe
				on rfe.id = rt.financial_event_id
				and rfe.user_id = rt.user_id
			left join public.categories as rc on rc.id = rt.category_id
			left join public.global_merchants as rgm on rgm.id = rt.merchant_id
			left join public.user_category_budget_rules as rucbr
				on rucbr.user_id = rt.user_id
				and rucbr.category_id = rt.category_id
			left join public.recurring_series as rrs
				on rrs.id = rt.recurring_series_id
				and rrs.user_id = rt.user_id
			where tr.user_id = t.user_id
				and (tr.source_transaction_id = t.id or tr.target_transaction_id = t.id)
			order by
				case when tr.source_transaction_id = t.id then 0 else 1 end,
				tr.created_at desc
			limit 1
		) as br on true
		where t.user_id = ${userId}
			and fe.status = 'ACTIVE'
			and t.user_deleted_at is null
			and (
				not ${hasSearch}
				or t.user_note ilike ${searchPattern} escape '\\'
				or gm.canonical_name ilike ${searchPattern} escape '\\'
				or fe.counterparty_raw ilike ${searchPattern} escape '\\'
				or fe.search_key ilike ${searchPattern} escape '\\'
				or (${hasAmountSearch} and (
					t.amount_in_paise = ${searchAmountInPaise}
					or to_char(t.amount_in_paise / 100.0, 'FM999999999999990.00')
						ilike ${searchAmountPattern} escape '\\'
				))
			)
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
		const direction = resolveManualTransactionDirection(input.type, input.direction);
		await assertCategoryMatchesTransactionType(
			transactionSql,
			userId,
			input.category_id ?? null,
			input.type,
		);

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
				${direction},
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
				ai_confidence_score,
				budget_treatment
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
				null,
				${input.budget_treatment ?? 'AUTO'}
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
			t.category_id as transaction_category_id,
			t.type as transaction_type,
			fe.direction as financial_event_direction,
			exists (
				select 1 from public.transaction_relationships as source_rel
				where source_rel.user_id = t.user_id
					and source_rel.source_transaction_id = t.id
			) as budget_relationship_as_source,
			exists (
				select 1 from public.transaction_relationships as target_rel
				where target_rel.user_id = t.user_id
					and target_rel.target_transaction_id = t.id
			) as budget_relationship_as_target,
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
			and t.user_deleted_at is null
		limit 1
	`;

	if (statusRows.length === 0) {
		throw notFound('TRANSACTION_NOT_FOUND', 'Transaction not found');
	}

	const current = statusRows[0];
	if (input.status) {
		validateTransactionStatusTransition(current.transaction_status, input.status);
	}

	const nextType = input.type ?? current.transaction_type;
	const nextCategoryId =
		input.category_id === undefined
			? current.transaction_category_id
			: (input.category_id ?? null);
	if (input.type !== undefined) {
		assertTransactionTypeMatchesDirection(nextType, current.financial_event_direction);
		assertTransactionTypeMatchesBudgetRelationship(
			nextType,
			current.budget_relationship_as_source,
			current.budget_relationship_as_target,
		);
	}
	if (input.type !== undefined || input.category_id !== undefined) {
		await assertCategoryMatchesTransactionType(sql, userId, nextCategoryId, nextType);
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

	const hasTypeUpdate = input.type !== undefined;
	const hasAccountUpdate = relationUpdate.shouldUpdateAccount;
	const hasCategoryUpdate = input.category_id !== undefined;
	const hasMerchantUpdate = input.merchant_id !== undefined;
	const hasCreditCardUpdate = relationUpdate.shouldUpdateCreditCard;
	const hasUserNoteUpdate = input.user_note !== undefined;
	const hasStatusUpdate = input.status !== undefined;
	const hasClassificationSourceUpdate = input.classification_source !== undefined;
	const hasAiConfidenceScoreUpdate = input.ai_confidence_score !== undefined;
	const hasBudgetTreatmentUpdate = input.budget_treatment !== undefined;

	await sql`
		update public.transactions as t
		set
			type = case when ${hasTypeUpdate} then ${input.type ?? null} else t.type end,
			recurring_series_id = case
				when ${hasTypeUpdate} and ${input.type ?? null} is distinct from t.type then null
				else t.recurring_series_id
			end,
			account_id = case when ${hasAccountUpdate} then ${relationUpdate.accountId} else t.account_id end,
			category_id = case when ${hasCategoryUpdate} then ${input.category_id ?? null} else t.category_id end,
			merchant_id = case when ${hasMerchantUpdate} then ${input.merchant_id ?? null} else t.merchant_id end,
			credit_card_id = case when ${hasCreditCardUpdate} then ${relationUpdate.creditCardId} else t.credit_card_id end,
			user_note = case when ${hasUserNoteUpdate} then ${input.user_note ?? null} else t.user_note end,
			status = case when ${hasStatusUpdate} then ${input.status ?? null} else t.status end,
			classification_source = case
				when ${hasTypeUpdate} then 'USER'
				when ${hasClassificationSourceUpdate} then ${input.classification_source ?? null}
				else t.classification_source
			end,
			ai_confidence_score = case
				when ${hasTypeUpdate} then null
				when ${hasAiConfidenceScoreUpdate} then ${input.ai_confidence_score ?? null}
				else t.ai_confidence_score
			end,
			budget_treatment = case
				when ${hasBudgetTreatmentUpdate} then ${input.budget_treatment ?? null}
				else t.budget_treatment
			end
		where t.id = ${transactionId}
			and t.user_id = ${userId}
			and t.user_deleted_at is null
	`;

	const updatedTransaction = await getTransactionFeedItemById(sql, userId, transactionId);
	if (!updatedTransaction) {
		throw notFound('TRANSACTION_NOT_FOUND', 'Transaction not found');
	}

	return updatedTransaction;
}

export async function reviewTransaction(
	sql: SqlClient,
	userId: string,
	transactionId: string,
	input: ReviewTransactionRequest,
): Promise<ReviewTransactionResponse> {
	return sql.begin(async (tx) => {
		const transactionSql = tx as unknown as SqlClient;
		const contextRows = await transactionSql<ReviewContextRow[]>`
			select
				t.id as transaction_id,
				t.status as transaction_status,
				t.category_id as transaction_category_id,
				t.merchant_id as transaction_merchant_id,
				t.type as transaction_type,
				fe.direction as financial_event_direction,
				exists (
					select 1 from public.transaction_relationships as source_rel
					where source_rel.user_id = t.user_id
						and source_rel.source_transaction_id = t.id
				) as budget_relationship_as_source,
				exists (
					select 1 from public.transaction_relationships as target_rel
					where target_rel.user_id = t.user_id
						and target_rel.target_transaction_id = t.id
				) as budget_relationship_as_target,
				fe.search_key as financial_event_search_key
			from public.transactions as t
			join public.financial_events as fe
				on fe.id = t.financial_event_id
				and fe.user_id = t.user_id
				and fe.status = 'ACTIVE'
			where t.id = ${transactionId}
				and t.user_id = ${userId}
				and t.user_deleted_at is null
			limit 1
			for update
		`;

		if (contextRows.length === 0) {
			throw notFound('TRANSACTION_NOT_FOUND', 'Transaction not found');
		}

		const context = contextRows[0];
		if (context.transaction_status !== 'NEEDS_REVIEW') {
			throw conflict(
				'TRANSACTION_REVIEW_CLOSED',
				'Transaction no longer requires review',
			);
		}

		const hasCategoryUpdate = input.category_id !== undefined;
		const hasMerchantUpdate = input.merchant_id !== undefined;
		const hasUserNoteUpdate = input.user_note !== undefined;
		const hasTypeUpdate = input.type !== undefined;
		const nextType = input.type ?? context.transaction_type;

		const nextCategoryId =
			input.category_id !== undefined
				? (input.category_id ?? null)
				: context.transaction_category_id;
		const nextMerchantId =
			input.merchant_id !== undefined
				? (input.merchant_id ?? null)
				: context.transaction_merchant_id;

		assertTransactionTypeMatchesDirection(nextType, context.financial_event_direction);
		if (hasTypeUpdate) {
			assertTransactionTypeMatchesBudgetRelationship(
				nextType,
				context.budget_relationship_as_source,
				context.budget_relationship_as_target,
			);
		}
		if (hasTypeUpdate || hasCategoryUpdate) {
			await assertCategoryMatchesTransactionType(
				transactionSql,
				userId,
				nextCategoryId,
				nextType,
			);
		}

		await transactionSql`
			update public.transactions as t
			set
				type = case
					when ${hasTypeUpdate} then ${input.type ?? null}
					else t.type
				end,
				recurring_series_id = case
					when ${hasTypeUpdate} and ${input.type ?? null} is distinct from t.type then null
					else t.recurring_series_id
				end,
				category_id = case
					when ${hasCategoryUpdate} then ${input.category_id ?? null}
					else t.category_id
				end,
				merchant_id = case
					when ${hasMerchantUpdate} then ${input.merchant_id ?? null}
					else t.merchant_id
				end,
				user_note = case
					when ${hasUserNoteUpdate} then ${input.user_note ?? null}
					else t.user_note
				end,
				status = 'VERIFIED',
				classification_source = 'USER',
				ai_confidence_score = case when ${hasTypeUpdate} then null else t.ai_confidence_score end
			where t.id = ${transactionId}
				and t.user_id = ${userId}
				and t.user_deleted_at is null
				and t.status = 'NEEDS_REVIEW'
		`;

		const updatedTransaction = await getTransactionFeedItemById(transactionSql, userId, transactionId);
		if (!updatedTransaction) {
			throw notFound('TRANSACTION_NOT_FOUND', 'Transaction not found');
		}

		// Probation feedback for AI-created merchant mappings: a review that
		// KEEPS the alias-suggested merchant confirms the mapping globally
		// (auto-verify turns on for later hits); a review that replaces or
		// clears it dissents — the mapping demotes back to probation.
		// Threshold 1 while single-user; revisit as voting for multi-tenant.
		const probationSearchKey = normalizeSearchKey(context.financial_event_search_key);
		if (probationSearchKey) {
			const aliasRows = await transactionSql<Array<{ merchant_id: unknown; source: unknown }>>`
				select gma.merchant_id, gm.source
				from public.global_merchant_aliases as gma
				join public.global_merchants as gm on gm.id = gma.merchant_id
				where gma.search_key = ${probationSearchKey}
				limit 1
			`;
			const aliasRow = aliasRows[0];
			// Only AI-sourced mappings live on probation; seed/manual are
			// human-owned and never auto-demote.
			if (aliasRow && aliasRow.source === 'AI') {
				const aliasMerchantId = toRequiredString(
					aliasRow.merchant_id,
					'global_merchant_aliases.merchant_id',
				);
				if (nextMerchantId === aliasMerchantId) {
					await transactionSql`
						update public.global_merchants as gm
						set confirmed = true, updated_at = now()
						where gm.id = ${aliasMerchantId}
							and gm.confirmed = false
					`;
				} else if (hasMerchantUpdate) {
					await transactionSql`
						update public.global_merchants as gm
						set confirmed = false, dissent_count = gm.dissent_count + 1, updated_at = now()
						where gm.id = ${aliasMerchantId}
					`;
				}
			}
		}

		const shouldApplyRule = input.apply_rule === true;
		if (!shouldApplyRule) {
			return {
				transaction: updatedTransaction,
				rule_applied: false,
				applied_search_key: null,
				backfilled_count: 0,
			};
		}

		const explicitSearchKey = normalizeSearchKey(input.rule_search_key);
		const eventSearchKey = normalizeSearchKey(context.financial_event_search_key);
		const appliedSearchKey = explicitSearchKey ?? eventSearchKey;
		if (!appliedSearchKey) {
			throw badRequest(
				'REVIEW_RULE_SEARCH_KEY_REQUIRED',
				'Cannot apply review rule because no search key is available',
			);
		}

		if (!nextCategoryId && !nextMerchantId) {
			throw badRequest(
				'REVIEW_RULE_TARGET_REQUIRED',
				'Cannot apply review rule without category_id or merchant_id',
			);
		}

		const ruleRows = await transactionSql<Array<{ id: unknown }>>`
			insert into public.user_merchant_rules (
				user_id,
				search_key,
				merchant_id,
				custom_category_id
			)
			values (
				${userId},
				${appliedSearchKey},
				${nextMerchantId},
				${nextCategoryId}
			)
			on conflict (user_id, search_key) do update
			set
				merchant_id = excluded.merchant_id,
				custom_category_id = excluded.custom_category_id,
				updated_at = now()
			returning id
		`;

		const ruleId = toRequiredString(ruleRows[0]?.id, 'user_merchant_rules.id');
		if (nextCategoryId) {
			await transactionSql`
				insert into public.user_merchant_rule_categories (
					user_merchant_rule_id,
					transaction_type,
					category_id
				)
				values (${ruleId}, ${nextType}, ${nextCategoryId})
				on conflict (user_merchant_rule_id, transaction_type) do update
				set category_id = excluded.category_id,
					updated_at = now()
			`;
		}

		// The search-key rule above only ever covers the exact counterparty
		// string this transaction arrived with, but one merchant owns many
		// aliases — which is why the same merchant kept coming back for review.
		// A merchant-level default closes that, and carrying it backwards over
		// the transactions already on file means the merchant is answered once.
		// Aggregators stay out for the same reason they skip the search-key
		// rule: the gateway is not the real merchant behind the charge.
		let backfilledCount = 0;
		if (nextMerchantId && nextCategoryId) {
			const merchantRows = await transactionSql<Array<{ type: unknown }>>`
				select gm.type
				from public.global_merchants as gm
				where gm.id = ${nextMerchantId}
				limit 1
			`;
			if (merchantRows[0]?.type !== 'AGGREGATOR') {
				await transactionSql`
					insert into public.user_merchant_category_rules (
						user_id,
						merchant_id,
						transaction_type,
						category_id
					)
					values (${userId}, ${nextMerchantId}, ${nextType}, ${nextCategoryId})
					on conflict (user_id, merchant_id, transaction_type) do update
					set category_id = excluded.category_id,
						updated_at = now()
				`;
				backfilledCount = await backfillMerchantCategory(
					transactionSql,
					userId,
					nextMerchantId,
					nextType,
					nextCategoryId,
					'AI_ONLY',
					transactionId,
				);
			}
		}

		return {
			transaction: updatedTransaction,
			rule_applied: true,
			applied_search_key: appliedSearchKey,
			backfilled_count: backfilledCount,
		};
	});
}

export async function deleteTransaction(
	sql: SqlClient,
	userId: string,
	transactionId: string,
): Promise<void> {
	const rows = await sql<Array<{ id: unknown }>>`
		update public.transactions as t
		set user_deleted_at = now()
		where t.id = ${transactionId}
			and t.user_id = ${userId}
			and t.user_deleted_at is null
		returning t.id
	`;
	if (rows.length === 0) {
		throw notFound('TRANSACTION_NOT_FOUND', 'Transaction not found');
	}
}

export async function restoreTransaction(
	sql: SqlClient,
	userId: string,
	transactionId: string,
): Promise<void> {
	const rows = await sql<Array<{ id: unknown }>>`
		update public.transactions as t
		set user_deleted_at = null
		where t.id = ${transactionId}
			and t.user_id = ${userId}
			and t.user_deleted_at is not null
		returning t.id
	`;
	if (rows.length === 0) {
		throw notFound('TRANSACTION_NOT_FOUND', 'Deleted transaction not found');
	}
}
