import type {
	BudgetCategoryRule,
	BudgetCategorySummary,
	BudgetMonthSummary,
	BudgetOverviewResponse,
	BudgetTreatment,
	ConfirmRecurringPatternRequest,
	EffectiveBudgetTreatment,
	EventDirection,
	CreateTransactionRelationshipRequest,
	DismissBudgetSuggestionRequest,
	RefundMatchSuggestion,
	RefundTargetOption,
	RecurringPatternSuggestion,
	RecurringSeriesSummary,
	SetCategoryBudgetRuleRequest,
	TransactionType,
	UpsertBudgetPlanRequest,
	UpsertMonthlyBudgetRequest,
	UpdateRecurringSeriesRequest,
} from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableString,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';
import { badRequest } from '../lib/http/errors';
import { asRecord, parsePositiveInteger, parseRequiredString, parseUuid } from '../lib/http/validation';
import {
	calculateBudget,
	type BudgetCalculationRelationship,
	type BudgetCalculationTransaction,
} from './budget-calculator';
import { rankRefundMatches, type RefundTargetCandidate } from './refund-matcher';
import { detectRecurringPatterns, type RecurringOccurrence } from './recurring-detector';

const MONTH_PATTERN = /^(\d{4})-(0[1-9]|1[0-2])$/;

interface BudgetLimitRowRaw {
	limit_in_paise: unknown;
	month_start: unknown;
}

interface CategoryRuleRowRaw {
	category_id: unknown;
	category_name: unknown;
	category_icon: unknown;
	treatment: unknown;
}

interface BudgetTransactionRowRaw {
	id: unknown;
	amount_in_paise: unknown;
	type: unknown;
	direction: unknown;
	txn_date: unknown;
	budget_treatment: unknown;
	category_id: unknown;
	category_name: unknown;
	category_icon: unknown;
	category_treatment: unknown;
	recurring_treatment: unknown;
	recurring_status: unknown;
}

interface RelationshipRowRaw {
	source_transaction_id: unknown;
	target_transaction_id: unknown;
}

interface RefundCandidateRowRaw {
	source_id: unknown;
	source_amount_in_paise: unknown;
	source_txn_date: unknown;
	source_card_id: unknown;
	source_merchant_id: unknown;
	source_search_key: unknown;
	target_id: unknown;
	target_amount_in_paise: unknown;
	target_remaining_in_paise: unknown;
	target_txn_date: unknown;
	target_card_id: unknown;
	target_merchant_id: unknown;
	target_search_key: unknown;
	target_label: unknown;
	target_budget_treatment: unknown;
	target_category_treatment: unknown;
	target_recurring_treatment: unknown;
	target_recurring_status: unknown;
}

interface RelationshipValidationRowRaw {
	source_type: unknown;
	source_direction: unknown;
	source_amount: unknown;
	source_date: unknown;
	target_type: unknown;
	target_direction: unknown;
	target_remaining: unknown;
	target_date: unknown;
}

interface RelationshipTargetRowRaw {
	target_id: unknown;
	target_label: unknown;
	target_amount: unknown;
	target_remaining: unknown;
	target_date: unknown;
}

interface RecurringCandidateRowRaw {
	id: unknown;
	amount_in_paise: unknown;
	txn_date: unknown;
	direction: unknown;
	merchant_id: unknown;
	search_key: unknown;
	category_id: unknown;
	label: unknown;
	category_treatment: unknown;
}

interface RecurringSeriesRowRaw {
	id: unknown;
	fingerprint: unknown;
	label: unknown;
	cadence: unknown;
	expected_day: unknown;
	amount_min_in_paise: unknown;
	amount_max_in_paise: unknown;
	budget_treatment: unknown;
	status: unknown;
	last_matched_at: unknown;
}

interface SuggestionDismissalRowRaw {
	kind: unknown;
	fingerprint: unknown;
}

interface ParsedBudgetTransaction extends BudgetCalculationTransaction {
	date: string;
	categoryId: string | null;
	categoryName: string;
	categoryIcon: string | null;
}

export function currentBudgetMonth(now = new Date()): string {
	const parts = new Intl.DateTimeFormat('en-CA', {
		timeZone: 'Asia/Kolkata',
		year: 'numeric',
		month: '2-digit',
	}).formatToParts(now);
	const year = parts.find((part) => part.type === 'year')?.value;
	const month = parts.find((part) => part.type === 'month')?.value;
	if (!year || !month) throw new Error('Could not resolve the current budget month');
	return `${year}-${month}`;
}

export function parseBudgetMonth(value: unknown): string {
	if (typeof value !== 'string' || !MONTH_PATTERN.test(value)) {
		throw badRequest('INVALID_MONTH', 'month must use YYYY-MM');
	}
	return value;
}

function monthBounds(month: string): { start: string; end: string; monthStart: string } {
	const parsed = MONTH_PATTERN.exec(month);
	if (!parsed) throw badRequest('INVALID_MONTH', 'month must use YYYY-MM');
	const year = Number(parsed[1]);
	const monthIndex = Number(parsed[2]) - 1;
	const next = new Date(Date.UTC(year, monthIndex + 1, 1));
	return {
		monthStart: `${month}-01`,
		start: `${month}-01T00:00:00+05:30`,
		end: `${next.getUTCFullYear()}-${String(next.getUTCMonth() + 1).padStart(2, '0')}-01T00:00:00+05:30`,
	};
}

function parseEffectiveTreatment(value: unknown, fieldName: string): EffectiveBudgetTreatment {
	if (value !== 'INCLUDE' && value !== 'EXCLUDE') {
		throw badRequest('SERIALIZATION_ERROR', `${fieldName} must be INCLUDE or EXCLUDE`);
	}
	return value;
}

function parseTreatment(value: unknown, fieldName: string): BudgetTreatment {
	if (value !== 'AUTO' && value !== 'INCLUDE' && value !== 'EXCLUDE') {
		throw badRequest('SERIALIZATION_ERROR', `${fieldName} has an invalid treatment`);
	}
	return value;
}

function parseTransaction(row: BudgetTransactionRowRaw): ParsedBudgetTransaction {
	const recurringTreatment = row.recurring_status === 'ACTIVE'
		? parseTreatment(row.recurring_treatment ?? 'AUTO', 'recurring_series.budget_treatment')
		: null;
	const categoryId = toNullableString(row.category_id, 'transactions.category_id');
	return {
		id: toRequiredString(row.id, 'transactions.id'),
		amountInPaise: toSafeInteger(row.amount_in_paise, 'transactions.amount_in_paise'),
		type: toRequiredString(row.type, 'transactions.type') as TransactionType,
		direction: toRequiredString(row.direction, 'financial_events.direction') as EventDirection,
		treatment: parseTreatment(row.budget_treatment, 'transactions.budget_treatment'),
		categoryTreatment: row.category_treatment === null || row.category_treatment === undefined
			? null
			: parseEffectiveTreatment(row.category_treatment, 'user_category_budget_rules.treatment'),
		recurringTreatment,
		date: toIsoDateTime(row.txn_date, 'transactions.txn_date'),
		categoryId,
		categoryName: categoryId === null
			? 'Uncategorized'
			: toRequiredString(row.category_name, 'categories.name'),
		categoryIcon: toNullableString(row.category_icon, 'categories.icon'),
	};
}

function parseRecurringOccurrence(row: RecurringCandidateRowRaw): RecurringOccurrence {
	return {
		id: toRequiredString(row.id, 'recurring_candidates.id'),
		amountInPaise: toSafeInteger(row.amount_in_paise, 'recurring_candidates.amount_in_paise'),
		date: toIsoDateTime(row.txn_date, 'recurring_candidates.txn_date'),
		direction: toRequiredString(row.direction, 'recurring_candidates.direction') as EventDirection,
		merchantId: toNullableString(row.merchant_id, 'recurring_candidates.merchant_id'),
		searchKey: toNullableString(row.search_key, 'recurring_candidates.search_key'),
		categoryId: toNullableString(row.category_id, 'recurring_candidates.category_id'),
		label: toRequiredString(row.label, 'recurring_candidates.label'),
		categoryTreatment: row.category_treatment === null || row.category_treatment === undefined
			? null
			: parseEffectiveTreatment(row.category_treatment, 'recurring_candidates.category_treatment'),
	};
}

function buildCategorySummaries(
	transactions: ParsedBudgetTransaction[],
	relationships: BudgetCalculationRelationship[],
	calculated: ReturnType<typeof calculateBudget>,
): BudgetCategorySummary[] {
	const transactionById = new Map(transactions.map((transaction) => [transaction.id, transaction]));
	const targetBySource = new Map(
		relationships.map((relationship) => [relationship.sourceTransactionId, relationship.targetTransactionId]),
	);
	const summaries = new Map<string, BudgetCategorySummary>();

	for (const transaction of transactions) {
		const resolved = calculated.transactions.get(transaction.id);
		if (!resolved || resolved.effect === 'NONE' || resolved.effectInPaise === 0) continue;
		const categoryOwner = resolved.effect === 'ADJUSTMENT'
			? transactionById.get(targetBySource.get(transaction.id) ?? '') ?? transaction
			: transaction;
		const key = categoryOwner.categoryId ?? 'uncategorized';
		const summary = summaries.get(key) ?? {
			category_id: categoryOwner.categoryId,
			category_name: categoryOwner.categoryName,
			category_icon: categoryOwner.categoryIcon,
			spent_in_paise: 0,
			adjustment_in_paise: 0,
			net_in_paise: 0,
			transaction_count: 0,
		};
		if (resolved.effect === 'SPEND') summary.spent_in_paise += resolved.effectInPaise;
		if (resolved.effect === 'ADJUSTMENT') summary.adjustment_in_paise += resolved.effectInPaise;
		summary.net_in_paise = Math.max(0, summary.spent_in_paise - summary.adjustment_in_paise);
		summary.transaction_count += 1;
		summaries.set(key, summary);
	}

	return [...summaries.values()].sort((left, right) => right.net_in_paise - left.net_in_paise);
}

export async function getBudgetOverview(
	sql: SqlClient,
	userId: string,
	month: string,
): Promise<BudgetOverviewResponse> {
	const { start, end, monthStart } = monthBounds(month);
	const [
		limitRows,
		ruleRows,
		transactionRows,
		relationshipRows,
		refundCandidateRows,
		recurringCandidateRows,
		recurringSeriesRows,
		dismissalRows,
	] = await Promise.all([
			sql<BudgetLimitRowRaw[]>`
			select mb.limit_in_paise, mb.month_start
			from public.monthly_budgets as mb
			where mb.user_id = ${userId}
				and mb.month_start <= ${monthStart}::date
			order by mb.month_start desc
			limit 1
		`,
		sql<CategoryRuleRowRaw[]>`
			select
				ucbr.category_id,
				c.name as category_name,
				c.icon as category_icon,
				ucbr.treatment
			from public.user_category_budget_rules as ucbr
			join public.categories as c on c.id = ucbr.category_id
			where ucbr.user_id = ${userId}
			order by c.name, c.id
		`,
		sql<BudgetTransactionRowRaw[]>`
			select
				t.id,
				t.amount_in_paise,
				t.type,
				fe.direction,
				t.txn_date,
				t.budget_treatment,
				t.category_id,
				c.name as category_name,
				c.icon as category_icon,
				ucbr.treatment as category_treatment,
				rs.budget_treatment as recurring_treatment,
				rs.status as recurring_status
			from public.transactions as t
			join public.financial_events as fe
				on fe.id = t.financial_event_id
				and fe.user_id = t.user_id
				and fe.status = 'ACTIVE'
			left join public.categories as c on c.id = t.category_id
			left join public.user_category_budget_rules as ucbr
				on ucbr.user_id = t.user_id
				and ucbr.category_id = t.category_id
			left join public.recurring_series as rs
				on rs.id = t.recurring_series_id
				and rs.user_id = t.user_id
			where t.user_id = ${userId}
				and t.user_deleted_at is null
				and (
					(
						t.txn_date >= ${start}
						and t.txn_date < ${end}
						and not exists (
							select 1
							from public.transaction_relationships as own_rel
							join public.transactions as own_target
								on own_target.id = own_rel.target_transaction_id
								and own_target.user_id = own_rel.user_id
								and own_target.user_deleted_at is null
							where own_rel.user_id = t.user_id
								and own_rel.source_transaction_id = t.id
						)
					)
					or exists (
						select 1
						from public.transaction_relationships as source_rel
						join public.transactions as target
							on target.id = source_rel.target_transaction_id
							and target.user_id = source_rel.user_id
							and target.user_deleted_at is null
						where source_rel.user_id = t.user_id
							and source_rel.source_transaction_id = t.id
							and target.txn_date >= ${start}
							and target.txn_date < ${end}
					)
				)
			order by t.txn_date, t.id
		`,
		sql<RelationshipRowRaw[]>`
			select rel.source_transaction_id, rel.target_transaction_id
			from public.transaction_relationships as rel
			join public.transactions as target
				on target.id = rel.target_transaction_id
				and target.user_id = rel.user_id
				and target.user_deleted_at is null
			join public.transactions as source
				on source.id = rel.source_transaction_id
				and source.user_id = rel.user_id
				and source.user_deleted_at is null
			where rel.user_id = ${userId}
				and target.txn_date >= ${start}
				and target.txn_date < ${end}
			order by source.txn_date, source.id
		`,
		sql<RefundCandidateRowRaw[]>`
			select
				source.id as source_id,
				source.amount_in_paise as source_amount_in_paise,
				source.txn_date as source_txn_date,
				source.credit_card_id as source_card_id,
				source.merchant_id as source_merchant_id,
				sfe.search_key as source_search_key,
				target.id as target_id,
				target.amount_in_paise as target_amount_in_paise,
				(target.amount_in_paise - refunded.refunded_in_paise)::bigint as target_remaining_in_paise,
				target.txn_date as target_txn_date,
				target.credit_card_id as target_card_id,
				target.merchant_id as target_merchant_id,
				tfe.search_key as target_search_key,
				coalesce(tgm.canonical_name, tfe.counterparty_raw, tc.name, 'Transaction') as target_label,
				target.budget_treatment as target_budget_treatment,
				tucbr.treatment as target_category_treatment,
				trs.budget_treatment as target_recurring_treatment,
				trs.status as target_recurring_status
			from public.transactions as source
			join public.financial_events as sfe
				on sfe.id = source.financial_event_id
				and sfe.user_id = source.user_id
				and sfe.status = 'ACTIVE'
			join public.transactions as target
				on target.user_id = source.user_id
				and target.id <> source.id
				and target.type = 'expense'
				and target.user_deleted_at is null
				and target.txn_date <= source.txn_date
				and target.txn_date >= source.txn_date - interval '120 days'
			join public.financial_events as tfe
				on tfe.id = target.financial_event_id
				and tfe.user_id = target.user_id
				and tfe.status = 'ACTIVE'
			left join public.global_merchants as tgm on tgm.id = target.merchant_id
			left join public.categories as tc on tc.id = target.category_id
			left join public.user_category_budget_rules as tucbr
				on tucbr.user_id = target.user_id
				and tucbr.category_id = target.category_id
			left join public.recurring_series as trs
				on trs.id = target.recurring_series_id
				and trs.user_id = target.user_id
			left join lateral (
				select coalesce(sum(linked.amount_in_paise), 0)::bigint as refunded_in_paise
				from public.transaction_relationships as linked_rel
				join public.transactions as linked
					on linked.id = linked_rel.source_transaction_id
					and linked.user_id = linked_rel.user_id
					and linked.user_deleted_at is null
				where linked_rel.user_id = target.user_id
					and linked_rel.target_transaction_id = target.id
			) as refunded on true
			left join public.transaction_relationships as existing_source_rel
				on existing_source_rel.user_id = source.user_id
				and existing_source_rel.source_transaction_id = source.id
			left join public.transactions as existing_source_target
				on existing_source_target.id = existing_source_rel.target_transaction_id
				and existing_source_target.user_id = existing_source_rel.user_id
				and existing_source_target.user_deleted_at is null
			left join public.budget_suggestion_dismissals as dismissed
				on dismissed.user_id = source.user_id
				and dismissed.kind = 'REFUND_MATCH'
				and dismissed.fingerprint = 'refund:' || source.id::text || ':' || target.id::text
			where source.user_id = ${userId}
				and source.type = 'income'
				and source.user_deleted_at is null
				and source.txn_date >= ${start}
				and source.txn_date < ${end}
				and (existing_source_rel.id is null or existing_source_target.id is null)
				and dismissed.fingerprint is null
				and source.amount_in_paise <= target.amount_in_paise - refunded.refunded_in_paise
				and (
					(
						source.credit_card_id is not null
						and source.credit_card_id = target.credit_card_id
					and (
						(source.merchant_id is not null and source.merchant_id = target.merchant_id)
						or (sfe.search_key is not null and sfe.search_key = tfe.search_key)
					)
				)
				or (
					(
						(source.merchant_id is not null and source.merchant_id = target.merchant_id)
						or (sfe.search_key is not null and sfe.search_key = tfe.search_key)
					)
					and source.amount_in_paise = target.amount_in_paise - refunded.refunded_in_paise
					)
				)
			order by source.txn_date desc, source.id, target.txn_date desc, target.id
			limit 120
		`,
		sql<RecurringCandidateRowRaw[]>`
			select
				t.id,
				t.amount_in_paise,
				t.txn_date,
				fe.direction,
				t.merchant_id,
				fe.search_key,
				t.category_id,
				coalesce(gm.canonical_name, fe.counterparty_raw, c.name, 'Recurring transaction') as label,
				ucbr.treatment as category_treatment
			from public.transactions as t
			join public.financial_events as fe
				on fe.id = t.financial_event_id
				and fe.user_id = t.user_id
				and fe.status = 'ACTIVE'
			left join public.global_merchants as gm on gm.id = t.merchant_id
			left join public.categories as c on c.id = t.category_id
			left join public.user_category_budget_rules as ucbr
				on ucbr.user_id = t.user_id
				and ucbr.category_id = t.category_id
			where t.user_id = ${userId}
				and t.type = 'expense'
				and fe.direction = 'debit'
				and t.user_deleted_at is null
				and t.recurring_series_id is null
				and t.txn_date >= ${end}::timestamptz - interval '400 days'
				and t.txn_date < ${end}
				and (t.merchant_id is not null or fe.search_key is not null)
			order by t.txn_date, t.id
		`,
		sql<RecurringSeriesRowRaw[]>`
			select
				rs.id,
				rs.fingerprint,
				coalesce(gm.canonical_name, rs.search_key, c.name, 'Recurring transaction') as label,
				rs.cadence,
				rs.expected_day,
				rs.amount_min_in_paise,
				rs.amount_max_in_paise,
				rs.budget_treatment,
				rs.status,
				rs.last_matched_at
			from public.recurring_series as rs
			left join public.global_merchants as gm on gm.id = rs.merchant_id
			left join public.categories as c on c.id = rs.category_id
			where rs.user_id = ${userId}
				and rs.status in ('ACTIVE', 'PAUSED')
			order by rs.status, rs.last_matched_at desc nulls last, rs.id
		`,
		sql<SuggestionDismissalRowRaw[]>`
			select kind, fingerprint
			from public.budget_suggestion_dismissals
			where user_id = ${userId}
				and kind = 'RECURRING_PATTERN'
		`,
	]);

	const transactions = transactionRows.map(parseTransaction);
	const relationships = relationshipRows.map((row): BudgetCalculationRelationship => ({
		sourceTransactionId: toRequiredString(
			row.source_transaction_id,
			'transaction_relationships.source_transaction_id',
		),
		targetTransactionId: toRequiredString(
			row.target_transaction_id,
			'transaction_relationships.target_transaction_id',
		),
	}));
	const calculated = calculateBudget(transactions, relationships);
	const limitInPaise = limitRows.length === 0
		? null
		: toSafeInteger(limitRows[0]?.limit_in_paise, 'monthly_budgets.limit_in_paise');
	const limitSourceMonth = limitRows.length === 0
		? null
		: toIsoDateTime(limitRows[0]?.month_start, 'monthly_budgets.month_start').slice(0, 7);
	const summary: BudgetMonthSummary = {
		month,
		limit_in_paise: limitInPaise,
		limit_source_month: limitSourceMonth,
		counted_spending_in_paise: calculated.countedSpendingInPaise,
		adjustments_in_paise: calculated.adjustmentsInPaise,
		used_in_paise: calculated.usedInPaise,
		remaining_in_paise: limitInPaise === null ? null : limitInPaise - calculated.usedInPaise,
		outside_plan_in_paise: calculated.outsidePlanInPaise,
		categories: buildCategorySummaries(transactions, relationships, calculated),
	};
	const categoryRules: BudgetCategoryRule[] = ruleRows.map((row) => ({
		category_id: toRequiredString(row.category_id, 'user_category_budget_rules.category_id'),
		category_name: toRequiredString(row.category_name, 'categories.name'),
		category_icon: toNullableString(row.category_icon, 'categories.icon'),
		treatment: parseEffectiveTreatment(row.treatment, 'user_category_budget_rules.treatment'),
	}));

	const refundRowsBySource = new Map<string, RefundCandidateRowRaw[]>();
	for (const row of refundCandidateRows) {
		const sourceId = toRequiredString(row.source_id, 'refund_suggestions.source_id');
		refundRowsBySource.set(sourceId, [...(refundRowsBySource.get(sourceId) ?? []), row]);
	}
	const refundSuggestions: RefundMatchSuggestion[] = [];
	for (const [sourceId, rows] of refundRowsBySource) {
		const first = rows[0];
		if (!first) continue;
		const sourceAmount = toSafeInteger(first.source_amount_in_paise, 'refund_suggestions.source_amount');
		const sourceDate = toIsoDateTime(first.source_txn_date, 'refund_suggestions.source_date');
		const matches = rankRefundMatches(
			{
				id: sourceId,
				amountInPaise: sourceAmount,
				date: sourceDate,
				cardId: toNullableString(first.source_card_id, 'refund_suggestions.source_card_id'),
				merchantId: toNullableString(first.source_merchant_id, 'refund_suggestions.source_merchant_id'),
				searchKey: toNullableString(first.source_search_key, 'refund_suggestions.source_search_key'),
			},
			rows.map((row): RefundTargetCandidate => ({
				id: toRequiredString(row.target_id, 'refund_suggestions.target_id'),
				amountInPaise: toSafeInteger(row.target_amount_in_paise, 'refund_suggestions.target_amount'),
				remainingInPaise: toSafeInteger(row.target_remaining_in_paise, 'refund_suggestions.target_remaining'),
				date: toIsoDateTime(row.target_txn_date, 'refund_suggestions.target_date'),
				cardId: toNullableString(row.target_card_id, 'refund_suggestions.target_card_id'),
				merchantId: toNullableString(row.target_merchant_id, 'refund_suggestions.target_merchant_id'),
				searchKey: toNullableString(row.target_search_key, 'refund_suggestions.target_search_key'),
				label: toRequiredString(row.target_label, 'refund_suggestions.target_label'),
			})),
		);
		for (const match of matches) {
			const targetRow = rows.find(row => row.target_id === match.target.id);
			if (!targetRow) continue;
			const targetCategoryTreatment = targetRow.target_category_treatment === null
				|| targetRow.target_category_treatment === undefined
				? null
				: parseEffectiveTreatment(
					targetRow.target_category_treatment,
					'refund_suggestions.target_category_treatment',
				);
			const targetRecurringTreatment = targetRow.target_recurring_status === 'ACTIVE'
				? parseTreatment(
					targetRow.target_recurring_treatment ?? 'AUTO',
					'refund_suggestions.target_recurring_treatment',
				)
				: null;
			const targetResolved = calculateBudget([{
				id: match.target.id,
				amountInPaise: match.target.amountInPaise,
				type: 'expense',
				direction: 'debit',
				treatment: parseTreatment(
					targetRow.target_budget_treatment,
					'refund_suggestions.target_budget_treatment',
				),
				categoryTreatment: targetCategoryTreatment,
				recurringTreatment: targetRecurringTreatment,
			}], []).transactions.get(match.target.id);
			refundSuggestions.push({
				type: 'REFUND_MATCH',
				fingerprint: `refund:${sourceId}:${match.target.id}`,
				confidence: match.confidence,
				source_transaction_id: sourceId,
				source_amount_in_paise: sourceAmount,
				source_txn_date: sourceDate,
				target_transaction_id: match.target.id,
				target_amount_in_paise: match.target.amountInPaise,
				target_remaining_in_paise: match.target.remainingInPaise,
				target_txn_date: match.target.date,
				label: match.target.label,
				can_auto_link: match.canAutoLink,
				budget_impact_in_paise:
					targetResolved?.effectiveTreatment === 'INCLUDE' ? sourceAmount : 0,
			});
		}
	}
	const dismissedRecurring = new Set(dismissalRows.map(row =>
		toRequiredString(row.fingerprint, 'budget_suggestion_dismissals.fingerprint'),
	));
	const persistedFingerprints = new Set(recurringSeriesRows.map(row =>
		toRequiredString(row.fingerprint, 'recurring_series.fingerprint'),
	));
	const recurringSuggestions: RecurringPatternSuggestion[] = detectRecurringPatterns(
		recurringCandidateRows.map(parseRecurringOccurrence),
	)
		.filter(pattern =>
			!dismissedRecurring.has(pattern.fingerprint)
			&& !persistedFingerprints.has(pattern.fingerprint),
		)
		.map(pattern => ({
			type: 'RECURRING_PATTERN',
			fingerprint: pattern.fingerprint,
			confidence: pattern.confidence,
			label: pattern.label,
			transaction_ids: pattern.occurrences.map(occurrence => occurrence.id),
			occurrence_count: pattern.occurrences.length,
			cadence: pattern.cadence,
			expected_day: pattern.expectedDay,
			amount_min_in_paise: pattern.amountMinInPaise,
			amount_max_in_paise: pattern.amountMaxInPaise,
			suggested_treatment: pattern.suggestedTreatment,
		}));
	const recurringSeries: RecurringSeriesSummary[] = recurringSeriesRows.map(row => {
		const cadence = toRequiredString(row.cadence, 'recurring_series.cadence');
		const status = toRequiredString(row.status, 'recurring_series.status');
		if (cadence !== 'WEEKLY' && cadence !== 'MONTHLY') {
			throw badRequest('SERIALIZATION_ERROR', 'recurring_series.cadence is invalid');
		}
		if (status !== 'ACTIVE' && status !== 'PAUSED') {
			throw badRequest('SERIALIZATION_ERROR', 'recurring_series.status is invalid');
		}
		return {
			id: toRequiredString(row.id, 'recurring_series.id'),
			fingerprint: toRequiredString(row.fingerprint, 'recurring_series.fingerprint'),
			label: toRequiredString(row.label, 'recurring_series.label'),
			cadence,
			expected_day: toSafeInteger(row.expected_day, 'recurring_series.expected_day'),
			amount_min_in_paise: toSafeInteger(row.amount_min_in_paise, 'recurring_series.amount_min'),
			amount_max_in_paise: toSafeInteger(row.amount_max_in_paise, 'recurring_series.amount_max'),
			budget_treatment: parseTreatment(row.budget_treatment, 'recurring_series.budget_treatment'),
			status,
			last_matched_at: row.last_matched_at === null
				? null
				: toIsoDateTime(row.last_matched_at, 'recurring_series.last_matched_at'),
		};
	});

	return {
		summary,
		category_rules: categoryRules,
		recurring_series: recurringSeries,
		suggestions: [...refundSuggestions, ...recurringSuggestions],
	};
}

export function parseUpsertMonthlyBudgetRequest(payload: unknown): UpsertMonthlyBudgetRequest {
	const body = asRecord(payload);
	return { limit_in_paise: parsePositiveInteger(body.limit_in_paise, 'limit_in_paise') };
}

export function parseUpsertBudgetPlanRequest(payload: unknown): UpsertBudgetPlanRequest {
	const body = asRecord(payload);
	if (!Array.isArray(body.category_rules) || body.category_rules.length > 100) {
		throw badRequest('INVALID_PAYLOAD', 'category_rules must be an array with at most 100 entries');
	}
	const seen = new Set<string>();
	const categoryRules = body.category_rules.map((value, index) => {
		const rule = asRecord(value);
		const categoryId = parseUuid(rule.category_id, `category_rules[${index}].category_id`);
		if (seen.has(categoryId)) {
			throw badRequest('INVALID_PAYLOAD', 'category_rules must not contain duplicate categories');
		}
		seen.add(categoryId);
		return {
			category_id: categoryId,
			treatment: rule.treatment === null
				? null
				: parseEffectiveTreatment(rule.treatment, `category_rules[${index}].treatment`),
		};
	});
	return {
		limit_in_paise: parsePositiveInteger(body.limit_in_paise, 'limit_in_paise'),
		category_rules: categoryRules,
	};
}

export function parseSetCategoryBudgetRuleRequest(payload: unknown): SetCategoryBudgetRuleRequest {
	const body = asRecord(payload);
	return {
		treatment: parseEffectiveTreatment(body.treatment, 'treatment'),
	};
}

export async function upsertMonthlyBudget(
	sql: SqlClient,
	userId: string,
	month: string,
	input: UpsertMonthlyBudgetRequest,
): Promise<void> {
	const { monthStart } = monthBounds(month);
	await sql`
		insert into public.monthly_budgets (user_id, month_start, limit_in_paise)
		values (${userId}, ${monthStart}::date, ${input.limit_in_paise})
		on conflict (user_id, month_start) do update
		set limit_in_paise = excluded.limit_in_paise,
			updated_at = now()
	`;
}

export async function upsertBudgetPlan(
	sql: SqlClient,
	userId: string,
	month: string,
	input: UpsertBudgetPlanRequest,
): Promise<void> {
	const { monthStart } = monthBounds(month);
	await sql.begin(async tx => {
		const transactionSql = tx as unknown as SqlClient;
		const categoryIds = input.category_rules.map(rule => rule.category_id);
		if (categoryIds.length > 0) {
			const availableRows = await transactionSql<Array<{ id: unknown }>>`
				select c.id
				from public.categories as c
				where c.id = any(${categoryIds}::uuid[])
					and (c.user_id = ${userId} or (c.user_id is null and c.is_system = true))
			`;
			if (availableRows.length !== categoryIds.length) {
				throw badRequest('CATEGORY_NOT_AVAILABLE', 'One or more categories are not available to this user');
			}
		}

		await transactionSql`
			insert into public.monthly_budgets (user_id, month_start, limit_in_paise)
			values (${userId}, ${monthStart}::date, ${input.limit_in_paise})
			on conflict (user_id, month_start) do update
			set limit_in_paise = excluded.limit_in_paise,
				updated_at = now()
		`;

		if (categoryIds.length === 0) return;
		await transactionSql`
			delete from public.user_category_budget_rules
			where user_id = ${userId}
				and category_id = any(${categoryIds}::uuid[])
		`;
		const explicitRules = input.category_rules.filter(
			(rule): rule is typeof rule & { treatment: EffectiveBudgetTreatment } => rule.treatment !== null,
		);
		if (explicitRules.length === 0) return;
		await transactionSql`
			insert into public.user_category_budget_rules (user_id, category_id, treatment)
			select
				${userId},
				rule.category_id,
				rule.treatment
			from unnest(
				${explicitRules.map(rule => rule.category_id)}::uuid[],
				${explicitRules.map(rule => rule.treatment)}::text[]
			) as rule(category_id, treatment)
		`;
	});
}

export async function setCategoryBudgetRule(
	sql: SqlClient,
	userId: string,
	categoryId: string,
	input: SetCategoryBudgetRuleRequest,
): Promise<void> {
	const rows = await sql<Array<{ id: unknown }>>`
		select c.id
		from public.categories as c
		where c.id = ${categoryId}
			and (c.user_id = ${userId} or (c.user_id is null and c.is_system = true))
		limit 1
	`;
	if (rows.length === 0) {
		throw badRequest('CATEGORY_NOT_AVAILABLE', 'Category is not available to this user');
	}
	await sql`
		insert into public.user_category_budget_rules (user_id, category_id, treatment)
		values (${userId}, ${categoryId}, ${input.treatment})
		on conflict (user_id, category_id) do update
		set treatment = excluded.treatment,
			updated_at = now()
	`;
}

export async function deleteCategoryBudgetRule(
	sql: SqlClient,
	userId: string,
	categoryId: string,
): Promise<void> {
	await sql`
		delete from public.user_category_budget_rules
		where user_id = ${userId} and category_id = ${categoryId}
	`;
}

export function parseCreateTransactionRelationshipRequest(
	payload: unknown,
): CreateTransactionRelationshipRequest {
	const body = asRecord(payload);
	const kind = body.kind;
	if (kind !== 'REFUND' && kind !== 'REIMBURSEMENT') {
		throw badRequest('INVALID_PAYLOAD', 'kind must be REFUND or REIMBURSEMENT');
	}
	return {
		source_transaction_id: parseUuid(body.source_transaction_id, 'source_transaction_id'),
		target_transaction_id: parseUuid(body.target_transaction_id, 'target_transaction_id'),
		kind,
	};
}

export function parseDismissBudgetSuggestionRequest(
	payload: unknown,
): DismissBudgetSuggestionRequest {
	const body = asRecord(payload);
	if (body.type !== 'REFUND_MATCH' && body.type !== 'RECURRING_PATTERN') {
		throw badRequest('INVALID_PAYLOAD', 'type must be REFUND_MATCH or RECURRING_PATTERN');
	}
	return {
		type: body.type,
		fingerprint: parseRequiredString(body.fingerprint, 'fingerprint'),
	};
}

export async function createTransactionRelationship(
	sql: SqlClient,
	userId: string,
	input: CreateTransactionRelationshipRequest,
): Promise<void> {
	if (input.source_transaction_id === input.target_transaction_id) {
		throw badRequest('INVALID_RELATIONSHIP', 'A transaction cannot be linked to itself');
	}

	await sql.begin(async tx => {
		const relationshipSql = tx as unknown as SqlClient;
		// Serialize changes against the same purchase so two partial credits
		// cannot both validate against the same remaining amount.
		await relationshipSql`
			select target.id
			from public.transactions as target
			where target.id = ${input.target_transaction_id}
				and target.user_id = ${userId}
				and target.user_deleted_at is null
			for update
		`;

		const validationRows = await relationshipSql<RelationshipValidationRowRaw[]>`
			select
				source.type as source_type,
				sfe.direction as source_direction,
				source.amount_in_paise as source_amount,
				source.txn_date as source_date,
				target.type as target_type,
				tfe.direction as target_direction,
				(target.amount_in_paise - coalesce(refunded.amount_in_paise, 0))::bigint as target_remaining,
				target.txn_date as target_date
			from public.transactions as source
			join public.financial_events as sfe
				on sfe.id = source.financial_event_id
				and sfe.user_id = source.user_id
				and sfe.status = 'ACTIVE'
			join public.transactions as target
				on target.id = ${input.target_transaction_id}
				and target.user_id = source.user_id
				and target.user_deleted_at is null
			join public.financial_events as tfe
				on tfe.id = target.financial_event_id
				and tfe.user_id = target.user_id
				and tfe.status = 'ACTIVE'
			left join lateral (
				select sum(linked.amount_in_paise)::bigint as amount_in_paise
				from public.transaction_relationships as existing_rel
				join public.transactions as linked
					on linked.id = existing_rel.source_transaction_id
					and linked.user_id = existing_rel.user_id
					and linked.user_deleted_at is null
				where existing_rel.user_id = target.user_id
					and existing_rel.target_transaction_id = target.id
					and existing_rel.source_transaction_id <> ${input.source_transaction_id}
			) as refunded on true
			where source.id = ${input.source_transaction_id}
				and source.user_id = ${userId}
				and source.user_deleted_at is null
			limit 1
		`;
		const row = validationRows[0];
		if (!row) throw badRequest('INVALID_RELATIONSHIP', 'Both transactions must be available');
		if (row.source_type !== 'income' || row.source_direction !== 'credit') {
			throw badRequest('INVALID_RELATIONSHIP', 'Refund source must be an incoming credit');
		}
		if (row.target_type !== 'expense' || row.target_direction !== 'debit') {
			throw badRequest('INVALID_RELATIONSHIP', 'Refund target must be an expense');
		}
		if (
			new Date(toIsoDateTime(row.source_date, 'relationships.source_date')).getTime()
			< new Date(toIsoDateTime(row.target_date, 'relationships.target_date')).getTime()
		) {
			throw badRequest('INVALID_RELATIONSHIP', 'Refund must be received after the expense');
		}
		if (
			toSafeInteger(row.source_amount, 'relationships.source_amount')
			> toSafeInteger(row.target_remaining, 'relationships.target_remaining')
		) {
			throw badRequest('REFUND_EXCEEDS_PURCHASE', 'Refund exceeds the purchase amount still available');
		}

		await relationshipSql`
			insert into public.transaction_relationships (
				user_id,
				source_transaction_id,
				target_transaction_id,
				kind,
				created_by,
				confidence
			)
			values (
				${userId},
				${input.source_transaction_id},
				${input.target_transaction_id},
				${input.kind},
				'USER',
				1
			)
			on conflict (source_transaction_id) do update
			set target_transaction_id = excluded.target_transaction_id,
				kind = excluded.kind,
				created_by = 'USER',
				confidence = 1,
				created_at = now()
		`;
	});
}

export async function deleteTransactionRelationship(
	sql: SqlClient,
	userId: string,
	relationshipId: string,
): Promise<void> {
	await sql`
		delete from public.transaction_relationships
		where id = ${relationshipId} and user_id = ${userId}
	`;
}

export async function listRelationshipTargets(
	sql: SqlClient,
	userId: string,
	sourceTransactionId: string,
): Promise<RefundTargetOption[]> {
	const rows = await sql<RelationshipTargetRowRaw[]>`
		select
			target.id as target_id,
			coalesce(gm.canonical_name, tfe.counterparty_raw, c.name, 'Transaction') as target_label,
			target.amount_in_paise as target_amount,
			(target.amount_in_paise - coalesce(refunded.amount_in_paise, 0))::bigint as target_remaining,
			target.txn_date as target_date
		from public.transactions as source
		join public.financial_events as sfe
			on sfe.id = source.financial_event_id
			and sfe.user_id = source.user_id
			and sfe.status = 'ACTIVE'
		join public.transactions as target
			on target.user_id = source.user_id
			and target.type = 'expense'
			and target.user_deleted_at is null
			and target.txn_date <= source.txn_date
			and target.txn_date >= source.txn_date - interval '120 days'
		join public.financial_events as tfe
			on tfe.id = target.financial_event_id
			and tfe.user_id = target.user_id
			and tfe.status = 'ACTIVE'
		left join public.global_merchants as gm on gm.id = target.merchant_id
		left join public.categories as c on c.id = target.category_id
		left join lateral (
			select sum(linked.amount_in_paise)::bigint as amount_in_paise
			from public.transaction_relationships as existing_rel
			join public.transactions as linked
				on linked.id = existing_rel.source_transaction_id
				and linked.user_id = existing_rel.user_id
				and linked.user_deleted_at is null
			where existing_rel.user_id = target.user_id
				and existing_rel.target_transaction_id = target.id
		) as refunded on true
		where source.id = ${sourceTransactionId}
			and source.user_id = ${userId}
			and source.type = 'income'
			and source.user_deleted_at is null
			and sfe.direction = 'credit'
			and source.amount_in_paise <= target.amount_in_paise - coalesce(refunded.amount_in_paise, 0)
		order by
			case when source.amount_in_paise = target.amount_in_paise - coalesce(refunded.amount_in_paise, 0) then 0 else 1 end,
			target.txn_date desc,
			target.id
		limit 30
	`;
	return rows.map(row => ({
		transaction_id: toRequiredString(row.target_id, 'relationship_targets.target_id'),
		label: toRequiredString(row.target_label, 'relationship_targets.target_label'),
		amount_in_paise: toSafeInteger(row.target_amount, 'relationship_targets.target_amount'),
		remaining_in_paise: toSafeInteger(row.target_remaining, 'relationship_targets.target_remaining'),
		txn_date: toIsoDateTime(row.target_date, 'relationship_targets.target_date'),
	}));
}

export async function dismissBudgetSuggestion(
	sql: SqlClient,
	userId: string,
	input: DismissBudgetSuggestionRequest,
): Promise<void> {
	await sql`
		insert into public.budget_suggestion_dismissals (user_id, kind, fingerprint)
		values (${userId}, ${input.type}, ${input.fingerprint})
		on conflict (user_id, kind, fingerprint) do update
		set dismissed_at = now()
	`;
}

export function parseConfirmRecurringPatternRequest(
	payload: unknown,
): ConfirmRecurringPatternRequest {
	const body = asRecord(payload);
	if (!Array.isArray(body.transaction_ids) || body.transaction_ids.length < 3 || body.transaction_ids.length > 50) {
		throw badRequest('INVALID_PAYLOAD', 'transaction_ids must contain between 3 and 50 transactions');
	}
	const transactionIds = [...new Set(body.transaction_ids.map((value, index) =>
		parseUuid(value, `transaction_ids[${index}]`),
	))];
	if (transactionIds.length < 3) {
		throw badRequest('INVALID_PAYLOAD', 'transaction_ids must contain at least 3 unique transactions');
	}
	return {
		fingerprint: parseRequiredString(body.fingerprint, 'fingerprint'),
		transaction_ids: transactionIds,
		treatment: parseEffectiveTreatment(body.treatment, 'treatment'),
	};
}

export function parseUpdateRecurringSeriesRequest(payload: unknown): UpdateRecurringSeriesRequest {
	const body = asRecord(payload);
	const result: UpdateRecurringSeriesRequest = {};
	if (body.budget_treatment !== undefined) {
		result.budget_treatment = parseTreatment(body.budget_treatment, 'budget_treatment');
	}
	if (body.status !== undefined) {
		if (body.status !== 'ACTIVE' && body.status !== 'PAUSED') {
			throw badRequest('INVALID_PAYLOAD', 'status must be ACTIVE or PAUSED');
		}
		result.status = body.status;
	}
	if (Object.keys(result).length === 0) {
		throw badRequest('INVALID_PAYLOAD', 'Provide budget_treatment or status');
	}
	return result;
}

export async function confirmRecurringPattern(
	sql: SqlClient,
	userId: string,
	input: ConfirmRecurringPatternRequest,
): Promise<void> {
	await sql.begin(async tx => {
		const transactionSql = tx as unknown as SqlClient;
		const rows = await transactionSql<RecurringCandidateRowRaw[]>`
			select
				t.id,
				t.amount_in_paise,
				t.txn_date,
				fe.direction,
				t.merchant_id,
				fe.search_key,
				t.category_id,
				coalesce(gm.canonical_name, fe.counterparty_raw, c.name, 'Recurring transaction') as label,
				ucbr.treatment as category_treatment
			from public.transactions as t
			join public.financial_events as fe
				on fe.id = t.financial_event_id
				and fe.user_id = t.user_id
				and fe.status = 'ACTIVE'
			left join public.global_merchants as gm on gm.id = t.merchant_id
			left join public.categories as c on c.id = t.category_id
			left join public.user_category_budget_rules as ucbr
				on ucbr.user_id = t.user_id
				and ucbr.category_id = t.category_id
			where t.user_id = ${userId}
				and t.id = any(${input.transaction_ids}::uuid[])
				and t.type = 'expense'
				and fe.direction = 'debit'
				and t.user_deleted_at is null
			order by t.txn_date, t.id
			for update of t
		`;
		if (rows.length !== input.transaction_ids.length) {
			throw badRequest('RECURRING_PATTERN_CHANGED', 'One or more pattern transactions are no longer available');
		}
		const patterns = detectRecurringPatterns(rows.map(parseRecurringOccurrence));
		const pattern = patterns.find(candidate => candidate.fingerprint === input.fingerprint);
		if (!pattern) {
			throw badRequest('RECURRING_PATTERN_CHANGED', 'Transactions no longer form this recurring pattern');
		}
		const confirmedTransactionIds = pattern.occurrences.map(occurrence => occurrence.id);
		if (
			confirmedTransactionIds.length !== input.transaction_ids.length
			|| confirmedTransactionIds.some(id => !input.transaction_ids.includes(id))
		) {
			throw badRequest(
				'RECURRING_PATTERN_CHANGED',
				'Transactions must exactly match one recurring pattern',
			);
		}
		const first = pattern.occurrences[0];
		if (!first) throw badRequest('RECURRING_PATTERN_CHANGED', 'Pattern has no occurrences');
		const lastMatchedAt = pattern.occurrences[pattern.occurrences.length - 1]?.date ?? first.date;

		const seriesRows = await transactionSql<Array<{ id: unknown }>>`
			insert into public.recurring_series (
				user_id,
				fingerprint,
				search_key,
				merchant_id,
				category_id,
				direction,
				cadence,
				expected_day,
				amount_min_in_paise,
				amount_max_in_paise,
				budget_treatment,
				status,
				last_matched_at
			)
			values (
				${userId},
				${pattern.fingerprint},
				${first.searchKey},
				${first.merchantId},
				${first.categoryId},
				${first.direction},
				${pattern.cadence},
				${pattern.expectedDay},
				${pattern.amountMinInPaise},
				${pattern.amountMaxInPaise},
				${input.treatment},
				'ACTIVE',
				${lastMatchedAt}
			)
			on conflict (user_id, fingerprint) do update
			set budget_treatment = excluded.budget_treatment,
				status = 'ACTIVE',
				last_matched_at = excluded.last_matched_at,
				updated_at = now()
			returning id
		`;
		const seriesId = toRequiredString(seriesRows[0]?.id, 'recurring_series.id');
		await transactionSql`
			update public.transactions
			set recurring_series_id = ${seriesId}
			where user_id = ${userId}
				and id = any(${confirmedTransactionIds}::uuid[])
				and user_deleted_at is null
		`;
	});
}

export async function updateRecurringSeries(
	sql: SqlClient,
	userId: string,
	seriesId: string,
	input: UpdateRecurringSeriesRequest,
): Promise<void> {
	const hasTreatment = input.budget_treatment !== undefined;
	const hasStatus = input.status !== undefined;
	const rows = await sql<Array<{ id: unknown }>>`
		update public.recurring_series as rs
		set budget_treatment = case
				when ${hasTreatment} then ${input.budget_treatment ?? null}
				else rs.budget_treatment
			end,
			status = case when ${hasStatus} then ${input.status ?? null} else rs.status end,
			updated_at = now()
		where rs.id = ${seriesId} and rs.user_id = ${userId}
		returning rs.id
	`;
	if (rows.length === 0) throw badRequest('RECURRING_SERIES_NOT_FOUND', 'Recurring rule was not found');
}
