import type {
	GlobalMerchantListItem,
	SetUserDescriptorCategoryRequest,
	SetUserMerchantCategoryRequest,
	TransactionStatus,
	TransactionType,
	UserMerchantCategoryBreakdown,
	UserMerchantDescriptorBreakdown,
	UserMerchantListItem,
	UserMerchantTransactionItem,
} from '../../../shared/types';
import {
	toIsoDateTime,
	toNullableString,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';
import type { SqlClient } from '../lib/db/client';
import { badRequest, notFound } from '../lib/http/errors';
import { asRecord, parseLimit, parseRequiredString, parseUuid } from '../lib/http/validation';

const MAX_LIST_MERCHANTS_LIMIT = 1000;
const DEFAULT_LIST_MERCHANTS_LIMIT = 200;
const MAX_SEARCH_QUERY_LENGTH = 120;
const MAX_USER_MERCHANTS_LIMIT = 500;
const MAX_MERCHANT_TRANSACTIONS_LIMIT = 500;
const MAX_SEARCH_KEY_LENGTH = 200;

interface ParsedListGlobalMerchantsQuery {
	q: string | null;
	limit: number;
}

interface MerchantListRowRaw {
	merchant_id: unknown;
	merchant_canonical_name: unknown;
	merchant_type: unknown;
}

interface UserMerchantRowRaw {
	merchant_id: unknown;
	canonical_name: unknown;
	merchant_type: unknown;
	logo_url: unknown;
	transaction_type: unknown;
	transaction_count: unknown;
	rule_category_id: unknown;
	rule_category_name: unknown;
	common_category_id: unknown;
	common_category_name: unknown;
}

interface UserMerchantDescriptorRowRaw {
	merchant_id: unknown;
	search_key: unknown;
	label: unknown;
	transaction_type: unknown;
	transaction_count: unknown;
	rule_category_id: unknown;
	rule_category_name: unknown;
	common_category_id: unknown;
	common_category_name: unknown;
}

interface UserMerchantTransactionRowRaw {
	transaction_id: unknown;
	txn_date: unknown;
	amount_in_paise: unknown;
	type: unknown;
	status: unknown;
	category_name: unknown;
	descriptor: unknown;
}

function parseTransactionType(value: unknown, fieldName: string): TransactionType {
	if (value !== 'income' && value !== 'expense' && value !== 'transfer') {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be income, expense or transfer`);
	}
	return value;
}

function parseOptionalSearchQuery(value: unknown): string | null {
	if (value === undefined) {
		return null;
	}

	if (typeof value !== 'string') {
		throw badRequest('INVALID_QUERY', 'q must be a string');
	}

	const normalized = value.trim();
	if (normalized.length === 0) {
		return null;
	}
	if (normalized.length > MAX_SEARCH_QUERY_LENGTH) {
		throw badRequest(
			'INVALID_QUERY',
			`q must be at most ${MAX_SEARCH_QUERY_LENGTH} characters`,
		);
	}

	return normalized;
}

function escapeSqlLikePattern(value: string): string {
	return value.replace(/[\\%_]/g, (match) => `\\${match}`);
}

export function parseListGlobalMerchantsQuery(
	query: Record<string, unknown>,
): ParsedListGlobalMerchantsQuery {
	const limit =
		query.limit === undefined
			? DEFAULT_LIST_MERCHANTS_LIMIT
			: parseLimit(query.limit, MAX_LIST_MERCHANTS_LIMIT);

	return {
		q: parseOptionalSearchQuery(query.q),
		limit,
	};
}

/**
 * Rejects a merchant the user cannot see: unknown, or a private payee owned
 * by someone else. Every write that accepts a merchant id calls this before
 * touching anything, so the caller gets a clean 404 instead of the scope
 * trigger's constraint error.
 */
export async function assertMerchantVisibleToUser(
	sql: SqlClient,
	userId: string,
	merchantId: string,
): Promise<void> {
	const rows = await sql<Array<{ id: unknown }>>`
		select gm.id
		from public.global_merchants as gm
		where gm.id = ${merchantId}
			and (gm.owner_user_id is null or gm.owner_user_id = ${userId})
		limit 1
	`;
	if (rows.length === 0) {
		throw notFound('MERCHANT_NOT_FOUND', 'Merchant not found');
	}
}

export type MerchantConfirmationVerdict = 'CONFIRM' | 'DISSENT';

export interface MerchantConfirmationState {
	confirmed: boolean;
	confirm_count: number;
	dissent_count: number;
}

/**
 * Records one user's verdict on an AI merchant mapping and recomputes the
 * merchant's confirmed / dissent_count caches from every user's verdict:
 * confirmed when at least `threshold` distinct users confirmed and fewer than
 * `threshold` dissented. A user changing their mind replaces their own row,
 * so nobody can vote twice. Threshold 1 is today's single-user behaviour.
 */
export async function recordMerchantConfirmation(
	sql: SqlClient,
	userId: string,
	merchantId: string,
	verdict: MerchantConfirmationVerdict,
	threshold: number,
): Promise<MerchantConfirmationState> {
	await sql`
		insert into public.merchant_confirmations (merchant_id, user_id, verdict)
		values (${merchantId}, ${userId}, ${verdict})
		on conflict (merchant_id, user_id) do update
		set verdict = excluded.verdict,
			updated_at = now()
	`;
	const rows = await sql<Array<{ confirmed: unknown; confirms: unknown; dissents: unknown }>>`
		with tally as (
			select
				count(*) filter (where mc.verdict = 'CONFIRM')::int as confirms,
				count(*) filter (where mc.verdict = 'DISSENT')::int as dissents
			from public.merchant_confirmations as mc
			where mc.merchant_id = ${merchantId}
		)
		update public.global_merchants as gm
		set confirmed = (tally.confirms >= ${threshold} and tally.dissents < ${threshold}),
			dissent_count = tally.dissents,
			updated_at = now()
		from tally
		where gm.id = ${merchantId}
		returning gm.confirmed, tally.confirms, tally.dissents
	`;
	const row = rows[0];
	if (!row) {
		throw notFound('MERCHANT_NOT_FOUND', 'Merchant not found');
	}
	return {
		confirmed: row.confirmed === true,
		confirm_count: toSafeInteger(row.confirms, 'merchant_confirmations.confirms'),
		dissent_count: toSafeInteger(row.dissents, 'merchant_confirmations.dissents'),
	};
}

/** Shared brands plus this user's own private payees; never another user's. */
export async function listGlobalMerchants(
	sql: SqlClient,
	userId: string,
	input: ParsedListGlobalMerchantsQuery,
): Promise<GlobalMerchantListItem[]> {
	const hasSearch = input.q !== null;
	const normalizedPattern = input.q === null ? null : `%${escapeSqlLikePattern(input.q)}%`;

	const rows = await sql<MerchantListRowRaw[]>`
		select
			gm.id as merchant_id,
			gm.canonical_name as merchant_canonical_name,
			gm.type as merchant_type
		from public.global_merchants as gm
		where (gm.owner_user_id is null or gm.owner_user_id = ${userId})
			and (
				not ${hasSearch}
				or gm.canonical_name ilike ${normalizedPattern} escape '\\'
			)
		order by gm.canonical_name asc, gm.id asc
		limit ${input.limit}
	`;

	return rows.map((row) => ({
		id: toRequiredString(row.merchant_id, 'global_merchants.id'),
		canonical_name: toRequiredString(
			row.merchant_canonical_name,
			'global_merchants.canonical_name',
		),
		type: toRequiredString(row.merchant_type, 'global_merchants.type') as GlobalMerchantListItem['type'],
	}));
}

/**
 * Merchants this user has actually transacted with, busiest first, with each
 * transaction type kept separate: one merchant legitimately owns a different
 * category per direction (a Swiggy order is expense, its refund is income).
 */
export async function listUserMerchants(
	sql: SqlClient,
	userId: string,
	limit: number,
): Promise<UserMerchantListItem[]> {
	const rows = await sql<UserMerchantRowRaw[]>`
		with owned as (
			select t.merchant_id, t.type, t.category_id
			from public.transactions as t
			where t.user_id = ${userId}
				and t.user_deleted_at is null
				and t.merchant_id is not null
		),
		per_type as (
			select merchant_id, type, count(*)::bigint as transaction_count
			from owned
			group by merchant_id, type
		),
		totals as (
			select merchant_id, sum(transaction_count)::bigint as total_count
			from per_type
			group by merchant_id
			order by total_count desc, merchant_id
			limit ${limit}
		),
		common as (
			select distinct on (merchant_id, type)
				merchant_id,
				type,
				category_id
			from (
				select merchant_id, type, category_id, count(*)::bigint as hits
				from owned
				where category_id is not null
				group by merchant_id, type, category_id
			) as tallied
			order by merchant_id, type, hits desc, category_id
		)
		select
			pt.merchant_id,
			gm.canonical_name,
			gm.type as merchant_type,
			ml.public_url as logo_url,
			pt.type as transaction_type,
			pt.transaction_count,
			umcr.category_id as rule_category_id,
			rc.name as rule_category_name,
			cm.category_id as common_category_id,
			cc.name as common_category_name
		from per_type as pt
		join totals as tot on tot.merchant_id = pt.merchant_id
		join public.global_merchants as gm on gm.id = pt.merchant_id
		left join public.merchant_logo_aliases as mla
			on mla.search_key = public.normalize_merchant_logo_key(gm.canonical_name)
		left join public.merchant_logos as ml
			on ml.id = mla.merchant_logo_id
			and ml.is_active = true
		left join common as cm
			on cm.merchant_id = pt.merchant_id
			and cm.type = pt.type
		left join public.categories as cc on cc.id = cm.category_id
		left join public.user_merchant_category_rules as umcr
			on umcr.user_id = ${userId}
			and umcr.merchant_id = pt.merchant_id
			and umcr.transaction_type = pt.type
		left join public.categories as rc on rc.id = umcr.category_id
		order by tot.total_count desc, gm.canonical_name asc, pt.merchant_id, pt.transaction_count desc, pt.type
	`;

	// The raw counterparties behind each merchant, with any search-key rule the
	// user already owns. Kept as a second pass because the descriptor grain is
	// one level below the per-type grain above.
	const descriptorRows = await sql<UserMerchantDescriptorRowRaw[]>`
		with owned as (
			select
				t.merchant_id,
				t.type,
				t.category_id,
				fe.search_key,
				fe.counterparty_raw
			from public.transactions as t
			join public.financial_events as fe
				on fe.id = t.financial_event_id
				and fe.user_id = t.user_id
			where t.user_id = ${userId}
				and t.user_deleted_at is null
				and t.merchant_id is not null
				and fe.search_key is not null
		),
		per_descriptor as (
			select
				merchant_id,
				search_key,
				type,
				count(*)::bigint as transaction_count,
				min(coalesce(counterparty_raw, search_key)) as label
			from owned
			group by merchant_id, search_key, type
		),
		common as (
			select distinct on (search_key, type)
				search_key,
				type,
				category_id
			from (
				select search_key, type, category_id, count(*)::bigint as hits
				from owned
				where category_id is not null
				group by search_key, type, category_id
			) as tallied
			order by search_key, type, hits desc, category_id
		)
		select
			pd.merchant_id,
			pd.search_key,
			pd.label,
			pd.type as transaction_type,
			pd.transaction_count,
			umrc.category_id as rule_category_id,
			rc.name as rule_category_name,
			cm.category_id as common_category_id,
			cc.name as common_category_name
		from per_descriptor as pd
		left join common as cm on cm.search_key = pd.search_key and cm.type = pd.type
		left join public.categories as cc on cc.id = cm.category_id
		left join public.user_merchant_rules as umr
			on umr.user_id = ${userId}
			and umr.search_key = pd.search_key
		left join public.user_merchant_rule_categories as umrc
			on umrc.user_merchant_rule_id = umr.id
			and umrc.transaction_type = pd.type
		left join public.categories as rc on rc.id = umrc.category_id
		order by pd.merchant_id, pd.transaction_count desc, pd.search_key, pd.type
	`;

	const merchants = new Map<string, UserMerchantListItem>();
	for (const row of rows) {
		const merchantId = toRequiredString(row.merchant_id, 'transactions.merchant_id');
		const merchant = merchants.get(merchantId) ?? {
			merchant_id: merchantId,
			canonical_name: toRequiredString(row.canonical_name, 'global_merchants.canonical_name'),
			type: toRequiredString(row.merchant_type, 'global_merchants.type') as UserMerchantListItem['type'],
			logo_url: toNullableString(row.logo_url, 'merchant_logos.public_url'),
			transaction_count: 0,
			breakdown: [] as UserMerchantCategoryBreakdown[],
			descriptors: [] as UserMerchantDescriptorBreakdown[],
		};
		const transactionCount = toSafeInteger(row.transaction_count, 'transactions.transaction_count');
		merchant.transaction_count += transactionCount;
		merchant.breakdown.push({
			transaction_type: parseTransactionType(row.transaction_type, 'transactions.type'),
			transaction_count: transactionCount,
			rule_category_id: toNullableString(
				row.rule_category_id,
				'user_merchant_category_rules.category_id',
			),
			rule_category_name: toNullableString(row.rule_category_name, 'categories.name'),
			common_category_id: toNullableString(row.common_category_id, 'transactions.category_id'),
			common_category_name: toNullableString(row.common_category_name, 'categories.name'),
		});
		merchants.set(merchantId, merchant);
	}

	for (const row of descriptorRows) {
		// Descriptors are listed for every merchant the page returned; ones
		// belonging to merchants past the limit are simply skipped.
		const merchant = merchants.get(toRequiredString(row.merchant_id, 'transactions.merchant_id'));
		if (!merchant) continue;
		merchant.descriptors.push({
			search_key: toRequiredString(row.search_key, 'financial_events.search_key'),
			label: toRequiredString(row.label, 'financial_events.counterparty_raw'),
			transaction_type: parseTransactionType(row.transaction_type, 'transactions.type'),
			transaction_count: toSafeInteger(row.transaction_count, 'transactions.transaction_count'),
			rule_category_id: toNullableString(
				row.rule_category_id,
				'user_merchant_rule_categories.category_id',
			),
			rule_category_name: toNullableString(row.rule_category_name, 'categories.name'),
			common_category_id: toNullableString(row.common_category_id, 'transactions.category_id'),
			common_category_name: toNullableString(row.common_category_name, 'categories.name'),
		});
	}

	return [...merchants.values()];
}

/** Transaction history for one merchant, newest first, for the detail list. */
export async function listUserMerchantTransactions(
	sql: SqlClient,
	userId: string,
	merchantId: string,
	limit: number,
): Promise<UserMerchantTransactionItem[]> {
	const rows = await sql<UserMerchantTransactionRowRaw[]>`
		select
			t.id as transaction_id,
			t.txn_date,
			t.amount_in_paise,
			t.type,
			t.status,
			c.name as category_name,
			coalesce(fe.counterparty_raw, fe.search_key) as descriptor
		from public.transactions as t
		join public.financial_events as fe
			on fe.id = t.financial_event_id
			and fe.user_id = t.user_id
		left join public.categories as c on c.id = t.category_id
		where t.user_id = ${userId}
			and t.merchant_id = ${merchantId}
			and t.user_deleted_at is null
		order by t.txn_date desc, t.id
		limit ${limit}
	`;

	return rows.map(row => ({
		transaction_id: toRequiredString(row.transaction_id, 'transactions.id'),
		txn_date: toIsoDateTime(row.txn_date, 'transactions.txn_date'),
		amount_in_paise: toSafeInteger(row.amount_in_paise, 'transactions.amount_in_paise'),
		type: parseTransactionType(row.type, 'transactions.type'),
		status: toRequiredString(row.status, 'transactions.status') as TransactionStatus,
		category_name: toNullableString(row.category_name, 'categories.name'),
		descriptor: toNullableString(row.descriptor, 'financial_events.counterparty_raw'),
	}));
}

/**
 * AI_ONLY spares categories the user chose by hand — that is the review-time
 * promise ("backfill the unmapped ones, not the mapped ones"). ALL is the
 * deliberate Settings override and rewrites every transaction for the slice.
 */
export type MerchantBackfillScope = 'AI_ONLY' | 'ALL';

/**
 * Applies one merchant/type category decision to transactions that already
 * exist, and clears them out of the review queue so the same merchant never
 * has to be answered twice.
 *
 * classification_source is deliberately left untouched: these rows were carried
 * along by a merchant-level decision, not individually judged, so a later
 * decision for the same merchant is still allowed to move them.
 */
export async function backfillMerchantCategory(
	sql: SqlClient,
	userId: string,
	merchantId: string,
	transactionType: TransactionType,
	categoryId: string,
	scope: MerchantBackfillScope,
	excludeTransactionId: string | null = null,
): Promise<number> {
	const onlyAiAssigned = scope === 'AI_ONLY';
	const rows = await sql<Array<{ id: unknown }>>`
		update public.transactions as t
		set category_id = ${categoryId},
			status = 'VERIFIED'
		where t.user_id = ${userId}
			and t.merchant_id = ${merchantId}
			and t.type = ${transactionType}
			and t.user_deleted_at is null
			and (
				${excludeTransactionId}::uuid is null
				or t.id <> ${excludeTransactionId}::uuid
			)
			and (not ${onlyAiAssigned} or t.classification_source <> 'USER')
			and (t.category_id is distinct from ${categoryId}::uuid or t.status <> 'VERIFIED')
		returning t.id
	`;
	return rows.length;
}

/**
 * The descriptor-scoped twin of backfillMerchantCategory. Matches on the raw
 * search key, so a merchant's other descriptors are never touched.
 */
export async function backfillDescriptorCategory(
	sql: SqlClient,
	userId: string,
	searchKey: string,
	transactionType: TransactionType,
	categoryId: string,
	scope: MerchantBackfillScope,
): Promise<number> {
	const onlyAiAssigned = scope === 'AI_ONLY';
	const rows = await sql<Array<{ id: unknown }>>`
		update public.transactions as t
		set category_id = ${categoryId},
			status = 'VERIFIED'
		from public.financial_events as fe
		where fe.id = t.financial_event_id
			and fe.user_id = t.user_id
			and fe.search_key = ${searchKey}
			and t.user_id = ${userId}
			and t.type = ${transactionType}
			and t.user_deleted_at is null
			and (not ${onlyAiAssigned} or t.classification_source <> 'USER')
			and (t.category_id is distinct from ${categoryId}::uuid or t.status <> 'VERIFIED')
		returning t.id
	`;
	return rows.length;
}

export function parseSetUserMerchantCategoryRequest(
	payload: unknown,
): SetUserMerchantCategoryRequest {
	const body = asRecord(payload);
	if (typeof body.backfill !== 'boolean') {
		throw badRequest('INVALID_PAYLOAD', 'backfill must be a boolean');
	}
	return {
		transaction_type: parseTransactionType(body.transaction_type, 'transaction_type'),
		category_id: parseUuid(body.category_id, 'category_id'),
		backfill: body.backfill,
	};
}

export function parseSetUserDescriptorCategoryRequest(
	payload: unknown,
): SetUserDescriptorCategoryRequest {
	const body = asRecord(payload);
	if (typeof body.backfill !== 'boolean') {
		throw badRequest('INVALID_PAYLOAD', 'backfill must be a boolean');
	}
	const searchKey = parseRequiredString(body.search_key, 'search_key');
	if (searchKey.length > MAX_SEARCH_KEY_LENGTH) {
		throw badRequest(
			'INVALID_PAYLOAD',
			`search_key must be at most ${MAX_SEARCH_KEY_LENGTH} characters`,
		);
	}
	return {
		search_key: searchKey,
		transaction_type: parseTransactionType(body.transaction_type, 'transaction_type'),
		category_id: parseUuid(body.category_id, 'category_id'),
		backfill: body.backfill,
	};
}

export function parseListMerchantTransactionsQuery(query: Record<string, unknown>): number {
	return query.limit === undefined
		? MAX_MERCHANT_TRANSACTIONS_LIMIT
		: parseLimit(query.limit, MAX_MERCHANT_TRANSACTIONS_LIMIT);
}

export function parseListUserMerchantsQuery(query: Record<string, unknown>): number {
	return query.limit === undefined
		? MAX_USER_MERCHANTS_LIMIT
		: parseLimit(query.limit, MAX_USER_MERCHANTS_LIMIT);
}

/**
 * Saves the user's merchant-level default so future ingestion resolves it, and
 * optionally rewrites the transactions already on file.
 */
/**
 * The category must be usable by this user and match the transaction type. The
 * table triggers enforce both, but a clear error beats a raised constraint
 * reaching the client.
 */
async function assertCategoryUsable(
	sql: SqlClient,
	userId: string,
	categoryId: string,
	transactionType: TransactionType,
): Promise<void> {
	const categoryRows = await sql<Array<{ type: unknown }>>`
		select c.type
		from public.categories as c
		where c.id = ${categoryId}
			and (c.user_id = ${userId} or (c.user_id is null and c.is_system = true))
		limit 1
	`;
	const categoryType = categoryRows[0]?.type;
	if (categoryType === undefined) {
		throw badRequest('CATEGORY_NOT_AVAILABLE', 'Category is not available to this user');
	}
	if (categoryType !== transactionType) {
		throw badRequest('CATEGORY_TYPE_MISMATCH', `Category type must match ${transactionType}`);
	}
}

export async function setUserMerchantCategory(
	sql: SqlClient,
	userId: string,
	merchantId: string,
	input: SetUserMerchantCategoryRequest,
): Promise<number> {
	return sql.begin(async (tx) => {
		const merchantSql = tx as unknown as SqlClient;
		await assertMerchantVisibleToUser(merchantSql, userId, merchantId);

		await assertCategoryUsable(merchantSql, userId, input.category_id, input.transaction_type);

		await merchantSql`
			insert into public.user_merchant_category_rules (
				user_id,
				merchant_id,
				transaction_type,
				category_id
			)
			values (${userId}, ${merchantId}, ${input.transaction_type}, ${input.category_id})
			on conflict (user_id, merchant_id, transaction_type) do update
			set category_id = excluded.category_id,
				updated_at = now()
		`;

		if (!input.backfill) return 0;
		return backfillMerchantCategory(
			merchantSql,
			userId,
			merchantId,
			input.transaction_type,
			input.category_id,
			'ALL',
		);
	});
}

/**
 * Saves a rule for one raw descriptor. This reuses public.user_merchant_rules,
 * which is already keyed by (user_id, search_key) and already outranks the
 * merchant-level rule during ingestion — so the narrower answer wins without
 * any change to the precedence chain.
 */
export async function setUserDescriptorCategory(
	sql: SqlClient,
	userId: string,
	merchantId: string,
	input: SetUserDescriptorCategoryRequest,
): Promise<number> {
	return sql.begin(async (tx) => {
		const descriptorSql = tx as unknown as SqlClient;
		await assertMerchantVisibleToUser(descriptorSql, userId, merchantId);

		// The descriptor has to be one this user actually transacted under, and
		// it has to belong to the merchant in the path — otherwise a rule could
		// be written for a counterparty the user has never seen.
		const descriptorRows = await descriptorSql<Array<{ search_key: unknown }>>`
			select fe.search_key
			from public.transactions as t
			join public.financial_events as fe
				on fe.id = t.financial_event_id
				and fe.user_id = t.user_id
			where t.user_id = ${userId}
				and t.merchant_id = ${merchantId}
				and fe.search_key = ${input.search_key}
				and t.user_deleted_at is null
			limit 1
		`;
		if (descriptorRows.length === 0) {
			throw notFound('DESCRIPTOR_NOT_FOUND', 'Descriptor not found for this merchant');
		}

		await assertCategoryUsable(descriptorSql, userId, input.category_id, input.transaction_type);

		const ruleRows = await descriptorSql<Array<{ id: unknown }>>`
			insert into public.user_merchant_rules (
				user_id,
				search_key,
				merchant_id,
				custom_category_id
			)
			values (${userId}, ${input.search_key}, ${merchantId}, ${input.category_id})
			on conflict (user_id, search_key) do update
			set merchant_id = excluded.merchant_id,
				custom_category_id = excluded.custom_category_id,
				updated_at = now()
			returning id
		`;
		const ruleId = toRequiredString(ruleRows[0]?.id, 'user_merchant_rules.id');
		await descriptorSql`
			insert into public.user_merchant_rule_categories (
				user_merchant_rule_id,
				transaction_type,
				category_id
			)
			values (${ruleId}, ${input.transaction_type}, ${input.category_id})
			on conflict (user_merchant_rule_id, transaction_type) do update
			set category_id = excluded.category_id,
				updated_at = now()
		`;

		if (!input.backfill) return 0;
		return backfillDescriptorCategory(
			descriptorSql,
			userId,
			input.search_key,
			input.transaction_type,
			input.category_id,
			'ALL',
		);
	});
}
