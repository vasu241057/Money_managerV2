import type {
	GlobalMerchantListItem,
	SetUserMerchantCategoryRequest,
	TransactionType,
	UserMerchantCategoryBreakdown,
	UserMerchantListItem,
} from '../../../shared/types';
import { toNullableString, toRequiredString, toSafeInteger } from '../lib/db/serialization';
import type { SqlClient } from '../lib/db/client';
import { badRequest, notFound } from '../lib/http/errors';
import { asRecord, parseLimit, parseUuid } from '../lib/http/validation';

const MAX_LIST_MERCHANTS_LIMIT = 1000;
const DEFAULT_LIST_MERCHANTS_LIMIT = 200;
const MAX_SEARCH_QUERY_LENGTH = 120;
const MAX_USER_MERCHANTS_LIMIT = 500;

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

export async function listGlobalMerchants(
	sql: SqlClient,
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
		where (
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

	return [...merchants.values()];
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

export function parseListUserMerchantsQuery(query: Record<string, unknown>): number {
	return query.limit === undefined
		? MAX_USER_MERCHANTS_LIMIT
		: parseLimit(query.limit, MAX_USER_MERCHANTS_LIMIT);
}

/**
 * Saves the user's merchant-level default so future ingestion resolves it, and
 * optionally rewrites the transactions already on file.
 */
export async function setUserMerchantCategory(
	sql: SqlClient,
	userId: string,
	merchantId: string,
	input: SetUserMerchantCategoryRequest,
): Promise<number> {
	return sql.begin(async (tx) => {
		const merchantSql = tx as unknown as SqlClient;
		const merchantRows = await merchantSql<Array<{ id: unknown }>>`
			select gm.id from public.global_merchants as gm where gm.id = ${merchantId} limit 1
		`;
		if (merchantRows.length === 0) {
			throw notFound('MERCHANT_NOT_FOUND', 'Merchant not found');
		}

		// The category must be usable by this user and match the transaction
		// type; the table trigger enforces both, but a clear error beats a
		// raised constraint reaching the client.
		const categoryRows = await merchantSql<Array<{ type: unknown }>>`
			select c.type
			from public.categories as c
			where c.id = ${input.category_id}
				and (c.user_id = ${userId} or (c.user_id is null and c.is_system = true))
			limit 1
		`;
		const categoryType = categoryRows[0]?.type;
		if (categoryType === undefined) {
			throw badRequest('CATEGORY_NOT_AVAILABLE', 'Category is not available to this user');
		}
		if (categoryType !== input.transaction_type) {
			throw badRequest(
				'CATEGORY_TYPE_MISMATCH',
				`Category type must match ${input.transaction_type}`,
			);
		}

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
