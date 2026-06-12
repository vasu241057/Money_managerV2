import type { GlobalMerchantListItem } from '../../../shared/types';
import { toRequiredString } from '../lib/db/serialization';
import type { SqlClient } from '../lib/db/client';
import { badRequest } from '../lib/http/errors';
import { parseLimit } from '../lib/http/validation';

const MAX_LIST_MERCHANTS_LIMIT = 1000;
const DEFAULT_LIST_MERCHANTS_LIMIT = 200;
const MAX_SEARCH_QUERY_LENGTH = 120;

interface ParsedListGlobalMerchantsQuery {
	q: string | null;
	limit: number;
}

interface MerchantListRowRaw {
	merchant_id: unknown;
	merchant_canonical_name: unknown;
	merchant_type: unknown;
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
