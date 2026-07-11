// Shared identity graph: canonical search keys, VPA/aggregator detection,
// and the user-rule / global-alias lookups consulted by every extraction
// path (template engine, AI engine, review service). Search keys are
// canonical uppercase-alphanumeric so a rule written anywhere resolves
// everywhere.

import type { MerchantType, TransactionType, UUID } from '../../../shared/types';
import type { SqlClient } from './db/client';
import { toNullableString, toRequiredString } from './db/serialization';

const SEARCH_KEY_MAX_LENGTH = 120;

const AGGREGATOR_PATTERNS: ReadonlyArray<{ key: string; pattern: RegExp }> = [
	{ key: 'RAZORPAY', pattern: /\brazorpay\b/i },
	{ key: 'BHARATPE', pattern: /\bbharatpe\b/i },
	{ key: 'PAYU', pattern: /\bpayu\b/i },
	{ key: 'CCAVENUE', pattern: /\bccavenue\b/i },
	{ key: 'CASHFREE', pattern: /\bcashfree\b/i },
	{ key: 'BILLDESK', pattern: /\bbilldesk\b/i },
];

export function toSearchKey(value: string | null): string | null {
	if (!value) {
		return null;
	}
	const normalized = value
		.toUpperCase()
		.replace(/[^A-Z0-9]/g, '')
		.slice(0, SEARCH_KEY_MAX_LENGTH);
	return normalized.length > 0 ? normalized : null;
}

export function detectVpaHandle(value: string): string | null {
	const match = value.match(/\b[a-z0-9._-]{2,}@[a-z][a-z0-9.-]{1,}\b/i);
	if (!match || typeof match[0] !== 'string') {
		return null;
	}
	return match[0].toLowerCase();
}

export function detectAggregatorKey(value: string): string | null {
	for (const entry of AGGREGATOR_PATTERNS) {
		if (entry.pattern.test(value)) {
			return entry.key;
		}
	}
	return null;
}

// Parses an amount token like '1,67,450.00' (rupees) to integer paise.
export function parseAmountTokenToPaise(token: string): number | null {
	const normalized = token.replace(/,/g, '');
	const rupees = Number.parseFloat(normalized);
	if (!Number.isFinite(rupees) || rupees <= 0) {
		return null;
	}

	const paise = Math.round(rupees * 100);
	if (!Number.isSafeInteger(paise) || paise <= 0) {
		return null;
	}
	return paise;
}

export interface SystemCategoryMap {
	income: UUID | null;
	expense: UUID | null;
	transfer: UUID | null;
}

export interface UserRuleResolution {
	merchant_id: UUID | null;
	custom_category_id: UUID | null;
}

export interface GlobalAliasResolution {
	merchant_id: UUID;
	default_category_id: UUID | null;
	merchant_type: MerchantType;
}

export interface IdentityGraphLookup {
	system_categories: SystemCategoryMap;
	user_rules: Map<UUID, Map<string, UserRuleResolution>>;
	global_aliases: Map<string, GlobalAliasResolution>;
}

interface SystemCategoryRowRaw {
	id: unknown;
	type: unknown;
}

interface UserRuleLookupRowRaw {
	user_id: unknown;
	search_key: unknown;
	merchant_id: unknown;
	custom_category_id: unknown;
}

interface GlobalAliasLookupRowRaw {
	search_key: unknown;
	merchant_id: unknown;
	default_category_id: unknown;
	merchant_type: unknown;
}

export async function loadSystemCategoryMap(sql: SqlClient): Promise<SystemCategoryMap> {
	const rows = await sql<SystemCategoryRowRaw[]>`
		select
			c.id,
			c.type
		from public.categories as c
		where c.user_id is null
			and c.is_system = true
			and c.parent_id is null
			and c.type in ('income', 'expense', 'transfer')
	`;

	const map: SystemCategoryMap = {
		income: null,
		expense: null,
		transfer: null,
	};
	for (const row of rows) {
		const type = toRequiredString(row.type, 'categories.type') as TransactionType;
		const id = toRequiredString(row.id, 'categories.id');
		if (type === 'income' || type === 'expense' || type === 'transfer') {
			map[type] = map[type] ?? id;
		}
	}
	return map;
}

export async function loadUserRulesForLookup(
	sql: SqlClient,
	userIds: UUID[],
	searchKeys: string[],
): Promise<Map<UUID, Map<string, UserRuleResolution>>> {
	if (userIds.length === 0 || searchKeys.length === 0) {
		return new Map<UUID, Map<string, UserRuleResolution>>();
	}

	const rows = await sql<UserRuleLookupRowRaw[]>`
		select distinct on (umr.user_id, umr.search_key)
			umr.user_id,
			umr.search_key,
			umr.merchant_id,
			umr.custom_category_id
		from public.user_merchant_rules as umr
		where umr.user_id = any(${userIds}::uuid[])
			and umr.search_key = any(${searchKeys}::text[])
		order by umr.user_id, umr.search_key, umr.updated_at desc, umr.created_at desc, umr.id desc
	`;

	const lookup = new Map<UUID, Map<string, UserRuleResolution>>();
	for (const row of rows) {
		const userId = toRequiredString(row.user_id, 'user_merchant_rules.user_id');
		const searchKey = toRequiredString(row.search_key, 'user_merchant_rules.search_key');
		const userMap = lookup.get(userId) ?? new Map<string, UserRuleResolution>();
		userMap.set(searchKey, {
			merchant_id: toNullableString(row.merchant_id, 'user_merchant_rules.merchant_id'),
			custom_category_id: toNullableString(
				row.custom_category_id,
				'user_merchant_rules.custom_category_id',
			),
		});
		lookup.set(userId, userMap);
	}

	return lookup;
}

export async function loadGlobalAliasesForLookup(
	sql: SqlClient,
	searchKeys: string[],
): Promise<Map<string, GlobalAliasResolution>> {
	if (searchKeys.length === 0) {
		return new Map<string, GlobalAliasResolution>();
	}

	const rows = await sql<GlobalAliasLookupRowRaw[]>`
		select
			gma.search_key,
			gm.id as merchant_id,
			gm.default_category_id,
			gm.type as merchant_type
		from public.global_merchant_aliases as gma
		join public.global_merchants as gm
			on gm.id = gma.merchant_id
		where gma.search_key = any(${searchKeys}::text[])
	`;

	const lookup = new Map<string, GlobalAliasResolution>();
	for (const row of rows) {
		const searchKey = toRequiredString(row.search_key, 'global_merchant_aliases.search_key');
		if (lookup.has(searchKey)) {
			continue;
		}
		lookup.set(searchKey, {
			merchant_id: toRequiredString(row.merchant_id, 'global_merchants.id'),
			default_category_id: toNullableString(
				row.default_category_id,
				'global_merchants.default_category_id',
			),
			merchant_type: toRequiredString(row.merchant_type, 'global_merchants.type') as MerchantType,
		});
	}

	return lookup;
}

export interface IdentityResolution {
	merchant_id: UUID | null;
	merchant_type: MerchantType | null;
	category_id: UUID | null;
	user_rule_hit: boolean;
	global_alias_hit: boolean;
}

export function resolveIdentityForSearchKeys(
	userId: UUID,
	searchKeys: string[],
	lookup: IdentityGraphLookup,
): IdentityResolution {
	let merchantId: UUID | null = null;
	let merchantType: MerchantType | null = null;
	let categoryId: UUID | null = null;
	let userRuleHit = false;
	let globalAliasHit = false;

	const userRules = lookup.user_rules.get(userId) ?? new Map<string, UserRuleResolution>();
	for (const key of searchKeys) {
		const userRule = userRules.get(key);
		if (!userRule) {
			continue;
		}
		userRuleHit = true;
		merchantId = merchantId ?? userRule.merchant_id;
		categoryId = categoryId ?? userRule.custom_category_id;
	}

	for (const key of searchKeys) {
		const alias = lookup.global_aliases.get(key);
		if (!alias) {
			continue;
		}
		globalAliasHit = true;
		merchantId = merchantId ?? alias.merchant_id;
		merchantType = merchantType ?? alias.merchant_type;
		categoryId = categoryId ?? alias.default_category_id;
		break;
	}

	return {
		merchant_id: merchantId,
		merchant_type: merchantType,
		category_id: categoryId,
		user_rule_hit: userRuleHit,
		global_alias_hit: globalAliasHit,
	};
}
