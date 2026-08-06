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

export interface InstrumentAccountCandidate {
	id: string;
	type: string;
	last4: string | null;
}

export interface InstrumentCardCandidate {
	id: string;
	account_id: string;
	last4: string;
	issuer_name: string | null;
}

export interface InstrumentLink {
	credit_card_id: string | null;
	account_id: string | null;
}

// Account types that can plausibly own a transaction of a given payment
// method. An account-number last4 must never grab a credit-card alert (or
// vice versa) just because the digits collide.
const ACCOUNT_TYPES_BY_METHOD: Record<string, readonly string[]> = {
	credit_card: ['card'],
	debit_card: ['bank'],
	netbanking: ['bank'],
	upi: ['bank'],
	unknown: ['bank', 'card', 'cash', 'other'],
};

// Links an extracted instrument to the user's registered cards/accounts.
// Never guesses: any ambiguity leaves the transaction unlinked for review.
// Linking is metadata only — it must not feed auto-verify (identity graph
// signals only).
export function resolveInstrumentLink(input: {
	payment_method: string;
	instrument_last4: string | null;
	from_address: string | null;
	accounts: ReadonlyArray<InstrumentAccountCandidate>;
	credit_cards: ReadonlyArray<InstrumentCardCandidate>;
}): InstrumentLink {
	const none: InstrumentLink = { credit_card_id: null, account_id: null };
	const last4 = input.instrument_last4;
	if (!last4) {
		return none;
	}

	// Credit-card alerts consult the card registry first; the linked card
	// also pins the backing account (the DB trigger enforces they agree).
	if (input.payment_method === 'credit_card') {
		const byLast4 = input.credit_cards.filter(card => card.last4 === last4);
		if (byLast4.length === 1 && byLast4[0]) {
			return { credit_card_id: byLast4[0].id, account_id: byLast4[0].account_id };
		}
		if (byLast4.length > 1) {
			// Two cards sharing a last4: only the sender's issuer can break
			// the tie ("hdfcbank.net" names HDFC). Still ambiguous → unlinked.
			const senderKey = toSearchKey(input.from_address);
			const byIssuer = senderKey
				? byLast4.filter(card => {
						const issuerKey = toSearchKey(card.issuer_name);
						return issuerKey !== null && senderKey.includes(issuerKey);
					})
				: [];
			if (byIssuer.length === 1 && byIssuer[0]) {
				return { credit_card_id: byIssuer[0].id, account_id: byIssuer[0].account_id };
			}
			return none;
		}
	}

	const allowedTypes = ACCOUNT_TYPES_BY_METHOD[input.payment_method];
	if (!allowedTypes) {
		return none;
	}
	const accountMatches = input.accounts.filter(
		account => account.last4 === last4 && allowedTypes.includes(account.type),
	);
	if (accountMatches.length === 1 && accountMatches[0]) {
		return { credit_card_id: null, account_id: accountMatches[0].id };
	}
	return none;
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
	category_ids: SystemCategoryMap;
}

export interface GlobalAliasResolution {
	merchant_id: UUID;
	category_ids: SystemCategoryMap;
	merchant_type: MerchantType;
	// Probation: unconfirmed (fresh AI) mappings inform but never auto-verify.
	confirmed: boolean;
}

export interface IdentityGraphLookup {
	system_categories: SystemCategoryMap;
	user_rules: Map<UUID, Map<string, UserRuleResolution>>;
	global_aliases: Map<string, GlobalAliasResolution>;
	/**
	 * The user's merchant-level category defaults, by user then merchant. Where
	 * user_rules only match the exact counterparty string, these reach every
	 * alias of a merchant, so one review answers the merchant for good.
	 */
	user_merchant_rules: Map<UUID, Map<UUID, SystemCategoryMap>>;
}

interface SystemCategoryRowRaw {
	id: unknown;
	type: unknown;
	name?: unknown;
}

interface UserRuleLookupRowRaw {
	user_id: unknown;
	search_key: unknown;
	merchant_id: unknown;
	category_type: unknown;
	custom_category_id: unknown;
}

interface UserMerchantRuleLookupRowRaw {
	user_id: unknown;
	merchant_id: unknown;
	transaction_type: unknown;
	category_id: unknown;
}

interface GlobalAliasLookupRowRaw {
	search_key: unknown;
	merchant_id: unknown;
	category_type: unknown;
	default_category_id: unknown;
	merchant_type: unknown;
	confirmed?: unknown;
}

// The per-type generic children (never the structural Income/Expense/Transfer
// roots) — a transaction falling all the way through the category chain gets
// Miscellaneous/Other rather than a bare root, and a fallback-sourced
// category never auto-verifies.
const SYSTEM_DEFAULT_CATEGORY_NAMES: Record<TransactionType, string> = {
	expense: 'Miscellaneous',
	income: 'Other Income',
	transfer: 'Other Transfer',
};

export async function loadSystemCategoryMap(sql: SqlClient): Promise<SystemCategoryMap> {
	const rows = await sql<SystemCategoryRowRaw[]>`
		select
			c.id,
			c.type,
			c.name
		from public.categories as c
		where c.user_id is null
			and c.is_system = true
			and c.parent_id is not null
			and c.type in ('income', 'expense', 'transfer')
			and c.name in ('Miscellaneous', 'Other Income', 'Other Transfer')
	`;

	const map: SystemCategoryMap = {
		income: null,
		expense: null,
		transfer: null,
	};
	for (const row of rows) {
		const type = toRequiredString(row.type, 'categories.type') as TransactionType;
		const id = toRequiredString(row.id, 'categories.id');
		const name = typeof row.name === 'string' ? row.name : '';
		if (
			(type === 'income' || type === 'expense' || type === 'transfer') &&
			name === SYSTEM_DEFAULT_CATEGORY_NAMES[type]
		) {
			map[type] = map[type] ?? id;
		}
	}
	return map;
}

// System expense subcategories keyed by exact name — the fixed taxonomy the
// merchant mapper offers the model as a category enum. Model output is
// validated against this map, so a hallucinated category can never land.
export async function loadSystemExpenseCategories(
	sql: SqlClient,
): Promise<Map<string, UUID>> {
	const rows = await sql<SystemCategoryRowRaw[]>`
		select c.id, c.type, c.name
		from public.categories as c
		where c.user_id is null
			and c.is_system = true
			and c.parent_id is not null
			and c.type = 'expense'
	`;

	const byName = new Map<string, UUID>();
	for (const row of rows) {
		const name = typeof row.name === 'string' ? row.name.trim() : '';
		if (name.length === 0) continue;
		byName.set(name, toRequiredString(row.id, 'categories.id') as UUID);
	}
	return byName;
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
		select
			umr.user_id,
			umr.search_key,
			umr.merchant_id,
			umrc.transaction_type as category_type,
			umrc.category_id as custom_category_id
		from public.user_merchant_rules as umr
		left join public.user_merchant_rule_categories as umrc
			on umrc.user_merchant_rule_id = umr.id
		where umr.user_id = any(${userIds}::uuid[])
			and umr.search_key = any(${searchKeys}::text[])
		order by umr.user_id, umr.search_key, umrc.transaction_type
	`;

	const lookup = new Map<UUID, Map<string, UserRuleResolution>>();
	for (const row of rows) {
		const userId = toRequiredString(row.user_id, 'user_merchant_rules.user_id');
		const searchKey = toRequiredString(row.search_key, 'user_merchant_rules.search_key');
		const userMap = lookup.get(userId) ?? new Map<string, UserRuleResolution>();
		const resolution = userMap.get(searchKey) ?? {
			merchant_id: toNullableString(row.merchant_id, 'user_merchant_rules.merchant_id'),
			category_ids: { income: null, expense: null, transfer: null },
		};
		const categoryType = toNullableString(row.category_type, 'user_merchant_rule_categories.transaction_type');
		const categoryId = toNullableString(row.custom_category_id, 'user_merchant_rule_categories.category_id');
		if (
			categoryId &&
			(categoryType === 'income' || categoryType === 'expense' || categoryType === 'transfer')
		) {
			resolution.category_ids[categoryType] = resolution.category_ids[categoryType] ?? categoryId;
		}
		userMap.set(searchKey, resolution);
		lookup.set(userId, userMap);
	}

	return lookup;
}

/**
 * Loads every merchant-level category default these users own. Unlike the
 * search-key rules this is not narrowed by the email's keys — the merchant is
 * only known after alias resolution, and a user holds few of these.
 */
export async function loadUserMerchantRulesForLookup(
	sql: SqlClient,
	userIds: UUID[],
): Promise<Map<UUID, Map<UUID, SystemCategoryMap>>> {
	if (userIds.length === 0) {
		return new Map<UUID, Map<UUID, SystemCategoryMap>>();
	}

	const rows = await sql<UserMerchantRuleLookupRowRaw[]>`
		select
			umcr.user_id,
			umcr.merchant_id,
			umcr.transaction_type,
			umcr.category_id
		from public.user_merchant_category_rules as umcr
		where umcr.user_id = any(${userIds}::uuid[])
		order by umcr.user_id, umcr.merchant_id, umcr.transaction_type
	`;

	const lookup = new Map<UUID, Map<UUID, SystemCategoryMap>>();
	for (const row of rows) {
		const userId = toRequiredString(row.user_id, 'user_merchant_category_rules.user_id');
		const merchantId = toRequiredString(
			row.merchant_id,
			'user_merchant_category_rules.merchant_id',
		);
		const transactionType = toRequiredString(
			row.transaction_type,
			'user_merchant_category_rules.transaction_type',
		);
		if (transactionType !== 'income' && transactionType !== 'expense' && transactionType !== 'transfer') {
			continue;
		}
		const userMap = lookup.get(userId) ?? new Map<UUID, SystemCategoryMap>();
		const categories = userMap.get(merchantId) ?? {
			income: null,
			expense: null,
			transfer: null,
		};
		categories[transactionType] = toRequiredString(
			row.category_id,
			'user_merchant_category_rules.category_id',
		) as UUID;
		userMap.set(merchantId, categories);
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
			gmcd.transaction_type as category_type,
			gmcd.category_id as default_category_id,
			gm.type as merchant_type,
			gm.confirmed
		from public.global_merchant_aliases as gma
		join public.global_merchants as gm
			on gm.id = gma.merchant_id
		left join public.global_merchant_category_defaults as gmcd
			on gmcd.merchant_id = gm.id
		where gma.search_key = any(${searchKeys}::text[])
		order by gma.search_key, gmcd.transaction_type
	`;

	const lookup = new Map<string, GlobalAliasResolution>();
	for (const row of rows) {
		const searchKey = toRequiredString(row.search_key, 'global_merchant_aliases.search_key');
		const resolution = lookup.get(searchKey) ?? {
			merchant_id: toRequiredString(row.merchant_id, 'global_merchants.id'),
			category_ids: { income: null, expense: null, transfer: null },
			merchant_type: toRequiredString(row.merchant_type, 'global_merchants.type') as MerchantType,
			// Missing column (spec harnesses) reads as confirmed so legacy
			// fixtures keep their auto-verify semantics.
			confirmed: row.confirmed !== false,
		};
		const categoryType = toNullableString(row.category_type, 'global_merchant_category_defaults.transaction_type');
		const categoryId = toNullableString(row.default_category_id, 'global_merchant_category_defaults.category_id');
		if (
			categoryId &&
			(categoryType === 'income' || categoryType === 'expense' || categoryType === 'transfer')
		) {
			resolution.category_ids[categoryType] = resolution.category_ids[categoryType] ?? categoryId;
		}
		lookup.set(searchKey, resolution);
	}

	return lookup;
}

export interface IdentityResolution {
	merchant_id: UUID | null;
	merchant_type: MerchantType | null;
	// Category by provenance: a rule category is the user's own decision, an
	// alias category is the global default. Auto-verify needs to know which
	// (or neither) actually resolved — the merged view hid that.
	rule_category_ids: SystemCategoryMap;
	alias_category_ids: SystemCategoryMap;
	user_rule_hit: boolean;
	global_alias_hit: boolean;
	// False while the hit alias's merchant is on probation (fresh AI mapping
	// no human has accepted yet) — such hits inform but never auto-verify.
	alias_confirmed: boolean;
}

export function resolveIdentityForSearchKeys(
	userId: UUID,
	searchKeys: string[],
	lookup: IdentityGraphLookup,
): IdentityResolution {
	let merchantId: UUID | null = null;
	let merchantType: MerchantType | null = null;
	const ruleCategoryIds: SystemCategoryMap = { income: null, expense: null, transfer: null };
	const aliasCategoryIds: SystemCategoryMap = { income: null, expense: null, transfer: null };
	let userRuleHit = false;
	let globalAliasHit = false;
	let aliasConfirmed = false;

	const userRules = lookup.user_rules.get(userId) ?? new Map<string, UserRuleResolution>();
	for (const key of searchKeys) {
		const userRule = userRules.get(key);
		if (!userRule) {
			continue;
		}
		userRuleHit = true;
		merchantId = merchantId ?? userRule.merchant_id;
		for (const type of ['income', 'expense', 'transfer'] as const) {
			ruleCategoryIds[type] = ruleCategoryIds[type] ?? userRule.category_ids[type];
		}
	}

	for (const key of searchKeys) {
		const alias = lookup.global_aliases.get(key);
		if (!alias) {
			continue;
		}
		globalAliasHit = true;
		aliasConfirmed = alias.confirmed;
		merchantId = merchantId ?? alias.merchant_id;
		merchantType = merchantType ?? alias.merchant_type;
		for (const type of ['income', 'expense', 'transfer'] as const) {
			aliasCategoryIds[type] = aliasCategoryIds[type] ?? alias.category_ids[type];
		}
		break;
	}

	// Merchant-level defaults apply once the merchant is known, whichever way it
	// resolved. They rank below a search-key rule (that one names this exact
	// counterparty) but count as the user's own decision all the same, so a
	// merchant the user has mapped verifies without another trip to review.
	// An aggregator alias never identifies the real merchant, so it is skipped.
	if (merchantId && merchantType !== 'AGGREGATOR') {
		const merchantRules = lookup.user_merchant_rules.get(userId)?.get(merchantId);
		if (merchantRules) {
			for (const type of ['income', 'expense', 'transfer'] as const) {
				if (ruleCategoryIds[type] === null && merchantRules[type] !== null) {
					ruleCategoryIds[type] = merchantRules[type];
					userRuleHit = true;
				}
			}
		}
	}

	return {
		merchant_id: merchantId,
		merchant_type: merchantType,
		rule_category_ids: ruleCategoryIds,
		alias_category_ids: aliasCategoryIds,
		user_rule_hit: userRuleHit,
		global_alias_hit: globalAliasHit,
		alias_confirmed: aliasConfirmed,
	};
}
