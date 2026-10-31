// Byte-comparable dump of everything one user owns. Discovers the user-owned
// tables from the catalog (every base table with a user_id column) so a table
// added later is snapshotted without editing this file; rule categories hang
// off rules and are included through their parent, and the user's private
// payees (owner_user_id on the merchant tables) are included explicitly.

import type { PGlite } from '@electric-sql/pglite';

export type UserSnapshot = Record<string, string[]>;

function canonical(value: unknown): unknown {
	if (value instanceof Date) return value.toISOString();
	if (typeof value === 'bigint') return value.toString();
	if (Array.isArray(value)) return value.map(canonical);
	if (value && typeof value === 'object') {
		const out: Record<string, unknown> = {};
		for (const key of Object.keys(value as Record<string, unknown>).sort()) {
			out[key] = canonical((value as Record<string, unknown>)[key]);
		}
		return out;
	}
	return value;
}

function serializeRows(rows: Record<string, unknown>[]): string[] {
	return rows.map((row) => JSON.stringify(canonical(row))).sort();
}

export async function listUserOwnedTables(db: PGlite): Promise<string[]> {
	const result = await db.query<{ table_name: string }>(`
		select c.table_name
		from information_schema.columns as c
		join information_schema.tables as t
			on t.table_schema = c.table_schema and t.table_name = c.table_name
		where c.table_schema = 'public'
			and c.column_name = 'user_id'
			and t.table_type = 'BASE TABLE'
		order by c.table_name
	`);
	return result.rows.map((row) => row.table_name);
}

export async function snapshotUser(db: PGlite, userId: string): Promise<UserSnapshot> {
	const snapshot: UserSnapshot = {};
	for (const table of await listUserOwnedTables(db)) {
		if (!/^[a-z_][a-z0-9_]*$/.test(table)) throw new Error(`unexpected table name ${table}`);
		const result = await db.query<Record<string, unknown>>(
			`select * from public.${table} where user_id = $1`,
			[userId],
		);
		snapshot[table] = serializeRows(result.rows);
	}
	const ruleCategories = await db.query<Record<string, unknown>>(
		`select umrc.*
		 from public.user_merchant_rule_categories as umrc
		 join public.user_merchant_rules as umr on umr.id = umrc.user_merchant_rule_id
		 where umr.user_id = $1`,
		[userId],
	);
	snapshot.user_merchant_rule_categories = serializeRows(ruleCategories.rows);
	const ownedMerchants = await db.query<Record<string, unknown>>(
		'select * from public.global_merchants where owner_user_id = $1',
		[userId],
	);
	snapshot.global_merchants_owned = serializeRows(ownedMerchants.rows);
	const ownedAliases = await db.query<Record<string, unknown>>(
		'select * from public.global_merchant_aliases where owner_user_id = $1',
		[userId],
	);
	snapshot.global_merchant_aliases_owned = serializeRows(ownedAliases.rows);
	const privateTemplates = await db.query<Record<string, unknown>>(
		'select * from public.email_templates where learned_from_user_id = $1',
		[userId],
	);
	snapshot.email_templates_private = serializeRows(privateTemplates.rows);
	return snapshot;
}
