// A postgres.js-shaped tagged-template client over PGlite, so the real
// services can run unchanged against an in-memory Postgres.
//
// The services use exactly two postgres.js features: the tagged template
// (`sql\`...${value}...\``) and `sql.begin(callback)`. Both are mapped here.
// Values become positional parameters; JS arrays are serialized to Postgres
// array literals so the services' explicit `::uuid[]` / `::text[]` casts keep
// working; bigint results are normalized to strings the way postgres.js
// returns them, which the serialization helpers already accept.

import type { PGlite, Transaction } from '@electric-sql/pglite';

import type { SqlClient } from '../../src/lib/db/client';

type Queryable = Pick<PGlite, 'query'> | Pick<Transaction, 'query'>;

function escapeArrayElement(value: unknown): string {
	if (value === null || value === undefined) return 'NULL';
	const text = value instanceof Date ? value.toISOString() : String(value);
	return `"${text.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

export function toPgArrayLiteral(values: unknown[]): string {
	return `{${values.map(escapeArrayElement).join(',')}}`;
}

function serializeParam(value: unknown): unknown {
	if (value === undefined) return null;
	if (Array.isArray(value)) return toPgArrayLiteral(value);
	if (typeof value === 'bigint') return value.toString();
	if (value instanceof Date) return value.toISOString();
	return value;
}

// Postgres int8 (bigint) and numeric come back from postgres.js as strings;
// PGlite parses int8 to a JS number. Normalize by field type so services see
// production-shaped rows (the serialization helpers accept both, but code that
// compares against strings must behave identically here).
const INT8_OID = 20;
const NUMERIC_OID = 1700;

function normalizeRow(
	row: Record<string, unknown>,
	stringTypedColumns: ReadonlySet<string>,
): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(row)) {
		if (typeof value === 'bigint') {
			out[key] = value.toString();
		} else if (typeof value === 'number' && stringTypedColumns.has(key)) {
			out[key] = String(value);
		} else {
			out[key] = value;
		}
	}
	return out;
}

export function buildQueryText(strings: TemplateStringsArray): string {
	let text = '';
	for (let index = 0; index < strings.length; index += 1) {
		text += strings[index];
		if (index < strings.length - 1) text += `$${index + 1}`;
	}
	return text;
}

function createTag(queryable: Queryable): (strings: TemplateStringsArray, ...values: unknown[]) => Promise<unknown[]> {
	return async (strings, ...values) => {
		const text = buildQueryText(strings);
		const params = values.map(serializeParam);
		const result = await queryable.query<Record<string, unknown>>(text, params);
		const stringTyped = new Set(
			result.fields
				.filter((field) => field.dataTypeID === INT8_OID || field.dataTypeID === NUMERIC_OID)
				.map((field) => field.name),
		);
		return result.rows.map((row) => normalizeRow(row, stringTyped));
	};
}

/**
 * Wraps a PGlite instance as the `SqlClient` the services expect. Transactions
 * bind a fresh tag to the PGlite transaction handle, exactly like postgres.js
 * hands `sql.begin` callbacks a transaction-scoped client.
 */
export function createSqlAdapter(db: PGlite): SqlClient {
	const tag = createTag(db) as unknown as Record<string, unknown>;
	tag.begin = (callback: (tx: SqlClient) => Promise<unknown>) =>
		db.transaction(async (tx) => callback(createTag(tx) as unknown as SqlClient));
	return tag as unknown as SqlClient;
}
