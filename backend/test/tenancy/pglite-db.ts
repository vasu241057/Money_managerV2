// Boots an in-memory Postgres (PGlite) and applies every checked-in migration
// in filename order, so tenancy tests run the real schema: composite FKs,
// scope triggers, indexes and, from Task 2 on, RLS and the backstop assert.
//
// Two shims stand in for Supabase-only surface:
//   - `create extension if not exists pg_cron;` is dropped (the baseline
//     migration already guards its cron.schedule block on pg_extension).
//   - `storage.buckets` is stubbed so the merchant-logo migrations' bucket
//     upsert has a table to land in.
// The Supabase API roles are pre-created because Task 2's migration revokes
// from them. Everything else is applied verbatim.

import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { PGlite } from '@electric-sql/pglite';
import { pgcrypto } from '@electric-sql/pglite/contrib/pgcrypto';

import type { SqlClient } from '../../src/lib/db/client';
import { createSqlAdapter } from './sql-adapter';

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const MIGRATIONS_DIR = path.resolve(HERE, '../../../supabase/migrations');

const PRELUDE = `
create role anon nologin;
create role authenticated nologin;
create role service_role nologin bypassrls;
create schema if not exists storage;
create table if not exists storage.buckets (
  id text primary key,
  name text not null,
  public boolean not null default false,
  file_size_limit bigint,
  allowed_mime_types text[]
);
`;

const BACKSTOP_ASSERT = `
do $$
begin
  if exists (
    select 1
    from pg_proc as p
    join pg_namespace as n on n.oid = p.pronamespace
    where n.nspname = 'public' and p.proname = 'assert_tenancy_backstops'
  ) then
    perform public.assert_tenancy_backstops();
  end if;
end;
$$;
`;

export function listMigrationFiles(): string[] {
	return readdirSync(MIGRATIONS_DIR)
		.filter((name) => name.endsWith('.sql'))
		.sort();
}

export function shimMigrationSql(sql: string): string {
	return sql.replace(/^\s*create extension if not exists pg_cron;\s*$/gim, '');
}

export interface TenancyDb {
	db: PGlite;
	sql: SqlClient;
	appliedMigrations: string[];
	close(): Promise<void>;
}

export interface CreateTenancyDbOptions {
	/**
	 * Stop before this migration filename (every file that sorts at or after
	 * it is left unapplied). Lets a rehearsal seed the pre-migration shape and
	 * then apply the file under test with applyMigration().
	 */
	stopBefore?: string;
}

/** Applies one checked-in migration in its own transaction, like the runner does. */
export async function applyMigration(db: PGlite, file: string): Promise<void> {
	const raw = readFileSync(path.join(MIGRATIONS_DIR, file), 'utf8');
	try {
		await db.transaction(async (tx) => {
			await tx.exec(shimMigrationSql(raw));
			await tx.exec(BACKSTOP_ASSERT);
		});
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		throw new Error(`Migration ${file} failed on PGlite: ${message}`);
	}
}

export async function createTenancyDb(options: CreateTenancyDbOptions = {}): Promise<TenancyDb> {
	const db = new PGlite({ extensions: { pgcrypto } });
	await db.waitReady;
	await db.exec(PRELUDE);

	const applied: string[] = [];
	for (const file of listMigrationFiles()) {
		if (options.stopBefore !== undefined && file >= options.stopBefore) break;
		const raw = readFileSync(path.join(MIGRATIONS_DIR, file), 'utf8');
		try {
			await db.exec(shimMigrationSql(raw));
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			throw new Error(`Migration ${file} failed on PGlite: ${message}`);
		}
		applied.push(file);
	}
	await db.exec(BACKSTOP_ASSERT);

	return {
		db,
		sql: createSqlAdapter(db),
		appliedMigrations: applied,
		close: () => db.close(),
	};
}
