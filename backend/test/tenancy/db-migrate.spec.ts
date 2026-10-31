// The runner's pure planning logic. psql itself is exercised only by hand
// against Supabase (see supabase/README.md), so everything that decides WHAT
// runs is tested here, and the composed SQL is verified to be valid by
// executing it on PGlite.

import { readFileSync } from 'node:fs';
import path from 'node:path';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	BOOKKEEPING_DDL,
	checksumOf,
	composeApplySql,
	composeBaselineSql,
	computePlan,
	findTransactionControl,
	listMigrationFiles,
	MIGRATIONS_DIR,
	parseArgs,
	parseDevVars,
} from '../../../scripts/db-migrate.mjs';
import { createTenancyDb, type TenancyDb } from './pglite-db';

describe('db-migrate: planning', () => {
	it('lists migrations in filename order and matches the harness', () => {
		const files = listMigrationFiles();
		expect(files).toEqual([...files].sort());
		expect(files.every((file) => file.endsWith('.sql'))).toBe(true);
	});

	it('computes pending, drift and unknown rows', () => {
		const files = ['001.sql', '002.sql', '003.sql'];
		const checksums = new Map([
			['001.sql', 'a'],
			['002.sql', 'b'],
			['003.sql', 'c'],
		]);
		const applied = new Map([
			['001.sql', 'a'],
			['002.sql', 'STALE'],
			['999.sql', 'z'],
		]);
		expect(computePlan(files, checksums, applied)).toEqual({
			applied: ['001.sql', '002.sql'],
			pending: ['003.sql'],
			drifted: ['002.sql'],
			unknownApplied: ['999.sql'],
		});
	});

	it('refuses files that manage their own transaction', () => {
		expect(findTransactionControl('select 1;\nBEGIN;\nselect 2;')).toBe('begin');
		expect(findTransactionControl('  commit;')).toBe('commit');
		expect(findTransactionControl("do $$ begin perform 1; end $$;")).toBeNull();
		for (const file of listMigrationFiles()) {
			const text = readFileSync(path.join(MIGRATIONS_DIR, file), 'utf8');
			expect(findTransactionControl(text), `${file} manages its own transaction`).toBeNull();
		}
	});

	it('parses .dev.vars style files', () => {
		const vars = parseDevVars('# comment\nA=1\nB="two"\nC=\'three\'\nBAD\n D = 4 \n');
		expect(vars).toEqual({ A: '1', B: 'two', C: 'three', D: '4' });
	});

	it('parses commands and options', () => {
		expect(parseArgs([])).toEqual({
			command: 'status',
			options: { dryRun: false, force: false, to: null, through: null },
		});
		expect(parseArgs(['apply', '--to', 'x.sql', '--dry-run'])).toEqual({
			command: 'apply',
			options: { dryRun: true, force: false, to: 'x.sql', through: null },
		});
		expect(parseArgs(['baseline', '--through', 'y.sql', '--force'])).toEqual({
			command: 'baseline',
			options: { dryRun: false, force: true, to: null, through: 'y.sql' },
		});
		expect(() => parseArgs(['apply', '--bogus'])).toThrow(/unknown argument/);
	});

	it('checksums are stable and content-sensitive', () => {
		expect(checksumOf('a')).toBe(checksumOf('a'));
		expect(checksumOf('a')).not.toBe(checksumOf('b'));
		expect(checksumOf('a')).toMatch(/^[0-9a-f]{64}$/);
	});
});

describe('db-migrate: composed SQL executes', () => {
	let tenancy: TenancyDb;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		await tenancy.db.exec(BOOKKEEPING_DDL);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	it('bookkeeping DDL is idempotent, RLS-enabled and hardened for app_backend', async () => {
		await tenancy.db.exec(BOOKKEEPING_DDL);
		const rls = await tenancy.db.query<{ relrowsecurity: boolean }>(
			"select relrowsecurity from pg_class where relname = 'schema_migrations'",
		);
		expect(rls.rows[0]?.relrowsecurity).toBe(true);
		const policy = await tenancy.db.query(
			"select 1 from pg_policies where schemaname = 'public' and tablename = 'schema_migrations' and policyname = 'app_backend_all'",
		);
		expect(policy.rows).toHaveLength(1);
	});

	it('apply SQL runs the file, the backstop assert and the bookkeeping row together', async () => {
		const filename = '20990101000000_probe.sql';
		const contents = [
			'create table public.db_migrate_probe (id int primary key);',
			"select public.grant_app_backend('public.db_migrate_probe');",
		].join('\n');
		const sqlText = composeApplySql(filename, contents, checksumOf(contents));
		await tenancy.db.exec(sqlText);

		const table = await tenancy.db.query(
			"select 1 from information_schema.tables where table_schema = 'public' and table_name = 'db_migrate_probe'",
		);
		expect(table.rows).toHaveLength(1);
		const row = await tenancy.db.query<{ checksum: string }>(
			'select checksum from public.schema_migrations where filename = $1',
			[filename],
		);
		expect(row.rows[0]?.checksum).toBe(checksumOf(contents));
	});

	it('a migration that leaves a table unhardened is rolled back by the backstop assert', async () => {
		const filename = '20990101000001_unhardened_probe.sql';
		const contents = 'create table public.db_migrate_unhardened (id int primary key);';
		const sqlText = composeApplySql(filename, contents, checksumOf(contents));
		// Mirrors psql --single-transaction: the runner wraps each file.
		await expect(tenancy.db.transaction((tx) => tx.exec(sqlText))).rejects.toThrow(
			/tenancy backstops violated: table db_migrate_unhardened: row level security disabled/,
		);

		const table = await tenancy.db.query(
			"select 1 from information_schema.tables where table_schema = 'public' and table_name = 'db_migrate_unhardened'",
		);
		expect(table.rows).toHaveLength(0);
		const row = await tenancy.db.query(
			'select 1 from public.schema_migrations where filename = $1',
			[filename],
		);
		expect(row.rows).toHaveLength(0);
	});

	it("apply SQL quotes filenames and checksums safely", () => {
		const sqlText = composeApplySql("we'ird.sql", 'select 1;', "sum'ck");
		expect(sqlText).toContain("('we''ird.sql', 'sum''ck')");
	});

	it('baseline SQL records rows and is idempotent', async () => {
		const entries = [
			{ filename: 'b1.sql', checksum: 'c1' },
			{ filename: 'b2.sql', checksum: 'c2' },
		];
		await tenancy.db.exec(composeBaselineSql(entries));
		await tenancy.db.exec(composeBaselineSql(entries));
		const rows = await tenancy.db.query(
			"select filename from public.schema_migrations where filename in ('b1.sql', 'b2.sql') order by filename",
		);
		expect(rows.rows).toHaveLength(2);
		expect(composeBaselineSql([])).toBe('');
	});
});
