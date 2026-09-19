#!/usr/bin/env node
// Tracked migration runner for supabase/migrations.
//
//   node scripts/db-migrate.mjs status
//   node scripts/db-migrate.mjs apply [--to <filename>] [--dry-run] [--force]
//   node scripts/db-migrate.mjs baseline --through <filename> [--dry-run] [--force]
//
// Bookkeeping lives in public.schema_migrations (filename, checksum,
// applied_at, applied_by). Each pending file is applied by psql inside ONE
// transaction together with its bookkeeping row and a guarded call to
// public.assert_tenancy_backstops() (a no-op until that function exists), so
// a migration that breaks a tenancy invariant rolls back instead of shipping.
//
// The connection string comes from SUPABASE_POOLER_URL (environment first,
// then backend/.dev.vars) and is never printed.

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const MIGRATIONS_DIR = path.join(ROOT, 'supabase', 'migrations');
const DEV_VARS_PATH = path.join(ROOT, 'backend', '.dev.vars');

export const BOOKKEEPING_DDL = `
create table if not exists public.schema_migrations (
  filename text primary key,
  checksum text not null,
  applied_at timestamptz not null default now(),
  applied_by text not null default current_user
);
alter table public.schema_migrations enable row level security;
do $$
begin
  if exists (
    select 1
    from pg_proc as p
    join pg_namespace as n on n.oid = p.pronamespace
    where n.nspname = 'public' and p.proname = 'grant_app_backend'
  ) then
    perform public.grant_app_backend('public.schema_migrations');
  end if;
end;
$$;
`.trim();

export const BACKSTOP_ASSERT_SQL = `
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
`.trim();

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested)
// ---------------------------------------------------------------------------

export function checksumOf(contents) {
	return createHash('sha256').update(contents, 'utf8').digest('hex');
}

export function parseDevVars(text) {
	const vars = {};
	for (const rawLine of text.split(/\r?\n/)) {
		const line = rawLine.trim();
		if (line.length === 0 || line.startsWith('#')) continue;
		const eq = line.indexOf('=');
		if (eq <= 0) continue;
		const key = line.slice(0, eq).trim();
		let value = line.slice(eq + 1).trim();
		if (
			(value.startsWith('"') && value.endsWith('"')) ||
			(value.startsWith("'") && value.endsWith("'"))
		) {
			value = value.slice(1, -1);
		}
		vars[key] = value;
	}
	return vars;
}

export function listMigrationFiles(dir = MIGRATIONS_DIR) {
	return readdirSync(dir)
		.filter((name) => name.endsWith('.sql'))
		.sort();
}

/** A migration file must not manage its own transaction; the runner does. */
export function findTransactionControl(contents) {
	const match = contents.match(/^\s*(begin|commit|rollback)\s*;/im);
	return match ? match[1].toLowerCase() : null;
}

/**
 * Compares the files on disk with the applied rows and returns the plan.
 * `applied` is a Map<filename, checksum>.
 */
export function computePlan(files, checksums, applied) {
	const drifted = [];
	const pending = [];
	const appliedInOrder = [];
	for (const file of files) {
		const recorded = applied.get(file);
		if (recorded === undefined) {
			pending.push(file);
			continue;
		}
		appliedInOrder.push(file);
		if (recorded !== checksums.get(file)) drifted.push(file);
	}
	const unknownApplied = [...applied.keys()].filter((name) => !files.includes(name)).sort();
	return { applied: appliedInOrder, pending, drifted, unknownApplied };
}

export function sqlLiteral(value) {
	return `'${String(value).replace(/'/g, "''")}'`;
}

/** The single-transaction body psql runs for one migration file. */
export function composeApplySql(filename, contents, checksum) {
	const body = contents.endsWith('\n') ? contents : `${contents}\n`;
	return [
		`-- db-migrate: ${filename}`,
		body,
		'-- db-migrate: tenancy backstops',
		BACKSTOP_ASSERT_SQL,
		'-- db-migrate: bookkeeping',
		`insert into public.schema_migrations (filename, checksum) values (${sqlLiteral(filename)}, ${sqlLiteral(checksum)});`,
		'',
	].join('\n');
}

export function composeBaselineSql(entries) {
	if (entries.length === 0) return '';
	const values = entries
		.map(({ filename, checksum }) => `(${sqlLiteral(filename)}, ${sqlLiteral(checksum)})`)
		.join(',\n  ');
	return `insert into public.schema_migrations (filename, checksum) values\n  ${values}\non conflict (filename) do nothing;\n`;
}

export function parseArgs(argv) {
	const [command, ...rest] = argv;
	const options = { dryRun: false, force: false, to: null, through: null };
	for (let index = 0; index < rest.length; index += 1) {
		const arg = rest[index];
		if (arg === '--dry-run') options.dryRun = true;
		else if (arg === '--force') options.force = true;
		else if (arg === '--to') options.to = rest[++index] ?? null;
		else if (arg === '--through') options.through = rest[++index] ?? null;
		else throw new Error(`unknown argument: ${arg}`);
	}
	return { command: command ?? 'status', options };
}

// ---------------------------------------------------------------------------
// psql plumbing
// ---------------------------------------------------------------------------

function resolveConnectionString() {
	const fromEnv = process.env.SUPABASE_POOLER_URL?.trim();
	if (fromEnv) return fromEnv;
	try {
		const vars = parseDevVars(readFileSync(DEV_VARS_PATH, 'utf8'));
		const value = vars.SUPABASE_POOLER_URL?.trim();
		if (value) return value;
	} catch {
		// fall through
	}
	throw new Error('SUPABASE_POOLER_URL is not set (environment or backend/.dev.vars)');
}

function runPsql(connectionString, args, { allowFailure = false } = {}) {
	const result = spawnSync('psql', [connectionString, '-X', '-v', 'ON_ERROR_STOP=1', ...args], {
		encoding: 'utf8',
		stdio: ['ignore', 'pipe', 'pipe'],
	});
	if (result.error) {
		throw new Error(`failed to start psql: ${result.error.message}`);
	}
	if (result.status !== 0 && !allowFailure) {
		const detail = (result.stderr || result.stdout || '').trim();
		throw new Error(`psql exited with status ${result.status}${detail ? `:\n${detail}` : ''}`);
	}
	return result;
}

function ensureBookkeeping(connectionString) {
	runPsql(connectionString, ['-q', '-c', BOOKKEEPING_DDL]);
}

/** `status` must never write: it reports a missing bookkeeping table instead of creating it. */
function bookkeepingExists(connectionString) {
	const result = runPsql(connectionString, [
		'-A',
		'-t',
		'-c',
		"select to_regclass('public.schema_migrations') is not null",
	]);
	return result.stdout.trim() === 't';
}

function loadApplied(connectionString) {
	const result = runPsql(connectionString, [
		'-A',
		'-t',
		'-F',
		'\t',
		'-c',
		'select filename, checksum from public.schema_migrations order by filename',
	]);
	const applied = new Map();
	for (const line of result.stdout.split('\n')) {
		if (!line.trim()) continue;
		const [filename, checksum] = line.split('\t');
		if (filename && checksum) applied.set(filename, checksum);
	}
	return applied;
}

function runSqlFileInTransaction(connectionString, sqlText, label) {
	const dir = mkdtempSync(path.join(os.tmpdir(), 'db-migrate-'));
	const file = path.join(dir, `${label}.sql`);
	try {
		writeFileSync(file, sqlText, 'utf8');
		runPsql(connectionString, ['-q', '--single-transaction', '-f', file]);
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
}

function loadLocalState() {
	const files = listMigrationFiles();
	const contents = new Map();
	const checksums = new Map();
	for (const file of files) {
		const text = readFileSync(path.join(MIGRATIONS_DIR, file), 'utf8');
		contents.set(file, text);
		checksums.set(file, checksumOf(text));
	}
	return { files, contents, checksums };
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

function printPlan(plan) {
	console.log(`applied:  ${plan.applied.length}`);
	console.log(`pending:  ${plan.pending.length}`);
	for (const file of plan.pending) console.log(`  - ${file}`);
	if (plan.drifted.length > 0) {
		console.log(`DRIFT (applied file changed on disk): ${plan.drifted.length}`);
		for (const file of plan.drifted) console.log(`  ! ${file}`);
	}
	if (plan.unknownApplied.length > 0) {
		console.log(`recorded but missing on disk: ${plan.unknownApplied.length}`);
		for (const file of plan.unknownApplied) console.log(`  ? ${file}`);
	}
}

function commandStatus(connectionString) {
	const { files, checksums } = loadLocalState();
	if (!bookkeepingExists(connectionString)) {
		console.log('schema_migrations table not found: run `baseline --through <last hand-applied file>` first');
		console.log(`files on disk: ${files.length}`);
		return 3;
	}
	const plan = computePlan(files, checksums, loadApplied(connectionString));
	printPlan(plan);
	return plan.drifted.length > 0 ? 2 : 0;
}

function commandApply(connectionString, options) {
	ensureBookkeeping(connectionString);
	const { files, contents, checksums } = loadLocalState();
	const plan = computePlan(files, checksums, loadApplied(connectionString));
	printPlan(plan);

	if (plan.drifted.length > 0 && !options.force) {
		console.error('refusing to apply: recorded migrations changed on disk (use --force to override)');
		return 2;
	}

	let toApply = plan.pending;
	if (options.to) {
		if (!files.includes(options.to)) {
			console.error(`--to ${options.to} is not a migration file`);
			return 1;
		}
		toApply = toApply.filter((file) => file <= options.to);
	}
	if (toApply.length === 0) {
		console.log('nothing to apply');
		return 0;
	}

	for (const file of toApply) {
		const text = contents.get(file);
		const control = findTransactionControl(text);
		if (control) {
			console.error(`refusing to apply ${file}: it contains its own '${control};' — the runner owns the transaction`);
			return 1;
		}
	}

	for (const file of toApply) {
		const sqlText = composeApplySql(file, contents.get(file), checksums.get(file));
		if (options.dryRun) {
			console.log(`[dry-run] would apply ${file} in one transaction:`);
			console.log(`  psql <SUPABASE_POOLER_URL> -X -v ON_ERROR_STOP=1 -q --single-transaction -f <tmp>/${file}`);
			console.log(`  trailer:\n${sqlText.slice(sqlText.indexOf('-- db-migrate: tenancy backstops'))}`);
			continue;
		}
		console.log(`applying ${file} ...`);
		runSqlFileInTransaction(connectionString, sqlText, file);
		console.log(`applied  ${file}`);
	}
	return 0;
}

function commandBaseline(connectionString, options) {
	if (!options.through) {
		console.error('baseline requires --through <filename> (the last migration already applied by hand)');
		return 1;
	}
	ensureBookkeeping(connectionString);
	const { files, checksums } = loadLocalState();
	if (!files.includes(options.through)) {
		console.error(`--through ${options.through} is not a migration file`);
		return 1;
	}
	const applied = loadApplied(connectionString);
	if (applied.size > 0 && !options.force) {
		console.error(`refusing to baseline: schema_migrations already has ${applied.size} rows (use --force to add missing rows)`);
		return 2;
	}
	const entries = files
		.filter((file) => file <= options.through && !applied.has(file))
		.map((file) => ({ filename: file, checksum: checksums.get(file) }));
	console.log(`baseline: recording ${entries.length} file(s) as applied, through ${options.through}`);
	for (const entry of entries) console.log(`  = ${entry.filename}`);
	if (entries.length === 0) return 0;
	const sqlText = composeBaselineSql(entries);
	if (options.dryRun) {
		console.log('[dry-run] would run:\n' + sqlText);
		return 0;
	}
	runSqlFileInTransaction(connectionString, sqlText, 'baseline');
	console.log('baseline recorded');
	return 0;
}

export function main(argv = process.argv.slice(2)) {
	const { command, options } = parseArgs(argv);
	const connectionString = resolveConnectionString();
	switch (command) {
		case 'status':
			return commandStatus(connectionString);
		case 'apply':
			return commandApply(connectionString, options);
		case 'baseline':
			return commandBaseline(connectionString, options);
		default:
			console.error(`unknown command: ${command} (expected status | apply | baseline)`);
			return 1;
	}
}

const invokedDirectly =
	process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (invokedDirectly) {
	try {
		process.exitCode = main();
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}
