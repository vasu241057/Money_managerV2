// Proves the Task 2 privilege model on the real schema: the Supabase Data API
// roles (anon, authenticated) can touch nothing in public, app_backend (the
// identity the Worker connects as after the role switch) reads and writes
// every row through the blanket policies with the scope triggers still
// firing, and assert_tenancy_backstops() fails loudly for a table a future
// migration forgets to harden. PGlite runs as a superuser, so each check
// switches role inside a transaction that is always rolled back.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { insertLooseEvent, seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

const SENTINEL = 'TENANCY_ROLLBACK_SENTINEL';
const API_ROLES = ['anon', 'authenticated'] as const;

interface RelationRow {
	relname: string;
	relkind: string;
}

interface CountRow {
	count: string;
}

function messageOf(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

describe('tenancy: roles and row level security', () => {
	let tenancy: TenancyDb;
	let fx: TwoTenantFixture;
	let relations: RelationRow[];

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		fx = await seedTwoTenants(tenancy.sql);
		relations = await tenancy.sql<RelationRow[]>`
			select c.relname, c.relkind::text as relkind
			from pg_class c
			join pg_namespace n on n.oid = c.relnamespace
			where n.nspname = 'public' and c.relkind in ('r', 'p', 'v', 'm')
			order by c.relname
		`;
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	/**
	 * Runs `statements` as `role` inside a transaction that never commits.
	 * Resolves to null when every statement was accepted, else the error text.
	 */
	async function attemptAs(role: string, statements: string[], params: unknown[][] = []): Promise<string | null> {
		try {
			await tenancy.db.transaction(async (tx) => {
				await tx.query("select set_config('role', $1, true)", [role]);
				for (let index = 0; index < statements.length; index += 1) {
					await tx.query(statements[index], params[index] ?? []);
				}
				throw new Error(SENTINEL);
			});
		} catch (error) {
			const message = messageOf(error);
			return message === SENTINEL ? null : message;
		}
		throw new Error('transaction unexpectedly committed');
	}

	it('the schema has tables to prove things on', () => {
		expect(relations.filter((r) => r.relkind === 'r').length).toBeGreaterThan(30);
		expect(relations.some((r) => r.relkind === 'v')).toBe(true);
		expect(relations.some((r) => r.relkind === 'm')).toBe(true);
	});

	it('anon and authenticated are denied on every table, view and materialized view in public', async () => {
		for (const role of API_ROLES) {
			for (const relation of relations) {
				const outcome = await attemptAs(role, [`select 1 from public."${relation.relname}" limit 1`]);
				expect(outcome, `${role} could read ${relation.relname}`).toMatch(/permission denied/i);
			}
		}
	});

	it('anon and authenticated cannot execute any function of ours in public', async () => {
		for (const role of API_ROLES) {
			for (const fn of ['normalize_merchant_logo_key', 'assert_tenancy_backstops', 'grant_app_backend']) {
				const rows = await tenancy.sql<{ ok: boolean }[]>`
					select bool_and(has_function_privilege(${role}, p.oid, 'EXECUTE')) as ok
					from pg_proc p
					join pg_namespace n on n.oid = p.pronamespace
					where n.nspname = 'public' and p.proname = ${fn}
				`;
				expect(rows[0]?.ok, `${role} can execute ${fn}`).toBe(false);
			}
		}
	});

	it('app_backend reads every relation and sees every row through the blanket policies', async () => {
		for (const relation of relations) {
			const outcome = await attemptAs('app_backend', [`select 1 from public."${relation.relname}" limit 1`]);
			expect(outcome, `app_backend denied on ${relation.relname}`).toBeNull();
		}

		const asOwner = await tenancy.sql<CountRow[]>`select count(*) from public.transactions`;
		let asApp = '';
		await tenancy.db.transaction(async (tx) => {
			await tx.query("select set_config('role', 'app_backend', true)");
			const result = await tx.query<{ count: string | number }>('select count(*)::text as count from public.transactions');
			asApp = String(result.rows[0]?.count);
		});
		expect(Number(asOwner[0]?.count)).toBeGreaterThan(0);
		expect(asApp).toBe(asOwner[0]?.count);
	});

	it('app_backend writes go through, with triggers and same-user backstops still enforced', async () => {
		const { a, b } = fx;
		const eventId = await insertLooseEvent(tenancy.sql, a.userId, null, 201);

		const accepted = await attemptAs(
			'app_backend',
			[
				`insert into public.transactions (user_id, financial_event_id, category_id, amount_in_paise, type, txn_date)
				 values ($1, $2, $3, 4200, 'expense', '2026-09-18T10:00:00Z')`,
				'update public.users set last_app_open_date = now() where id = $1',
				'delete from public.transactions where financial_event_id = $1 and user_id = $2',
			],
			[[a.userId, eventId, a.expenseCategoryId], [a.userId], [eventId, a.userId]],
		);
		expect(accepted).toBeNull();

		const crossUser = await attemptAs(
			'app_backend',
			[
				`insert into public.transactions (user_id, financial_event_id, category_id, amount_in_paise, type, txn_date)
				 values ($1, $2, $3, 4200, 'expense', '2026-09-18T10:00:00Z')`,
			],
			[[a.userId, eventId, b.expenseCategoryId]],
		);
		expect(crossUser).toMatch(/must belong to the same user/);
	});

	it('every table in public carries the app_backend_all policy and RLS', async () => {
		const rows = await tenancy.sql<{ tablename: string }[]>`
			select c.relname as tablename
			from pg_class c
			join pg_namespace n on n.oid = c.relnamespace
			where n.nspname = 'public' and c.relkind in ('r', 'p')
			  and (not c.relrowsecurity or not exists (
			    select 1 from pg_policies p
			    where p.schemaname = 'public' and p.tablename = c.relname and p.policyname = 'app_backend_all'
			  ))
		`;
		expect(rows.map((r) => r.tablename)).toEqual([]);
	});

	it('assert_tenancy_backstops passes on the loaded schema and names every violation of an unhardened table', async () => {
		await tenancy.db.query('select public.assert_tenancy_backstops()');

		const failures: string[] = [];
		let defaultsForNewObjects: { app_select: boolean; anon_select: boolean; app_exec: boolean; anon_exec: boolean } | undefined;
		try {
			await tenancy.db.transaction(async (tx) => {
				async function assertFails(): Promise<string> {
					await tx.query('savepoint probe');
					try {
						await tx.query('select public.assert_tenancy_backstops()');
					} catch (error) {
						await tx.query('rollback to savepoint probe');
						return messageOf(error);
					}
					await tx.query('release savepoint probe');
					return 'PASSED';
				}

				await tx.query('create table public.tenancy_probe (id int primary key)');
				await tx.query("create function public.tenancy_probe_fn() returns int language sql as 'select 1'");
				const defaults = await tx.query<{ app_select: boolean; anon_select: boolean; app_exec: boolean; anon_exec: boolean }>(`
					select
						has_table_privilege('app_backend', 'public.tenancy_probe', 'SELECT') as app_select,
						has_table_privilege('anon', 'public.tenancy_probe', 'SELECT') as anon_select,
						has_function_privilege('app_backend', 'public.tenancy_probe_fn()', 'EXECUTE') as app_exec,
						has_function_privilege('anon', 'public.tenancy_probe_fn()', 'EXECUTE') as anon_exec
				`);
				defaultsForNewObjects = defaults.rows[0];

				failures.push(await assertFails());

				await tx.query("select public.grant_app_backend('public.tenancy_probe')");
				failures.push(await assertFails());

				await tx.query('grant select on public.tenancy_probe to anon');
				failures.push(await assertFails());

				throw new Error(SENTINEL);
			});
		} catch (error) {
			expect(messageOf(error)).toBe(SENTINEL);
		}

		// Default privileges: new objects are born app_backend-only.
		expect(defaultsForNewObjects).toEqual({ app_select: true, anon_select: false, app_exec: true, anon_exec: false });

		expect(failures).toHaveLength(3);
		expect(failures[0]).toMatch(/table tenancy_probe: row level security disabled/);
		expect(failures[0]).toMatch(/table tenancy_probe: missing app_backend_all policy/);
		expect(failures[0]).not.toMatch(/tenancy_probe_fn/);
		expect(failures[1]).toBe('PASSED');
		expect(failures[2]).toMatch(/relation tenancy_probe: anon still has privileges/);
	});

	it('grant_app_backend refuses anything that is not a table', async () => {
		const outcome = await attemptAs('postgres', ["select public.grant_app_backend('public.v_credit_card_transactions')"]);
		expect(outcome).toMatch(/not a table/);
	});
});
