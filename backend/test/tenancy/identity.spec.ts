// Task 2 identity changes on the real schema: users carry the provider
// subject with per-provider uniqueness, email is no longer unique, the
// card_statements composite FK nulls only the card pointer on card deletion,
// and the new category scope triggers still accept legitimate rows.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

const SENTINEL = 'TENANCY_ROLLBACK_SENTINEL';

type Attempt = (tx: SqlClient) => Promise<unknown>;

describe('tenancy: identity hardening (users, card statements, category scope)', () => {
	let tenancy: TenancyDb;
	let fx: TwoTenantFixture;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		fx = await seedTwoTenants(tenancy.sql);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	/** Runs the statement in a transaction that never commits; resolves to SENTINEL when accepted, else the rejection. */
	async function attempt(statement: Attempt): Promise<string> {
		try {
			await tenancy.sql.begin(async (tx) => {
				await statement(tx as unknown as SqlClient);
				throw new Error(SENTINEL);
			});
		} catch (error) {
			return error instanceof Error ? error.message : String(error);
		}
		throw new Error('transaction unexpectedly committed');
	}

	it('fixture users carry auth_provider and auth_subject', async () => {
		const rows = await tenancy.sql<{ auth_provider: string; auth_subject: string | null }[]>`
			select auth_provider, auth_subject from public.users where id = ${fx.a.userId}
		`;
		expect(rows[0]).toEqual({ auth_provider: 'clerk', auth_subject: 'user_fixture_alpha' });
	});

	it('the same provider subject cannot own two user rows', async () => {
		const outcome = await attempt(
			(tx) => tx`
				insert into public.users (email, auth_provider, auth_subject)
				values (${'dup@example.test'}, ${'clerk'}, ${'user_fixture_alpha'})
			`,
		);
		expect(outcome).toMatch(/duplicate key|users_auth_identity_uidx/);
	});

	it('auth_subject may be null on legacy rows but never blank, and the provider must be known', async () => {
		expect(
			await attempt((tx) => tx`insert into public.users (email, auth_subject) values (${'legacy@example.test'}, null)`),
		).toBe(SENTINEL);
		expect(
			await attempt((tx) => tx`insert into public.users (email, auth_subject) values (${'blank@example.test'}, ${'  '})`),
		).toMatch(/users_auth_subject_nonempty_check/);
		expect(
			await attempt(
				(tx) => tx`insert into public.users (email, auth_provider, auth_subject) values (${'x@example.test'}, ${'other'}, ${'sub'})`,
			),
		).toMatch(/users_auth_provider_check/);
	});

	it('two identities may share an email address', async () => {
		const outcome = await attempt(
			(tx) => tx`
				insert into public.users (email, auth_provider, auth_subject)
				values (${'alpha@example.test'}, ${'clerk'}, ${'user_fixture_alpha_second_identity'})
			`,
		);
		expect(outcome).toBe(SENTINEL);

		const uniques = await tenancy.sql<{ conname: string }[]>`
			select conname from pg_constraint
			where conrelid = 'public.users'::regclass and contype = 'u'
		`;
		expect(uniques.map((row) => row.conname)).not.toContain('users_email_key');

		const indexes = await tenancy.sql<{ indexname: string }[]>`
			select indexname from pg_indexes where schemaname = 'public' and tablename = 'users' order by indexname
		`;
		expect(indexes.map((row) => row.indexname)).toEqual(
			expect.arrayContaining(['idx_users_email', 'users_auth_identity_uidx']),
		);
	});

	it('the users.id derivation is documented as frozen on the column', async () => {
		const rows = await tenancy.sql<{ comment: string | null }[]>`
			select col_description('public.users'::regclass, attnum) as comment
			from pg_attribute where attrelid = 'public.users'::regclass and attname = 'id'
		`;
		expect(rows[0]?.comment).toMatch(/FROZEN/);
		expect(rows[0]?.comment).toContain('2d0ec934-68a4-4184-adf1-46d5f9d5cc0f');
	});

	it('deleting a credit card detaches its statements without deleting them', async () => {
		const { a } = fx;
		const outcome = await attempt(async (tx) => {
			await tx`delete from public.user_credit_cards where id = ${a.creditCardId}`;
			const rows = await tx<{ id: string; user_id: string; credit_card_id: string | null }[]>`
				select id, user_id, credit_card_id from public.card_statements where id = ${a.statementId}
			`;
			expect(rows).toEqual([{ id: a.statementId, user_id: a.userId, credit_card_id: null }]);
		});
		expect(outcome).toBe(SENTINEL);

		const fk = await tenancy.sql<{ condef: string }[]>`
			select pg_get_constraintdef(oid) as condef from pg_constraint
			where conrelid = 'public.card_statements'::regclass and conname = 'card_statements_credit_card_id_fkey'
		`;
		expect(fk[0]?.condef).toBe(
			'FOREIGN KEY (credit_card_id, user_id) REFERENCES user_credit_cards(id, user_id) ON DELETE SET NULL (credit_card_id)',
		);
	});

	it('category scope triggers accept the user’s own and system categories', async () => {
		const { a } = fx;
		const outcomes = await Promise.all([
			attempt(
				(tx) => tx`
					insert into public.user_category_budget_rules (user_id, category_id, treatment)
					values (${a.userId}, ${fx.systemExpenseCategoryId}, ${'EXCLUDE'})
					on conflict (user_id, category_id) do update set treatment = excluded.treatment
				`,
			),
			attempt(
				(tx) => tx`
					insert into public.user_hidden_categories (user_id, category_id)
					values (${a.userId}, ${a.expenseCategoryId})
					on conflict do nothing
				`,
			),
			attempt(
				(tx) => tx`
					insert into public.recurring_series (
						user_id, fingerprint, category_id, direction, cadence, expected_day,
						amount_min_in_paise, amount_max_in_paise
					)
					values (${a.userId}, ${'own-category-series'}, ${a.expenseCategoryId}, ${'debit'}, ${'MONTHLY'}, ${5}, ${100}, ${200})
				`,
			),
			attempt(
				(tx) => tx`
					update public.recurring_series set category_id = null where id = ${a.seriesId}
				`,
			),
		]);
		expect(outcomes).toEqual([SENTINEL, SENTINEL, SENTINEL, SENTINEL]);
	});

	it('the category scope trigger is attached to all three tables', async () => {
		const rows = await tenancy.sql<{ tgrelid: string }[]>`
			select tgrelid::regclass::text as tgrelid from pg_trigger
			where tgfoid = 'public.enforce_user_category_scope'::regproc and not tgisinternal
			order by 1
		`;
		expect(rows.map((row) => row.tgrelid)).toEqual([
			'recurring_series',
			'user_category_budget_rules',
			'user_hidden_categories',
		]);
	});
});
