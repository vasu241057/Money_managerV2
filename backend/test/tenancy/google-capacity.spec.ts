// The Google test-user capacity query against the real schema (Task 8).
//
// Same reason as stale-extraction.spec.ts: the unit test for this path
// mocks the SQL client, so the statement itself never runs. A wrong column
// name would pass the suite and then throw once a day in production, where
// the caller's own catch would reduce it to a CHECK_FAILED line — a tripwire
// that never trips, guarding the one thing that hard-caps signups at 100.

import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import type { SqlClient } from '../../src/lib/db/client';
import { seedTenant } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

const harness: { sql: SqlClient | null } = { sql: null };
vi.mock('../../src/lib/db/client', async (importOriginal) => {
	const actual = await importOriginal<typeof import('../../src/lib/db/client')>();
	return {
		...actual,
		getSqlClient: () => {
			if (!harness.sql) throw new Error('harness sql not ready');
			return harness.sql;
		},
	};
});

const { reportGoogleTestUserCapacity } = await import('../../src/workers/scheduled.worker');

let db: TenancyDb;
let sql: SqlClient;

const ENV = {} as unknown as Env;

/** One more user holding one more Google connection. */
async function addConnectedUser(label: string): Promise<string> {
	const rows = await sql<{ id: string }[]>`
		insert into public.users (email, auth_provider, auth_subject)
		values (${`${label}@example.test`}, ${'clerk'}, ${`user_cap_${label}`})
		returning id
	`;
	const userId = rows[0].id;
	await sql`
		insert into public.oauth_connections (user_id, provider, email_address, sync_status)
		values (${userId}, ${'google'}, ${`${label}@gmail.test`}, ${'ACTIVE'})
	`;
	return userId;
}

beforeAll(async () => {
	db = await createTenancyDb();
	sql = db.sql;
	harness.sql = sql;
	const merchant = await sql<{ id: string }[]>`
		insert into public.global_merchants (canonical_name, type, source, confirmed)
		values (${'Capacity Fixture Brand'}, ${'MERCHANT'}, ${'SEED'}, ${true})
		returning id
	`;
	await seedTenant(sql, 'capacity', { sharedMerchantId: merchant[0].id }, 'Pets');
}, 120_000);

afterAll(async () => {
	harness.sql = null;
	await db?.close();
});

describe('tenancy: Google test-user capacity', () => {
	it('counts the seeded tenant and stays below the warning threshold', async () => {
		const info = vi.spyOn(console, 'info').mockImplementation(() => {});
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});

		await reportGoogleTestUserCapacity(ENV);

		expect(info.mock.calls.find((call) => call[0] === 'GOOGLE_TEST_USER_CAPACITY')?.[1]).toMatchObject({
			connectedUserCount: 1,
			cap: 100,
		});
		expect(error).not.toHaveBeenCalled();
		info.mockRestore();
		error.mockRestore();
	});

	// One user with two mailboxes is one Google test user, not two: the
	// consent-screen list is keyed by the account that granted access.
	it('counts users, not connections', async () => {
		const info = vi.spyOn(console, 'info').mockImplementation(() => {});
		const userId = await addConnectedUser('capacity_two');
		await sql`
			insert into public.oauth_connections (user_id, provider, email_address, sync_status)
			values (${userId}, ${'google'}, ${'capacity_two_alt@gmail.test'}, ${'ACTIVE'})
		`;

		await reportGoogleTestUserCapacity(ENV);

		expect(info.mock.calls.at(-1)?.[1]).toMatchObject({ connectedUserCount: 2 });
		info.mockRestore();
	});

	// A revoked connection has not freed the slot on Google's list, so it
	// still counts.
	it('keeps counting a user whose connection was revoked', async () => {
		const info = vi.spyOn(console, 'info').mockImplementation(() => {});
		const userId = await addConnectedUser('capacity_revoked');
		await sql`
			update public.oauth_connections
			set sync_status = ${'AUTH_REVOKED'}
			where user_id = ${userId}
		`;

		await reportGoogleTestUserCapacity(ENV);

		expect(info.mock.calls.at(-1)?.[1]).toMatchObject({ connectedUserCount: 3 });
		info.mockRestore();
	});

	it('warns once the count reaches the threshold', async () => {
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});
		for (let index = 0; index < 77; index += 1) {
			await addConnectedUser(`capacity_bulk_${index}`);
		}

		await reportGoogleTestUserCapacity(ENV);

		expect(error.mock.calls.find((call) => call[0] === 'GOOGLE_TEST_USER_CAP_WARNING')?.[1]).toMatchObject({
			connectedUserCount: 80,
			cap: 100,
			warnThreshold: 80,
			countIsLowerBound: true,
		});
		error.mockRestore();
	});
});
