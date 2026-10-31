import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { toHttpError } from '../../src/lib/http/errors';
import { seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';
import { MATRIX_CASES, type MatrixContext } from './service-matrix.cases';
import { snapshotUser, type UserSnapshot } from './snapshot';

describe('tenancy: services called as A with B’s identifiers', () => {
	let tenancy: TenancyDb;
	let fx: TwoTenantFixture;
	let ctx: MatrixContext;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		fx = await seedTwoTenants(tenancy.sql);
		ctx = { sql: tenancy.sql, fx };
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	async function snapshotBoth(): Promise<{ a: UserSnapshot; b: UserSnapshot }> {
		return {
			a: await snapshotUser(tenancy.db, fx.a.userId),
			b: await snapshotUser(tenancy.db, fx.b.userId),
		};
	}

	for (const matrixCase of MATRIX_CASES) {
		it(matrixCase.name, async () => {
			const before = await snapshotBoth();

			let outcome: 'rejected' | 'resolved';
			let result: unknown;
			try {
				result = await matrixCase.run(ctx);
				outcome = 'resolved';
			} catch (error) {
				const http = toHttpError(error);
				if (http.statusCode >= 500) {
					const detail = error instanceof Error ? error.message : String(error);
					throw new Error(`server-class failure instead of a clean rejection: ${detail}`);
				}
				outcome = 'rejected';
			}

			if (outcome === 'resolved') {
				if (!matrixCase.allowNoop) {
					throw new Error('call resolved; a cross-tenant request must be rejected');
				}
				matrixCase.assertResolved?.(result, ctx);
			}

			const after = await snapshotBoth();
			expect(after.b, 'user B’s rows changed').toEqual(before.b);
			expect(after.a, 'user A’s rows changed').toEqual(before.a);
		});
	}
});
