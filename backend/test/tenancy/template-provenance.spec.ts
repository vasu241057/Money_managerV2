// Template scope on the real schema through the real save/load functions: a
// learned template is PRIVATE to its learner and invisible to everyone else,
// a second user independently inducing the same structural template promotes
// it to one SHARED row (hit counts summed, higher confidence), a different
// structural slot map never promotes, promotion also fires when the
// learner's row already existed, a shared twin makes private duplicates
// redundant, each partition is pruned to its cap, and the scope invariant is
// enforced by the database.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { UUID } from '../../../shared/types';
import type { SqlClient } from '../../src/lib/db/client';
import {
	loadTemplatesForSenders,
	recordTemplateHit,
	saveTemplate,
	skeletonHash,
	type TemplateSlotMap,
} from '../../src/lib/email-templates';
import { seedTwoTenants, type TwoTenantFixture } from './fixtures';
import { createTenancyDb, type TenancyDb } from './pglite-db';

const SENDER = 'alerts@bank.test';
const SENTINEL = 'TENANCY_ROLLBACK_SENTINEL';

function slotMap(overrides: Partial<TemplateSlotMap> = {}): TemplateSlotMap {
	return {
		version: 1,
		kind: 'EXTRACT',
		direction: 'debit',
		payment_method: 'upi',
		amount_ordinal: 1,
		date_pattern: '{{3}}-{{4}}-{{5}}',
		instrument_ordinal: 2,
		merchant: { kind: 'WILDCARD' },
		confidence: 0.6,
		...overrides,
	};
}

function skeleton(label: string): string {
	return `Dear {{S}}, Rs.{{N}} has been debited from your account **{{N}} on {{N}}-{{N}}-{{N}} to VPA {{M}}. ${label} Ref No {{W}}. Not you? Call {{W}}.`;
}

interface TemplateRow {
	id: string;
	scope: string;
	learned_from_user_id: string | null;
	learned_from_raw_email_id: string | null;
	hit_count: number;
	slot_map: TemplateSlotMap;
}

describe('tenancy: template provenance and promotion', () => {
	let tenancy: TenancyDb;
	let fx: TwoTenantFixture;

	beforeAll(async () => {
		tenancy = await createTenancyDb();
		fx = await seedTwoTenants(tenancy.sql);
	}, 120_000);

	afterAll(async () => {
		await tenancy?.close();
	});

	async function rowsFor(skel: string): Promise<TemplateRow[]> {
		return tenancy.sql<TemplateRow[]>`
			select id, scope, learned_from_user_id, learned_from_raw_email_id, hit_count, slot_map
			from public.email_templates
			where from_address = ${SENDER} and skeleton_hash = ${skeletonHash(skel)}
			order by scope, learned_from_user_id
		`;
	}

	async function visibleTo(userId: string): Promise<string[]> {
		const bySender = await loadTemplatesForSenders(tenancy.sql, [SENDER], [userId as UUID]);
		return (bySender.get(SENDER) ?? []).map((template) => template.skeleton);
	}

	it('a learned template is PRIVATE to its learner and invisible to the other user', async () => {
		const skel = skeleton('ALPHA');
		const outcome = await saveTemplate(tenancy.sql, {
			from_address: SENDER,
			skeleton: skel,
			slot_map: slotMap({ confidence: 0.6 }),
			learned_from_raw_email_id: fx.a.rawEmailIds[0] as UUID,
			learned_from_user_id: fx.a.userId as UUID,
		});
		expect(outcome).toEqual({ inserted: true, promoted: false });

		const rows = await rowsFor(skel);
		expect(rows).toHaveLength(1);
		expect(rows[0]).toMatchObject({ scope: 'PRIVATE', learned_from_user_id: fx.a.userId });

		expect(await visibleTo(fx.a.userId)).toContain(skel);
		expect(await visibleTo(fx.b.userId)).not.toContain(skel);
		const forA = (await loadTemplatesForSenders(tenancy.sql, [SENDER], [fx.a.userId as UUID])).get(SENDER) ?? [];
		expect(forA[0]).toMatchObject({ scope: 'PRIVATE', learned_from_user_id: fx.a.userId });
	});

	it('a second user inducing the same structural template promotes it to one SHARED row', async () => {
		const skel = skeleton('ALPHA');
		const aRow = (await rowsFor(skel))[0] as TemplateRow;
		await recordTemplateHit(tenancy.sql, aRow.id as UUID);
		await recordTemplateHit(tenancy.sql, aRow.id as UUID);

		const outcome = await saveTemplate(tenancy.sql, {
			from_address: SENDER,
			skeleton: skel,
			slot_map: slotMap({ confidence: 0.9 }),
			learned_from_raw_email_id: fx.b.rawEmailIds[0] as UUID,
			learned_from_user_id: fx.b.userId as UUID,
		});
		expect(outcome).toEqual({ inserted: true, promoted: true });

		const rows = await rowsFor(skel);
		expect(rows).toHaveLength(1);
		expect(rows[0]).toMatchObject({
			scope: 'SHARED',
			learned_from_user_id: null,
			learned_from_raw_email_id: null,
			hit_count: 2,
		});
		expect(rows[0]?.slot_map.confidence).toBe(0.9);

		expect(await visibleTo(fx.a.userId)).toContain(skel);
		expect(await visibleTo(fx.b.userId)).toContain(skel);
	});

	it('a differing structural slot map never promotes', async () => {
		const skel = skeleton('BRAVO');
		await saveTemplate(tenancy.sql, {
			from_address: SENDER,
			skeleton: skel,
			slot_map: slotMap({ payment_method: 'upi' }),
			learned_from_raw_email_id: null,
			learned_from_user_id: fx.a.userId as UUID,
		});
		const outcome = await saveTemplate(tenancy.sql, {
			from_address: SENDER,
			skeleton: skel,
			slot_map: slotMap({ payment_method: 'debit_card' }),
			learned_from_raw_email_id: null,
			learned_from_user_id: fx.b.userId as UUID,
		});
		expect(outcome).toEqual({ inserted: true, promoted: false });

		const rows = await rowsFor(skel);
		expect(rows.map((row) => row.scope)).toEqual(['PRIVATE', 'PRIVATE']);
		expect(rows.map((row) => row.learned_from_user_id).sort()).toEqual([fx.a.userId, fx.b.userId].sort());
		expect(await visibleTo(fx.a.userId)).toContain(skel);
		expect(await visibleTo(fx.b.userId)).toContain(skel);
		const forA = (await loadTemplatesForSenders(tenancy.sql, [SENDER], [fx.a.userId as UUID])).get(SENDER) ?? [];
		expect(forA.filter((template) => template.skeleton === skel)).toHaveLength(1);
	});

	it('promotes even when the learner’s own row already existed (no-op insert)', async () => {
		const skel = skeleton('CHARLIE');
		await saveTemplate(tenancy.sql, {
			from_address: SENDER,
			skeleton: skel,
			slot_map: slotMap(),
			learned_from_raw_email_id: null,
			learned_from_user_id: fx.a.userId as UUID,
		});
		// B's equal row landed without a promotion scan (e.g. a concurrent learner).
		await tenancy.sql`
			insert into public.email_templates (from_address, skeleton_hash, skeleton, slot_map, learned_from_user_id, scope)
			values (${SENDER}, ${skeletonHash(skel)}, ${skel}, ${JSON.stringify(slotMap({ confidence: 0.7 }))}::jsonb, ${fx.b.userId}, ${'PRIVATE'})
		`;

		const outcome = await saveTemplate(tenancy.sql, {
			from_address: SENDER,
			skeleton: skel,
			slot_map: slotMap(),
			learned_from_raw_email_id: null,
			learned_from_user_id: fx.a.userId as UUID,
		});
		expect(outcome).toEqual({ inserted: false, promoted: true });
		expect((await rowsFor(skel)).map((row) => row.scope)).toEqual(['SHARED']);
	});

	it('a shared twin makes a later private duplicate redundant, not learned', async () => {
		const skel = skeleton('ALPHA');
		const outcome = await saveTemplate(tenancy.sql, {
			from_address: SENDER,
			skeleton: skel,
			slot_map: slotMap({ confidence: 0.5 }),
			learned_from_raw_email_id: null,
			learned_from_user_id: fx.a.userId as UUID,
		});
		expect(outcome).toEqual({ inserted: false, promoted: false });
		expect((await rowsFor(skel)).map((row) => row.scope)).toEqual(['SHARED']);
	});

	it('prunes each partition to its cap without touching the other user or shared rows', async () => {
		const caps = { private: 2, shared: 40 };
		for (const label of ['PRUNE1', 'PRUNE2', 'PRUNE3']) {
			await saveTemplate(tenancy.sql, {
				from_address: SENDER,
				skeleton: skeleton(label),
				slot_map: slotMap(),
				learned_from_raw_email_id: null,
				learned_from_user_id: fx.a.userId as UUID,
				caps,
			});
		}
		const privateA = await tenancy.sql<Array<{ skeleton: string }>>`
			select skeleton from public.email_templates
			where from_address = ${SENDER} and scope = 'PRIVATE' and learned_from_user_id = ${fx.a.userId}
			order by created_at
		`;
		expect(privateA).toHaveLength(2);
		expect(privateA.map((row) => row.skeleton)).not.toContain(skeleton('PRUNE1'));

		const others = await tenancy.sql<Array<{ scope: string; count: string }>>`
			select scope, count(*) from public.email_templates
			where from_address = ${SENDER} and (scope = 'SHARED' or learned_from_user_id = ${fx.b.userId})
			group by scope order by scope
		`;
		expect(others).toEqual([
			{ scope: 'PRIVATE', count: '1' },
			{ scope: 'SHARED', count: '2' },
		]);
	});

	it('the database enforces the scope invariant', async () => {
		// Each statement runs inside a transaction that never commits, on the
		// transaction's own client (PGlite has one connection).
		async function attempt(run: (tx: SqlClient) => Promise<unknown>): Promise<string> {
			try {
				await tenancy.sql.begin(async (tx) => {
					await run(tx as unknown as SqlClient);
					throw new Error(SENTINEL);
				});
			} catch (error) {
				return error instanceof Error ? error.message : String(error);
			}
			return 'committed';
		}
		const skel = skeleton('INVARIANT');
		expect(
			await attempt(
				(tx) => tx`
					insert into public.email_templates (from_address, skeleton_hash, skeleton, slot_map, learned_from_user_id, scope)
					values (${SENDER}, ${skeletonHash(skel)}, ${skel}, ${'{}'}::jsonb, ${fx.a.userId}, ${'SHARED'})
				`,
			),
		).toMatch(/email_templates_scope_owner_check/);
		expect(
			await attempt(
				(tx) => tx`
					insert into public.email_templates (from_address, skeleton_hash, skeleton, slot_map, learned_from_user_id, scope)
					values (${SENDER}, ${skeletonHash(skel)}, ${skel}, ${'{}'}::jsonb, null, ${'PRIVATE'})
				`,
			),
		).toMatch(/email_templates_scope_owner_check/);
	});
});
