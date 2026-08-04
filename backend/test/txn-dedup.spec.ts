import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	findLikelyDuplicateTransfer,
	resolveDedupWindowMinutes,
} from '../src/lib/txn-dedup';

function sqlReturning(rows: unknown[]): { sql: SqlClient; queries: string[] } {
	const queries: string[] = [];
	const sql = (async (strings: TemplateStringsArray) => {
		queries.push(strings.join(' ').replace(/\s+/g, ' ').trim());
		return rows;
	}) as unknown as SqlClient;
	return { sql, queries };
}

const PROBE = {
	user_id: '11111111-1111-4111-8111-111111111111',
	raw_email_id: '22222222-2222-4222-8222-222222222222',
	amount_in_paise: 265100,
	direction: 'debit' as const,
	txn_timestamp: '2026-08-03T00:00:00.000Z',
	window_minutes: 10,
	account_id: null,
};

describe('transfer deduplication', () => {
	it('only looks for transfer rows where exactly one side has an account', async () => {
		const { sql, queries } = sqlReturning([{
			transaction_id: '33333333-3333-4333-8333-333333333333',
			financial_event_id: '44444444-4444-4444-8444-444444444444',
			account_id: '55555555-5555-4555-8555-555555555555',
		}]);

		await expect(findLikelyDuplicateTransfer(sql, PROBE)).resolves.toEqual({
			transaction_id: '33333333-3333-4333-8333-333333333333',
			financial_event_id: '44444444-4444-4444-8444-444444444444',
			account_id: '55555555-5555-4555-8555-555555555555',
		});
		expect(queries[0]).toContain("t.type = 'transfer'");
		expect(queries[0]).toContain('t.account_id is null');
		expect(queries[0]).toContain('t.account_id is not null');
	});

	it('returns null when there is no narrow duplicate candidate', async () => {
		const { sql } = sqlReturning([]);
		await expect(findLikelyDuplicateTransfer(sql, PROBE)).resolves.toBeNull();
	});

	it('defaults invalid configuration to a ten-minute window', () => {
		expect(resolveDedupWindowMinutes({} as Env)).toBe(10);
		expect(resolveDedupWindowMinutes({ TXN_DEDUP_WINDOW_MINUTES: '-1' } as Env)).toBe(10);
		expect(resolveDedupWindowMinutes({ TXN_DEDUP_WINDOW_MINUTES: '0' } as Env)).toBe(0);
	});
});
