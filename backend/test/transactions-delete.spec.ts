import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { deleteTransaction, restoreTransaction } from '../src/services/transactions.service';

function createSql(result: unknown): { sql: SqlClient; queries: string[] } {
	const queries: string[] = [];
	const tag = async (strings: TemplateStringsArray): Promise<unknown> => {
		queries.push(strings.join(' ').replace(/\s+/g, ' ').trim());
		return result;
	};
	return { sql: tag as unknown as SqlClient, queries };
}

describe('recoverable transaction deletion', () => {
	it('soft-deletes any active transaction without changing its bank fact', async () => {
		const { sql, queries } = createSql([{ id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' }]);
		await deleteTransaction(sql, '11111111-1111-4111-8111-111111111111', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa');
		expect(queries).toHaveLength(1);
		expect(queries[0]).toContain('set user_deleted_at = now()');
		expect(queries[0]).not.toContain('financial_events');
	});

	it('restores a soft-deleted transaction', async () => {
		const { sql, queries } = createSql([{ id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' }]);
		await restoreTransaction(sql, '11111111-1111-4111-8111-111111111111', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa');
		expect(queries[0]).toContain('set user_deleted_at = null');
		expect(queries[0]).toContain('user_deleted_at is not null');
	});

	it('returns not found when delete or restore cannot change a row', async () => {
		const { sql } = createSql([]);
		await expect(deleteTransaction(sql, '11111111-1111-4111-8111-111111111111', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')).rejects.toMatchObject({ statusCode: 404 });
		await expect(restoreTransaction(sql, '11111111-1111-4111-8111-111111111111', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')).rejects.toMatchObject({ statusCode: 404 });
	});
});
