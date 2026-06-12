import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { deleteManualTransaction } from '../src/services/transactions.service';

type QueryResult = unknown;
type QueryHandler = (query: string, values: unknown[]) => QueryResult | Promise<QueryResult>;

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlForDeleteTests(handlers: QueryHandler[]): {
	sql: SqlClient;
	queries: string[];
} {
	const queries: string[] = [];
	let index = 0;

	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		queries.push(query);
		const handler = handlers[index];
		index += 1;

		if (!handler) {
			throw new Error(`Unexpected query execution: ${query}`);
		}

		return handler(query, values);
	};

	const sql = tag as unknown as SqlClient;
	(sql as unknown as { begin: (cb: (tx: SqlClient) => Promise<void>) => Promise<void> }).begin = async (
		callback: (tx: SqlClient) => Promise<void>,
	) => {
		await callback(sql);
	};

	return { sql, queries };
}

describe('deleteManualTransaction', () => {
	it('deletes manual transaction and reverses linked financial event in one DB transaction', async () => {
		const { sql, queries } = createSqlForDeleteTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'VERIFIED',
					financial_event_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
					raw_email_id: null,
				},
			],
			() => [],
			() => [],
		]);

		await deleteManualTransaction(
			sql,
			'11111111-1111-4111-8111-111111111111',
			'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
		);

		expect(queries).toHaveLength(3);
		expect(queries[0]).toContain('for update');
		expect(queries[1]).toContain('delete from public.transactions');
		expect(queries[2]).toContain('update public.financial_events as fe set status =');
	});

	it('rejects delete for non-manual transaction rows', async () => {
		const { sql, queries } = createSqlForDeleteTests([
			() => [
				{
					transaction_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
					transaction_status: 'VERIFIED',
					financial_event_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
					raw_email_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
				},
			],
		]);

		await expect(
			deleteManualTransaction(
				sql,
				'11111111-1111-4111-8111-111111111111',
				'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			),
		).rejects.toMatchObject({
			statusCode: 409,
			errorCode: 'TRANSACTION_NOT_MANUAL',
		});

		expect(queries).toHaveLength(1);
	});

	it('returns not found when transaction does not exist for user', async () => {
		const { sql, queries } = createSqlForDeleteTests([() => []]);

		await expect(
			deleteManualTransaction(
				sql,
				'11111111-1111-4111-8111-111111111111',
				'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
			),
		).rejects.toMatchObject({
			statusCode: 404,
			errorCode: 'TRANSACTION_NOT_FOUND',
		});

		expect(queries).toHaveLength(1);
	});
});
