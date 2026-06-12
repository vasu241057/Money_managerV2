import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { updateCategory } from '../src/services/categories.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlMock(handlers: QueryHandler[]): SqlClient {
	let cursor = 0;

	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		const handler = handlers[cursor];
		cursor += 1;

		if (!handler) {
			throw new Error(`Unexpected query execution: ${query}`);
		}

		return handler(query, values);
	};

	return tag as unknown as SqlClient;
}

describe('categories.service', () => {
	it('rejects self-referential parent update', async () => {
		const categoryId = '22222222-2222-4222-8222-222222222222';
		const sql = createSqlMock([
			() => [
				{
					id: categoryId,
					user_id: '11111111-1111-4111-8111-111111111111',
					type: 'expense',
					parent_id: null,
				},
			],
		]);

		await expect(
			updateCategory(
				sql,
				'11111111-1111-4111-8111-111111111111',
				categoryId,
				{ parent_id: categoryId },
			),
		).rejects.toThrow('parent_id cannot reference the category itself');
	});

	it('rejects parent assignment that creates a descendant cycle', async () => {
		const categoryId = '22222222-2222-4222-8222-222222222222';
		const sql = createSqlMock([
			() => [
				{
					id: categoryId,
					user_id: '11111111-1111-4111-8111-111111111111',
					type: 'expense',
					parent_id: null,
				},
			],
			() => [{ id: categoryId }],
		]);

		await expect(
			updateCategory(
				sql,
				'11111111-1111-4111-8111-111111111111',
				categoryId,
				{ parent_id: '33333333-3333-4333-8333-333333333333' },
			),
		).rejects.toThrow('parent_id cannot reference a descendant category');
	});
});
