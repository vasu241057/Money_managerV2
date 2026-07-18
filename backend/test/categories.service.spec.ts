import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	deleteCategory,
	restoreCategory,
	updateCategory,
} from '../src/services/categories.service';

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

describe('categories.service delete/hide/restore', () => {
	const USER_ID = '11111111-1111-4111-8111-111111111111';
	const CATEGORY_ID = '22222222-2222-4222-8222-222222222222';

	it('really deletes a user-owned category', async () => {
		const queries: string[] = [];
		const sql = createSqlMock([
			(query) => {
				queries.push(query);
				return [{ id: CATEGORY_ID, name: 'My Cat', type: 'expense', is_system: false }];
			},
			(query) => {
				queries.push(query);
				return [{ id: CATEGORY_ID }];
			},
		]);

		const outcome = await deleteCategory(sql, USER_ID, CATEGORY_ID);
		expect(outcome).toEqual({ hidden: false });
		expect(queries[1]).toContain('delete from public.categories');
	});

	it('hides a system category (plus its direct system children) instead of deleting it', async () => {
		const queries: string[] = [];
		let hideValues: unknown[] = [];
		const sql = createSqlMock([
			(query) => {
				queries.push(query);
				return [{ id: CATEGORY_ID, name: 'Food', type: 'expense', is_system: true }];
			},
			(query, values) => {
				queries.push(query);
				hideValues = values;
				return [];
			},
		]);

		const outcome = await deleteCategory(sql, USER_ID, CATEGORY_ID);
		expect(outcome).toEqual({ hidden: true });
		// The shared row must never be deleted — only a per-user hide written,
		// covering the category and its direct system children.
		expect(queries[1]).toContain('insert into public.user_hidden_categories');
		expect(queries[1]).not.toContain('delete from public.categories');
		expect(queries[1]).toContain('c.parent_id =');
		expect(hideValues[0]).toBe(USER_ID);
	});

	it('refuses to hide the fallback generics', async () => {
		const sql = createSqlMock([
			() => [{ id: CATEGORY_ID, name: 'Miscellaneous', type: 'expense', is_system: true }],
		]);

		await expect(deleteCategory(sql, USER_ID, CATEGORY_ID)).rejects.toMatchObject({
			statusCode: 400,
			errorCode: 'CATEGORY_NOT_HIDEABLE',
		});
	});

	it('restores a hidden system category and 404s when nothing was hidden', async () => {
		const sql = createSqlMock([
			() => [{ category_id: CATEGORY_ID }],
		]);
		await expect(restoreCategory(sql, USER_ID, CATEGORY_ID)).resolves.toBeUndefined();

		const emptySql = createSqlMock([() => []]);
		await expect(restoreCategory(emptySql, USER_ID, CATEGORY_ID)).rejects.toMatchObject({
			statusCode: 404,
			errorCode: 'CATEGORY_NOT_HIDDEN',
		});
	});
});
