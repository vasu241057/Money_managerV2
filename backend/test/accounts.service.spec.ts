import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { updateAccount } from '../src/services/accounts.service';

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

describe('accounts.service', () => {
	it('rejects changing account type away from card when linked credit cards exist', async () => {
		const sql = createSqlMock([
			() => [{ id: 'card-1' }],
		]);

		await expect(
			updateAccount(
				sql,
				'11111111-1111-4111-8111-111111111111',
				'22222222-2222-4222-8222-222222222222',
				{ type: 'bank' },
			),
		).rejects.toMatchObject({
			statusCode: 409,
			errorCode: 'ACCOUNT_TYPE_CONFLICT',
		});
	});
});
