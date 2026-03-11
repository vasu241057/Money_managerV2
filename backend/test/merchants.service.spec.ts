import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import {
	listGlobalMerchants,
	parseListGlobalMerchantsQuery,
} from '../src/services/merchants.service';

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlMock(handler: (query: string, values: unknown[]) => unknown): SqlClient {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		return handler(normalizeQuery(strings), values);
	};
	return tag as unknown as SqlClient;
}

describe('merchants service', () => {
	it('parses query defaults', () => {
		expect(parseListGlobalMerchantsQuery({})).toEqual({
			q: null,
			limit: 200,
		});
	});

	it('rejects non-string merchant search query', () => {
		expect(() =>
			parseListGlobalMerchantsQuery({
				q: 123,
			}),
		).toThrow('q must be a string');
	});

	it('rejects merchant list limit above cap', () => {
		expect(() =>
			parseListGlobalMerchantsQuery({
				limit: '1001',
			}),
		).toThrow('limit must be less than or equal to 1000');
	});

	it('rejects merchant search query that is too long', () => {
		expect(() =>
			parseListGlobalMerchantsQuery({
				q: 'a'.repeat(121),
			}),
		).toThrow('q must be at most 120 characters');
	});

	it('queries and maps merchants list', async () => {
		const sql = createSqlMock((query, values) => {
			expect(query).toContain('from public.global_merchants as gm');
			expect(query).toContain("gm.canonical_name ilike");
			expect(values).toContain('%shop%');
			expect(values).toContain(25);

			return [
				{
					merchant_id: '00000000-0000-4000-8000-000000000001',
					merchant_canonical_name: 'Shop One',
					merchant_type: 'MERCHANT',
				},
				{
					merchant_id: '00000000-0000-4000-8000-000000000002',
					merchant_canonical_name: 'Shop Two',
					merchant_type: 'P2P',
				},
			];
		});

		const query = parseListGlobalMerchantsQuery({
			q: 'shop',
			limit: '25',
		});

		const result = await listGlobalMerchants(sql, query);
		expect(result).toEqual([
			{
				id: '00000000-0000-4000-8000-000000000001',
				canonical_name: 'Shop One',
				type: 'MERCHANT',
			},
			{
				id: '00000000-0000-4000-8000-000000000002',
				canonical_name: 'Shop Two',
				type: 'P2P',
			},
		]);
	});
});
