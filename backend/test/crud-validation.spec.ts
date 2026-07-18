import { describe, expect, it } from 'vitest';

import { parseUuid } from '../src/lib/http/validation';
import {
	parseCreateAccountRequest,
	parseUpdateAccountRequest,
} from '../src/services/accounts.service';
import {
	parseCreateCategoryRequest,
	parseUpdateCategoryRequest,
} from '../src/services/categories.service';

describe('uuid validation', () => {
	it('accepts system seed ids (version/variant nibbles are 0, not RFC-4122)', () => {
		// Seed rows use readable ids; a version-strict regex 400s every
		// hide/restore/review call that carries one.
		expect(parseUuid('00000000-0000-0000-0000-000000000148', 'categoryId')).toBe(
			'00000000-0000-0000-0000-000000000148',
		);
	});

	it('still rejects non-uuid strings', () => {
		expect(() => parseUuid('not-a-uuid', 'categoryId')).toThrow(
			'categoryId must be a valid UUID',
		);
	});
});

describe('accounts/categories payload validation', () => {
	it('rejects invalid account type', () => {
		expect(() =>
			parseCreateAccountRequest({
				name: 'Primary',
				type: 'wallet',
			}),
		).toThrow('type must be one of: cash, bank, card, other');
	});

	it('rejects account update with empty payload', () => {
		expect(() => parseUpdateAccountRequest({})).toThrow(
			'At least one account field must be provided for update',
		);
	});

	it('rejects category update with empty payload', () => {
		expect(() => parseUpdateCategoryRequest({})).toThrow(
			'At least one category field must be provided for update',
		);
	});

	it('rejects invalid category UUID parent_id', () => {
		expect(() =>
			parseCreateCategoryRequest({
				name: 'Groceries',
				type: 'expense',
				parent_id: 'not-a-uuid',
			}),
		).toThrow('parent_id must be a valid UUID');
	});
});
