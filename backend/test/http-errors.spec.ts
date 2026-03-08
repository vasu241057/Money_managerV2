import { describe, expect, it } from 'vitest';

import { toHttpError } from '../src/lib/http/errors';

describe('http error mapping', () => {
	it('maps postgres foreign-key violations to conflict', () => {
		const error = toHttpError({
			code: '23503',
			constraint: 'fk_transactions_account_user',
			detail: 'Key (account_id) is not present',
		});

		expect(error.statusCode).toBe(409);
		expect(error.errorCode).toBe('REFERENTIAL_CONFLICT');
		expect(error.details).toBe('fk_transactions_account_user');
	});
});
