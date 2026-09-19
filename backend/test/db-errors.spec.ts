import { describe, expect, it } from 'vitest';

import { isMissingUserReferenceError } from '../src/lib/db/errors';

describe('isMissingUserReferenceError', () => {
	it('recognises a foreign key violation against users by its detail', () => {
		expect(
			isMissingUserReferenceError({
				code: '23503',
				detail: 'Key (user_id)=(11111111-1111-4111-8111-111111111111) is not present in table "users".',
			}),
		).toBe(true);
	});

	it('recognises the default constraint name for user_id foreign keys', () => {
		expect(isMissingUserReferenceError({ code: '23503', constraint_name: 'raw_emails_user_id_fkey' })).toBe(true);
		expect(isMissingUserReferenceError({ code: '23503', constraint: 'financial_events_user_id_fkey' })).toBe(true);
	});

	it('ignores other foreign keys, other codes and non-database errors', () => {
		expect(
			isMissingUserReferenceError({
				code: '23503',
				detail: 'Key (raw_email_id, user_id)=(a, b) is not present in table "raw_emails".',
				constraint_name: 'fk_financial_events_raw_email_user',
			}),
		).toBe(false);
		expect(isMissingUserReferenceError({ code: '23505', detail: 'is not present in table "users"' })).toBe(false);
		expect(isMissingUserReferenceError(new Error('users gone'))).toBe(false);
		expect(isMissingUserReferenceError(null)).toBe(false);
	});
});
