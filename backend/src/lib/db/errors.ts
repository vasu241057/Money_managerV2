// Classifies the one database error that account deletion makes ordinary: a
// queue job that started before a user deleted their account and tries to
// write a row for them after the cascade. The users row is gone, so every
// insert keyed on it fails the foreign key; the job must ack that as
// "nothing left to do", never retry it into the dead-letter queue.

interface PostgresErrorShape {
	code?: unknown;
	detail?: unknown;
	constraint_name?: unknown;
	constraint?: unknown;
}

const FOREIGN_KEY_VIOLATION = '23503';

export function isMissingUserReferenceError(error: unknown): boolean {
	if (typeof error !== 'object' || error === null) {
		return false;
	}
	const shape = error as PostgresErrorShape;
	if (shape.code !== FOREIGN_KEY_VIOLATION) {
		return false;
	}
	const detail = typeof shape.detail === 'string' ? shape.detail : '';
	if (/is not present in table "users"/.test(detail)) {
		return true;
	}
	const constraint =
		typeof shape.constraint_name === 'string'
			? shape.constraint_name
			: typeof shape.constraint === 'string'
				? shape.constraint
				: '';
	return /_user_id_fkey$/.test(constraint) || /_users_fkey$/.test(constraint);
}
