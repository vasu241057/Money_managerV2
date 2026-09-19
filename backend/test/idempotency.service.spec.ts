// Key parsing and body hashing (Task 7). The ledger behaviour itself runs
// against the real schema in test/tenancy/idempotency.spec.ts.

import { describe, expect, it } from 'vitest';

import { HttpError } from '../src/lib/http/errors';
import {
	canonicalJson,
	hashRequestBody,
	IDEMPOTENCY_KEY_MAX_LENGTH,
	parseIdempotencyKey,
} from '../src/services/idempotency.service';

describe('idempotency key parsing', () => {
	it('treats a missing or blank header as opt-out', () => {
		expect(parseIdempotencyKey(undefined)).toBeNull();
		expect(parseIdempotencyKey(null)).toBeNull();
		expect(parseIdempotencyKey('   ')).toBeNull();
		expect(parseIdempotencyKey(42)).toBeNull();
	});

	it('trims and accepts a normal key', () => {
		expect(parseIdempotencyKey('  0f1a-2b3c  ')).toBe('0f1a-2b3c');
	});

	it('takes the first value when the header repeats', () => {
		expect(parseIdempotencyKey(['first', 'second'])).toBe('first');
	});

	it('rejects a key longer than the column allows', () => {
		const error = (() => {
			try {
				parseIdempotencyKey('x'.repeat(IDEMPOTENCY_KEY_MAX_LENGTH + 1));
				return null;
			} catch (caught) {
				return caught;
			}
		})();

		expect(error).toBeInstanceOf(HttpError);
		expect((error as HttpError).statusCode).toBe(400);
		expect((error as HttpError).errorCode).toBe('INVALID_IDEMPOTENCY_KEY');
	});

	it('rejects control characters and spaces inside the key', () => {
		expect(() => parseIdempotencyKey('has space')).toThrow(HttpError);
		expect(() => parseIdempotencyKey('has\nnewline')).toThrow(HttpError);
	});
});

describe('request body hashing', () => {
	it('ignores key order', async () => {
		const left = await hashRequestBody({ amount: 100, note: 'lunch' });
		const right = await hashRequestBody({ note: 'lunch', amount: 100 });
		expect(left).toBe(right);
	});

	it('respects array order', async () => {
		const left = await hashRequestBody({ items: [1, 2] });
		const right = await hashRequestBody({ items: [2, 1] });
		expect(left).not.toBe(right);
	});

	it('separates a changed value', async () => {
		const left = await hashRequestBody({ amount: 100 });
		const right = await hashRequestBody({ amount: 101 });
		expect(left).not.toBe(right);
	});

	it('distinguishes an absent field from an explicit null', async () => {
		const left = await hashRequestBody({ amount: 100 });
		const right = await hashRequestBody({ amount: 100, note: null });
		expect(left).not.toBe(right);
	});

	it('canonicalises nested objects and drops undefined', () => {
		expect(canonicalJson({ b: 1, a: { d: 2, c: 3 }, e: undefined })).toBe('{"a":{"c":3,"d":2},"b":1}');
	});

	it('produces a hex sha-256 digest', async () => {
		expect(await hashRequestBody({})).toMatch(/^[0-9a-f]{64}$/);
	});
});
