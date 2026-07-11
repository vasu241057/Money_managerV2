import { describe, expect, it } from 'vitest';

import { HttpError } from '../src/lib/http/errors';
import {
	assertAiRequiresWebhookSecret,
	getAiRequiresWebhookSecretHeaderName,
} from '../src/services/ai-webhook.service';

describe('internal webhook secret guard', () => {
	it('exposes the secret header name', () => {
		expect(getAiRequiresWebhookSecretHeaderName()).toBe('x-ai-webhook-secret');
	});

	it('accepts a matching secret', () => {
		expect(() => assertAiRequiresWebhookSecret('s3cret', 's3cret')).not.toThrow();
	});

	it('accepts a matching secret with surrounding whitespace', () => {
		expect(() => assertAiRequiresWebhookSecret('  s3cret  ', 's3cret')).not.toThrow();
	});

	it('rejects a missing header with 401', () => {
		try {
			assertAiRequiresWebhookSecret(undefined, 's3cret');
			expect.unreachable('should have thrown');
		} catch (error) {
			expect(error).toBeInstanceOf(HttpError);
			expect((error as HttpError).statusCode).toBe(401);
		}
	});

	it('rejects a mismatched secret with 401', () => {
		try {
			assertAiRequiresWebhookSecret('wrong', 's3cret');
			expect.unreachable('should have thrown');
		} catch (error) {
			expect(error).toBeInstanceOf(HttpError);
			expect((error as HttpError).statusCode).toBe(401);
		}
	});

	it('rejects a same-length mismatched secret with 401', () => {
		try {
			assertAiRequiresWebhookSecret('s3cres', 's3cret');
			expect.unreachable('should have thrown');
		} catch (error) {
			expect(error).toBeInstanceOf(HttpError);
			expect((error as HttpError).statusCode).toBe(401);
		}
	});
});
