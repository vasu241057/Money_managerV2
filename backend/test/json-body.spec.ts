import { EventEmitter } from 'node:events';

import type { NextFunction, Request, Response } from 'express';
import { describe, expect, it, vi } from 'vitest';

import { HttpError } from '../src/lib/http/errors';
import { createJsonBodyMiddleware } from '../src/lib/http/json-body';

interface MockRequest extends Request, EventEmitter {
	headers: Record<string, string | string[] | undefined>;
	method: string;
	body: unknown;
	destroy: ReturnType<typeof vi.fn>;
}

function createMockRequest(input: {
	contentType?: string;
	contentLength?: string;
	method?: string;
	body?: unknown;
}): MockRequest {
	const request = new EventEmitter() as MockRequest;
	request.method = input.method ?? 'POST';
	request.headers = {};
	if (input.contentType) {
		request.headers['content-type'] = input.contentType;
	}
	if (input.contentLength) {
		request.headers['content-length'] = input.contentLength;
	}
	request.body = input.body;
	request.destroy = vi.fn();
	return request;
}

async function invokeMiddleware(
	req: MockRequest,
): Promise<{ next: ReturnType<typeof vi.fn>; error: unknown }> {
	const middleware = createJsonBodyMiddleware({ limitBytes: 16 });
	const next = vi.fn() as unknown as NextFunction;

	await middleware(req, {} as Response, next);

	return {
		next: next as unknown as ReturnType<typeof vi.fn>,
		error: (next as unknown as ReturnType<typeof vi.fn>).mock.calls[0]?.[0],
	};
}

describe('json body middleware', () => {
	it('parses valid JSON body for application/json content type', async () => {
		const req = createMockRequest({
			contentType: 'application/json; charset=utf-8',
		});
		const pending = invokeMiddleware(req);
		req.emit('data', Buffer.from('{"amount":123}'));
		req.emit('end');

		const result = await pending;

		expect(result.error).toBeUndefined();
		expect(req.body).toEqual({ amount: 123 });
	});

	it('returns INVALID_JSON for malformed payload', async () => {
		const req = createMockRequest({
			contentType: 'application/json',
		});
		const pending = invokeMiddleware(req);
		req.emit('data', Buffer.from('{"amount":'));
		req.emit('end');

		const result = await pending;

		expect(result.error).toBeInstanceOf(HttpError);
		const error = result.error as HttpError;
		expect(error.statusCode).toBe(400);
		expect(error.errorCode).toBe('INVALID_JSON');
	});

	it('maps empty JSON body to object', async () => {
		const req = createMockRequest({
			contentType: 'application/json',
		});
		const pending = invokeMiddleware(req);
		req.emit('end');

		const result = await pending;

		expect(result.error).toBeUndefined();
		expect(req.body).toEqual({});
	});

	it('returns PAYLOAD_TOO_LARGE when body exceeds configured limit', async () => {
		const req = createMockRequest({
			contentType: 'application/json',
		});
		const pending = invokeMiddleware(req);
		req.emit('data', Buffer.from('{"toolong":1234567890}'));

		const result = await pending;

		expect(result.error).toBeInstanceOf(HttpError);
		const error = result.error as HttpError;
		expect(error.statusCode).toBe(413);
		expect(error.errorCode).toBe('PAYLOAD_TOO_LARGE');
		expect(req.destroy).toHaveBeenCalledTimes(1);
	});

	it('skips parsing for non-json content type', async () => {
		const req = createMockRequest({
			contentType: 'text/plain',
		});

		const result = await invokeMiddleware(req);

		expect(result.error).toBeUndefined();
		expect(req.body).toBeUndefined();
	});
});

