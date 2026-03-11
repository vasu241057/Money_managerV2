import type { ErrorRequestHandler, NextFunction, Request, RequestHandler, Response } from 'express';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { env as runtimeEnv } from 'cloudflare:workers';

import { createApp } from '../src/app';

function createBaseEnv(): Env {
	return {
		APP_NAME: 'money-manager-backend',
		APP_VERSION: '0.1.0',
		NODE_ENV: 'test',
		SUPABASE_POOLER_URL: 'postgres://postgres:postgres@localhost:6543/postgres',
		CLERK_JWKS_URL: 'https://example.clerk.accounts.dev/.well-known/jwks.json',
		CLERK_JWT_ISSUER: 'https://clerk.example.com',
		DB_MAX_CONNECTIONS: '5',
		DB_CONNECT_TIMEOUT_SECONDS: '5',
		AI_REQUIRES_WEBHOOK_SECRET: undefined,
		AI_QUEUE_DELAY_SECONDS: undefined,
		AI_CLASSIFICATION_QUEUE: undefined,
	} as unknown as Env;
}

function applyTestEnv(overrides: Partial<Env>): void {
	Object.assign(runtimeEnv as unknown as Record<string, unknown>, createBaseEnv(), overrides);
}

interface ExpressLayer {
	handle?: unknown;
	route?: {
		path?: string;
		methods?: Record<string, boolean>;
		stack?: Array<{ handle?: unknown }>;
	};
}

function findPostHandlerByPath(stack: ExpressLayer[] | undefined, path: string): RequestHandler | null {
	if (!Array.isArray(stack)) {
		return null;
	}

	for (const layer of stack) {
		const route = layer.route;
		if (route?.path === path && route.methods?.post === true) {
			const candidate = route.stack?.[0]?.handle;
			if (typeof candidate === 'function') {
				return candidate as RequestHandler;
			}
		}

		const nestedStack = (layer.handle as { stack?: ExpressLayer[] } | undefined)?.stack;
		const foundNested = findPostHandlerByPath(nestedStack, path);
		if (foundNested) {
			return foundNested;
		}
	}

	return null;
}

function getInternalAiRequiresHandler(): RequestHandler {
	const app = createApp() as unknown as {
		_router?: {
			stack?: ExpressLayer[];
		};
	};
	const handler = findPostHandlerByPath(app._router?.stack, '/requires');
	if (!handler) {
		throw new Error('Failed to locate /internal/ai/requires route handler');
	}

	return handler;
}

function getAppErrorHandler(): ErrorRequestHandler {
	const app = createApp() as unknown as {
		_router?: {
			stack?: Array<{ handle?: unknown }>;
		};
	};
	const layer = app._router?.stack?.find(
		(entry) =>
			typeof entry.handle === 'function' &&
			(entry.handle as (...args: unknown[]) => unknown).length === 4,
	);
	if (!layer || typeof layer.handle !== 'function') {
		throw new Error('Failed to locate app error middleware');
	}

	return layer.handle as ErrorRequestHandler;
}

async function invokeRequiresRoute(input: {
	body: unknown;
	headers?: Record<string, string | undefined>;
}): Promise<{
	status: ReturnType<typeof vi.fn>;
	json: ReturnType<typeof vi.fn>;
	error: unknown;
}> {
	const handler = getInternalAiRequiresHandler();
	const appErrorHandler = getAppErrorHandler();
	const headerMap = new Map<string, string>();
	Object.entries(input.headers ?? {}).forEach(([key, value]) => {
		if (typeof value === 'string') {
			headerMap.set(key.toLowerCase(), value);
		}
	});

	const status = vi.fn().mockReturnThis();
	const json = vi.fn();
	let nextError: unknown;

	const req = {
		body: input.body,
		header: (name: string) => headerMap.get(name.toLowerCase()),
	} as unknown as Request;
	const res = {
		status,
		json: vi.fn().mockImplementation((payload: unknown) => {
			json(payload);
			return res;
		}),
	} as unknown as Response;

	await new Promise<void>((resolve, reject) => {
		let settled = false;
		const timeoutId = setTimeout(() => {
			if (!settled) {
				reject(new Error('Route handler invocation timed out'));
			}
		}, 3_000);

		const finish = () => {
			if (settled) {
				return;
			}
			settled = true;
			clearTimeout(timeoutId);
			resolve();
		};

		const next: NextFunction = (error?: unknown) => {
			nextError = error;
			if (error) {
				try {
					appErrorHandler(error, req, res, () => undefined);
				} catch (handlerError) {
					reject(handlerError);
					return;
				}
			}
			finish();
		};

		res.json = vi.fn().mockImplementation((payload: unknown) => {
			json(payload);
			finish();
			return res;
		});

		handler(req, res, next);
	});

	return { status, json, error: nextError };
}

afterEach(() => {
	vi.restoreAllMocks();
	applyTestEnv({});
});

describe('internal AI webhook route', () => {
	it('returns 503 when webhook secret is not configured', async () => {
		const send = vi.fn().mockResolvedValue(undefined);
		applyTestEnv({
			AI_REQUIRES_WEBHOOK_SECRET: undefined,
			AI_CLASSIFICATION_QUEUE: { send } as unknown as Queue,
		});

		const result = await invokeRequiresRoute({
			headers: {
				'content-type': 'application/json',
			},
			body: {
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
			},
		});

		expect(result.error).toBeDefined();
		expect(result.status).toHaveBeenCalledWith(503);
		expect(result.json).toHaveBeenCalledWith({
			error: 'AI_WEBHOOK_NOT_CONFIGURED',
			message: 'AI_REQUIRES_WEBHOOK_SECRET is required for internal AI webhook ingestion',
		});
		expect(send).not.toHaveBeenCalled();
	});

	it('returns 401 when webhook secret header is missing or invalid', async () => {
		const send = vi.fn().mockResolvedValue(undefined);
		applyTestEnv({
			AI_REQUIRES_WEBHOOK_SECRET: 'test-secret',
			AI_CLASSIFICATION_QUEUE: { send } as unknown as Queue,
		});

		const missingHeaderResult = await invokeRequiresRoute({
			headers: {
				'content-type': 'application/json',
			},
			body: {
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
			},
		});

		expect(missingHeaderResult.status).toHaveBeenCalledWith(401);
		expect(missingHeaderResult.json).toHaveBeenCalledWith({
			error: 'UNAUTHORIZED',
			message: 'Invalid AI webhook secret',
		});

		const invalidHeaderResult = await invokeRequiresRoute({
			headers: {
				'content-type': 'application/json',
				'x-ai-webhook-secret': 'wrong-secret',
			},
			body: {
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
			},
		});

		expect(invalidHeaderResult.status).toHaveBeenCalledWith(401);
		expect(invalidHeaderResult.json).toHaveBeenCalledWith({
			error: 'UNAUTHORIZED',
			message: 'Invalid AI webhook secret',
		});
		expect(send).not.toHaveBeenCalled();
	});

	it('returns 202 envelope and enqueues AI_CLASSIFICATION payload for valid secret', async () => {
		const send = vi.fn().mockResolvedValue(undefined);
		applyTestEnv({
			AI_REQUIRES_WEBHOOK_SECRET: 'test-secret',
			AI_QUEUE_DELAY_SECONDS: '45',
			AI_CLASSIFICATION_QUEUE: { send } as unknown as Queue,
		});

		const result = await invokeRequiresRoute({
			headers: {
				'content-type': 'application/json',
				'x-ai-webhook-secret': 'test-secret',
			},
			body: {
				job_type: 'REQUIRES_AI',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:00:00.000Z',
			},
		});

		expect(result.error).toBeUndefined();
		expect(result.status).toHaveBeenCalledWith(202);
		expect(result.json).toHaveBeenCalledWith({
			data: {
				accepted: true,
				queued_at: '2026-03-11T10:00:00.000Z',
			},
		});
		expect(send).toHaveBeenCalledTimes(1);
		expect(send).toHaveBeenCalledWith(
			{
				job_type: 'AI_CLASSIFICATION',
				transaction_id: '00000000-0000-4000-8000-0000000000a1',
				requested_at: '2026-03-11T10:00:00.000Z',
			},
			{
				contentType: 'json',
				delaySeconds: 45,
			},
		);
	});
});
