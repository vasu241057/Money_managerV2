// The perimeter as a caller actually meets it (Task 7). The unit specs prove
// the decisions; this proves the wiring — middleware order, the 429 reaching
// the wire with its Retry-After, and the header only being spent on the
// requests that are meant to spend it.

import type { Server } from 'node:http';

import { env as runtimeEnv } from 'cloudflare:workers';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { createApp } from '../src/app';

type MutableEnv = Record<string, unknown>;

function useIpLimiter(success: boolean): { limit: ReturnType<typeof vi.fn> } {
	const limiter = { limit: vi.fn().mockResolvedValue({ success }) };
	(runtimeEnv as MutableEnv).IP_RATE_LIMITER = limiter;
	return limiter;
}

async function withServer<T>(run: (baseUrl: string) => Promise<T>): Promise<T> {
	const server: Server = createApp().listen(0);
	try {
		const address = server.address();
		const port = typeof address === 'object' && address ? address.port : 0;
		return await run(`http://127.0.0.1:${port}`);
	} finally {
		await new Promise<void>((resolve) => server.close(() => resolve()));
	}
}

afterEach(() => {
	delete (runtimeEnv as MutableEnv).IP_RATE_LIMITER;
	vi.restoreAllMocks();
});

describe('perimeter end to end', () => {
	it('answers 429 with Retry-After on the wire', async () => {
		useIpLimiter(false);
		vi.spyOn(console, 'warn').mockImplementation(() => {});

		const response = await withServer((baseUrl) =>
			fetch(`${baseUrl}/health`, { headers: { 'cf-connecting-ip': '203.0.113.7' } }),
		);

		expect(response.status).toBe(429);
		expect(response.headers.get('retry-after')).toBe('60');
		expect(await response.json()).toMatchObject({ error: 'RATE_LIMITED' });
	});

	it('lets an under-limit request reach its route', async () => {
		useIpLimiter(true);

		const response = await withServer((baseUrl) =>
			fetch(`${baseUrl}/version`, { headers: { 'cf-connecting-ip': '203.0.113.7' } }),
		);

		expect(response.status).toBe(200);
		expect(response.headers.get('retry-after')).toBeNull();
	});

	it('charges an unauthenticated path even when a junk token is attached', async () => {
		const limiter = useIpLimiter(true);

		await withServer((baseUrl) =>
			fetch(`${baseUrl}/health`, {
				headers: { 'cf-connecting-ip': '203.0.113.7', authorization: 'Bearer junk' },
			}),
		);

		expect(limiter.limit).toHaveBeenCalledWith({ key: '203.0.113.7' });
	});

	it('does not charge the IP window for an authenticated route with a token', async () => {
		const limiter = useIpLimiter(true);

		// 401 from the auth middleware (the token is junk and there is no
		// JWKS here); what matters is that the IP limiter stayed out of it.
		const response = await withServer((baseUrl) =>
			fetch(`${baseUrl}/transactions`, {
				headers: { 'cf-connecting-ip': '203.0.113.7', authorization: 'Bearer junk' },
			}),
		);

		expect(response.status).toBe(401);
		expect(limiter.limit).toHaveBeenCalledTimes(1); // the auth-failure charge, not the middleware
	});

	it('rejects an over-limit caller before reading the request body', async () => {
		useIpLimiter(false);
		vi.spyOn(console, 'warn').mockImplementation(() => {});

		const response = await withServer((baseUrl) =>
			fetch(`${baseUrl}/internal/requeue/pending`, {
				method: 'POST',
				headers: { 'cf-connecting-ip': '203.0.113.7', 'content-type': 'application/json' },
				body: JSON.stringify({ padding: 'x'.repeat(50_000) }),
			}),
		);

		expect(response.status).toBe(429);
	});

	it('does not limit at all when the binding is absent', async () => {
		const response = await withServer((baseUrl) =>
			fetch(`${baseUrl}/version`, { headers: { 'cf-connecting-ip': '203.0.113.7' } }),
		);

		expect(response.status).toBe(200);
	});

	it('charges the IP window once for an OAuth call with a bad token', async () => {
		// The middleware charges it; the auth failure must not charge again.
		const limiter = useIpLimiter(true);

		await withServer((baseUrl) =>
			fetch(`${baseUrl}/oauth/google/connection`, {
				headers: { 'cf-connecting-ip': '203.0.113.7', authorization: 'Bearer junk' },
			}),
		);

		expect(limiter.limit).toHaveBeenCalledTimes(1);
	});
});
