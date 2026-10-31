// Perimeter rate limiting (Task 7). The properties that matter: the right
// requests are counted, the limiter never takes the API down, and a rejected
// caller learns when to come back.

import { describe, expect, it, vi } from 'vitest';

import { HttpError } from '../src/lib/http/errors';
import {
	AUTHENTICATED_MOUNTS,
	createIpRateLimitMiddleware,
	enforceIpRateLimit,
	enforceUserRateLimit,
	isIpRateLimitedRequest,
	resolveClientIp,
	RATE_LIMIT_WINDOW_SECONDS,
} from '../src/lib/http/rate-limit';

type Req = import('express').Request;

function request(overrides: Partial<Req> & { headers?: Record<string, string> } = {}): Req {
	return {
		path: '/transactions',
		method: 'GET',
		...overrides,
		headers: { 'cf-connecting-ip': '203.0.113.7', ...(overrides.headers ?? {}) },
	} as unknown as Req;
}

function limiter(success: boolean) {
	return { limit: vi.fn().mockResolvedValue({ success }) } as unknown as RateLimit;
}

describe('rate limit scope', () => {
	it('counts requests that carry no bearer token', () => {
		expect(isIpRateLimitedRequest(request({ path: '/health' }))).toBe(true);
	});

	it('leaves authenticated requests to the per-user limiter', () => {
		const req = request({ headers: { authorization: 'Bearer token' } });
		expect(isIpRateLimitedRequest(req)).toBe(false);
	});

	it('counts OAuth calls by IP even when authenticated', () => {
		const req = request({
			headers: { authorization: 'Bearer token' },
			path: '/oauth/google/start',
		});
		expect(isIpRateLimitedRequest(req)).toBe(true);
	});

	it('counts an unauthenticated path even when a bearer header is present', () => {
		// /health, /version and /internal/* are not behind the auth
		// middleware, so nothing else can charge these requests: keying scope
		// off the header alone would leave them unmetered entirely.
		for (const path of ['/health', '/version', '/internal/requeue/pending', '/internal/map-preview']) {
			expect(
				isIpRateLimitedRequest(request({ headers: { authorization: 'Bearer junk' }, path })),
				path,
			).toBe(true);
		}
	});

	it('counts an unmatched path, token or not', () => {
		// A 404 flood is still a flood; /oauthx is not an OAuth call but is
		// not a route either.
		expect(isIpRateLimitedRequest(request({ headers: { authorization: 'Bearer t' }, path: '/oauthx' }))).toBe(true);
		expect(isIpRateLimitedRequest(request({ path: '/nope' }))).toBe(true);
	});

	it('leaves every authenticated mount to the user limiter when a token is present', () => {
		for (const mount of AUTHENTICATED_MOUNTS) {
			if (mount === '/oauth') continue;
			expect(
				isIpRateLimitedRequest(request({ headers: { authorization: 'Bearer t' }, path: `${mount}/x` })),
				mount,
			).toBe(false);
		}
	});
});

describe('client ip resolution', () => {
	it('reads the Cloudflare header', () => {
		const req = request({
			headers: { 'cf-connecting-ip': '203.0.113.7', 'x-forwarded-for': '198.51.100.1' },
		});
		expect(resolveClientIp(req)).toBe('203.0.113.7');
	});

	it('ignores a forgeable forwarded header', () => {
		// Otherwise a caller could spend a victim's window by naming them.
		const req = { headers: { 'x-forwarded-for': '198.51.100.1' } } as unknown as Req;
		expect(resolveClientIp(req)).toBeNull();
	});

	it('answers null when Cloudflare set no address', () => {
		expect(resolveClientIp({ headers: {} } as unknown as Req)).toBeNull();
	});

	it('answers for a request object that only exposes header()', () => {
		const req = {
			header: (name: string) => (name === 'cf-connecting-ip' ? '203.0.113.5' : undefined),
		} as unknown as Req;
		expect(resolveClientIp(req)).toBe('203.0.113.5');
	});
});

describe('enforcement', () => {
	it('throws 429 with Retry-After when the user limiter refuses', async () => {
		const env = { USER_RATE_LIMITER: limiter(false) };

		const error = await enforceUserRateLimit(env, 'user-1').catch((caught: unknown) => caught);

		expect(error).toBeInstanceOf(HttpError);
		const httpError = error as HttpError;
		expect(httpError.statusCode).toBe(429);
		expect(httpError.errorCode).toBe('RATE_LIMITED');
		expect(httpError.headers?.['Retry-After']).toBe(String(RATE_LIMIT_WINDOW_SECONDS));
	});

	it('keys the user limiter by user id', async () => {
		const userLimiter = limiter(true);
		await enforceUserRateLimit({ USER_RATE_LIMITER: userLimiter }, 'user-42');
		expect(userLimiter.limit).toHaveBeenCalledWith({ key: 'user-42' });
	});

	it('keys the ip limiter by client address', async () => {
		const ipLimiter = limiter(true);
		await enforceIpRateLimit(
			{ IP_RATE_LIMITER: ipLimiter },
			request({ headers: { 'cf-connecting-ip': '203.0.113.7' } }),
		);
		expect(ipLimiter.limit).toHaveBeenCalledWith({ key: '203.0.113.7' });
	});

	it('allows the request when no binding is configured', async () => {
		await expect(enforceUserRateLimit({}, 'user-1')).resolves.toBeUndefined();
		await expect(enforceIpRateLimit({}, request())).resolves.toBeUndefined();
	});

	it('allows the request rather than bucketing everyone when the address is unknown', async () => {
		const ipLimiter = limiter(false);

		await expect(
			enforceIpRateLimit({ IP_RATE_LIMITER: ipLimiter }, { headers: {}, path: '/health' } as unknown as Req),
		).resolves.toBeUndefined();
		expect(ipLimiter.limit).not.toHaveBeenCalled();
	});

	it('fails open when the binding throws', async () => {
		const broken = {
			limit: vi.fn().mockRejectedValue(new Error('binding unavailable')),
		} as unknown as RateLimit;

		await expect(enforceUserRateLimit({ USER_RATE_LIMITER: broken }, 'user-1')).resolves
			.toBeUndefined();
	});

	it('does not resolve the client ip when no ip binding exists', async () => {
		const req = request();
		Object.defineProperty(req, 'headers', {
			get() {
				throw new Error('headers must not be read');
			},
		});

		await expect(enforceIpRateLimit({}, req)).resolves.toBeUndefined();
	});
});

describe('ip middleware', () => {
	it('passes an in-scope request through when under the limit', async () => {
		const middleware = createIpRateLimitMiddleware({ IP_RATE_LIMITER: limiter(true) });
		const next = vi.fn();

		middleware(request({ path: '/health' }), {} as import('express').Response, next);
		await vi.waitFor(() => expect(next).toHaveBeenCalledTimes(1));
		expect(next).toHaveBeenCalledWith();
	});

	it('hands the 429 to the error handler', async () => {
		const middleware = createIpRateLimitMiddleware({ IP_RATE_LIMITER: limiter(false) });
		const next = vi.fn();

		middleware(request({ path: '/health' }), {} as import('express').Response, next);
		await vi.waitFor(() => expect(next).toHaveBeenCalledTimes(1));
		expect(next.mock.calls[0][0]).toBeInstanceOf(HttpError);
		expect((next.mock.calls[0][0] as HttpError).statusCode).toBe(429);
	});

	it('skips the limiter entirely for authenticated non-OAuth traffic', async () => {
		const ipLimiter = limiter(false);
		const middleware = createIpRateLimitMiddleware({ IP_RATE_LIMITER: ipLimiter });
		const next = vi.fn();

		middleware(
			request({ headers: { authorization: 'Bearer token' } }),
			{} as import('express').Response,
			next,
		);
		await vi.waitFor(() => expect(next).toHaveBeenCalledTimes(1));
		expect(next).toHaveBeenCalledWith();
		expect(ipLimiter.limit).not.toHaveBeenCalled();
	});
});
