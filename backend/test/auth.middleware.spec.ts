import { env as runtimeEnv } from 'cloudflare:workers';
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
	deriveUserUuidFromClerkSubject,
	requireAuthenticatedUser,
	verifyClerkJwt,
} from '../src/lib/auth';
import { getSqlClient, type SqlClient } from '../src/lib/db/client';
import { HttpError } from '../src/lib/http/errors';
import { enforceIpRateLimit, wasIpRateLimitConsidered } from '../src/lib/http/rate-limit';

vi.mock('../src/lib/db/client', async (importOriginal) => {
	const actual = await importOriginal<typeof import('../src/lib/db/client')>();
	return { ...actual, getSqlClient: vi.fn() };
});

interface SignedJwtFixture {
	token: string;
	jwks: { keys: JsonWebKey[] };
	subject: string;
	issuer: string;
	audience: string;
}

function toBase64Url(data: string | Uint8Array): string {
	if (typeof data === 'string') {
		return Buffer.from(data, 'utf8').toString('base64url');
	}

	return Buffer.from(data).toString('base64url');
}

async function createSignedJwtFixture(
	overrides: Partial<Record<'sub' | 'iss' | 'aud' | 'kid', string>> & {
		email?: string | null;
		azp?: string | null;
	} = {},
): Promise<SignedJwtFixture> {
	const now = Math.floor(Date.now() / 1000);
	const subject = overrides.sub ?? 'user_2zClerkSub';
	const issuer = overrides.iss ?? 'https://clerk.example.com';
	const audience = overrides.aud ?? 'money-manager';
	const kid = overrides.kid ?? 'test-clerk-key-1';

	const keyPair = await crypto.subtle.generateKey(
		{
			name: 'RSASSA-PKCS1-v1_5',
			modulusLength: 2048,
			publicExponent: new Uint8Array([1, 0, 1]),
			hash: 'SHA-256',
		},
		true,
		['sign', 'verify'],
	);
	const publicJwk = (await crypto.subtle.exportKey('jwk', keyPair.publicKey)) as JsonWebKey;
	publicJwk.kid = kid;
	publicJwk.alg = 'RS256';
	publicJwk.use = 'sig';

	const header = toBase64Url(JSON.stringify({ alg: 'RS256', typ: 'JWT', kid }));
	const payload = toBase64Url(
		JSON.stringify({
			sub: subject,
			iss: issuer,
			aud: audience,
			iat: now,
			nbf: now - 5,
			exp: now + 300,
			...(overrides.email === null ? {} : { email: overrides.email ?? 'clerk-user@example.com' }),
			...(overrides.azp === null || overrides.azp === undefined ? {} : { azp: overrides.azp }),
		}),
	);
	const signingInput = `${header}.${payload}`;
	const signatureBuffer = await crypto.subtle.sign(
		'RSASSA-PKCS1-v1_5',
		keyPair.privateKey,
		new TextEncoder().encode(signingInput),
	);
	const signature = toBase64Url(new Uint8Array(signatureBuffer));

	return {
		token: `${signingInput}.${signature}`,
		jwks: { keys: [publicJwk] },
		subject,
		issuer,
		audience,
	};
}

function runAuthMiddleware(
	headers: Record<string, string | undefined>,
	// An authenticated mount by default: that is the case where the IP
	// middleware waved the request through and auth must charge it.
	path = '/transactions',
): Promise<unknown> {
	return new Promise((resolve) => {
		const req = {
			header: (name: string) => headers[name.toLowerCase()],
			headers,
			path,
			method: 'GET',
		} as unknown as import('express').Request;

		requireAuthenticatedUser(
			req,
			{} as import('express').Response,
			(error?: unknown) => resolve(error),
		);
	});
}

interface CapturedQuery {
	text: string;
	values: unknown[];
}

/**
 * Runs the middleware with a recording SQL client and the DB configured, so
 * the provisioning upsert is observable. Resolves with the error passed to
 * next() (undefined on success), the request, and the captured queries.
 */
async function runAuthMiddlewareWithDb(
	headers: Record<string, string | undefined>,
): Promise<{ error: unknown; req: import('express').Request; queries: CapturedQuery[] }> {
	const queries: CapturedQuery[] = [];
	const recordingSql = (async (strings: TemplateStringsArray, ...values: unknown[]) => {
		queries.push({ text: strings.join('$'), values });
		return [
			{
				id: values[0],
				email: values[1],
				auth_provider: values[2],
				auth_subject: values[3],
				last_app_open_date: '2026-09-18T00:00:00.000Z',
				created_at: '2026-09-18T00:00:00.000Z',
			},
		];
	}) as unknown as SqlClient;
	vi.mocked(getSqlClient).mockReturnValue(recordingSql);

	const previousPoolerUrl = runtimeEnv.SUPABASE_POOLER_URL;
	runtimeEnv.SUPABASE_POOLER_URL = 'postgresql://app_backend.project:secret@db.example.supabase.com:6543/postgres';
	try {
		const req = {
			header: (name: string) => headers[name.toLowerCase()],
		} as unknown as import('express').Request;
		const error = await new Promise<unknown>((resolve) => {
			requireAuthenticatedUser(req, {} as import('express').Response, (nextError?: unknown) => resolve(nextError));
		});
		return { error, req, queries };
	} finally {
		runtimeEnv.SUPABASE_POOLER_URL = previousPoolerUrl;
	}
}

function mockJwksFetch(jwks: SignedJwtFixture['jwks']): void {
	vi.spyOn(globalThis, 'fetch').mockResolvedValue(
		new Response(JSON.stringify(jwks), {
			status: 200,
			headers: { 'cache-control': 'max-age=0' },
		}),
	);
}

afterEach(() => {
	vi.restoreAllMocks();
});

describe('auth middleware + Clerk JWT verification', () => {
	it('returns unauthorized when Authorization header is missing', async () => {
		const error = await runAuthMiddleware({});

		expect(error).toBeInstanceOf(HttpError);
		const httpError = error as HttpError;
		expect(httpError.statusCode).toBe(401);
		expect(httpError.errorCode).toBe('UNAUTHORIZED');
	});

	it('returns unauthorized when Authorization scheme is invalid', async () => {
		const error = await runAuthMiddleware({ authorization: 'Token abc123' });

		expect(error).toBeInstanceOf(HttpError);
		const httpError = error as HttpError;
		expect(httpError.statusCode).toBe(401);
		expect(httpError.errorCode).toBe('UNAUTHORIZED');
	});

	it('verifies a valid Clerk JWT using JWKS', async () => {
		const fixture = await createSignedJwtFixture();
		const jwksUrl = 'https://clerk.example.com/.well-known/jwks-valid.json';
		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValue(
				new Response(JSON.stringify(fixture.jwks), {
					status: 200,
					headers: { 'cache-control': 'max-age=60' },
				}),
			);

		const payload = await verifyClerkJwt(fixture.token, {
			jwksUrl,
			issuer: fixture.issuer,
			audience: [fixture.audience],
			fetcher,
		});

		expect(payload.sub).toBe(fixture.subject);
		expect(fetcher).toHaveBeenCalledTimes(1);
	});

	it('rejects Clerk JWT issuer mismatch', async () => {
		const fixture = await createSignedJwtFixture();
		const jwksUrl = 'https://clerk.example.com/.well-known/jwks-issuer-mismatch.json';
		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValue(
				new Response(JSON.stringify(fixture.jwks), {
					status: 200,
					headers: { 'cache-control': 'max-age=60' },
				}),
			);

		await expect(
			verifyClerkJwt(fixture.token, {
				jwksUrl,
				issuer: 'https://different-issuer.example.com',
				audience: [fixture.audience],
				fetcher,
			}),
		).rejects.toMatchObject({
			statusCode: 401,
			errorCode: 'UNAUTHORIZED',
		});
	});

	it('refreshes JWKS once when token kid is missing from cached key set', async () => {
		const staleFixture = await createSignedJwtFixture({ kid: 'stale-key' });
		const validFixture = await createSignedJwtFixture({ kid: 'fresh-key' });
		const jwksUrl = 'https://clerk.example.com/.well-known/jwks-rotation.json';

		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValueOnce(
				new Response(JSON.stringify(staleFixture.jwks), {
					status: 200,
					headers: { 'cache-control': 'max-age=300' },
				}),
			)
			.mockResolvedValueOnce(
				new Response(JSON.stringify(validFixture.jwks), {
					status: 200,
					headers: { 'cache-control': 'max-age=300' },
				}),
			);

		const payload = await verifyClerkJwt(validFixture.token, {
			jwksUrl,
			issuer: validFixture.issuer,
			audience: [validFixture.audience],
			fetcher,
		});

		expect(payload.sub).toBe(validFixture.subject);
		expect(fetcher).toHaveBeenCalledTimes(2);
	});

	it('provisions the user with the Clerk subject recorded as auth_subject', async () => {
		const fixture = await createSignedJwtFixture({ sub: 'user_2zProvisioned', kid: 'provision-key-1' });
		mockJwksFetch(fixture.jwks);

		const { error, req, queries } = await runAuthMiddlewareWithDb({ authorization: `Bearer ${fixture.token}` });

		expect(error).toBeUndefined();
		const expectedUserId = await deriveUserUuidFromClerkSubject(fixture.subject);
		expect((req as { auth?: { userId: string } }).auth?.userId).toBe(expectedUserId);
		expect(queries).toHaveLength(1);
		expect(queries[0].text).toMatch(/insert into public\.users as u \(id, email, auth_provider, auth_subject, last_app_open_date\)/);
		expect(queries[0].text).toMatch(/auth_provider = excluded\.auth_provider/);
		expect(queries[0].text).toMatch(/auth_subject = excluded\.auth_subject/);
		expect(queries[0].text).toMatch(/returning u\.id, u\.email, u\.auth_provider, u\.auth_subject/);
		expect(queries[0].values).toEqual([expectedUserId, 'clerk-user@example.com', 'clerk', fixture.subject]);
	});

	it('records auth_subject on the fallback-email path when the token carries no email', async () => {
		const fixture = await createSignedJwtFixture({ sub: 'user_2zNoEmail', email: null, kid: 'provision-key-2' });
		mockJwksFetch(fixture.jwks);

		const { error, queries } = await runAuthMiddlewareWithDb({ authorization: `Bearer ${fixture.token}` });

		expect(error).toBeUndefined();
		const expectedUserId = await deriveUserUuidFromClerkSubject(fixture.subject);
		expect(queries).toHaveLength(1);
		expect(queries[0].text).toMatch(/auth_subject = excluded\.auth_subject/);
		expect(queries[0].values).toEqual([
			expectedUserId,
			`${expectedUserId}@users.clerk.local`,
			'clerk',
			fixture.subject,
			expectedUserId,
		]);
	});

	it('derives stable UUIDv5 from Clerk subject', async () => {
		const subject = 'user_2zClerkSub';
		const first = await deriveUserUuidFromClerkSubject(subject);
		const second = await deriveUserUuidFromClerkSubject(subject);

		expect(first).toBe(second);
		expect(first).toMatch(
			/^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
		);
	});
});

describe('authorized party (azp) validation', () => {
	it('ignores azp when CLERK_AUTHORIZED_PARTIES is unset', async () => {
		const fixture = await createSignedJwtFixture({ kid: 'azp-unset', azp: 'https://evil.example.com' });
		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValue(new Response(JSON.stringify(fixture.jwks), { status: 200 }));

		const payload = await verifyClerkJwt(fixture.token, {
			jwksUrl: 'https://clerk.example.com/.well-known/jwks-azp-unset.json',
			issuer: fixture.issuer,
			fetcher,
		});

		expect(payload.sub).toBe(fixture.subject);
	});

	it('accepts a token whose azp is on the list', async () => {
		const fixture = await createSignedJwtFixture({ kid: 'azp-ok', azp: 'https://app.example.com' });
		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValue(new Response(JSON.stringify(fixture.jwks), { status: 200 }));

		const payload = await verifyClerkJwt(fixture.token, {
			jwksUrl: 'https://clerk.example.com/.well-known/jwks-azp-ok.json',
			issuer: fixture.issuer,
			authorizedParties: ['https://app.example.com', 'http://localhost:5173'],
			fetcher,
		});

		expect(payload.azp).toBe('https://app.example.com');
	});

	it('rejects a token minted for another origin', async () => {
		const fixture = await createSignedJwtFixture({ kid: 'azp-bad', azp: 'https://other.example.com' });
		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValue(new Response(JSON.stringify(fixture.jwks), { status: 200 }));

		await expect(
			verifyClerkJwt(fixture.token, {
				jwksUrl: 'https://clerk.example.com/.well-known/jwks-azp-bad.json',
				issuer: fixture.issuer,
				authorizedParties: ['https://app.example.com'],
				fetcher,
			}),
		).rejects.toMatchObject({ statusCode: 401, errorCode: 'UNAUTHORIZED' });
	});

	it('rejects a token with no azp claim once the list is set', async () => {
		const fixture = await createSignedJwtFixture({ kid: 'azp-missing' });
		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValue(new Response(JSON.stringify(fixture.jwks), { status: 200 }));

		await expect(
			verifyClerkJwt(fixture.token, {
				jwksUrl: 'https://clerk.example.com/.well-known/jwks-azp-missing.json',
				issuer: fixture.issuer,
				authorizedParties: ['https://app.example.com'],
				fetcher,
			}),
		).rejects.toMatchObject({ statusCode: 401, errorCode: 'UNAUTHORIZED' });
	});

	it('still accepts the default session token shape (aud absent, azp present)', async () => {
		const fixture = await createSignedJwtFixture({ kid: 'azp-no-aud', azp: 'https://app.example.com' });
		const fetcher = vi
			.fn<typeof fetch>()
			.mockResolvedValue(new Response(JSON.stringify(fixture.jwks), { status: 200 }));

		const payload = await verifyClerkJwt(fixture.token, {
			jwksUrl: 'https://clerk.example.com/.well-known/jwks-azp-no-aud.json',
			issuer: fixture.issuer,
			authorizedParties: ['https://app.example.com'],
			fetcher,
		});

		expect(payload.sub).toBe(fixture.subject);
	});
});

describe('auth middleware rate limiting', () => {
	function withLimiter<T>(
		binding: 'USER_RATE_LIMITER' | 'IP_RATE_LIMITER',
		success: boolean,
		run: () => Promise<T>,
	): Promise<T> {
		const limiter = { limit: vi.fn().mockResolvedValue({ success }) } as unknown as RateLimit;
		const previous = runtimeEnv[binding];
		runtimeEnv[binding] = limiter;
		return run().finally(() => {
			runtimeEnv[binding] = previous;
		});
	}

	it('returns 429 with Retry-After once the user is over their window', async () => {
		const fixture = await createSignedJwtFixture({ sub: 'user_2zLimited', kid: 'limit-key-1' });
		mockJwksFetch(fixture.jwks);

		const error = await withLimiter('USER_RATE_LIMITER', false, async () => {
			const { error: caught } = await runAuthMiddlewareWithDb({
				authorization: `Bearer ${fixture.token}`,
			});
			return caught;
		});

		expect(error).toBeInstanceOf(HttpError);
		expect((error as HttpError).statusCode).toBe(429);
		expect((error as HttpError).errorCode).toBe('RATE_LIMITED');
		expect((error as HttpError).headers?.['Retry-After']).toBe('60');
	});

	it('does not touch the database for a rate-limited caller', async () => {
		const fixture = await createSignedJwtFixture({ sub: 'user_2zLimitedDb', kid: 'limit-key-2' });
		mockJwksFetch(fixture.jwks);

		const queries = await withLimiter('USER_RATE_LIMITER', false, async () => {
			const result = await runAuthMiddlewareWithDb({ authorization: `Bearer ${fixture.token}` });
			return result.queries;
		});

		expect(queries).toHaveLength(0);
	});

	it('does not charge a request the IP middleware already considered', async () => {
		// The app-level middleware charges OAuth and unauthenticated paths;
		// auth must not charge them a second time. It cannot tell from the
		// path (express has rewritten it by then), so the request carries the
		// fact — the same marker enforceIpRateLimit sets. The end-to-end
		// version of this lives in perimeter.e2e.spec.ts.
		const ipLimiter = { limit: vi.fn().mockResolvedValue({ success: true }) } as unknown as RateLimit;
		const previous = runtimeEnv.IP_RATE_LIMITER;
		runtimeEnv.IP_RATE_LIMITER = ipLimiter;
		try {
			const headers = { authorization: 'Bearer not-a-jwt', 'cf-connecting-ip': '203.0.113.7' };
			const req = {
				header: (name: string) => (headers as Record<string, string>)[name.toLowerCase()],
				headers,
				path: '/oauth/google/start',
				method: 'POST',
			} as unknown as import('express').Request;

			await enforceIpRateLimit(runtimeEnv, req);
			expect(ipLimiter.limit).toHaveBeenCalledTimes(1);
			expect(wasIpRateLimitConsidered(req)).toBe(true);

			const error = await new Promise<unknown>((resolve) => {
				requireAuthenticatedUser(req, {} as import('express').Response, resolve);
			});

			expect(ipLimiter.limit).toHaveBeenCalledTimes(1);
			expect((error as HttpError).statusCode).toBe(401);
		} finally {
			runtimeEnv.IP_RATE_LIMITER = previous;
		}
	});

	it('charges a rejected token to the IP limiter', async () => {
		const error = await withLimiter('IP_RATE_LIMITER', false, async () =>
			runAuthMiddleware({
				authorization: 'Bearer not-a-jwt',
				'cf-connecting-ip': '203.0.113.7',
			}),
		);

		expect(error).toBeInstanceOf(HttpError);
		expect((error as HttpError).statusCode).toBe(429);
	});

	it('still returns 401 for a bad token when the IP is under its limit', async () => {
		const error = await withLimiter('IP_RATE_LIMITER', true, async () =>
			runAuthMiddleware({
				authorization: 'Bearer not-a-jwt',
				'cf-connecting-ip': '203.0.113.7',
			}),
		);

		expect(error).toBeInstanceOf(HttpError);
		expect((error as HttpError).statusCode).toBe(401);
	});

	it('still returns 401 for a bad token when Cloudflare set no address', async () => {
		// No address to charge: the limiter is skipped, never guessed.
		const error = await withLimiter('IP_RATE_LIMITER', false, async () =>
			runAuthMiddleware({ authorization: 'Bearer not-a-jwt' }),
		);

		expect((error as HttpError).statusCode).toBe(401);
	});
});
