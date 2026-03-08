import { afterEach, describe, expect, it, vi } from 'vitest';

import {
	deriveUserUuidFromClerkSubject,
	requireAuthenticatedUser,
	verifyClerkJwt,
} from '../src/lib/auth';
import { HttpError } from '../src/lib/http/errors';

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
	overrides: Partial<Record<'sub' | 'iss' | 'aud' | 'kid', string>> = {},
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
			email: 'clerk-user@example.com',
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

function runAuthMiddleware(headers: Record<string, string | undefined>): Promise<unknown> {
	return new Promise((resolve) => {
		const req = {
			header: (name: string) => headers[name.toLowerCase()],
		} as unknown as import('express').Request;

		requireAuthenticatedUser(
			req,
			{} as import('express').Response,
			(error?: unknown) => resolve(error),
		);
	});
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
