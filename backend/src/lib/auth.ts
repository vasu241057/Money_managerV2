import { env as runtimeEnv } from 'cloudflare:workers';
import type { Request, RequestHandler } from 'express';

import type { UserRow, UUID } from '../../../shared/types';
import { getAppConfig } from './config';
import { getSqlClient } from './db/client';
import { asyncHandler } from './http/async';
import { serviceUnavailable, unauthorized } from './http/errors';
import { parseEmail } from './http/validation';

const AUTHORIZATION_HEADER = 'authorization';
// FROZEN. users.id is derived from this namespace and the Clerk subject
// (deriveUserUuidFromClerkSubject); every user-owned row keys on that id, so
// changing the namespace or the derivation would orphan all existing data.
const CLERK_SUBJECT_NAMESPACE = '2d0ec934-68a4-4184-adf1-46d5f9d5cc0f';
const AUTH_PROVIDER_CLERK = 'clerk';
const DEFAULT_CLOCK_SKEW_SECONDS = 60;
const DEFAULT_JWKS_CACHE_SECONDS = 300;

interface ClerkJwtHeader {
	alg?: string;
	kid?: string;
	typ?: string;
}

interface ClerkJwtPayload {
	sub?: string;
	exp?: number;
	nbf?: number;
	iat?: number;
	iss?: string;
	aud?: string | string[];
	/** Authorized party: the frontend origin Clerk issued the token to. */
	azp?: string;
	email?: string;
	email_address?: string;
	[key: string]: unknown;
}

interface ClerkVerificationConfig {
	jwksUrl: string;
	issuer?: string;
	audience?: string[];
	/**
	 * Origins allowed to mint tokens for this backend (CLERK_AUTHORIZED_PARTIES).
	 * Clerk's default session tokens carry `azp`, not `aud`, so this — not the
	 * audience list — is what stops a token from another Clerk app in the same
	 * instance being replayed here (A.3 §1). Unset = not enforced.
	 */
	authorizedParties?: string[];
	nowSeconds?: number;
	clockSkewSeconds?: number;
	fetcher?: typeof fetch;
}

type ClerkJsonWebKey = JsonWebKey & {
	kid?: string;
	alg?: string;
	use?: string;
	kty?: string;
	n?: string;
	e?: string;
};

interface JsonWebKeySet {
	keys?: ClerkJsonWebKey[];
}

interface ParsedJwt {
	header: ClerkJwtHeader;
	payload: ClerkJwtPayload;
	signingInput: string;
	signature: Uint8Array;
}

interface CachedJwks {
	url: string;
	expiresAtMs: number;
	keys: ClerkJsonWebKey[];
}

let cachedJwks: CachedJwks | null = null;
const verificationKeyCache = new Map<string, Promise<CryptoKey>>();

function getHeaderValue(value: string | undefined): string | undefined {
	if (value === undefined) {
		return undefined;
	}

	const normalized = value.trim();
	return normalized.length > 0 ? normalized : undefined;
}

function parsePositiveIntegerEnv(value: string | undefined, fallback: number): number {
	const normalized = getHeaderValue(value);
	if (!normalized || !/^[0-9]+$/.test(normalized)) {
		return fallback;
	}

	const parsed = Number.parseInt(normalized, 10);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		return fallback;
	}

	return parsed;
}

function parseCsvEnv(value: string | undefined): string[] | undefined {
	const normalized = getHeaderValue(value);
	if (!normalized) {
		return undefined;
	}

	const items = normalized
		.split(',')
		.map((item) => item.trim())
		.filter((item) => item.length > 0);

	return items.length > 0 ? items : undefined;
}

function parseBearerToken(headerValue: string | undefined): string {
	if (!headerValue) {
		throw unauthorized('UNAUTHORIZED', 'Missing Authorization header for authenticated request');
	}

	const [scheme, token] = headerValue.split(/\s+/, 2);
	if (!scheme || scheme.toLowerCase() !== 'bearer' || !token || token.trim().length === 0) {
		throw unauthorized(
			'UNAUTHORIZED',
			'Authorization header must use Bearer token format',
		);
	}

	return token.trim();
}

function decodeBase64Url(input: string): Uint8Array {
	try {
		const normalized = input.replace(/-/g, '+').replace(/_/g, '/');
		const paddingLength = (4 - (normalized.length % 4)) % 4;
		const padded = normalized + '='.repeat(paddingLength);
		const binary = atob(padded);
		const bytes = new Uint8Array(binary.length);

		for (let index = 0; index < binary.length; index += 1) {
			bytes[index] = binary.charCodeAt(index);
		}

		return bytes;
	} catch {
		throw unauthorized('UNAUTHORIZED', 'Malformed JWT token encoding');
	}
}

function decodeJsonSegment<T>(segment: string, label: string): T {
	const bytes = decodeBase64Url(segment);
	const text = new TextDecoder().decode(bytes);

	try {
		return JSON.parse(text) as T;
	} catch {
		throw unauthorized('UNAUTHORIZED', `Malformed JWT ${label} segment`);
	}
}

function parseJwt(token: string): ParsedJwt {
	const parts = token.split('.');
	if (parts.length !== 3) {
		throw unauthorized('UNAUTHORIZED', 'JWT token must contain exactly 3 segments');
	}

	const [headerSegment, payloadSegment, signatureSegment] = parts;
	const header = decodeJsonSegment<ClerkJwtHeader>(headerSegment, 'header');
	const payload = decodeJsonSegment<ClerkJwtPayload>(payloadSegment, 'payload');
	const signature = decodeBase64Url(signatureSegment);

	return {
		header,
		payload,
		signingInput: `${headerSegment}.${payloadSegment}`,
		signature,
	};
}

function parseCacheControlMaxAge(headerValue: string | null): number | undefined {
	if (!headerValue) {
		return undefined;
	}

	const match = /max-age=(\d+)/i.exec(headerValue);
	if (!match) {
		return undefined;
	}

	const parsed = Number.parseInt(match[1], 10);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		return undefined;
	}

	return parsed;
}

async function getJwksKeys(
	jwksUrl: string,
	fetcher: typeof fetch,
	forceRefresh = false,
): Promise<ClerkJsonWebKey[]> {
	const now = Date.now();
	if (!forceRefresh && cachedJwks && cachedJwks.url === jwksUrl && cachedJwks.expiresAtMs > now) {
		return cachedJwks.keys;
	}

	let response: Response;
	try {
		response = await fetcher(jwksUrl, {
			method: 'GET',
			headers: { accept: 'application/json' },
		});
	} catch {
		throw serviceUnavailable('AUTH_UNAVAILABLE', 'Unable to fetch Clerk JWKS endpoint');
	}

	if (!response.ok) {
		throw serviceUnavailable('AUTH_UNAVAILABLE', 'Unable to fetch Clerk JWKS endpoint');
	}

	let body: JsonWebKeySet;
	try {
		body = (await response.json()) as JsonWebKeySet;
	} catch {
		throw serviceUnavailable('AUTH_UNAVAILABLE', 'Clerk JWKS response is not valid JSON');
	}

	if (!Array.isArray(body.keys)) {
		throw serviceUnavailable('AUTH_UNAVAILABLE', 'Clerk JWKS response missing keys array');
	}

	const maxAgeSeconds =
		parseCacheControlMaxAge(response.headers.get('cache-control')) ?? DEFAULT_JWKS_CACHE_SECONDS;
	cachedJwks = {
		url: jwksUrl,
		expiresAtMs: now + maxAgeSeconds * 1000,
		keys: body.keys,
	};

	return body.keys;
}

function buildVerificationKeyCacheKey(jwk: ClerkJsonWebKey): string {
	return `${jwk.kid ?? ''}:${jwk.n ?? ''}:${jwk.e ?? ''}`;
}

async function importVerificationKey(jwk: ClerkJsonWebKey): Promise<CryptoKey> {
	if (jwk.kty !== 'RSA' || typeof jwk.n !== 'string' || typeof jwk.e !== 'string') {
		throw unauthorized('UNAUTHORIZED', 'Unsupported Clerk JWT verification key type');
	}

	const cacheKey = buildVerificationKeyCacheKey(jwk);
	const cached = verificationKeyCache.get(cacheKey);
	if (cached) {
		return cached;
	}

	const importedKeyPromise = crypto.subtle.importKey(
		'jwk',
		jwk,
		{ name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
		false,
		['verify'],
	);
	verificationKeyCache.set(cacheKey, importedKeyPromise);

	return importedKeyPromise;
}

function validateTokenClaims(payload: ClerkJwtPayload, config: ClerkVerificationConfig): void {
	const nowSeconds = config.nowSeconds ?? Math.floor(Date.now() / 1000);
	const clockSkewSeconds = config.clockSkewSeconds ?? DEFAULT_CLOCK_SKEW_SECONDS;

	if (typeof payload.exp !== 'number' || !Number.isFinite(payload.exp)) {
		throw unauthorized('UNAUTHORIZED', 'Clerk JWT missing exp claim');
	}

	if (nowSeconds - clockSkewSeconds >= payload.exp) {
		throw unauthorized('UNAUTHORIZED', 'Clerk JWT is expired');
	}

	if (
		payload.nbf !== undefined &&
		(typeof payload.nbf !== 'number' || !Number.isFinite(payload.nbf) || nowSeconds + clockSkewSeconds < payload.nbf)
	) {
		throw unauthorized('UNAUTHORIZED', 'Clerk JWT is not yet active');
	}

	if (config.issuer && payload.iss !== config.issuer) {
		throw unauthorized('UNAUTHORIZED', 'Clerk JWT issuer claim mismatch');
	}

	if (config.audience && config.audience.length > 0) {
		const tokenAudiences = Array.isArray(payload.aud)
			? payload.aud.filter((aud): aud is string => typeof aud === 'string')
			: typeof payload.aud === 'string'
				? [payload.aud]
				: [];

		if (tokenAudiences.length === 0) {
			throw unauthorized('UNAUTHORIZED', 'Clerk JWT missing audience claim');
		}

		const hasMatchingAudience = tokenAudiences.some((audience) => config.audience?.includes(audience));
		if (!hasMatchingAudience) {
			throw unauthorized('UNAUTHORIZED', 'Clerk JWT audience claim mismatch');
		}
	}

	if (config.authorizedParties && config.authorizedParties.length > 0) {
		// Enforced only when the secret is set, so setting it is a deliberate
		// operator step and an unset deployment behaves exactly as before.
		if (typeof payload.azp !== 'string' || payload.azp.trim().length === 0) {
			throw unauthorized('UNAUTHORIZED', 'Clerk JWT missing authorized party claim');
		}

		if (!config.authorizedParties.includes(payload.azp.trim())) {
			throw unauthorized('UNAUTHORIZED', 'Clerk JWT authorized party is not allowed');
		}
	}

	if (typeof payload.sub !== 'string' || payload.sub.trim().length === 0) {
		throw unauthorized('UNAUTHORIZED', 'Clerk JWT missing subject claim');
	}
}

export async function verifyClerkJwt(
	token: string,
	config: ClerkVerificationConfig,
): Promise<ClerkJwtPayload> {
	const parsedJwt = parseJwt(token);
	if (parsedJwt.header.alg !== 'RS256') {
		throw unauthorized('UNAUTHORIZED', 'Unsupported Clerk JWT algorithm');
	}

	if (typeof parsedJwt.header.kid !== 'string' || parsedJwt.header.kid.length === 0) {
		throw unauthorized('UNAUTHORIZED', 'Clerk JWT missing key identifier');
	}

	const fetcher = config.fetcher ?? fetch;
	let jwksKeys = await getJwksKeys(config.jwksUrl, fetcher);
	let jwk = jwksKeys.find((candidate) => candidate.kid === parsedJwt.header.kid);
	if (!jwk) {
		jwksKeys = await getJwksKeys(config.jwksUrl, fetcher, true);
		jwk = jwksKeys.find((candidate) => candidate.kid === parsedJwt.header.kid);
	}

	if (!jwk) {
		throw unauthorized('UNAUTHORIZED', 'Unable to find Clerk verification key for token');
	}

	const verificationKey = await importVerificationKey(jwk);
	const encodedSigningInput = new TextEncoder().encode(parsedJwt.signingInput);
	const isSignatureValid = await crypto.subtle.verify(
		'RSASSA-PKCS1-v1_5',
		verificationKey,
		parsedJwt.signature,
		encodedSigningInput,
	);
	if (!isSignatureValid) {
		throw unauthorized('UNAUTHORIZED', 'Invalid Clerk JWT signature');
	}

	validateTokenClaims(parsedJwt.payload, config);
	return parsedJwt.payload;
}

function uuidToBytes(uuid: string): Uint8Array {
	const normalized = uuid.replace(/-/g, '').toLowerCase();
	if (!/^[0-9a-f]{32}$/.test(normalized)) {
		throw new Error('Namespace UUID is invalid');
	}

	const bytes = new Uint8Array(16);
	for (let index = 0; index < 16; index += 1) {
		const offset = index * 2;
		bytes[index] = Number.parseInt(normalized.slice(offset, offset + 2), 16);
	}

	return bytes;
}

function bytesToUuid(bytes: Uint8Array): UUID {
	const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

export async function deriveUserUuidFromClerkSubject(clerkSubject: string): Promise<UUID> {
	const normalizedSubject = clerkSubject.trim();
	if (normalizedSubject.length === 0) {
		throw unauthorized('UNAUTHORIZED', 'Authenticated Clerk subject is empty');
	}

	const namespaceBytes = uuidToBytes(CLERK_SUBJECT_NAMESPACE);
	const subjectBytes = new TextEncoder().encode(normalizedSubject);
	const hashInput = new Uint8Array(namespaceBytes.length + subjectBytes.length);
	hashInput.set(namespaceBytes, 0);
	hashInput.set(subjectBytes, namespaceBytes.length);

	const digest = new Uint8Array(await crypto.subtle.digest('SHA-1', hashInput));
	const uuidBytes = digest.slice(0, 16);

	uuidBytes[6] = (uuidBytes[6] & 0x0f) | 0x50;
	uuidBytes[8] = (uuidBytes[8] & 0x3f) | 0x80;

	return bytesToUuid(uuidBytes);
}

function extractTokenEmail(payload: ClerkJwtPayload): string | undefined {
	const emailClaim = payload.email ?? payload.email_address;
	if (typeof emailClaim !== 'string') {
		return undefined;
	}

	return parseEmail(emailClaim, 'clerk_token.email');
}

function buildFallbackUserEmail(userId: UUID): string {
	return `${userId}@users.clerk.local`;
}

export const requireAuthenticatedUser: RequestHandler = asyncHandler(async (req, _res, next) => {
	const clerkJwksUrl = getHeaderValue(runtimeEnv.CLERK_JWKS_URL);
	if (!clerkJwksUrl) {
		throw serviceUnavailable('AUTH_UNAVAILABLE', 'CLERK_JWKS_URL is not configured');
	}
	const clerkIssuer = getHeaderValue(runtimeEnv.CLERK_JWT_ISSUER);
	if (!clerkIssuer) {
		throw serviceUnavailable('AUTH_UNAVAILABLE', 'CLERK_JWT_ISSUER is not configured');
	}

	const authorizationHeader = getHeaderValue(req.header(AUTHORIZATION_HEADER) ?? undefined);
	const bearerToken = parseBearerToken(authorizationHeader);
	const payload = await verifyClerkJwt(bearerToken, {
		jwksUrl: clerkJwksUrl,
		issuer: clerkIssuer,
		audience: parseCsvEnv(runtimeEnv.CLERK_JWT_AUDIENCE),
		authorizedParties: parseCsvEnv(runtimeEnv.CLERK_AUTHORIZED_PARTIES),
		clockSkewSeconds: parsePositiveIntegerEnv(
			runtimeEnv.CLERK_JWT_CLOCK_SKEW_SECONDS,
			DEFAULT_CLOCK_SKEW_SECONDS,
		),
	});

	const clerkSubject = (payload.sub as string).trim();
	const userId = await deriveUserUuidFromClerkSubject(clerkSubject);
	// The email comes ONLY from the verified Clerk token. A client-supplied
	// header (the old x-user-email path) would let any authenticated caller
	// write an arbitrary — including someone else's — address into their
	// users.email row. Tokens without an email claim get the synthetic
	// fallback address instead.
	const userEmail = extractTokenEmail(payload);

	const config = getAppConfig(runtimeEnv);
	if (!config.supabasePoolerUrl) {
		throw serviceUnavailable('DB_UNAVAILABLE', 'Database is not configured');
	}

	const sql = getSqlClient(config);

	// auth_provider/auth_subject record which provider identity owns this row
	// (the raw Clerk `sub`, which the id derivation otherwise hashes away).
	// Account deletion needs it to remove the Clerk user; writing it on every
	// request fills it in for existing rows on their next visit.
	let userRows: UserRow[];
	if (userEmail) {
		userRows = await sql<UserRow[]>`
			insert into public.users as u (id, email, auth_provider, auth_subject, last_app_open_date)
			values (${userId}, ${userEmail}, ${AUTH_PROVIDER_CLERK}, ${clerkSubject}, now())
			on conflict (id) do update
			set email = excluded.email,
					auth_provider = excluded.auth_provider,
					auth_subject = excluded.auth_subject,
					last_app_open_date = now()
				returning u.id, u.email, u.auth_provider, u.auth_subject, u.last_app_open_date, u.created_at
		`;
	} else {
		userRows = await sql<UserRow[]>`
			insert into public.users as u (id, email, auth_provider, auth_subject, last_app_open_date)
			values (${userId}, ${buildFallbackUserEmail(userId)}, ${AUTH_PROVIDER_CLERK}, ${clerkSubject}, now())
			on conflict (id) do update
			set auth_provider = excluded.auth_provider,
					auth_subject = excluded.auth_subject,
					last_app_open_date = now()
			where u.id = ${userId}
			returning u.id, u.email, u.auth_provider, u.auth_subject, u.last_app_open_date, u.created_at
		`;
	}

	if (userRows.length === 0) {
		throw unauthorized(
			'USER_NOT_FOUND',
			'Authenticated user could not be provisioned',
		);
	}

	// No per-user seed categories: the system taxonomy (exhaustive-categories
	// migration) covers fresh users, including the Miscellaneous fallback.

	req.auth = { userId };
	next();
});

export function getAuthenticatedUserId(req: Request): string {
	const userId = req.auth?.userId;
	if (!userId) {
		throw unauthorized('UNAUTHORIZED', 'Missing authenticated user context');
	}

	return userId;
}
