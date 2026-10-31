// Abuse control at the perimeter (Task 7). Two Cloudflare rate-limit
// bindings, each counting in a fixed 60-second window:
//
//   USER_RATE_LIMITER — keyed by users.id, checked as soon as the token is
//     verified and before the provisioning upsert, so a flood costs one JWT
//     verification instead of a database round trip per request.
//   IP_RATE_LIMITER — keyed by the client IP, for the traffic that has no
//     user yet: every unauthenticated path (health, version, the
//     secret-gated /internal endpoints) and every unmatched path, every
//     /oauth/* call, and every request to an authenticated route that
//     arrives without a token or whose token fails verification.
//
// Scope is decided by path first, never by the Authorization header alone:
// /health or /internal/* with a junk `Authorization` header reaches no
// authenticated route, so the user limiter never sees it, and keying off
// "has a bearer token" would leave exactly that traffic unmetered — which
// matters most on /internal, where the only thing standing between a caller
// and the ops endpoints is a guessable secret.
//
// Both fail OPEN. The limiter is a guardrail, not an authorization check: a
// missing binding (local dev, `wrangler dev` without the config, unit tests)
// or a binding that throws must never take the API down. Cloudflare counts
// per colo, not globally (A.2 §13), so the effective ceiling is the limit
// times the number of colos a caller reaches — fine for abuse control, not a
// billing boundary.

import type { Request, RequestHandler } from 'express';

import { tooManyRequests } from './errors';

/** Both bindings use Cloudflare's fixed 60-second window. */
export const RATE_LIMIT_WINDOW_SECONDS = 60;
/** Requests per window per authenticated user. */
export const USER_RATE_LIMIT_PER_WINDOW = 300;
/** Requests per window per client IP on unauthenticated and OAuth paths. */
export const IP_RATE_LIMIT_PER_WINDOW = 60;

/** Path prefixes that always take the IP limit, even when a token is present. */
const IP_LIMITED_PREFIXES = ['/oauth'] as const;

/**
 * Every mount in routes/index.ts that sits behind `requireAuthenticatedUser`.
 * A request to one of these carrying a token is the user limiter's job;
 * anything else — unauthenticated routes, unmatched paths — is the IP
 * limiter's. A tenancy contract test fails if this drifts from the router.
 */
export const AUTHENTICATED_MOUNTS = [
	'/account',
	'/accounts',
	'/ai-usage',
	'/analytics',
	'/budget',
	'/card-statements',
	'/categories',
	'/credit-cards',
	'/merchants',
	'/oauth',
	'/transactions',
] as const;

/**
 * The only address header Cloudflare sets and a client cannot forge.
 * `x-forwarded-for` is deliberately not consulted: a caller could put a
 * victim's address there and spend the victim's window.
 */
const CLIENT_IP_HEADER = 'cf-connecting-ip';
const AUTHORIZATION_HEADER = 'authorization';

/**
 * Marks a request the IP limiter has already considered.
 *
 * The auth middleware needs to know this, and it cannot work it out from the
 * path: express rewrites `req.url` when it dispatches into a mounted router,
 * so inside `requireAuthenticatedUser` the path of a `/transactions` request
 * reads as `/`. Asking the request itself is the only reliable answer.
 */
const IP_RATE_LIMIT_CONSIDERED = Symbol.for('money-manager.ipRateLimitConsidered');

export function wasIpRateLimitConsidered(req: Request): boolean {
	return (req as unknown as Record<symbol, unknown>)[IP_RATE_LIMIT_CONSIDERED] === true;
}

interface RateLimitBindings {
	USER_RATE_LIMITER?: RateLimit;
	IP_RATE_LIMITER?: RateLimit;
}

/**
 * True when the limiter said no. A missing binding or a throwing binding
 * answers false: see the fail-open note above.
 */
async function isRateLimited(
	limiter: RateLimit | undefined,
	key: string,
	scope: 'user' | 'ip',
): Promise<boolean> {
	if (!limiter) {
		return false;
	}

	try {
		const outcome = await limiter.limit({ key });
		return outcome.success === false;
	} catch (error) {
		console.warn('RATE_LIMIT_BINDING_ERROR', {
			scope,
			message: error instanceof Error ? error.message : 'unknown',
		});
		return false;
	}
}

function headerOf(req: Request, name: string): string | string[] | undefined {
	// Hand-built request objects (tests, non-express callers) may carry only
	// `header()`; neither shape may crash the limiter.
	return req.headers?.[name] ?? req.header?.(name) ?? undefined;
}

function firstHeaderValue(value: string | string[] | undefined): string | null {
	const raw = Array.isArray(value) ? value[0] : value;
	if (typeof raw !== 'string') {
		return null;
	}

	// Defensive: a comma-joined value keeps only its first entry.
	const normalized = raw.split(',')[0]?.trim() ?? '';
	return normalized.length > 0 ? normalized : null;
}

/**
 * The caller's address as Cloudflare sees it, or null when the header is
 * absent. Null means no limiting: the socket address inside the Worker is
 * the internal loopback, shared by every request, so falling back to it
 * would put the whole world in one 60/minute bucket and let a single caller
 * lock everyone out. Failing open is the lesser risk, and in production
 * Cloudflare always sets this header.
 */
export function resolveClientIp(req: Request): string | null {
	return firstHeaderValue(headerOf(req, CLIENT_IP_HEADER));
}

function isIpLimitedPath(path: string): boolean {
	return IP_LIMITED_PREFIXES.some(
		(prefix) => path === prefix || path.startsWith(`${prefix}/`),
	);
}

function hasBearerToken(req: Request): boolean {
	const header = headerOf(req, AUTHORIZATION_HEADER);
	const value = Array.isArray(header) ? header[0] : header;
	return typeof value === 'string' && value.trim().length > 0;
}

function isAuthenticatedMountPath(path: string): boolean {
	return AUTHENTICATED_MOUNTS.some(
		(mount) => path === mount || path.startsWith(`${mount}/`),
	);
}

/**
 * Which requests the IP limiter counts (see the scope note at the top):
 * every OAuth call, everything that is not an authenticated route at all,
 * and any authenticated route reached without a bearer token.
 */
export function isIpRateLimitedRequest(req: Request): boolean {
	const path = req.path ?? '';
	if (isIpLimitedPath(path)) {
		return true;
	}

	if (!isAuthenticatedMountPath(path)) {
		return true;
	}

	return !hasBearerToken(req);
}

/** Throws 429 when this IP has spent its window. */
export async function enforceIpRateLimit(env: RateLimitBindings, req: Request): Promise<void> {
	(req as unknown as Record<symbol, unknown>)[IP_RATE_LIMIT_CONSIDERED] = true;

	if (!env.IP_RATE_LIMITER) {
		return;
	}

	const ip = resolveClientIp(req);
	if (ip === null) {
		console.warn('RATE_LIMIT_IP_UNKNOWN', { path: req.path ?? null });
		return;
	}

	if (!(await isRateLimited(env.IP_RATE_LIMITER, ip, 'ip'))) {
		return;
	}

	console.warn('RATE_LIMIT_IP_EXCEEDED', {
		path: req.path ?? null,
		method: req.method ?? null,
		limitPerWindow: IP_RATE_LIMIT_PER_WINDOW,
		windowSeconds: RATE_LIMIT_WINDOW_SECONDS,
	});
	throw tooManyRequests(
		'RATE_LIMITED',
		'Too many requests from this address; retry shortly',
		RATE_LIMIT_WINDOW_SECONDS,
	);
}

/** Throws 429 when this user has spent their window. */
export async function enforceUserRateLimit(
	env: RateLimitBindings,
	userId: string,
): Promise<void> {
	if (!(await isRateLimited(env.USER_RATE_LIMITER, userId, 'user'))) {
		return;
	}

	console.warn('RATE_LIMIT_USER_EXCEEDED', {
		userId,
		limitPerWindow: USER_RATE_LIMIT_PER_WINDOW,
		windowSeconds: RATE_LIMIT_WINDOW_SECONDS,
	});
	throw tooManyRequests(
		'RATE_LIMITED',
		'Too many requests; retry shortly',
		RATE_LIMIT_WINDOW_SECONDS,
	);
}

/**
 * Express middleware for the IP limit. Mounted ahead of the routes so it also
 * covers unmatched paths (a 404 flood is still a flood); the per-user limit
 * lives inside the auth middleware, where the user is known.
 */
export function createIpRateLimitMiddleware(env: RateLimitBindings): RequestHandler {
	return (req, _res, next) => {
		if (!isIpRateLimitedRequest(req)) {
			next();
			return;
		}

		enforceIpRateLimit(env, req).then(
			() => next(),
			(error: unknown) => next(error),
		);
	};
}
