// Keeps the matrix honest against the real route table: every authenticated
// route with an id in its path must be covered by a matrix case or be on the
// explicit allowlist, every body-id route must be covered, and every route key
// a case names must exist. No database involved; this is a source-level check.

import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import { AUTHENTICATED_MOUNTS } from '../../src/lib/http/rate-limit';
import { BODY_ID_ROUTES, MATRIX_CASES, ROUTE_ALLOWLIST } from './service-matrix.cases';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROUTES_DIR = path.resolve(HERE, '../../src/routes');

interface RouteKey {
	key: string;
	authenticated: boolean;
}

function parseMounts(): Map<string, { mount: string; authenticated: boolean }> {
	const source = readFileSync(path.join(ROUTES_DIR, 'index.ts'), 'utf8');
	const importByFactory = new Map<string, string>();
	for (const match of source.matchAll(/import \{ (create\w+Router) \} from '\.\/([\w-]+)'/g)) {
		importByFactory.set(match[1] as string, match[2] as string);
	}
	const mounts = new Map<string, { mount: string; authenticated: boolean }>();
	for (const match of source.matchAll(
		/router\.use\(\s*'([^']+)'\s*,\s*(requireAuthenticatedUser\s*,\s*)?(create\w+Router)\(\)\s*\)/g,
	)) {
		const file = importByFactory.get(match[3] as string);
		if (!file) throw new Error(`route factory ${match[3]} has no import in routes/index.ts`);
		mounts.set(file, { mount: match[1] as string, authenticated: Boolean(match[2]) });
	}
	return mounts;
}

export function listAuthenticatedMounts(): string[] {
	return [...parseMounts().values()]
		.filter((mount) => mount.authenticated)
		.map((mount) => mount.mount)
		.sort();
}

export function listRouteKeys(): RouteKey[] {
	const mounts = parseMounts();
	const keys: RouteKey[] = [];
	for (const file of readdirSync(ROUTES_DIR)) {
		if (!file.endsWith('.ts') || file === 'index.ts') continue;
		const name = file.replace(/\.ts$/, '');
		const mountInfo = mounts.get(name);
		if (!mountInfo) throw new Error(`routes/${file} is not mounted in routes/index.ts`);
		const source = readFileSync(path.join(ROUTES_DIR, file), 'utf8');
		for (const match of source.matchAll(/router\.(get|post|patch|put|delete)\(\s*'([^']+)'/g)) {
			const method = (match[1] as string).toUpperCase();
			const subPath = match[2] as string;
			const fullPath = subPath === '/' ? mountInfo.mount : `${mountInfo.mount}${subPath}`;
			keys.push({ key: `${method} ${fullPath}`, authenticated: mountInfo.authenticated });
		}
	}
	return keys;
}

describe('tenancy: route enrollment', () => {
	const routes = listRouteKeys();
	const routeKeys = new Set(routes.map((route) => route.key));
	const covered = new Set(MATRIX_CASES.flatMap((matrixCase) => matrixCase.routes));

	it('parses the route table', () => {
		expect(routeKeys.has('PATCH /transactions/:transactionId')).toBe(true);
		expect(routeKeys.has('GET /merchants/mine')).toBe(true);
		expect(routes.length).toBeGreaterThan(30);
	});

	it('covers every authenticated route with an id in its path', () => {
		const uncovered = routes
			.filter((route) => route.authenticated && route.key.includes('/:'))
			.map((route) => route.key)
			.filter((key) => !covered.has(key) && !ROUTE_ALLOWLIST.includes(key));
		expect(uncovered, 'enroll these routes in service-matrix.cases.ts or allowlist them').toEqual([]);
	});

	it('covers every route whose body carries identifiers', () => {
		const uncovered = BODY_ID_ROUTES.filter((key) => !covered.has(key));
		expect(uncovered).toEqual([]);
	});

	it('names only routes that exist', () => {
		const unknown = [...covered, ...BODY_ID_ROUTES, ...ROUTE_ALLOWLIST].filter((key) => !routeKeys.has(key));
		expect(unknown, 'stale route keys in service-matrix.cases.ts').toEqual([]);
	});

	it('keeps the rate limiter\u2019s authenticated-mount list in step with the router', () => {
		// The IP limiter decides scope by path: anything not on this list is
		// treated as unauthenticated and charged per IP. A new authenticated
		// mount missing here would be IP-limited at 60/min instead of 300/min
		// per user; a stale entry here would leave an unauthenticated route
		// metered by nothing (see rate-limit.ts).
		expect([...AUTHENTICATED_MOUNTS].sort()).toEqual(listAuthenticatedMounts());
	});
});
