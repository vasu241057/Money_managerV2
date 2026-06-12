const DEFAULT_APP_NAME = 'money-manager-backend';
const DEFAULT_APP_VERSION = '0.1.0';
const DEFAULT_NODE_ENV = 'production';
const DEFAULT_DB_MAX_CONNECTIONS = 5;
const DEFAULT_DB_CONNECT_TIMEOUT_SECONDS = 5;
const PGBOUNCER_PORT = '6543';

export interface AppConfig {
	appName: string;
	appVersion: string;
	nodeEnv: string;
	supabasePoolerUrl: string | null;
	dbMaxConnections: number;
	dbConnectTimeoutSeconds: number;
}

function normalizeText(value: string | undefined): string | null {
	if (value === undefined) {
		return null;
	}

	const normalized = value.trim();
	return normalized.length > 0 ? normalized : null;
}

function parsePositiveInteger(value: string | undefined, fallback: number): number {
	const normalized = normalizeText(value);
	if (!normalized) {
		return fallback;
	}

	const parsed = Number.parseInt(normalized, 10);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		return fallback;
	}

	return parsed;
}

function validatePoolerUrl(url: string): string {
	let parsed: URL;
	try {
		parsed = new URL(url);
	} catch {
		throw new Error('SUPABASE_POOLER_URL must be a valid postgres connection URL');
	}

	if (parsed.protocol !== 'postgres:' && parsed.protocol !== 'postgresql:') {
		throw new Error('SUPABASE_POOLER_URL must use postgres:// or postgresql://');
	}

	if (parsed.port !== PGBOUNCER_PORT) {
		throw new Error(
			`SUPABASE_POOLER_URL must target PgBouncer on port ${PGBOUNCER_PORT}; received ${parsed.port || 'default port'}`,
		);
	}

	return parsed.toString();
}

export function getAppConfig(runtimeEnv: Env): AppConfig {
	const supabasePoolerUrlRaw = normalizeText(runtimeEnv.SUPABASE_POOLER_URL);

	return {
		appName: normalizeText(runtimeEnv.APP_NAME) ?? DEFAULT_APP_NAME,
		appVersion: normalizeText(runtimeEnv.APP_VERSION) ?? DEFAULT_APP_VERSION,
		nodeEnv: normalizeText(runtimeEnv.NODE_ENV) ?? DEFAULT_NODE_ENV,
		supabasePoolerUrl: supabasePoolerUrlRaw ? validatePoolerUrl(supabasePoolerUrlRaw) : null,
		dbMaxConnections: parsePositiveInteger(runtimeEnv.DB_MAX_CONNECTIONS, DEFAULT_DB_MAX_CONNECTIONS),
		dbConnectTimeoutSeconds: parsePositiveInteger(
			runtimeEnv.DB_CONNECT_TIMEOUT_SECONDS,
			DEFAULT_DB_CONNECT_TIMEOUT_SECONDS,
		),
	};
}
