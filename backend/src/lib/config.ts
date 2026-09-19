import { DEFAULT_USER_DAILY_AI_CALL_CAP, DEFAULT_USER_DAILY_EMAIL_FETCH_CAP } from './quotas';

const DEFAULT_APP_NAME = 'money-manager-backend';
const DEFAULT_APP_VERSION = '0.1.0';
const DEFAULT_NODE_ENV = 'production';
const DEFAULT_DB_MAX_CONNECTIONS = 5;
const DEFAULT_DB_CONNECT_TIMEOUT_SECONDS = 5;
// One user's review confirms or demotes an AI merchant mapping: today's
// single-tenant behaviour. Raise once several users review the same brands.
const DEFAULT_MERCHANT_CONFIRMATION_THRESHOLD = 1;
// Days a judged email keeps its stored body before the nightly purge blanks
// it (Task 5). FAILED rows are exempt: their body is what a retry needs.
const DEFAULT_RAW_EMAIL_BODY_RETENTION_DAYS = 90;
// Nightly job caps; a run takes min(cap, backlog) in the existing chunks.
const DEFAULT_MAX_MERCHANTS_PER_RUN = 200;
const DEFAULT_MAX_SENDERS_PER_RUN = 200;
const PGBOUNCER_PORT = '6543';

/** Where the connection string came from; reported, never configured. */
export type DbConnectionSource = 'hyperdrive' | 'pooler';

export interface AppConfig {
	appName: string;
	appVersion: string;
	nodeEnv: string;
	/**
	 * The connection string the postgres client uses, whatever its origin
	 * (Task 8). Named for the pooler because that is what it was, and what
	 * every consumer still calls it.
	 */
	supabasePoolerUrl: string | null;
	/** null when no database is configured at all. */
	dbConnectionSource: DbConnectionSource | null;
	dbMaxConnections: number;
	dbConnectTimeoutSeconds: number;
	merchantConfirmationThreshold: number;
	rawEmailBodyRetentionDays: number;
	/** Independent ceiling for listed IDs and fetched bodies per user per IST day. */
	userDailyEmailFetchCap: number;
	/** Model calls one user may consume per IST day before their emails wait as PENDING. */
	userDailyAiCallCap: number;
	maxMerchantsPerRun: number;
	maxSendersPerRun: number;
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

/**
 * Hyperdrive hands the Worker a connection string for its own local endpoint,
 * on a port it chooses — so the PgBouncer port check that guards the operator
 * against pointing at Supabase's direct 5432 (which exhausts connections from
 * a Worker) cannot apply to it. Everything else still does: it must parse,
 * and it must be postgres.
 *
 * Origin still matters behind it — the Hyperdrive resource itself must be
 * created against the pooler URL, port 6543. That is an operator step
 * (OPERATIONS.md), unverifiable from inside the Worker: all the binding
 * exposes is its own localhost address.
 */
function validateHyperdriveUrl(url: string): string {
	let parsed: URL;
	try {
		parsed = new URL(url);
	} catch {
		throw new Error('HYPERDRIVE binding did not provide a valid postgres connection URL');
	}

	if (parsed.protocol !== 'postgres:' && parsed.protocol !== 'postgresql:') {
		throw new Error('HYPERDRIVE connection string must use postgres:// or postgresql://');
	}

	return parsed.toString();
}

/**
 * Hyperdrive wins when bound, because binding it is a deliberate act: it
 * exists only if the operator created the resource and wired it in. The
 * pooler URL stays configured underneath as the rollback path — unbind the
 * Hyperdrive block, redeploy, and the Worker is back on the direct pooler
 * connection with no code change.
 */
function resolveDatabaseConnection(runtimeEnv: Env): {
	url: string | null;
	source: DbConnectionSource | null;
} {
	const hyperdriveUrl = normalizeText(runtimeEnv.HYPERDRIVE?.connectionString);
	if (hyperdriveUrl) {
		return { url: validateHyperdriveUrl(hyperdriveUrl), source: 'hyperdrive' };
	}

	const poolerUrl = normalizeText(runtimeEnv.SUPABASE_POOLER_URL);
	if (poolerUrl) {
		return { url: validatePoolerUrl(poolerUrl), source: 'pooler' };
	}

	return { url: null, source: null };
}

export function getAppConfig(runtimeEnv: Env): AppConfig {
	const connection = resolveDatabaseConnection(runtimeEnv);

	return {
		appName: normalizeText(runtimeEnv.APP_NAME) ?? DEFAULT_APP_NAME,
		appVersion: normalizeText(runtimeEnv.APP_VERSION) ?? DEFAULT_APP_VERSION,
		nodeEnv: normalizeText(runtimeEnv.NODE_ENV) ?? DEFAULT_NODE_ENV,
		supabasePoolerUrl: connection.url,
		dbConnectionSource: connection.source,
		dbMaxConnections: parsePositiveInteger(runtimeEnv.DB_MAX_CONNECTIONS, DEFAULT_DB_MAX_CONNECTIONS),
		dbConnectTimeoutSeconds: parsePositiveInteger(
			runtimeEnv.DB_CONNECT_TIMEOUT_SECONDS,
			DEFAULT_DB_CONNECT_TIMEOUT_SECONDS,
		),
		merchantConfirmationThreshold: parsePositiveInteger(
			runtimeEnv.MERCHANT_CONFIRMATION_THRESHOLD,
			DEFAULT_MERCHANT_CONFIRMATION_THRESHOLD,
		),
		rawEmailBodyRetentionDays: parsePositiveInteger(
			runtimeEnv.RAW_EMAIL_BODY_RETENTION_DAYS,
			DEFAULT_RAW_EMAIL_BODY_RETENTION_DAYS,
		),
		userDailyEmailFetchCap: parsePositiveInteger(
			runtimeEnv.USER_DAILY_EMAIL_FETCH_CAP,
			DEFAULT_USER_DAILY_EMAIL_FETCH_CAP,
		),
		userDailyAiCallCap: parsePositiveInteger(
			runtimeEnv.USER_DAILY_AI_CALL_CAP,
			DEFAULT_USER_DAILY_AI_CALL_CAP,
		),
		maxMerchantsPerRun: parsePositiveInteger(
			runtimeEnv.MAX_MERCHANTS_PER_RUN,
			DEFAULT_MAX_MERCHANTS_PER_RUN,
		),
		maxSendersPerRun: parsePositiveInteger(
			runtimeEnv.MAX_SENDERS_PER_RUN,
			DEFAULT_MAX_SENDERS_PER_RUN,
		),
	};
}
