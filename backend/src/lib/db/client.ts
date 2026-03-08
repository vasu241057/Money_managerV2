import postgres from 'postgres';

export type DatabaseHealthStatus = 'ok' | 'unconfigured' | 'error';

export interface DatabaseHealth {
	status: DatabaseHealthStatus;
	latencyMs?: number;
	error?: string;
}

interface DbClientConfig {
	supabasePoolerUrl: string | null;
	dbMaxConnections: number;
	dbConnectTimeoutSeconds: number;
}

export type SqlClient = ReturnType<typeof postgres>;

interface CachedClient {
	connectionString: string;
	maxConnections: number;
	connectTimeoutSeconds: number;
	sql: SqlClient;
}

let cachedClient: CachedClient | null = null;

function createClient(config: DbClientConfig): SqlClient {
	return postgres(config.supabasePoolerUrl as string, {
		max: config.dbMaxConnections,
		connect_timeout: config.dbConnectTimeoutSeconds,
		prepare: false,
	});
}

function shouldReuseClient(config: DbClientConfig): boolean {
	if (!cachedClient || !config.supabasePoolerUrl) {
		return false;
	}

	return (
		cachedClient.connectionString === config.supabasePoolerUrl &&
		cachedClient.maxConnections === config.dbMaxConnections &&
		cachedClient.connectTimeoutSeconds === config.dbConnectTimeoutSeconds
	);
}

function getOrCreateClient(config: DbClientConfig): SqlClient | null {
	if (!config.supabasePoolerUrl) {
		if (cachedClient) {
			void cachedClient.sql.end({ timeout: 5 }).catch(() => undefined);
			cachedClient = null;
		}
		return null;
	}

	if (shouldReuseClient(config)) {
		return cachedClient?.sql ?? null;
	}

	if (cachedClient) {
		void cachedClient.sql.end({ timeout: 5 }).catch(() => undefined);
	}

	const sql = createClient(config);
	cachedClient = {
		connectionString: config.supabasePoolerUrl,
		maxConnections: config.dbMaxConnections,
		connectTimeoutSeconds: config.dbConnectTimeoutSeconds,
		sql,
	};

	return sql;
}

export function getSqlClient(config: DbClientConfig): SqlClient {
	const sql = getOrCreateClient(config);
	if (!sql) {
		throw new Error('SUPABASE_POOLER_URL is not configured');
	}

	return sql;
}

export async function checkDatabaseHealth(config: DbClientConfig): Promise<DatabaseHealth> {
	const sql = getOrCreateClient(config);
	if (!sql) {
		return { status: 'unconfigured' };
	}

	const startedAt = Date.now();
	try {
		await sql`select 1 as ok`;
		return {
			status: 'ok',
			latencyMs: Date.now() - startedAt,
		};
	} catch (error) {
		return {
			status: 'error',
			error: error instanceof Error ? error.message : 'Unknown database error',
		};
	}
}

export async function closeSqlClient(): Promise<void> {
	if (!cachedClient) {
		return;
	}

	await cachedClient.sql.end({ timeout: 5 });
	cachedClient = null;
}
