import { checkDatabaseHealth } from '../lib/db/client';

export interface DatabaseHealth {
	status: 'ok' | 'unconfigured' | 'error';
	latencyMs?: number;
	error?: string;
}

export interface HealthPayload {
	status: 'ok' | 'degraded';
	service: string;
	version: string;
	database: DatabaseHealth;
	timestamp: string;
}

export interface HealthResponse {
	statusCode: number;
	payload: HealthPayload;
}

export async function getHealthResponse(config: {
	appName: string;
	appVersion: string;
	supabasePoolerUrl: string | null;
	dbMaxConnections: number;
	dbConnectTimeoutSeconds: number;
}): Promise<HealthResponse> {
	const database = await checkDatabaseHealth(config);
	const status: HealthPayload['status'] = database.status === 'ok' ? 'ok' : 'degraded';

	return {
		statusCode: status === 'ok' ? 200 : 503,
		payload: {
			status,
			service: config.appName,
			version: config.appVersion,
			database,
			timestamp: new Date().toISOString(),
		},
	};
}
