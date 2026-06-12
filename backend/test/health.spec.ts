import { describe, expect, it } from 'vitest';

import { getHealthResponse } from '../src/services/health.service';

describe('health.service', () => {
	it('returns degraded when DB is unconfigured', async () => {
		const response = await getHealthResponse({
			appName: 'money-manager-backend',
			appVersion: '0.1.0',
			supabasePoolerUrl: null,
			dbMaxConnections: 5,
			dbConnectTimeoutSeconds: 5,
		});

		expect(response.statusCode).toBe(503);
		expect(response.payload.status).toBe('degraded');
		expect(response.payload.database.status).toBe('unconfigured');
	});
});
