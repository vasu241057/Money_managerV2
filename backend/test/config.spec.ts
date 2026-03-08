import { describe, expect, it } from 'vitest';

import { getAppConfig } from '../src/lib/config';

describe('config', () => {
	it('returns defaults when env values are empty', () => {
		const env = {
			APP_NAME: '',
			APP_VERSION: '',
			NODE_ENV: '',
			SUPABASE_POOLER_URL: '',
			DB_MAX_CONNECTIONS: '',
			DB_CONNECT_TIMEOUT_SECONDS: '',
		} as unknown as Env;

		const config = getAppConfig(env);

		expect(config.appName).toBe('money-manager-backend');
		expect(config.appVersion).toBe('0.1.0');
		expect(config.nodeEnv).toBe('production');
		expect(config.supabasePoolerUrl).toBeNull();
		expect(config.dbMaxConnections).toBe(5);
		expect(config.dbConnectTimeoutSeconds).toBe(5);
	});

	it('uses env values when provided', () => {
		const env = {
			APP_NAME: 'test-app',
			APP_VERSION: '2.0.0',
			NODE_ENV: 'test',
			SUPABASE_POOLER_URL: 'postgres://user:pass@host:6543/db',
			DB_MAX_CONNECTIONS: '10',
			DB_CONNECT_TIMEOUT_SECONDS: '3',
		} as unknown as Env;

		const config = getAppConfig(env);

		expect(config.appName).toBe('test-app');
		expect(config.appVersion).toBe('2.0.0');
		expect(config.nodeEnv).toBe('test');
		expect(config.supabasePoolerUrl).toBe('postgres://user:pass@host:6543/db');
		expect(config.dbMaxConnections).toBe(10);
		expect(config.dbConnectTimeoutSeconds).toBe(3);
	});

	it('rejects non-pooler connection URLs', () => {
		const env = {
			APP_NAME: 'test-app',
			APP_VERSION: '2.0.0',
			NODE_ENV: 'test',
			SUPABASE_POOLER_URL: 'postgres://user:pass@host:5432/db',
			DB_MAX_CONNECTIONS: '10',
			DB_CONNECT_TIMEOUT_SECONDS: '3',
		} as unknown as Env;

		expect(() => getAppConfig(env)).toThrow('SUPABASE_POOLER_URL must target PgBouncer on port 6543');
	});
});
