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
		expect(config.merchantConfirmationThreshold).toBe(1);
		expect(config.rawEmailBodyRetentionDays).toBe(90);
		expect(config.userDailyEmailFetchCap).toBe(500);
		expect(config.userDailyAiCallCap).toBe(200);
		expect(config.maxMerchantsPerRun).toBe(200);
		expect(config.maxSendersPerRun).toBe(200);
	});

	it('reads the fair-use caps and nightly caps from the env, ignoring junk', () => {
		const base = { SUPABASE_POOLER_URL: '' } as unknown as Env;
		const config = getAppConfig({
			...base,
			USER_DAILY_EMAIL_FETCH_CAP: '1000',
			USER_DAILY_AI_CALL_CAP: '50',
			MAX_MERCHANTS_PER_RUN: '80',
			MAX_SENDERS_PER_RUN: 'lots',
		} as Env);
		expect(config.userDailyEmailFetchCap).toBe(1000);
		expect(config.userDailyAiCallCap).toBe(50);
		expect(config.maxMerchantsPerRun).toBe(80);
		expect(config.maxSendersPerRun).toBe(200);
		expect(getAppConfig({ ...base, USER_DAILY_AI_CALL_CAP: '0' } as Env).userDailyAiCallCap).toBe(200);
	});

	it('reads the merchant confirmation threshold and falls back to 1 on junk', () => {
		const base = { SUPABASE_POOLER_URL: '' } as unknown as Env;
		expect(getAppConfig({ ...base, MERCHANT_CONFIRMATION_THRESHOLD: '3' } as Env).merchantConfirmationThreshold).toBe(3);
		expect(getAppConfig({ ...base, MERCHANT_CONFIRMATION_THRESHOLD: '0' } as Env).merchantConfirmationThreshold).toBe(1);
		expect(getAppConfig({ ...base, MERCHANT_CONFIRMATION_THRESHOLD: 'many' } as Env).merchantConfirmationThreshold).toBe(1);
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

	describe('Hyperdrive (Task 8)', () => {
		const poolerUrl = 'postgres://user:pass@host:6543/db';
		const hyperdriveUrl = 'postgres://user:pass@localhost:34917/db';

		it('reports the pooler as the source when nothing else is bound', () => {
			const config = getAppConfig({ SUPABASE_POOLER_URL: poolerUrl } as unknown as Env);

			expect(config.supabasePoolerUrl).toBe(poolerUrl);
			expect(config.dbConnectionSource).toBe('pooler');
		});

		it('prefers the binding over the pooler URL when both are present', () => {
			const config = getAppConfig({
				SUPABASE_POOLER_URL: poolerUrl,
				HYPERDRIVE: { connectionString: hyperdriveUrl },
			} as unknown as Env);

			expect(config.supabasePoolerUrl).toBe(hyperdriveUrl);
			expect(config.dbConnectionSource).toBe('hyperdrive');
		});

		// Hyperdrive listens on a local port of its own choosing, so the
		// PgBouncer port check cannot apply to it.
		it('accepts the arbitrary local port Hyperdrive hands out', () => {
			expect(() =>
				getAppConfig({ HYPERDRIVE: { connectionString: hyperdriveUrl } } as unknown as Env),
			).not.toThrow();
		});

		it('still insists the binding gave it a postgres URL', () => {
			expect(() =>
				getAppConfig({
					HYPERDRIVE: { connectionString: 'https://localhost:34917/db' },
				} as unknown as Env),
			).toThrow('HYPERDRIVE connection string must use postgres');
			expect(() =>
				getAppConfig({ HYPERDRIVE: { connectionString: 'nonsense' } } as unknown as Env),
			).toThrow('HYPERDRIVE binding did not provide a valid postgres connection URL');
		});

		// Unbinding the block is the rollback path: no code change, straight
		// back to the pooler.
		it('falls back to the pooler when the binding carries no connection string', () => {
			const config = getAppConfig({
				SUPABASE_POOLER_URL: poolerUrl,
				HYPERDRIVE: { connectionString: '' },
			} as unknown as Env);

			expect(config.supabasePoolerUrl).toBe(poolerUrl);
			expect(config.dbConnectionSource).toBe('pooler');
		});

		it('reports no source at all when the database is unconfigured', () => {
			const config = getAppConfig({ SUPABASE_POOLER_URL: '' } as unknown as Env);

			expect(config.supabasePoolerUrl).toBeNull();
			expect(config.dbConnectionSource).toBeNull();
		});
	});
});
