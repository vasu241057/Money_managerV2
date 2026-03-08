// Mock for cloudflare:workers used in unit tests
export const env = {
	APP_NAME: 'money-manager-backend',
	APP_VERSION: '0.1.0',
	NODE_ENV: 'development',
	SUPABASE_POOLER_URL: '',
	DB_MAX_CONNECTIONS: '5',
	DB_CONNECT_TIMEOUT_SECONDS: '5',
};
