// Mock for cloudflare:workers used in unit tests
export const env = {
	APP_NAME: 'money-manager-backend',
	APP_VERSION: '0.1.0',
	NODE_ENV: 'development',
	SUPABASE_POOLER_URL: '',
	CLERK_JWKS_URL: 'https://example.clerk.accounts.dev/.well-known/jwks.json',
	CLERK_JWT_ISSUER: 'https://clerk.example.com',
	DB_MAX_CONNECTIONS: '5',
	DB_CONNECT_TIMEOUT_SECONDS: '5',
};
