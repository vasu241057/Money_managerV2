// Additional env bindings not emitted by `wrangler types` (secrets are configured out-of-band).
declare namespace Cloudflare {
	interface Env {
		SUPABASE_POOLER_URL?: string;
		CLERK_JWKS_URL?: string;
		CLERK_JWT_ISSUER?: string;
		CLERK_JWT_AUDIENCE?: string;
		CLERK_JWT_CLOCK_SKEW_SECONDS?: string;
		GOOGLE_CLIENT_ID?: string;
		GOOGLE_CLIENT_SECRET?: string;
		GOOGLE_OAUTH_REDIRECT_URI?: string;
		OPENROUTER_API_KEY?: string;
	}
}

interface Env extends Cloudflare.Env {}
