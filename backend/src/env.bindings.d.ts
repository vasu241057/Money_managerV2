// Additional env bindings not emitted by `wrangler types` (secrets are configured out-of-band).
interface Env {
	SUPABASE_POOLER_URL?: string;
	GOOGLE_CLIENT_ID?: string;
	GOOGLE_CLIENT_SECRET?: string;
	GOOGLE_OAUTH_REDIRECT_URI?: string;
	OPENROUTER_API_KEY?: string;
}
