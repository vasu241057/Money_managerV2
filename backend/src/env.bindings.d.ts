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
		GOOGLE_OAUTH_STATE_SECRET?: string;
		GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY?: string;
		OPENCODE_GO_API_KEY?: string;
		OPENCODE_GO_MODEL?: string;
		AI_AUTO_VERIFY_MIN_CONFIDENCE?: string;
		AI_REQUIRES_WEBHOOK_SECRET?: string;
		AI_QUEUE_DELAY_SECONDS?: string;
		QUEUE_ALERT_RETRY_RATE_PERCENT?: string;
		QUEUE_ALERT_POISON_ACK_COUNT?: string;
		QUEUE_ALERT_FINAL_RETRY_COUNT?: string;
		NORMALIZATION_ENGINE?: string;
		AI_NORMALIZER_TIERS?: string;
		AI_NORMALIZER_TIER_ROTATE_FAILURES?: string;
		AI_NORMALIZER_TIER_COOLDOWN_MS?: string;
		AI_NORMALIZER_MAX_RETRIES?: string;
		AI_NORMALIZER_TIMEOUT_MS?: string;
		AI_NORMALIZER_TEMPERATURE?: string;
		AI_NORMALIZER_MAX_TOKENS?: string;
		AI_NORMALIZER_AUTO_VERIFY_CONFIDENCE?: string;
		AI_NORMALIZER_MIN_CONFIDENCE?: string;
		AI_NORMALIZER_NO_TRAINING?: string;
	}
}

interface Env extends Cloudflare.Env {}
