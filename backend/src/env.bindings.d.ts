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
		AI_REQUIRES_WEBHOOK_SECRET?: string;
		QUEUE_ALERT_RETRY_RATE_PERCENT?: string;
		QUEUE_ALERT_POISON_ACK_COUNT?: string;
		QUEUE_ALERT_FINAL_RETRY_COUNT?: string;
		EMAIL_TEMPLATES?: string;
		EMAIL_SENDER_GATE?: string;
		EMAIL_SENDER_ALLOWLIST_EXTRA?: string;
		AI_NORMALIZER_TIERS?: string;
		AI_NORMALIZER_TIER_ROTATE_FAILURES?: string;
		AI_NORMALIZER_TIER_COOLDOWN_MS?: string;
		AI_NORMALIZER_MAX_RETRIES?: string;
		AI_NORMALIZER_TIMEOUT_MS?: string;
		AI_NORMALIZER_TEMPERATURE?: string;
		AI_NORMALIZER_MAX_TOKENS?: string;
		AI_NORMALIZER_AUTO_VERIFY_CONFIDENCE?: string;
		AI_NORMALIZER_MIN_CONFIDENCE?: string;
		AI_NORMALIZER_MASK_NUMBERS?: string;
		AI_NORMALIZER_BATCH_SIZE?: string;
		EMAIL_SYNC_INITIAL_LOOKBACK_DAYS?: string;
		TXN_DEDUP_WINDOW_MINUTES?: string;
	}
}

interface Env extends Cloudflare.Env {}
