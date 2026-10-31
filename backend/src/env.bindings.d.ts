// Additional env bindings not emitted by `wrangler types` (secrets are configured out-of-band).
declare namespace Cloudflare {
	interface Env {
		SUPABASE_POOLER_URL?: string;
		CLERK_JWKS_URL?: string;
		CLERK_JWT_ISSUER?: string;
		CLERK_JWT_AUDIENCE?: string;
		/**
		 * Comma-separated frontend origins allowed to mint tokens for this
		 * backend, checked against the token's `azp` claim (Task 7, A.3 §1).
		 * Clerk's default session tokens carry no `aud`, so this is the
		 * cross-app protection. Unset = not enforced.
		 */
		CLERK_AUTHORIZED_PARTIES?: string;
		CLERK_JWT_CLOCK_SKEW_SECONDS?: string;
		GOOGLE_CLIENT_ID?: string;
		GOOGLE_CLIENT_SECRET?: string;
		GOOGLE_OAUTH_REDIRECT_URI?: string;
		GOOGLE_OAUTH_STATE_SECRET?: string;
		GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY?: string;
		OPENCODE_GO_API_KEY?: string;
		OPENCODE_GO_MODEL?: string;
		AI_PROVIDER?: string;
		OPENROUTER_API_KEY?: string;
		OPENROUTER_MANAGEMENT_API_KEY?: string;
		OPENROUTER_MODEL?: string;
		AI_USAGE_ADMIN_USER_ID?: string;
		AI_REQUIRES_WEBHOOK_SECRET?: string;
		QUEUE_ALERT_RETRY_RATE_PERCENT?: string;
		QUEUE_ALERT_POISON_ACK_COUNT?: string;
		QUEUE_ALERT_FINAL_RETRY_COUNT?: string;
		EMAIL_TEMPLATES?: string;
		/** Max PRIVATE templates kept per (sender, owner); default 40. */
		EMAIL_TEMPLATE_PRIVATE_CAP?: string;
		/** Max SHARED templates kept per sender; default 40. */
		EMAIL_TEMPLATE_SHARED_CAP?: string;
		EMAIL_PIPELINE_TRACE?: string;
		EMAIL_SENDER_GATE?: string;
		EMAIL_SENDER_ALLOWLIST_EXTRA?: string;
		AI_NORMALIZER_TIERS?: string;
		AI_NORMALIZER_TIER_ROTATE_FAILURES?: string;
		AI_NORMALIZER_TIER_ATTEMPTS?: string;
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
		/**
		 * Fail-closed merchant mapper switch. Only 1/on/true/yes enable it;
		 * absent or any other value keeps the mapper disabled.
		 */
		MERCHANT_MAPPING_ENABLED?: string;
		/**
		 * Distinct users whose review must keep an AI merchant mapping before it
		 * counts as confirmed (and whose dissent demotes it). Default 1.
		 */
		MERCHANT_CONFIRMATION_THRESHOLD?: string;
		/**
		 * Clerk Backend API secret (sk_…); lets DELETE /account remove the
		 * Clerk user. Absent = the tombstone keeps clerk_deleted = false and the
		 * nightly purge job retries once it is set.
		 */
		CLERK_SECRET_KEY?: string;
		/**
		 * Days a processed / ignored / unrecognized email keeps its stored body
		 * before the nightly purge blanks it. Default 90.
		 */
		RAW_EMAIL_BODY_RETENTION_DAYS?: string;
		/**
		 * Fair-use caps per user and IST day (Task 6). Defaults 500 listed IDs / bodies
		 * fetched independently and 200 model attempts; user_limits overrides them per user.
		 */
		USER_DAILY_EMAIL_FETCH_CAP?: string;
		USER_DAILY_AI_CALL_CAP?: string;
		/** Nightly job caps, backlog-scaled: min(cap, backlog). Default 200 each. */
		MAX_MERCHANTS_PER_RUN?: string;
		MAX_SENDERS_PER_RUN?: string;
		/** Tavily key(s), comma-separated for quota rotation; absent = no searches. */
		TAVILY_API_KEY?: string;
		/**
		 * Perimeter rate limiters (Task 7): 300 requests/minute per user after
		 * auth, 60/minute per IP on unauthenticated and OAuth paths. Optional
		 * so local dev and the unit suite run without the bindings; absent
		 * means no limiting, never a failed request.
		 */
		USER_RATE_LIMITER?: RateLimit;
		IP_RATE_LIMITER?: RateLimit;
		/**
		 * Cloudflare Hyperdrive in front of the Supabase pooler (Task 8).
		 * Optional: unbound, the Worker connects straight to
		 * SUPABASE_POOLER_URL exactly as before. Bound, its connection string
		 * wins — see resolveDatabaseConnection in lib/config.ts.
		 */
		HYPERDRIVE?: Hyperdrive;
		/**
		 * Nightly-job queue (Task 8). Optional so local dev, the unit suite
		 * and the window between creating the queue and deploying all keep
		 * working: without it nightly jobs fall back to EMAIL_SYNC_QUEUE,
		 * which handles them identically.
		 */
		NIGHTLY_QUEUE?: Queue;
		/**
		 * Error reporting sink (Task 8). Unset = off, which is the default
		 * and costs nothing. https only, or http on localhost for dev.
		 */
		ERROR_REPORTING_WEBHOOK_URL?: string;
		/** Optional bearer token for the sink above. */
		ERROR_REPORTING_WEBHOOK_TOKEN?: string;
		/** Per-isolate send budget; default 10. */
		ERROR_REPORTING_MAX_PER_MINUTE?: string;
		/** Delivery timeout in ms; default 3000. */
		ERROR_REPORTING_TIMEOUT_MS?: string;
	}
}

interface Env extends Cloudflare.Env {}
