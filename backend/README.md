# Backend Worker Infra (Milestone 3)

## Wired bindings

- Cron trigger: `*/10 * * * *`
- Queue producer bindings:
  - `EMAIL_SYNC_QUEUE` -> `money-manager-email-sync`
  - `AI_CLASSIFICATION_QUEUE` -> `money-manager-ai-classification`
- Queue consumer bindings:
  - `money-manager-email-sync` (`max_batch_size=10`, DLQ `money-manager-email-sync-dlq`)
  - `money-manager-ai-classification` (`max_batch_size=10`, DLQ `money-manager-ai-classification-dlq`)

## Milestone 4 API (manual CRUD)

Authenticated routes require:

- `Authorization: Bearer <clerk_session_jwt>` (required)
- `x-user-email: <email>` (optional bootstrap fallback when token does not include email claim)

Backend verifies Clerk tokens using JWKS:
- `CLERK_JWKS_URL` (required)
- `CLERK_JWT_ISSUER` (required strict issuer validation)
- `CLERK_JWT_AUDIENCE` (optional comma-separated accepted audiences)
- `CLERK_JWT_CLOCK_SKEW_SECONDS` (optional, default 60)

Internal user mapping:
- Clerk `sub` is deterministically mapped to an internal UUIDv5 for `users.id`.

Routes:

- `GET/POST/PATCH/DELETE /accounts`
- `GET/POST/PATCH/DELETE /categories`
- `GET/POST/PATCH/DELETE /transactions`
- `POST /oauth/google/start`
- `POST /oauth/google/callback`
- `GET/DELETE /oauth/google/connection`

Manual transaction writes are transaction-safe:

- `POST /transactions` inserts immutable `financial_events` + mutable `transactions` in one SQL transaction
- `DELETE /transactions/:id` deletes only manual rows (`raw_email_id is null`) and marks linked `financial_events.status = 'REVERSED'`

## Secrets

Set secrets per environment (do not store in `wrangler.jsonc`):

```bash
cd backend
npx wrangler secret put SUPABASE_POOLER_URL
npx wrangler secret put CLERK_JWKS_URL
npx wrangler secret put CLERK_JWT_ISSUER
npx wrangler secret put CLERK_JWT_AUDIENCE
npx wrangler secret put GOOGLE_CLIENT_ID
npx wrangler secret put GOOGLE_CLIENT_SECRET
npx wrangler secret put GOOGLE_OAUTH_REDIRECT_URI
npx wrangler secret put GOOGLE_OAUTH_STATE_SECRET
npx wrangler secret put GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY
npx wrangler secret put OPENROUTER_API_KEY
```

For local development, create `backend/.dev.vars` from `backend/.dev.vars.example`.

## Local invocation checks

```bash
cd backend
TMPDIR=/tmp WRANGLER_LOG_PATH=/tmp/wrangler-logs npx wrangler dev --test-scheduled
```

In another terminal, trigger cron:

```bash
curl "http://127.0.0.1:8787/__scheduled?cron=*/10+*+*+*+*"
```

You should see scheduled handler logs, then queue handler logs for `EMAIL_SYNC_DISPATCH`.

`EMAIL_SYNC_DISPATCH` now executes Phase 1 control-plane behavior:
- scans Google `oauth_connections` in pages of `LIMIT 1000 OFFSET n`
- auto-marks users as `DORMANT` when `users.last_app_open_date` is older than 45 days
- reactivates `DORMANT` users to `ACTIVE` when they become active again
- enqueues `EMAIL_SYNC_USER` jobs in `sendBatch` slices of 100 messages
- uses continuation payload (`start_offset`, `scan_upper_user_id`) when a single invocation reaches page budget
