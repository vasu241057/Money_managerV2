# Backend Worker Infra (Milestone 3)

## Wired bindings

Mirrors `wrangler.jsonc`; the infra contract test fails if the names drift.

- Cron trigger: `*/10 * * * *` (the single driver; nightly work is scheduled
  in `job_schedules` and claimed by this tick)
- Queue producer bindings:
  - `EMAIL_SYNC_QUEUE` -> `money-manager-email-sync`
  - `NIGHTLY_QUEUE` -> `money-manager-nightly` (Task 8)
- Queue consumer bindings:
  - `money-manager-email-sync` (`max_batch_size=1`, `max_batch_timeout=5`,
    `max_retries=5`, `max_concurrency=10`, DLQ `money-manager-email-sync-dlq`)
  - `money-manager-email-sync-dlq` (`max_batch_size=10`,
    `max_batch_timeout=30`, `max_retries=0`) — logs `DLQ_MESSAGE` and acks
  - `money-manager-nightly` (`max_batch_size=1`, `max_batch_timeout=5`,
    `max_retries=5`, `max_concurrency=2`, DLQ `money-manager-nightly-dlq`)
  - `money-manager-nightly-dlq` (`max_batch_size=10`,
    `max_batch_timeout=30`, `max_retries=0`)
- Rate limiters (Task 7):
  - `USER_RATE_LIMITER` — 300 requests / 60s, keyed by `users.id`
  - `IP_RATE_LIMITER` — 60 requests / 60s, keyed by `CF-Connecting-IP`
- `HYPERDRIVE` (Task 8) — declared but **commented out** in
  `wrangler.jsonc` until the resource exists

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
- `POST /transactions/:transactionId/review`
- `POST /oauth/google/start`
- `POST /oauth/google/callback`
- `GET/DELETE /oauth/google/connection`
- `DELETE /account` (body `{ "confirm": "DELETE" }`; erases the caller's whole account, see `docs/CONTRACT.md`)
- `GET /ai-usage/summary` (configured admin only)
- `GET /ai-usage?month=YYYY-MM&page=N` (configured admin only; user-scoped spend, processing, retries, and attempt receipts)
- `POST /internal/ai/requires` (secret-protected internal webhook)

Manual transaction writes are transaction-safe:

- `POST /transactions` inserts immutable `financial_events` + mutable `transactions` in one SQL transaction
- `DELETE /transactions/:id` deletes only manual rows (`raw_email_id is null`) and marks linked `financial_events.status = 'REVERSED'`

## Secrets

Set secrets per environment (do not store in `wrangler.jsonc`):

`SUPABASE_POOLER_URL` is the transaction-pooler URL (port 6543). In production
its user is `app_backend.<project-ref>`, a role with blanket row-level-security
policies and no DDL rights (see the Task 2 runbook in
`docs/MULTITENANT_TASKS.md`). `backend/.dev.vars` keeps the `postgres` URL,
because the migration runner reads it and must own the schema.

Optional: `MERCHANT_CONFIRMATION_THRESHOLD` (default `1`) is how many distinct
users must keep an AI merchant mapping in review before it counts as
confirmed, and how many must dissent to demote it. `EMAIL_TEMPLATE_PRIVATE_CAP`
(per sender and owner) and `EMAIL_TEMPLATE_SHARED_CAP` (per sender) bound the
learned email templates kept per partition, default `40` each.
`RAW_EMAIL_BODY_RETENTION_DAYS` (default `90`) is how long a processed,
ignored or unrecognized email keeps its stored body before the nightly purge
job blanks it (the job ships disabled in `job_schedules`). `CLERK_SECRET_KEY`
(Clerk Backend API, `sk_…`) lets `DELETE /account` remove the Clerk user;
without it the tombstone stays `clerk_deleted = false` and the nightly job
retries once the secret is set.

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
npx wrangler secret put OPENROUTER_MANAGEMENT_API_KEY
npx wrangler secret put OPENROUTER_MODEL
npx wrangler secret put AI_AUTO_VERIFY_MIN_CONFIDENCE
npx wrangler secret put AI_QUEUE_DELAY_SECONDS
npx wrangler secret put AI_REQUIRES_WEBHOOK_SECRET
npx wrangler secret put CLERK_SECRET_KEY
```

For local development, create `backend/.dev.vars` from `backend/.dev.vars.example`.

`AI_USAGE_ADMIN_USER_ID` is a non-secret internal app UUID. It controls who can
see the AI spend tile and ledger; non-matching authenticated users receive 404.

Merchant mapping is disabled unless `MERCHANT_MAPPING_ENABLED` is explicitly
set to `1`, `on`, `true`, or `yes`. The scheduler will not enqueue mapper jobs
while disabled, and the queue consumer acknowledges any previously queued
mapper job without running it.

The root `npm run deploy:cloudflare` command builds the frontend and deploys it
with the API Worker as same-origin static assets. `keep_vars` is enabled, so
deployments preserve Cloudflare dashboard variables. The deploy command does
not upload `backend/.dev.vars`; secrets and variables are managed separately.

Optional queue alert threshold vars (can be plain vars or secrets):

- `QUEUE_ALERT_RETRY_RATE_PERCENT` (default `15`)
- `QUEUE_ALERT_POISON_ACK_COUNT` (default `3`)
- `QUEUE_ALERT_FINAL_RETRY_COUNT` (default `1`)

## Local invocation checks

```bash
cd backend
TMPDIR=/tmp WRANGLER_LOG_PATH=/tmp/wrangler-logs npx wrangler dev --test-scheduled
```

In another terminal, trigger cron:

```bash
curl "http://127.0.0.1:8787/__scheduled?cron=*+*+*+*+*"
```

You should see scheduled handler logs, then queue handler logs for `EMAIL_SYNC_DISPATCH`.

`EMAIL_SYNC_DISPATCH` now executes Phase 1 control-plane behavior:
- scans Google `oauth_connections` in pages of `LIMIT 1000 OFFSET n`
- auto-marks users as `DORMANT` when `users.last_app_open_date` is older than 45 days
- reactivates `DORMANT` users to `ACTIVE` when they become active again
- enqueues `EMAIL_SYNC_USER` jobs in `sendBatch` slices of 100 messages
- uses continuation payload (`start_offset`, `scan_upper_user_id`) when a single invocation reaches page budget

## Milestone 12 observability

Queue worker emits structured monitoring logs:

- `QUEUE_BATCH_SUMMARY` for each queue batch
- `QUEUE_ALERT_THRESHOLD_BREACH` when retry/poison/final-retry thresholds are exceeded

Use these with Cloudflare Queue metrics + Supabase SQL panels from:

- `docs/OPERATIONS.md`

## Quotas and fairness (Task 6)

Apply `20260918220000_user_quotas.sql` before deploying the quota-aware code.
Optional defaults: `USER_DAILY_EMAIL_FETCH_CAP=500` (independent ceilings on
listed messages and fetched bodies), `USER_DAILY_AI_CALL_CAP=200`,
`MAX_MERCHANTS_PER_RUN=200`, `MAX_SENDERS_PER_RUN=200`. Days use Asia/Kolkata.
The existing AI usage admin can read `GET /ai-usage/users?day=YYYY-MM-DD` and
edit `PUT /ai-usage/users/:userId/limits`; null clears an override. See
`docs/CONTRACT.md` for deferral/recovery semantics and concurrency limitations,
and `docs/MULTITENANT_TASKS.md` for the migration/deployment runbook.

## Perimeter and operations hardening (Task 7)

Apply `20260919090000_perimeter_hardening.sql` before deploying this code.

- **Rate limits** come from the two bindings above and need no env vars. Both
  fail open: without the bindings (local dev, unit tests) nothing is limited.
  Over the limit is `429 RATE_LIMITED` with `Retry-After: 60`. Cloudflare
  counts per colo, so treat the numbers as abuse control, not exact ceilings.
- **`POST /transactions` accepts `Idempotency-Key`** (optional, printable
  ASCII, <= 255 chars). Same key + same body replays the first response with
  `Idempotent-Replay: true`; same key + different body is
  `409 IDEMPOTENCY_KEY_REUSED`; a first request still running is
  `409 IDEMPOTENCY_KEY_IN_FLIGHT`. Rows are purged 24 hours after creation by
  the nightly retention job.
- **`CLERK_AUTHORIZED_PARTIES`** (optional, comma-separated origins) is
  checked against the token's `azp` claim when set, and must be set *before*
  the deploy that enforces it. `CLERK_JWT_AUDIENCE` stays optional.
- **`AI_USAGE_ADMIN_USER_ID` is no longer a committed var.** Set it with
  `wrangler secret put`; `keep_vars: true` preserves the already-deployed
  value in the meantime.
- New log lines: `RATE_LIMIT_USER_EXCEEDED`, `RATE_LIMIT_IP_EXCEEDED`,
  `RATE_LIMIT_BINDING_ERROR`, `RATE_LIMIT_IP_UNKNOWN`, `DLQ_MESSAGE`,
  `STALE_PENDING_EXTRACTION`, `STALE_PENDING_EXTRACTION_SNAPSHOT_STALE`,
  `IDEMPOTENCY_CLAIM_RECLAIMED`, `IDEMPOTENCY_COMPLETE_FAILED`. See
  `docs/OPERATIONS.md` for what each one means and the secret-rotation table.

## Scale runway (Task 8)

No migration. Create the two nightly queues before the deploy that carries
this config, or the deploy is rejected.

- **Nightly work has its own queue.** `SENDER_CLASSIFICATION`,
  `SENDER_BACKFILL`, `MERCHANT_MAPPING` and `RAW_EMAIL_BODY_PURGE` are
  produced onto `money-manager-nightly`. The consumer dispatches on
  `job_type` and both work queues accept every type, so pre-split messages
  drain normally and the split is reversible. Without the `NIGHTLY_QUEUE`
  binding they fall back to the sync queue and log
  `NIGHTLY_QUEUE_BINDING_MISSING` once.
- **Consumer concurrency is capped** at 10 (sync) and 2 (nightly), mirrored
  by `QUEUE_MAX_CONCURRENCY` in `lib/infra.ts` and checked by the infra
  contract test. The ceiling they protect is pooler slots, roughly
  `concurrency x DB_MAX_CONNECTIONS`.
- **Hyperdrive is code-ready, not enabled.** Bound, its connection string
  wins over `SUPABASE_POOLER_URL` and the PgBouncer port check is skipped
  (it listens on a local port of its own). `DB_CONNECTION_SOURCE` logs which
  source is live, once per isolate. Rollback is to unbind and redeploy.
- **`ERROR_REPORTING_WEBHOOK_URL`** (optional) pushes 5xx responses, dead
  letters, queue threshold breaches, scheduled-job failures, stale
  extraction and the Google cap warning to a webhook. Off by default; never
  throws, never retries, 3s timeout, 10/minute per isolate, https only.
  Related: `ERROR_REPORTING_WEBHOOK_TOKEN`,
  `ERROR_REPORTING_MAX_PER_MINUTE`, `ERROR_REPORTING_TIMEOUT_MS`.
- **Google test-user tripwire.** Once a day the tick counts distinct users
  with a Google connection and logs `GOOGLE_TEST_USER_CAPACITY`, or
  `GOOGLE_TEST_USER_CAP_WARNING` from 80 of 100. The count is a lower bound;
  the Google Cloud console holds the real list.
- New log lines: `NIGHTLY_QUEUE_BINDING_MISSING`, `QUEUE_ROUTING_UNEXPECTED`,
  `DB_CONNECTION_SOURCE`, `GOOGLE_TEST_USER_CAPACITY`,
  `GOOGLE_TEST_USER_CAP_WARNING`, `GOOGLE_TEST_USER_CAPACITY_CHECK_FAILED`,
  `ERROR_REPORT_DELIVERY_FAILED`, `ERROR_REPORT_SUPPRESSED`,
  `ERROR_REPORTING_URL_INVALID`. See `docs/OPERATIONS.md`.
