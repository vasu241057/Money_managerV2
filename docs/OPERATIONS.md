# Milestone 12 Operations Runbook

This runbook defines queue dashboards, alert thresholds, and SLO targets for the email ingestion pipeline.

## 1. Queue Dashboards

Create one Cloudflare Queue dashboard per queue:

- `money-manager-email-sync`
- `money-manager-email-sync-dlq` (has a consumer since Task 7)

(`money-manager-ai-classification` was retired with the triage worker in July
2026 and no longer exists.)

Required panels:

1. `Backlog depth`:
   - Metric: messages available / backlog
   - Split by queue
   - Window: 1m + 15m
2. `Retries and DLQ handoff`:
   - Metric: retry count, dead-letter routed count
   - Split by queue
3. `Consumer throughput`:
   - Metric: dequeued, acked, failed
   - Split by queue
4. `End-to-end lag proxy`:
   - SQL panel (below) for `raw_emails` + stale extraction materialized view

## 2. Worker Log Dashboards

`backend/src/workers/queue.worker.ts` now emits:

- `QUEUE_BATCH_SUMMARY`
- `QUEUE_ALERT_THRESHOLD_BREACH`

Build a log dashboard with these fields:

- `queue`
- `total_messages`
- `acked_messages`
- `poison_acked_messages`
- `retried_messages`
- `final_retry_messages`
- `retry_rate_percent`
- `max_attempts_seen`

Recommended panels:

1. `Retry rate (%)` over time by queue (`retry_rate_percent` average/p95)
2. `Poison ack count` over time by queue
3. `Final retry count` over time by queue
4. `Alert breach events` count grouped by `queue`

## 3. Alert Thresholds

Worker-level defaults:

- `QUEUE_ALERT_RETRY_RATE_PERCENT=15`
- `QUEUE_ALERT_POISON_ACK_COUNT=3`
- `QUEUE_ALERT_FINAL_RETRY_COUNT=1`

Treat these as paging thresholds when breached continuously for 10 minutes.

Additional pipeline alerts:

1. `Email sync queue backlog`:
   - page when backlog > `2,000` for 15 minutes
2. `Dead-letter queue ingress`:
   - page on any `DLQ_MESSAGE` (a job exhausted all six attempts)
3. `raw_emails pending extraction`:
   - page when count > `500` for 20 minutes
4. `stale pending extraction MV`:
   - page on `STALE_PENDING_EXTRACTION` repeating for 30 minutes, and on any
     `STALE_PENDING_EXTRACTION_SNAPSHOT_STALE` (the view stopped refreshing,
     so the counts describe an earlier moment)

## 4. SQL Panels (Supabase)

Use these read-only queries in Supabase dashboards/alerts.

```sql
-- Pending/failed extraction backlog
select
  count(*) filter (where status = 'PENDING_EXTRACTION') as pending_extraction,
  count(*) filter (where status = 'FAILED') as failed_extraction,
  count(*) filter (where status = 'UNRECOGNIZED') as unrecognized_count
from public.raw_emails;
```

```sql
-- Stale extraction detector (materialized view refreshed every 10 minutes)
select
  user_id,
  stale_count,
  oldest_created_at,
  newest_created_at,
  snapshot_at
from public.mv_stale_pending_extraction
where stale_count > 0
order by stale_count desc, oldest_created_at asc;
```

```sql
-- Body retention: what the nightly purge still has to do (0 once caught up)
select count(*) as purge_backlog
from public.raw_emails
where status in ('PROCESSED', 'IGNORED', 'UNRECOGNIZED')
  and clean_text <> ''
  and body_purged_at is null
  and coalesce(processing_completed_at, created_at) < now() - interval '90 days';
```

```sql
-- Account deletions whose Clerk user is still pending (retried nightly)
select user_id, deleted_at, clerk_delete_attempts, clerk_last_attempt_at, clerk_last_error
from public.user_deletion_log
where clerk_deleted = false
order by deleted_at asc;
```

## 5. Production SLOs

Milestone 12 cutover should be considered complete only when these hold for 7 consecutive days:

1. `>= 99.5%` of `EMAIL_SYNC_USER` jobs succeed without DLQ routing
2. `>= 99.0%` of `NORMALIZE_RAW_EMAILS` runs do not produce `FAILED` rows
3. P95 `raw_email created_at -> transaction created_at` latency is `< 10 minutes`
4. P95 Review Inbox resolution latency is `< 24 hours`

## 6. Failure Triage Order

1. Check `QUEUE_ALERT_THRESHOLD_BREACH` logs for queue and retry mode
2. Inspect queue backlog and DLQ ingress for the affected queue
3. Inspect `raw_emails` pending/failed counts and stale MV rows
4. Replay from DLQ (Cloudflare DLQ remains authoritative)

## Per-user quotas (Task 6)

Settings -> AI operations -> Per-user fair use shows today's IST message
listings, fetched bodies and AI attempts, plus pending mail and parked
connections. The configured admin can raise a cap or clear it to restore the
env default. `GET /ai-usage/users?day=YYYY-MM-DD` supports historical counters;
limits and backlog counts reflect the current settings/state.

- Defaults: 500 listed messages and 500 bodies per user/day (the common
  `USER_DAILY_EMAIL_FETCH_CAP` ceiling); 200 AI attempts
  (`USER_DAILY_AI_CALL_CAP`). Attempts include retries and fallback models.
- Capped syncs preserve their Gmail token and resume after midnight IST or a
  cap increase. Sender backfills are delayed until the next IST day.
- Pending recovery: at most 250 rows older than 30 minutes per user per
  rolling hour, skipped at the AI cap. It runs on fresh eligible sync jobs.
- `EMAIL_SYNC_DISPATCH_QUOTA_SKIPPED`, `EMAIL_SYNC_QUOTA_DEFERRED`,
  `SENDER_BACKFILL_QUOTA_DEFERRED`, `AI_NORMALIZER_QUOTA_DEFERRED` are expected
  deferrals, not data loss. `EMAIL_SYNC_PENDING_REQUEUE_FAILED` needs queue
  investigation; the hourly claim is released for a later attempt.
- Nightly `MERCHANT_MAPPER_CAPPED` / `SENDER_CLASSIFIER_CAPPED` report a backlog
  above `MAX_MERCHANTS_PER_RUN` / `MAX_SENDERS_PER_RUN` (default 200 each).
- These are fair-use checks against committed counters, so concurrent
  in-flight work can overshoot; do not treat them as strict billing limits.

Use the Task 6 runbook in `MULTITENANT_TASKS.md` for pre-deploy volume checks,
backup, migration invariants, deployment and rollback. The migration alone
changes no existing data and starts both quota tables empty.

## Perimeter and operations hardening (Task 7)

### Rate limits

`USER_RATE_LIMITER` (300/min, keyed by `users.id`) is charged as soon as a
token verifies; `IP_RATE_LIMITER` (60/min, keyed by `CF-Connecting-IP`)
covers unauthenticated paths, every `/oauth/*` call and every request whose
token fails verification. Over-limit callers get `429` with `Retry-After: 60`.

- Logs: `RATE_LIMIT_USER_EXCEEDED`, `RATE_LIMIT_IP_EXCEEDED`. A burst of
  either from one key is abuse or a client retry loop, not an outage.
- **`/internal/*` is IP-limited too** (it is unauthenticated, secret-gated
  only), so a hand-run loop against the ops endpoints shares the same
  60/minute budget. No checked-in script is affected: they all reach the
  database and Storage directly, never the Worker over HTTP.
- `RATE_LIMIT_IP_UNKNOWN` means the request arrived without
  `CF-Connecting-IP` and was therefore not limited. Expected in local dev;
  in production it should never appear.
- `RATE_LIMIT_BINDING_ERROR` means the limiter itself failed; the request was
  allowed through (fail-open by design). Sustained occurrences mean the
  perimeter is effectively off — check the bindings in `wrangler.jsonc`.
- Cloudflare counts **per colo**, so a distributed caller can exceed the
  nominal number by a factor of the colos it reaches. Size alerts on
  multiples of the limit, not on the limit itself.
- To change a limit, edit `ratelimits` in `wrangler.jsonc` and the matching
  constants in `backend/src/lib/http/rate-limit.ts` (a contract test fails if
  they drift), then deploy.

### Dead-letter queue

`money-manager-email-sync-dlq` now has a consumer: it logs `DLQ_MESSAGE`
(message id, job type, user/sync/connection ids, raw-email count — never
payload content) and acks. Cloudflare remains authoritative for the message
itself. `dlqDeliveryAttempts` counts deliveries *on the DLQ*, which starts
its own count; the job already spent all six attempts on the main queue to
arrive here.

The queue must exist before a deploy that declares a consumer for it.
Confirm once, and create it if the list comes back without it:

```bash
npx wrangler queues list
```

```bash
npx wrangler queues info money-manager-email-sync-dlq
```

Redriving is a dashboard action (Queues -> `money-manager-email-sync-dlq` ->
Redrive, targeting `money-manager-email-sync`); alternatively re-enqueue the
specific jobs from the identifiers in `DLQ_MESSAGE`. Fix the cause first: a
redriven message gets the same six attempts and comes straight back. To stop
the bleeding while investigating, `npx wrangler queues pause-delivery
money-manager-email-sync` holds the main queue (`resume-delivery` restores
it) — messages keep accumulating, nothing is lost.

### Stale extraction alert

The 10-minute tick logs `STALE_PENDING_EXTRACTION` with `userCount`,
`staleCount`, `oldestCreatedAt`, `snapshotAt` and `snapshotAgeSeconds` whenever
`mv_stale_pending_extraction` has rows (pg_cron refreshes that view on the
same cadence). This is the only signal for mail that arrived and silently
never became a transaction — nothing errored and nothing retried, so no queue
metric moves.

- Page when it repeats for 30 minutes (alert 4 in section 3).
- First response: check the AI cap (`AI_NORMALIZER_QUOTA_DEFERRED` — deferred
  rows are pending on purpose) before treating it as a fault. The self-heal
  requeue clears genuinely lost rows within the hour.
- `STALE_PENDING_EXTRACTION_CHECK_FAILED` is the read itself failing; the
  dispatch in that tick still happened.
- **The view is refreshed by pg_cron, not by the Worker** (`refresh-stale-mv`,
  every 10 minutes, installed by the baseline migration only when the pg_cron
  extension exists). If that job stops, the counts freeze at whatever they
  were. Rather than alert on stale numbers, the tick logs
  `STALE_PENDING_EXTRACTION_SNAPSHOT_STALE` once the snapshot is over an hour
  old — that means fix the refresh, not the pipeline:
  ```sql
  select jobname, schedule, active from cron.job where jobname = 'refresh-stale-mv';
  select public.refresh_mv_stale_pending_extraction();  -- manual catch-up, as postgres
  ```

### Idempotency ledger

`POST /transactions` honours an optional `Idempotency-Key`. Operationally:

```sql
-- Ledger health: in-flight claims should be seconds old, never hours
select count(*) filter (where completed_at is null) as in_flight,
       count(*) filter (where completed_at is not null) as completed,
       min(created_at) as oldest
from public.idempotency_keys;
```

Old in-flight rows mean requests that died mid-handler; they are reclaimable
after 5 minutes and purged after 24 hours by the nightly job
(`idempotencyPurgedCount` in the purge log, `IDEMPOTENCY_PURGE_BACKLOG` when
one run could not finish). `IDEMPOTENCY_CLAIM_RECLAIMED` is a request taking
over such a row — expected after a crash, suspicious in bulk.

### Secret rotation

All of these are `cd backend && npx wrangler secret put <NAME>`, then
re-check `/health` and one authenticated request. Secrets are never committed
and never pasted into chat.

| Secret | Rotation notes |
|---|---|
| `AI_REQUIRES_WEBHOOK_SECRET` | Gates every `/internal/*` endpoint. Rotating it immediately invalidates saved curl commands and scripts; update them in the same sitting. No user-visible effect. |
| `GOOGLE_OAUTH_STATE_SECRET` | Signs the OAuth `state`. A rotation invalidates consent flows already in flight; a user mid-connect sees a state error and simply retries. Never rotate while running a backfill that reconnects accounts. |
| `AI_USAGE_ADMIN_USER_ID` | No longer a committed var (Task 7). Set it as a secret; `keep_vars: true` means the previously deployed value survives until it is replaced. If it is ever unset, the admin dashboard 404s for everyone — that is the failure mode to expect. |
| `CLERK_SECRET_KEY` | Used only by account deletion and the nightly retry. A stale key shows up as `ACCOUNT_DELETION_CLERK_RETRY_FAILED`. |
| `SUPABASE_POOLER_URL` | Carries the `app_backend` password. Rotation = `alter role app_backend password ...` in the SQL editor, then put the new URL. Roll back by re-putting the old one. |
| `GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY` | **Do not rotate casually**: stored refresh tokens are encrypted with it, and changing it makes every existing connection undecryptable (every user must reconnect). |

`CLERK_AUTHORIZED_PARTIES` is a secret in the same sense: set it **before**
the deploy that enforces it. It must list every origin the frontend is served
from, or every request 401s with `Clerk JWT authorized party is not allowed`.

### Content in logs

Worker logs carry identifiers, counts, lengths and enum values. Sender
addresses are logged (bank senders, needed for triage); counterparty names,
amounts, subjects and bodies are not. The single content-bearing trace,
`PIPELINE_TRACE_RESOLVED`, needs `EMAIL_PIPELINE_TRACE` and is refused when
`NODE_ENV=production`. Keep it that way when adding logs: a new field
carrying user content belongs behind that flag or nowhere.
