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

## Scale runway (Task 8)

Four independent tracks. Only the first is blocking, and it is the one that
runs on a calendar rather than on engineering time — start it first.

### Google verification and the 100-user cap

`gmail.readonly` is a **restricted** OAuth scope. Until the app is verified,
Google caps it at **100 test users**, listed by address on the OAuth consent
screen. Signup is not gated (A.1 §6, decision D10): user 101 registers with
Clerk, gets provisioned, and then fails at Google's consent step with an
error the app cannot explain. Until verification completes, **the Clerk
allowlist must mirror the Google test-user list**, and the mirror is manual.

The tripwire, once a day at the first 10-minute tick after 02:00 UTC:

- `GOOGLE_TEST_USER_CAPACITY` — an info line with the current count.
- `GOOGLE_TEST_USER_CAP_WARNING` — an error line from 80 of 100, and a
  webhook report if one is configured.
- `GOOGLE_TEST_USER_CAPACITY_CHECK_FAILED` — the read failed; the tick was
  unaffected.

**The count is a lower bound.** It counts distinct users holding a Google
`oauth_connections` row — users, not mailboxes, and revoked connections still
count because the slot on Google's list is still spent. What it cannot see is
a deleted account: the row cascades away here while the entry on Google's
list stays until it is removed by hand. The authoritative number is in the
Google Cloud console. Treat the warning as "go and count properly".

Verification track, in the order the lead times demand:

1. Publish a privacy policy and a homepage on the app's own domain; the
   policy must describe the Gmail data use in the terms Google asks for. The
   data-flow list in `CONTRACT.md` ("What leaves the system") is the source
   for it — it is kept accurate for exactly this reason.
2. In the Google Cloud console, complete the OAuth consent screen: scope
   justification for `gmail.readonly`, a demo video showing the consent flow
   and what the app does with the data, and the domain verification.
3. Submit for verification. Restricted scopes additionally require a **CASA**
   security assessment through an authorised third-party assessor, which is
   annual, paid, and the long pole. Budget months, not weeks.
4. Separately, request a **Gmail API quota increase** in the console if the
   per-user or daily unit limits become the constraint. That is a different
   form from verification and much faster.
5. Only once verification is granted: remove the test-user gating on the
   Clerk side, and retire the mirror.

Nothing in this track is a deploy. None of it can be done from this repo.

### Cloudflare Hyperdrive

Every HTTP request currently builds its own postgres client, paying a TLS
handshake and pooler auth before the first query. Hyperdrive keeps warm
connections at the edge and removes that cost without changing the code
shape.

Rollout:

1. Create the resource against the **PgBouncer** URL (port 6543), with query
   caching **disabled** — this app reads its own writes, and a cached read
   after a write would show a user stale data:

```bash
npx wrangler hyperdrive create money-manager-db --connection-string="postgres://USER:PASSWORD@HOST:6543/postgres" --caching-disabled
```

2. Uncomment the `hyperdrive` block in `wrangler.jsonc` and paste in the id
   the command printed.
3. Deploy. Confirm with the `DB_CONNECTION_SOURCE` log line, which is written
   once per isolate and reads `{ source: "hyperdrive" }`.

`SUPABASE_POOLER_URL` stays configured underneath and unused; the binding
wins while it exists. **Rollback is to comment the block out and redeploy** —
no code change, straight back to the pooler.

Two things the Worker cannot check for you: that the connection string you
gave Hyperdrive really is the pooler on 6543 (all the binding exposes is its
own localhost address, so `config.ts` cannot apply the port check to it), and
that caching is off. Both are properties of the resource you created.

### Nightly queue split and consumer concurrency

Nightly work runs on `money-manager-nightly`, with its own dead-letter queue.
Sender classification, merchant mapping, the raw-email body purge and the
sender-backfill fan-out go there; email sync keeps `money-manager-email-sync`
to itself, so a batch job that runs for minutes never sits in front of a
user's mail.

Create both queues **before** the first deploy carrying this config, or the
deploy is rejected:

```bash
npx wrangler queues create money-manager-nightly
```

```bash
npx wrangler queues create money-manager-nightly-dlq
```

- The consumer dispatches on `job_type`, not on the queue, and the main queue
  still accepts every nightly type. So messages enqueued before the split
  drain normally, and nothing breaks if a job lands on the "wrong" queue.
- If the `NIGHTLY_QUEUE` binding is missing, nightly jobs fall back to the
  sync queue and `NIGHTLY_QUEUE_BINDING_MISSING` is logged once per isolate.
  That is the deploy-window safety net, not a steady state.
- `QUEUE_ROUTING_UNEXPECTED` means a non-nightly job arrived on the nightly
  queue. It still ran; a steady stream means a producer is misrouting.
- `DLQ_MESSAGE` now names its own queue, so nightly dead letters are
  distinguishable from sync ones.

**Concurrency** is capped explicitly: 10 for email sync, 2 for nightly
(`max_concurrency` in `wrangler.jsonc`, asserted against
`QUEUE_MAX_CONCURRENCY` in `lib/infra.ts` by a contract test). Unset means
Cloudflare auto-scales, which is what these numbers exist to prevent: the
binding constraint is pooler slots, roughly `concurrency x
DB_MAX_CONNECTIONS` (5), so 10 and 2 hold the worst case near 60 client
connections.

These are a starting point chosen from arithmetic, not from a measurement.
Raise the sync number if the backlog stops clearing inside a 10-minute cycle,
and watch Supabase's pooler connection count while you do.

### Error reporting webhook

Off by default. Set `ERROR_REPORTING_WEBHOOK_URL` to a private https endpoint
(anything that accepts a POST: ntfy, a Discord or Slack hook, a Worker of
your own) and the events below are pushed as JSON instead of waiting in the
logs for someone to look.

```bash
npx wrangler secret put ERROR_REPORTING_WEBHOOK_URL
```

What is sent, and only this: `source`, `service`, `version`, `environment`,
`severity`, `event`, a message truncated to 500 characters, an `occurred_at`
timestamp, and a flat `context` of identifiers and counts, each value
truncated to 200 characters and capped at 20 keys.

| Event | Fires when |
|---|---|
| `UNHANDLED_REQUEST_ERROR` | any 5xx leaves the API (method, path, status, error code — never the query string) |
| `DLQ_MESSAGE` | a batch of dead letters was consumed; one report per batch, with a count |
| `QUEUE_ALERT_THRESHOLD_BREACH` | a queue batch breached a retry/poison/final-retry threshold |
| `SCHEDULED_JOB_CLAIM_FAILED` | the tick could not read the job schedule |
| `SCHEDULED_JOB_ENQUEUE_FAILED` | a claimed nightly job could not be enqueued |
| `STALE_PENDING_EXTRACTION` | mail is stuck before extraction; at most hourly |
| `STALE_PENDING_EXTRACTION_SNAPSHOT_STALE` | pg_cron stopped refreshing the view; at most hourly |
| `GOOGLE_TEST_USER_CAP_WARNING` | the test-user count reached 80; at most daily |

Guarantees worth knowing before pointing this at a third-party sink:

- **The console line is always written first**, whatever the webhook does.
  The sink is a convenience, never the record.
- Delivery never throws, never retries, and times out after 3 seconds
  (`ERROR_REPORTING_TIMEOUT_MS`). A failed delivery logs
  `ERROR_REPORT_DELIVERY_FAILED` and nothing else — reporting a failed report
  would recurse.
- At most 10 reports per minute per isolate
  (`ERROR_REPORTING_MAX_PER_MINUTE`); the rest are counted and logged as
  `ERROR_REPORT_SUPPRESSED`. Isolates are many and short-lived, so this is a
  brake on a storm, not a global rate limit.
- Only https is accepted, plus http on localhost for `wrangler dev`. A bad
  URL logs `ERROR_REPORTING_URL_INVALID` once and disables delivery.
- `ERROR_REPORTING_WEBHOOK_TOKEN` is sent as `Authorization: Bearer` if set.
- Reports from the HTTP path have no `ExecutionContext` to keep the isolate
  alive, so they race the response and can be dropped. Queue and scheduled
  reports use `waitUntil` and do not.

A postgres error message can quote a constraint name or a failing value, so
treat the sink as capable of receiving fragments of user data and keep it
private. `CONTRACT.md` lists it among the outbound data paths for that
reason.

### Backups: PITR and the restore drill

Not yet enabled. At the current data size this is a checkbox, and it is
cheapest to decide before the data grows.

1. Enable **PITR** on the Supabase project (paid tiers; daily backups alone
   restore to a day boundary, which for financial data is a bad worst case).
2. Do the drill once, and write the numbers down: restore to a *new*
   project, check its schema, and record how long the restore took end to
   end.

```bash
SUPABASE_POOLER_URL="postgres://USER:PASSWORD@RESTORED-HOST:6543/postgres" npm run db:status
```

   **Set that variable inline, on the command itself.** `scripts/db-migrate.mjs`
   reads the environment first and falls back to `backend/.dev.vars` — which
   points at production. A forgotten variable is not an error, it is a silent
   run against the live database, and it has happened here before.
3. Never point the Worker at the restored project. The drill is to prove the
   restore works and to know its duration, not to fail over.

Restore granularity is the whole database. Per-user "undelete" is served by
the soft-delete design (`user_deleted_at`, REVERSED events), not by backups —
a restore to undo one user's mistake would roll back everyone.

## Android app

`frontend/android` is a Capacitor shell that loads the deployed site
(`server.url` in `frontend/capacitor.config.ts`). A deploy updates the app on
next launch; rebuild the APK only for native changes (icon, permissions,
plugins, the site URL):

```bash
npm run android:apk   # -> frontend/android/app/build/outputs/apk/release/app-release.apk
```

Needs JDK 21 and the Android SDK (`sdk.dir` in the untracked
`frontend/android/local.properties`).

- **Release key.** `~/.money-manager-android/release.jks`, with its passwords
  in `keystore.properties` next to it. Never in the repo — it is public.
  Back the folder up off this laptop: without the same key, a new APK will
  not install over the old one (uninstalling first works; data is
  server-side).
- **`frontend/public/.well-known/assetlinks.json`** carries that key's
  SHA-256. It is what lets Android hand Google's Gmail consent redirect
  (`/oauth/google/callback`) to the app instead of Chrome. A new key means a
  new fingerprint here, deployed **before** installing, because Android
  verifies the link at install time. To re-check on a device:
  `adb shell pm get-app-links com.vasu.moneymanager` (re-run with
  `pm verify-app-links --re-verify com.vasu.moneymanager`).
- If verification fails, the redirect opens in Chrome, which shows a
  "Return to the app" button (custom scheme `com.vasu.moneymanager://`).
- Google blocks its sign-in page inside the app's web view, so the app hides
  Clerk's "Continue with Google"; sign-in there is by email code.
