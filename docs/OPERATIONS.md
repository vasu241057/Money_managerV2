# Milestone 12 Operations Runbook

This runbook defines queue dashboards, alert thresholds, and SLO targets for the email ingestion pipeline.

## 1. Queue Dashboards

Create two Cloudflare Queue dashboards (one per queue):

- `money-manager-email-sync`
- `money-manager-ai-classification`

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
2. `AI queue backlog`:
   - page when backlog > `1,000` for 30 minutes
3. `raw_emails pending extraction`:
   - page when count > `500` for 20 minutes
4. `stale pending extraction MV`:
   - page when `stale_count > 0` for 30 minutes

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
