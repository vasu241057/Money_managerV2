# Supabase DB Baseline (Milestone 1)

This folder contains the canonical database setup for Money Manager.

## Files

- `migrations/20260308170000_baseline_schema.sql`
  - Core tables, triggers, indexes
  - `user_credit_cards` mapping model (first4 + last4 + label)
  - `transactions.credit_card_id` linkage
  - Alerting materialized view: `mv_stale_pending_extraction`
  - `pg_cron` job: refreshes stale MV every 10 minutes
- `migrations/20260308171000_seed_system_data.sql`
  - Deterministic idempotent system categories + merchant seed
- `migrations/20260308172000_schema_hardening.sql`
  - Enforces `transactions.financial_event_id` as `NOT NULL`
  - Enforces same-user composite FK coupling across ingestion/projection tables
  - Adds DB triggers to enforce same-user/system-category scope guards
- `seed.sql`
  - Deterministic idempotent seed data for system categories and initial merchant aliases
- `queries/stale_pending_extraction_alert.sql`
  - Operational query for alerting systems
- `queries/credit_card_monthly_spend.sql`
  - Analytics query for per-card spend breakdown

## Recommended execution order

1. Run migration SQL in `migrations/`
2. Run `seed.sql`
3. Refresh alerting MV when needed:
   - `select public.refresh_mv_stale_pending_extraction();`
   - (Normally automatic via pg_cron schedule `refresh-stale-mv`)

## Supabase CLI workflow (example)

```bash
supabase db push
psql "$SUPABASE_DB_URL" -f supabase/seed.sql
```

## Determinism guarantee

- Seed rows use fixed UUIDs for canonical system categories on fresh installs.
- Merchant default-category mapping resolves by canonical UUID with natural-key fallback.
- Seed is idempotent via conflict-safe inserts/updates (`ON CONFLICT` + guarded inserts).
