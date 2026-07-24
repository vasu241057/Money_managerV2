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

## Merchant logo catalogue

Merchant artwork is stored separately from `global_merchants`:

- `merchant_logos` contains verified asset metadata.
- `merchant_logo_aliases` maps normalized brand names to artwork.
- Catalogue rows never create speculative global merchants.
- A current or future global merchant automatically gains artwork when its
  canonical name matches a catalogue alias.
- Files live in the public `merchant-logos` Supabase Storage bucket; PostgreSQL
  stores only paths, URLs, source provenance, hashes, and brand colors.

The checked-in catalogue contains 300 additional India-focused merchants and
services plus verified mappings for current branded merchants. Private P2P
names and local businesses without a trustworthy official source are omitted.

To rebuild the catalogue and temporary upload assets:

```bash
npm pack simple-icons@16.27.0 --pack-destination /tmp/money-manager-simple-icons
tar -xzf /tmp/money-manager-simple-icons/simple-icons-16.27.0.tgz \
  -C /tmp/money-manager-simple-icons
node scripts/build-merchant-logo-catalog.mjs
node scripts/fetch-official-merchant-logos.mjs
```

Storage writes require an elevated server-side API key. Prefer a current
Supabase secret key and keep it local in `backend/.dev.vars` as
`SUPABASE_SECRET_KEY`. A legacy `SUPABASE_SERVICE_ROLE_KEY` is also supported.
Never commit or expose either key. After applying the merchant-logo
migrations, upload with:

```bash
set -a
source backend/.dev.vars
set +a
node scripts/upload-merchant-logos.mjs
```

To fetch and upload verified artwork for branded merchants observed in the
current global/user-rule data (including Blinkit, Instamart, Nykaa, Zepto,
Bistro, Ownly, and Akbar Travels), apply the pending migration and run:

```bash
psql "$(grep '^SUPABASE_POOLER_URL' backend/.dev.vars | cut -d= -f2- | tr -d '"')" \
  -v ON_ERROR_STOP=1 \
  --single-transaction \
  -f supabase/migrations/20260724102000_expand_current_merchant_logos.sql
npm run logos:sync-current
```

The targeted sync validates the actual image bytes instead of trusting file
extensions or HTTP content types, rejects HTML/anti-bot responses, uploads only
successful assets, upserts aliases, and prints the current global merchants
that matched. It never inserts rows into `global_merchants`.
