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

The checked-in catalogue contains 400 additional India-focused merchants and
services plus verified mappings for current branded merchants. The latest 100
rows are tagged as batch `india-next-100-2026-08` and include selection rank,
evidence URLs, and acquisition provenance. Private P2P names and local
businesses without a trustworthy official source are omitted.

To rebuild the catalogue and temporary upload assets:

```bash
npm pack simple-icons@16.27.0 --pack-destination /tmp/money-manager-simple-icons
tar -xzf /tmp/money-manager-simple-icons/simple-icons-16.27.0.tgz \
  -C /tmp/money-manager-simple-icons
node scripts/build-merchant-logo-catalog.mjs
node scripts/fetch-official-merchant-logos.mjs
npm run logos:expand-next-100
npm run logos:validate-next-100
```

The expansion report at `merchant-logo-next-100-report.json` records selected,
verified backup, and rejected candidates, supporting evidence, source pages,
and asset hashes.
The expansion script checks the full existing catalogue before selecting a
candidate, verifies downloaded image bytes, rejects weak identity matches and
duplicate assets, and requires exactly 100 valid rows before writing output.

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

To upload only the latest verified batch after applying
`20260809100000_seed_next_100_merchant_logo_catalog.sql`, run:

```bash
npm run logos:upload-next-100
```

The uploader verifies each local file against its checked-in SHA-256 hash
before any storage or database write.

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

### Live unmapped-merchant logo queue

The manual research queue contains only global merchants of type `MERCHANT`
that have no active public logo. It intentionally excludes people, banks, and
transfer labels. A merchant with no trustworthy logo is counted at most twice,
so repeated manual runs do not keep spending effort on the same small business.

After applying `20260809110000_cap_unmapped_merchant_logo_lookups.sql`, inspect
the eligible queue without consuming an attempt:

```bash
npm run logos:list-unmapped
```

Use the scan command only when you are actually going to research the listed
businesses. It records one attempt per returned merchant, and rows disappear
after their second unresolved attempt:

```bash
npm run logos:scan-unmapped
```

Four high-confidence matches from the current live audit are in
`20260809111000_seed_live_unmapped_merchant_logos.sql`. Apply that migration,
then upload just that batch with `npm run logos:upload-live-unmapped`. The
Amazon Pay on Delivery descriptor is an alias of the existing Amazon Pay logo;
it does not create a duplicate asset. Resetting a capped merchant is deliberate
and must be done directly in `merchant_logo_lookup_attempts` after new evidence
appears.
