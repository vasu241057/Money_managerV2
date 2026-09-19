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

## Applying migrations (tracked runner)

Migrations are applied with `scripts/db-migrate.mjs`, which records every
applied file in `public.schema_migrations` (filename, sha256 checksum,
applied_at, applied_by). Each file runs in one psql transaction together with
its bookkeeping row and a guarded call to `public.assert_tenancy_backstops()`
(a no-op until that function exists), so a migration that breaks a tenancy
invariant rolls back instead of shipping. The connection string is read from
`SUPABASE_POOLER_URL` (environment, else `backend/.dev.vars`) and is never
printed. `psql` must be on the PATH.

```bash
npm run db:status                                   # read-only: applied / pending / drift
npm run db:migrate                                  # apply every pending file, in order
npm run db:migrate -- --to <filename> --dry-run     # show what would run, up to one file
npm run db:baseline -- --through <filename>         # one-time: record hand-applied files
```

One-time bootstrap on an existing database: run `db:baseline --through` with
the last file that was applied by hand. `baseline` refuses to run when the
table already has rows (`--force` adds only missing rows). `db:status` exits
3 when the bookkeeping table is missing, 2 on checksum drift (an applied file
changed on disk; `apply` refuses until `--force`), else 0.

Rules the runner enforces: files must not contain their own `begin;` /
`commit;` (the runner owns the transaction), and the older seed files are no
longer re-runnable by hand once ownership-aware uniqueness lands (see
`docs/MULTITENANT_TASKS.md`).

Every migration is also loaded on an in-memory Postgres (PGlite) by the
backend tenancy suite (`npm run test:tenancy`), which proves the schema's
cross-user backstops with two seeded tenants before anything reaches Supabase.

## Roles, row level security and the backstop assert

Since `20260918090000_multitenant_identity_hardening.sql` every table in
`public` has row level security enabled and one permissive policy,
`app_backend_all`, for the `app_backend` role: the identity the Worker
connects as (`SUPABASE_POOLER_URL` user `app_backend.<project-ref>`; the
password is set once by the operator with `alter role`, never in a file).
Migrations run as `postgres`, which owns every object and is therefore never
restricted by RLS. The Supabase Data API roles (`anon`, `authenticated`) hold
no privileges on any relation or function in `public`, and the Data API is
switched off in the dashboard.

## Merchant ownership

`global_merchants.owner_user_id` and `global_merchant_aliases.owner_user_id`
(migration `20260918120000_owned_merchants.sql`) separate shared brands
(owner null) from private payees (owner set, type `P2P`). Uniqueness is per
scope through partial unique indexes: shared writes use
`on conflict (canonical_name) where owner_user_id is null` and
`on conflict (search_key) where owner_user_id is null`. Triggers keep an
alias's owner equal to its merchant's, carry an owner change on the merchant
to its aliases, and refuse a `merchant_id` owned by another user on
`transactions`, `user_merchant_rules`, `user_merchant_category_rules` and
`recurring_series`. `merchant_ownership_migration_log` records what the
migration did to each pre-existing P2P row (reversal SQL in
`docs/MULTITENANT_TASKS.md`, Task 4a runbook).

A migration that creates a table must call
`select public.grant_app_backend('public.<table>');` (RLS + policy + CRUD
grant + Data API revoke). `public.assert_tenancy_backstops()` raises, listing
every violation, when any table lacks that or when `anon`/`authenticated`
can touch any relation or execute any function in `public`; the runner
appends it to every file so an unhardened table rolls the whole migration
back, and the PGlite suite runs it after loading. Views, sequences and
functions created by `postgres` receive `app_backend`-only privileges through
default privileges, so only tables need the explicit call.

## Body retention and account deletion

Since `20260918200000_account_lifecycle_and_retention.sql`:

- `raw_emails.body_purged_at` marks rows whose body the nightly
  `raw_email_body_purge` job blanked (`PROCESSED | IGNORED | UNRECOGNIZED`
  rows older than `RAW_EMAIL_BODY_RETENTION_DAYS`, default 90; `FAILED` rows
  keep their body). The job is seeded **disabled** at 03:30 IST in
  `job_schedules` and is switched on from the admin dashboard. Any query that
  identifies a sender-gated row by `status = 'IGNORED' and clean_text = ''`
  must also require `body_purged_at is null`.
- `user_deletion_log` is the PII-free tombstone `DELETE /account` writes
  after the `users` row is gone. It keeps the opaque provider subject only
  until the Clerk deletion succeeds; the purge job retries pending ones.
- Deleting a `users` row cascades every user-owned table. One two-hop child,
  `user_merchant_rule_categories` (reached through `user_merchant_rules`,
  `category_id` FK `NO ACTION`), fails that cascade unless its rows are
  removed first; the account service does so inside the same transaction. A
  manual `delete from public.users` must do the same.

## Recommended execution order (fresh database)

1. `npm run db:migrate` (the seed migrations already carry everything in
   `seed.sql`; that file is kept for reference only and is no longer
   re-runnable once `20260918120000_owned_merchants.sql` is applied, because
   its `on conflict (canonical_name)` no longer infers a unique index)
2. Refresh alerting MV when needed:
   - `select public.refresh_mv_stale_pending_extraction();`
   - (Normally automatic via pg_cron schedule `refresh-stale-mv`)

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

The checked-in catalogue contains 600 additional India-focused merchants and
services plus verified mappings for current branded merchants. The newest 100
rows are tagged as batch `india-next-100-2026-08-batch-3` and include selection
rank, evidence URLs, transaction relevance, actual-byte dimensions and hashes,
and identity-verification provenance. Private P2P names and local businesses
without a trustworthy official source are omitted.

To rebuild the catalogue and temporary upload assets:

```bash
npm pack simple-icons@16.27.0 --pack-destination /tmp/money-manager-simple-icons
tar -xzf /tmp/money-manager-simple-icons/simple-icons-16.27.0.tgz \
  -C /tmp/money-manager-simple-icons
node scripts/build-merchant-logo-catalog.mjs
node scripts/fetch-official-merchant-logos.mjs
npm run logos:expand-next-100
npm run logos:validate-next-100
npm run logos:expand-next-100-batch-2
npm run logos:validate-next-100-batch-2
npm run logos:expand-next-100-batch-3
npm run logos:validate-next-100-batch-3
```

The expansion report at `merchant-logo-next-100-report.json` records selected,
verified backup, and rejected candidates, supporting evidence, source pages,
and asset hashes.
The expansion script checks the full existing catalogue before selecting a
candidate, verifies downloaded image bytes, rejects weak identity matches and
duplicate assets, and requires exactly 100 valid rows before writing output.

The second expansion uses a 164-candidate ranked pool. Its report is
`merchant-logo-next-100-batch-2-report.json`, and its complete visual review
sheet is `merchant-logo-next-100-batch-2-contact-sheet.png`. Re-running the
batch preserves hash-verified cached assets and only reacquires missing or
explicitly corrected artwork.

The third expansion re-audits the 565-row catalogue and uses a conflict-free
153-candidate pool. Its report and corrected visual review sheet are
`merchant-logo-next-100-batch-3-report.json` and
`merchant-logo-next-100-batch-3-contact-sheet.png`. Verified reserves from the
second report are hash-checked and reused; pinned identity or visual-review
corrections explicitly invalidate weaker cached choices.

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

For the second forward-only batch, apply
`20260809120000_seed_next_100_merchant_logo_catalog_batch_2.sql`, validate, and
then upload only its assets:

```bash
npm run logos:validate-next-100-batch-2
npm run logos:upload-next-100-batch-2
```

For the third forward-only batch, apply
`20260809130000_seed_next_100_merchant_logo_catalog_batch_3.sql`, validate, and
then upload only its assets:

```bash
npm run logos:validate-next-100-batch-3
npm run logos:upload-next-100-batch-3
```

For the fourth forward-only batch, apply
`20260809140000_seed_next_100_merchant_logo_catalog_batch_4.sql`, validate, and
then upload only its assets:

```bash
npm run logos:validate-next-100-batch-4
npm run logos:upload-next-100-batch-4
```

For the fifth forward-only batch (the next 100 beyond the prior 700 additions),
apply `20260809150000_seed_next_100_merchant_logo_catalog_batch_5.sql`,
validate, and then upload only its verified assets:

```bash
npm run logos:validate-next-100-batch-5
npm run logos:upload-next-100-batch-5
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
