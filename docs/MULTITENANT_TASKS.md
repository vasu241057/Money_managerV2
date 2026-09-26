# Multi-Tenant Rollout — Design Review and Task Plan

> Status: **All eight tasks implemented (Tasks 7 and 8 completed September
> 19, 2026). Task 8's code is done; its largest item — Google verification
> and CASA — is operator work that has not been started.**
> Task 1 delivered: `scripts/db-migrate.mjs` (+ `db:status`, `db:migrate`,
> `db:baseline`), `backend/test/tenancy/` (PGlite harness, adapter, two-tenant
> fixture, snapshots: constraints, service matrix, route enrollment, runner
> planning). Findings: no cross-tenant path in any enrolled service; the four
> DB backstop gaps in A.1 §6 were confirmed and are now closed by Task 2.
> Incident (Task 1): a `status` run during verification created the empty
> `public.schema_migrations` table in production; `status` is now strictly
> read-only and the table is exactly what `baseline` needs, so nothing to undo.
> Task 2 delivered: migration `20260918090000_multitenant_identity_hardening.sql`
> (identity columns, composite FK, category scope triggers, `app_backend`
> role, RLS + blanket policies, Data API revocation, `grant_app_backend()`,
> `assert_tenancy_backstops()`), auth upsert writing `auth_subject`, tenancy
> suite now 7 files / 84 tests. Baseline recorded (34 files) and the Task 2
> migration applied to production on September 18, 2026 (runbook steps 1–3).
> Steps 4–7 (deploy, role password, pooler secret switch, Data API off) are
> Vasu's to run.
> Task 4a delivered: migration `20260918120000_owned_merchants.sql`
> (owner_user_id on merchants and aliases, per-scope partial unique indexes,
> logged data migration for existing P2P rows, same-user-or-shared merchant
> triggers), owner filters on every merchant read and write, mapper deferring
> P2P persistence to 4b; tenancy suite 9 files / 108 tests. Applied to
> production by Vasu on September 18, 2026 by pasting the file into the SQL
> editor (assert returned); bookkeeping row recorded via `db:baseline --force`.
> Task 4b delivered: migration `20260918150000_merchant_confirmations.sql`
> (per-user verdicts with backfill, `merchant_mapping_attempts.last_resolution`
> + `person_display_name`), review voting against
> `MERCHANT_CONFIRMATION_THRESHOLD` (default 1), mapper per-user P2P fan-out
> and known-person fan-out without a model call; tenancy suite 12 files /
> 120 tests. **Not yet applied to production** (runbook below).
> Task 3 delivered: migration `20260918170000_email_templates_provenance.sql`
> (scope + owner, per-scope uniqueness, legacy attribution), `{{S}}`
> salutation marker, zero-occurrence FIXED-merchant refusal, PRIVATE→SHARED
> promotion on independent induction, per-partition caps; tenancy suite 14
> files / 130 tests, backend 58 / 562. **Not yet applied to production**
> (runbook below).
> Task 5 delivered: migration `20260918200000_account_lifecycle_and_retention.sql`
> (`raw_emails.body_purged_at` + scan index, `raw_email_body_purge` schedule
> seeded off at 03:30 IST, `user_deletion_log`), `DELETE /account` (Google
> revoke → one-transaction cascade → Clerk delete, PII-free tombstone with
> nightly retry), Google revocation on disconnect, purge-aware gated-row
> queries, missing-user poison handling in the workers, Settings → Delete
> account screen and the purge schedule card; tenancy suite 17 files / 143
> tests, backend 64 / 597. **Not yet applied to production** (runbook
> below). Deployment strategy Vasu chose: apply each
> task's migration as it lands, deploy the Worker once at the end; the
> per-task runbooks note what degrades in between.

---

## Part A — Review verdict

The proposal's foundations are right and match the code: one user = one tenant,
`user_id` on every user-owned table, composite `(id, user_id)` FKs, scope
triggers, identity only from the verified Clerk JWT, no client-supplied
`user_id` anywhere (verified: no route reads one), per-user AI batching in the
normalizer (verified: `eligibleByUser` grouping, `processMaskedBatch` takes one
user's emails). The three real leaks it names are real:

- **R1 confirmed and it is in the UI today**: `ReviewQueue.tsx` calls
  `listGlobalMerchants({ q })` for its merchant picker, and `GET /merchants`
  returns P2P rows with no owner filter. Any user can search every other user's
  payees by name.
- **R2 confirmed**: templates are global, `email_templates` has no user column,
  the 40-per-sender cap and 3-strike counters are global, and names in
  "Dear Vasu," boilerplate bake into skeletons.
- **R3 confirmed**: `reviewTransaction` flips `global_merchants.confirmed` for
  everyone on one user's verdict.

### A.1 Changes to the recommended design (flaws found)

1. **Template PII scrub (§5.1) is unreliable as specified — replaced.**
   The proposal scrubs spans matching "the learning user's profile signals
   (name parts from the Clerk token, email local-part)". The token carries no
   name claim today, and the name a bank prints ("VASU KHANDELWAL", "VASU K")
   rarely equals a profile name, so name-bearing skeletons would pass the
   scrub and its refuse-to-learn fallback. It also misses the other PII channel:
   a `FIXED` merchant slot stores the AI-normalized payee name (a person's name
   for UPI mail) inside `slot_map`, outside the skeleton.
   **Replacement (structural, no heuristics on names):**
   - Templates get `learned_from_user_id` and `scope in ('PRIVATE','SHARED')`.
     A template is `PRIVATE` to its learner until a *different* user's email
     independently induces the same `(from_address, skeleton_hash)` with an
     identical `slot_map`; that collision is proof the skeleton contains
     nothing user-specific, and it promotes the row to `SHARED`. A `PRIVATE`
     template is only ever applied to its owner's mail, so it can never leak.
   - A salutation marker `{{S}}` wildcards the greeting span ("Dear X," / "Hi X,")
     so most bank formats promote on the second user instead of the third.
   - `FIXED` merchant templates are learned only when the name matches a shared
     (non-owned) `global_merchants` row; a person name never becomes a FIXED
     slot. Promotion additionally requires `slot_map` equality — this also
     closes a latent single-user bug: today two formats with the same skeleton
     but different FIXED names collapse into the first one's slot_map via
     `on conflict do nothing`.
   - Cost model preserved: one format costs at most two AI calls across the
     whole user base (first two users), not one per user.
   - **No wipe-and-relearn.** All existing templates are backfilled as
     `PRIVATE` owned by Vasu; his pipeline matches exactly as today.
   - Per-sender cap becomes per `(from_address, scope, owner)` partition.
2. **P2P → `user_merchant_rules` (§5.2) does not fit the code — replaced.**
   `user_merchant_rules` has no display-name column, the feed joins
   `transactions.merchant_id → global_merchants` for the name, and the frontend
   renders person artwork and the "Payee" label from `merchantType === 'P2P'`
   (`TransactionArtwork.tsx`, `TransactionEditor.tsx`). Moving P2P out of the
   merchant table breaks all three.
   **Replacement (the `categories` pattern the proposal itself praises):**
   `global_merchants.owner_user_id uuid null` (null = shared brand) and
   `global_merchant_aliases.owner_user_id uuid null`; uniqueness becomes
   `(owner, canonical_name)` and `(search_key, owner)`. The mapper writes P2P
   results as one owned merchant + owned alias **per user who has events with
   that key**; brands stay shared. Every read that touches merchants filters
   `owner_user_id is null or owner_user_id = :user` (search, identity lookup,
   relink, review probation, category rules). Existing P2P rows keep their ids
   and become owned by the user whose transactions reference them, so nothing
   in Vasu's feed changes.
3. **`users.auth_subject` needs no Clerk backfill.** The `sub` is in every
   verified token; the auth upsert writes it on the next request. It must exist
   before account deletion (Task 5) because deleting the Clerk user requires the
   Clerk user id, which today is hashed away.
4. **RLS mechanics.** Supabase's `postgres` role cannot reliably grant
   `BYPASSRLS`, so the backend role gets blanket `for all ... using (true) with
   check (true)` policies instead. Because `postgres` owns every table, enabling
   RLS is a no-op for the current connection — the migration is safe to apply
   before the role switch, and the role switch is a secret change with instant
   rollback. Disabling the Data API is a dashboard action, not a migration.
   Migrations keep running as `postgres`; the app runs as `app_backend`.
5. **Quotas must be enforced inside the fetcher and normalizer, not only at
   dispatch.** The expensive case is a new user's initial lookback fetch; the
   fetcher must stop paginating at the cap while keeping the cursor, and the
   normalizer must leave over-cap emails `PENDING_EXTRACTION`. That needs a
   self-healing re-enqueue of stale PENDING rows per user (which also fixes
   today's gap: a lost `NORMALIZE_RAW_EMAILS` job leaves rows pending forever
   unless someone calls the internal requeue endpoint).
6. **Missing DB backstops the proposal did not list.** `card_statements.credit_card_id`
   is a plain FK (not composite with `user_id`); `user_category_budget_rules.category_id`,
   `user_hidden_categories.category_id`, `recurring_series.category_id` have no
   same-user-or-system trigger. Services validate ownership today; the DB does
   not. All additive, all pre-checked before enforcement.
7. **Migration bookkeeping must come first, not mid-Phase-1.** Migrations are
   applied by hand with `psql -f`, one provenance migration is unsafe to rerun,
   and every task below ships a migration. A tracked runner is the single
   cheapest protection for Vasu's data.
8. **Rate limiting belongs in code.** Cloudflare Workers' Rate Limiting binding
   (`ratelimits` in `wrangler.jsonc`) keys per user after auth and per IP before
   it; no dashboard rules to drift.
9. **Account deletion details.** `DELETE /oauth/google/connection` clears tokens
   but never revokes at Google; deletion must revoke, delete PRIVATE templates,
   cascade the user row, delete the Clerk user, and write a PII-free tombstone.
10. **`sender_rules.source = 'USER'` is global.** No user-facing endpoint writes it
    today; keep it admin-only and say so in CONTRACT.md. A per-user allowlist, if
    ever wanted, is a new user-owned table.

Everything else in the proposal stands: no `tenants` table, sender rules and
brand merchants global, per-user confirmations table, deny-all RLS +
non-superuser role, per-user quotas, 90-day body retention (Vasu's number),
Hyperdrive and queue split only when scale demands, Google verification as a
calendar item.

### A.2 Second-pass corrections to my own design (self-review, same day)

The task specs in Part C already include these fixes.

1. **Promotion would never fire.** `slot_map` carries `confidence` (the AI's
   learn-time confidence), which differs between users' inductions, so
   "identical slot_map" would never be true. Fix: compare a *structural*
   slot_map (`kind, direction, payment_method, amount_ordinal, date_pattern,
   instrument_ordinal, merchant`), never `confidence`; the SHARED row keeps the
   higher confidence.
2. **Uniqueness on `(from_address, skeleton_hash)` starves the second user.**
   If user B's induction has the same skeleton but a different structural
   slot_map, the guarded upsert writes nothing and B never gets a template.
   Fix: uniqueness is per owner — `(from_address, skeleton_hash,
   coalesce(learned_from_user_id, zero-uuid))` — so every learner gets their
   PRIVATE row; promotion is a separate step inside the same transaction:
   after inserting B's PRIVATE row, look for another owner's PRIVATE row with
   the same hash and equal structural slot_map; if found, insert one SHARED row
   (owner null, hit counts summed) and delete the matching PRIVATE rows.
3. **SHARED rows carry no owner.** `learned_from_user_id` is `on delete
   cascade` (PRIVATE rows die with their user, no orphaned PII) and is set to
   null on promotion. Provenance for shared templates is deliberately "learned
   independently by two or more users".
4. **Writes that accept a `merchant_id` were unguarded.** Review, transaction
   PATCH/create, recurring-series confirm and rule writers must reject a
   merchant owned by another user, and the DB must back that up with a
   same-user-or-shared trigger on `transactions.merchant_id`,
   `user_merchant_rules.merchant_id`, `user_merchant_category_rules.merchant_id`
   and `recurring_series.merchant_id` (the category-scope trigger pattern).
5. **Expression uniqueness breaks `on conflict` inference.** The seed and the
   mapper use `on conflict (canonical_name)` and `on conflict (search_key)`;
   a `coalesce(owner, …)` index would not be inferred. Fix: partial unique
   indexes — shared rows `unique (canonical_name) where owner_user_id is null`
   and `unique (search_key) where owner_user_id is null`; owned rows
   `unique (owner_user_id, canonical_name)` / `unique (search_key,
   owner_user_id) where owner_user_id is not null`. Code uses
   `on conflict (…) where owner_user_id is null` for shared writes. Old seed
   files are no longer re-runnable after this; the runner is the only path.
6. **RLS footgun.** With RLS on and the app running as `app_backend`, any new
   table that forgets its blanket policy makes the app read *zero rows*
   silently. Fix: (a) `public.grant_app_backend(regclass)` helper every
   migration calls for new tables; (b) `public.assert_tenancy_backstops()`
   raises when any `public` table lacks RLS, lacks the `app_backend` policy,
   or still grants `anon`/`authenticated`; (c) the runner appends a call to
   that function to every migration it applies (no-op until Task 2 creates
   it), so a missing policy rolls the migration back instead of shipping;
   (d) `alter default privileges for role postgres in schema public revoke all
   on tables from anon, authenticated` so future tables are never exposed
   through the Data API even before the assert runs. The runner enables RLS
   on its own `schema_migrations` table.
7. **Legacy template backfill must be conditional.** Ownerless legacy rows are
   assigned to the admin UUID only if that user exists; otherwise (staging
   restore) they are deleted — a PRIVATE row without an owner is dead weight.
   Optional add-on for Task 3: an internal ops endpoint that re-applies the
   `{{S}}` pass to existing PRIVATE skeletons, re-verifies against the source
   email when it still exists, and rewrites skeleton + hash, so Vasu's legacy
   templates can promote too. Cost convergence only; not needed for safety.
8. **Self-heal requeue would churn under a quota.** Bound it: at most once per
   hour per user, skipped while the user is over the AI cap.
9. **Do not double-count AI usage.** `ai_usage_events.user_id` already ledgers
   normalization calls per user with an index on `(user_id, created_at)`; the
   AI cap reads that. `user_usage_daily` only holds what nothing else records:
   messages listed and bodies fetched (two caps; bodies is the AI-bound one).
   Day boundaries use `Asia/Kolkata` like every other daily job.
10. **Deletion races.** A sync or normalize job in flight for a deleted user
    must ack as poison (user gone), not retry into the DLQ. Before the cascade,
    delete `merchant_mapping_attempts` rows whose search keys appear only in
    the deleted user's `financial_events`. A nightly retry covers Clerk
    deletions that failed (`user_deletion_log.clerk_deleted = false`); a user
    who signs in before the retry gets a fresh empty account under the same
    UUID, which is acceptable and logged.
11. **Task 4 is the riskiest and has a natural seam.** Recommend two sessions:
    4a = ownership schema + data migration + every read/write owner filter +
    triggers + `GET /merchants` filter, with the mapper temporarily *not
    persisting* P2P results (records the attempt only); 4b = mapper per-user
    fan-out + `merchant_confirmations` voting. 4a alone stops the leak.
12. **Admin sees everyone's payee names** in the mapping tab of the AI usage
    dashboard. Acceptable while admin = Vasu; must be stated in the privacy
    policy; mask person names there if a second operator ever exists.
13. **Workers rate limiting is per-colo**, not global. Fine as abuse control;
    document the caveat.
14. **Harness shims.** PGlite also needs `create extension pgcrypto` skipped
    (`gen_random_uuid` is core), the `anon`/`authenticated`/`service_role`
    roles created before migrations load (Task 2 revokes from them), and
    `set role anon` is how RLS is exercised since PGlite runs as superuser.
    The harness calls `assert_tenancy_backstops()` after loading, like the
    runner. When Docker is available, `supabase start` is the faithful
    pre-production rehearsal (real storage schema, pg_cron); PGlite is the
    CI-speed proof.

### A.3 Third-pass corrections

1. **Requiring `CLERK_JWT_AUDIENCE` would lock everyone out.** The frontend
   calls `getToken()` with no JWT template, so tokens are Clerk's default
   session tokens, which carry `azp` (authorized party = frontend origin) and
   no `aud`. Task 7 instead validates `azp` against a new
   `CLERK_AUTHORIZED_PARTIES` list (comma-separated origins), which is Clerk's
   recommended cross-app protection; `aud` stays optional. The secret is set
   before the deploy that enforces it.
2. **Simpler and stronger FIXED-merchant guard, no DB lookup.** A merchant slot
   is FIXED when the AI's counterparty text occurs zero times or two-plus times
   in the body. Zero occurrences means the name came from the subject or the
   model's normalization, and the skeleton has no anchor for it: the template
   would stamp that payee onto every future email of that body shape. That is a
   latent single-user extraction bug today, and it is also the only way a
   person's name reaches `slot_map` without being in the skeleton. Fix: refuse
   to learn when occurrences = 0; allow FIXED when occurrences ≥ 2 (the name is
   then literal in the skeleton, so the template is inherently payee-specific).
   This replaces the "must match a shared brand" lookup and removes Task 3's
   dependency on Task 4. Promotion considers only `ACTIVE` private rows.
3. **Quota stop must persist the Gmail page token.** The continuation token
   lives only in the queue payload today. When the fetch cap stops a sync
   mid-listing, Task 6 stores `email_sync_continuation_token` (and the
   connection id) on `oauth_connections` so the next day's job resumes there
   instead of re-listing from the cursor. Re-listing would be idempotent but
   would re-spend Gmail metadata quota.
4. **Rollback is not symmetric for Tasks 3 and 4.** Old code's
   `on conflict (from_address, skeleton_hash)` and `on conflict (canonical_name)`
   stop inferring once those constraints become partial or per-owner. After a
   code rollback the API keeps working, but template *learning* (Task 3) logs
   warnings and the nightly mapper (Task 4) errors, so the runbooks say: keep
   `MERCHANT_MAPPING_ENABLED=off` during any Task 4 rollback and roll forward
   to restore learning. Matching, reads and writes are unaffected.
5. **Verified, no change needed:** the logo scripts upload through the Storage
   API with the secret key and write the database through the pooler, never
   through PostgREST, so disabling the Data API does not break them.
   `relinkTransactions` must prefer an owned alias over a shared one for the
   same key (`distinct on`), or the update-from join picks arbitrarily.

### A.4 Fourth-pass corrections

1. **The P2P data migration would fire two `transactions` triggers.**
   Repointing `merchant_id` on cloned rows (the more-than-one-user case) fires
   `auto_link_unambiguous_card_refund` (deletes and recomputes AUTO refund
   links; the target's merchant may not be repointed yet, so a link can be
   dropped) and `match_confirmed_recurring_series` (its "broken" branch can
   PAUSE a series mid-repoint). Fix: the migration runs
   `alter table public.transactions disable trigger
   auto_link_unambiguous_card_refund, disable trigger
   match_confirmed_recurring_series` around the repoint and re-enables them
   after; it repoints `recurring_series.merchant_id` before `transactions`.
   The single-owner case (Vasu today) repoints nothing, so nothing fires. The
   new same-user-or-shared merchant triggers are created *after* the data
   migration in the same file, or the interim state would violate them.
2. **The body purge would masquerade as sender gating.** Four queries
   identify gated rows by `status = 'IGNORED' and clean_text = ''`
   (sender-classifier candidates and backfill, the fetcher's backfill job,
   the retroactive gate). A purged normalizer-IGNORED row matches that shape,
   so a newly allowed sender would trigger re-downloads and re-normalization
   of already-judged mail. Fix: every one of those predicates gains
   `and body_purged_at is null`; the purge's age test falls back to
   `created_at` for rows that predate `processing_completed_at`.
3. **Promotion must scan even when the private insert was a no-op.** Two users
   inducing the same skeleton concurrently can each miss the other's
   uncommitted row; the next induction by either learner hits `on conflict do
   nothing` and must still run the other-owner scan, so the promotion
   self-heals on the next email instead of never.
4. **Person keys would be re-asked once per new user.** With owned aliases, a
   VPA already resolved as a person for user A becomes a candidate again when
   user C first pays it, costing a model call and up to a 10-day cooldown wait
   before C's payee is named. Fix: `merchant_mapping_attempts` gains
   `last_resolution` (`MERCHANT | P2P | UNSURE`) and `person_display_name`;
   the mapper fans a known P2P key out to a new user with no model call and
   no cooldown. The key already identifies the person, so this adds no new
   exposure; the row is deleted when its last referencing user is deleted
   (Task 5).
5. **Zero-occurrence guard confirmed safe against the prompt.** Both prompt
   variants instruct the model to *copy* the counterparty from the email and
   "never invent a name", so a counterparty absent from the body is either
   subject-sourced or a contract violation; refusing to learn is correct in
   both cases. The normalizer's `for update` + status check also confirms the
   self-heal requeue cannot double-process a row.
6. **Signup is not gated.** Anyone who can register in Clerk gets a
   provisioned account, but Gmail's restricted scope caps the unverified app
   at 100 test users. Until verification completes (Task 8), Clerk's
   allowlist/waitlist must mirror the Google test-user list, or user 101 hits
   an OAuth error after signing up. Operator item, listed in Task 8 and in
   Part D as D10.

---

## Part B — Non-negotiables for the existing tenant (every task obeys these)

1. **Migrations are additive-first and idempotent**, run one file per
   transaction through the runner (Task 1), and each includes pre-checks that
   `raise` on unexpected data instead of proceeding.
2. **Every migration is rehearsed on the PGlite harness** (Task 1) with a
   fixture shaped like production (one real user with transactions, templates,
   P2P merchants, rules, cards, statements) before it touches Supabase.
3. **Invariant check before and after every production migration** (SQL in the
   runbook of each task): per-user counts of `transactions`, `financial_events`,
   `raw_emails`, `email_templates` (by owner), `global_merchants`,
   `global_merchant_aliases`; every `transactions.merchant_id` still resolves;
   no transaction references a merchant owned by another user.
4. **Point-in-time safety**: take a `pg_dump` (or confirm the Supabase backup
   timestamp) before each production migration; the command is in each runbook.
5. **Identity is frozen**: the UUID namespace `2d0ec934-…` and the derivation
   never change. `users.id` never changes.
6. **User-visible behavior defaults to today's behavior** until Vasu flips it:
   body purge job ships disabled, confirmation threshold ships at 1, quota
   defaults far above his volume, template sharing changes nothing for the
   only current tenant.
7. **Rollback path per task**: code deploys roll back with `wrangler rollback`;
   secrets roll back by re-putting the previous value; additive columns can be
   left in place harmlessly. Data migrations (Task 4) record a mapping table so
   they can be reversed. Tasks 3 and 4 are roll-forward for template learning
   and the nightly mapper respectively (A.3 §4); reads and writes stay intact.
8. The uncommitted merchant-logo batch 5 files are not touched by any task.

---

## Part C — Tasks (one per session, in this order: 1, 2, 4, 3, 5, 6, 7, 8)

Dependencies: 1 → 2 → {3, 4} → 5 (needs 2) → 6 → 7 → 8. Tasks 3 and 4 are
independent of each other; 4 goes first because R1 is the worst leak.

### Task 1 — Foundations: migration runner + tenancy test harness (no production change)

Goal: make every later task provable and every migration tracked.

- `scripts/db-migrate.mjs` (+ npm `db:status`, `db:migrate`, `db:baseline`):
  creates `public.schema_migrations(filename pk, checksum, applied_at)` if
  missing; `--status` lists pending; `--apply` runs pending files in filename
  order, each in its own transaction, records checksum; `--baseline --through
  <filename>` marks files as applied without running (one-time bootstrap;
  Vasu decides whether batch 5 is already applied); refuses to run when a
  recorded file's checksum changed (prints a warning, requires `--force`).
  Uses `SUPABASE_POOLER_URL` from `backend/.dev.vars` like the README does and
  applies via `psql -v ON_ERROR_STOP=1 --single-transaction` on a temp file =
  migration contents + `select public.assert_tenancy_backstops()` (guarded
  `do` block, no-op until the function exists). `schema_migrations` is created
  with RLS enabled.
- `backend/test/tenancy/`: PGlite (`@electric-sql/pglite`, devDependency)
  loading every file in `supabase/migrations/` with a small shim (skip
  `create extension pg_cron`, stub `storage.buckets`, load pgcrypto contrib);
  an adapter exposing the `SqlClient` tagged-template shape services expect
  (template → `$n` params, `sql.begin`, arrays). Fallback if PGlite rejects a
  migration: `TENANCY_TEST_DATABASE_URL` pointing at a local Postgres, suite
  skipped when unset.
- First tests: (a) constraint proofs — cross-user inserts rejected on
  `transactions`, `financial_events`, `raw_emails`, `user_credit_cards`,
  `transaction_relationships`, category-scope triggers; (b) service matrix v1:
  users A and B with full object graphs, call `transactions`, `accounts`,
  `categories`, `credit-cards`, `budget` services as A with B's ids in every
  id-shaped slot → 404/400 and B's rows byte-identical (snapshot compare).
  Table-driven so a new route fails until enrolled.
- Docs: `supabase/README.md` migration section rewritten around the runner.

Not changed for Vasu: nothing in production. Rollback: none needed.

### Task 2 — Identity and DB hardening

Migration `…_multitenant_identity_hardening.sql`:
- `users.auth_provider text not null default 'clerk'`, `users.auth_subject text`
  (nullable), unique partial index `(auth_provider, auth_subject)`.
- Drop `users_email_key`; add non-unique `idx_users_email`.
- `card_statements`: replace FK with composite
  `(credit_card_id, user_id) → user_credit_cards(id, user_id) on delete set null (credit_card_id)`;
  pre-check mismatched rows.
- Same-user-or-system scope triggers on `user_category_budget_rules.category_id`,
  `user_hidden_categories.category_id`, `recurring_series.category_id`;
  pre-check existing rows.
- Role `app_backend` (`login`, no password in the file), `grant usage on schema
  public`, CRUD on all tables and sequences, `alter default privileges for role
  postgres in schema public grant … to app_backend` and `… revoke all on tables
  from anon, authenticated`. `revoke all` from `anon`/`authenticated` on every
  existing table. `public.grant_app_backend(regclass)` creates the blanket
  policy and enables RLS on one table; called for every existing table here and
  by every later migration for its new tables. `public.assert_tenancy_backstops()`
  raises if any `public` table lacks RLS, lacks the `app_backend` policy, or
  grants `anon`/`authenticated`; the runner calls it after every migration
  (A.2 §6).
- Comment on `users` freezing the namespace constant.

Code:
- `lib/auth.ts`: upsert writes `auth_subject = sub`, `auth_provider = 'clerk'`
  in both branches; `UserRow` gains the columns; namespace-freeze comment.
- `lib/config.ts`: nothing (pooler URL format unchanged).
- No audience change here (see A.3 §1; `azp` validation lands in Task 7).

Operator steps (runbook in the task): set `app_backend` password with `alter
role` in the SQL editor (never committed), switch `SUPABASE_POOLER_URL` to
`app_backend.<project-ref>` via `wrangler secret put`, hit `/health`, check
Supabase → Settings → API → disable Data API (or confirm `anon` gets nothing on
every table). Rollback: re-put the old pooler URL.

Tests: auth spec for `auth_subject`; harness proves `anon` is denied on every
table and `app_backend` is allowed; constraint tests for the new backstops.

Not changed for Vasu: same `users.id`; `auth_subject` fills on his next
request; email uniqueness drop is invisible.

#### Task 2 — delivered (September 18, 2026)

Files: `supabase/migrations/20260918090000_multitenant_identity_hardening.sql`;
`backend/src/lib/auth.ts` and `shared/types.ts` (`UserRow`); the runner's
bookkeeping DDL now hardens `schema_migrations` itself once the helper exists.
Tests: `backend/test/tenancy/rls.spec.ts` (Data API roles denied on every
relation and function, `app_backend` reads and writes every row with triggers
and backstops still enforced, the assert names every violation of an
unhardened table, default privileges for new objects), `identity.spec.ts`
(subject uniqueness, blank/unknown provider rejected, shared email accepted,
composite FK detaches statements on card deletion, scope triggers accept
legitimate rows), the four constraint gaps flipped from `it.fails` to `it`,
the runner spec proves an unhardened table rolls the whole file back, and the
auth spec proves `auth_subject` is written on both upsert paths.

Deviations from the spec above (all stricter, none user-visible):
- One trigger function, `public.enforce_user_category_scope()`, serves all
  three tables (the message carries `TG_TABLE_NAME`).
- The Data API roles also lose every grant on views, the materialized view,
  sequences and every non-extension function in `public` (PUBLIC execute
  revoked, `app_backend` granted), and the assert checks all of those.
- The PUBLIC-execute default revoke is global for the migration role: a
  per-schema default `REVOKE` can only reverse per-schema grants (which is
  what Supabase's `anon`/`authenticated` grants are), never Postgres's
  built-in defaults.
- `anon`/`authenticated` are resolved at run time, so the file also applies
  on a plain Postgres that lacks them.
- `users_auth_provider_check` (only `'clerk'`) and
  `users_auth_subject_nonempty_check` were added.

Runbook (production; the app keeps connecting as `postgres` until step 6,
and `postgres` owns every table, so nothing in steps 1–5 changes behaviour):

1. **Pre-flight, read-only.** `npm run db:status` must list exactly one
   pending file (this one) after the baseline from Task 1 is recorded.
   Confirm the latest Supabase backup timestamp (Database → Backups) or take
   a `pg_dump` through the *session* pooler (port 5432 on the same host; the
   transaction pooler cannot serve `pg_dump`).
2. **Invariant snapshot, before and after** (SQL editor; the two results must
   be identical):
   ```sql
   select 'users' as t, count(*) from public.users
   union all select 'transactions', count(*) from public.transactions
   union all select 'financial_events', count(*) from public.financial_events
   union all select 'raw_emails', count(*) from public.raw_emails
   union all select 'email_templates', count(*) from public.email_templates
   union all select 'global_merchants', count(*) from public.global_merchants
   union all select 'global_merchant_aliases', count(*) from public.global_merchant_aliases
   union all select 'card_statements', count(*) from public.card_statements
   union all select 'unresolved merchant refs', count(*) from public.transactions t
     where t.merchant_id is not null
       and not exists (select 1 from public.global_merchants m where m.id = t.merchant_id);
   ```
   The migration's own pre-checks (it raises and rolls back on any non-zero):
   ```sql
   select count(*) from public.card_statements cs
   join public.user_credit_cards c on c.id = cs.credit_card_id
   where c.user_id <> cs.user_id;
   select count(*) from public.user_category_budget_rules r
   join public.categories c on c.id = r.category_id
   where (c.user_id is null and c.is_system is distinct from true)
      or (c.user_id is not null and c.user_id <> r.user_id);
   -- same shape for user_hidden_categories (h) and recurring_series (s)
   ```
3. **Apply.** `npm run db:migrate`. The file ends with
   `select public.assert_tenancy_backstops();`, the runner asserts again and
   records the row, all in one transaction. Re-run the step 2 snapshot.
4. **Deploy the backend** (`npm run deploy:cloudflare`), *after* the migration
   (the new upsert writes columns the old schema lacks). Open the app once;
   then
   `select auth_provider, auth_subject from public.users where id = 'a661a921-cdb0-503b-a810-4b25e4f15ca0';`
   shows `clerk` and the Clerk user id (`user_…`).
5. **Role password** (SQL editor, never committed, never pasted into chat):
   `alter role app_backend password '<generated>';`
6. **Switch the Worker.** `cd backend && npx wrangler secret put
   SUPABASE_POOLER_URL` with
   `postgresql://app_backend.<project-ref>:<password>@<pooler-host>:6543/postgres`
   (same host and port as today; only the user changes). Check `/health`,
   open the app, run a sync; Worker logs must show no `permission denied`.
   `backend/.dev.vars` keeps the `postgres` URL: the migration runner reads
   it and must own the schema, and local dev as the owner is unaffected by
   RLS.
7. **Disable the Data API.** Supabase → Project Settings → API → Data API →
   off (or remove `public` from the exposed schemas). Optional proof:
   `curl -H "apikey: <anon key>" "https://<project-ref>.supabase.co/rest/v1/users?select=id"`
   returns an error, never rows (it already cannot after step 3, because
   `anon` holds no grants).
8. **Rollback.** Re-put the previous pooler URL (`wrangler secret put`) and
   `wrangler rollback` for the code. The columns, triggers, role and policies
   stay and are harmless: RLS never applies to `postgres`, the owner.

### Task 4 — Merchants: P2P ownership + per-user confirmations (do before Task 3)

Migration `…_owned_merchants_and_confirmations.sql`:
- `global_merchants.owner_user_id uuid null references users(id) on delete cascade`;
  drop `canonical_name` unique; partial unique indexes `(canonical_name) where
  owner_user_id is null` and `(owner_user_id, canonical_name) where
  owner_user_id is not null` (A.2 §5).
- `global_merchant_aliases.owner_user_id uuid null references users(id) on
  delete cascade`; drop the `search_key` PK; partial unique indexes
  `(search_key) where owner_user_id is null` and `(search_key, owner_user_id)
  where owner_user_id is not null`; plain index on `search_key`.
- Same-user-or-shared triggers on `transactions.merchant_id`,
  `user_merchant_rules.merchant_id`, `user_merchant_category_rules.merchant_id`,
  `recurring_series.merchant_id` (A.2 §4); pre-check existing rows.
- Data migration for existing `type = 'P2P'` merchants: for each, collect the
  distinct users referencing it (`transactions`, `user_merchant_rules`,
  `user_merchant_category_rules`, `recurring_series`); 0 users → delete
  (aliases cascade); 1 user → set owner; >1 users → first (lowest user id)
  keeps the row, others get a cloned merchant + cloned aliases and their
  references are repointed. Every alias of an owned merchant gets the same
  owner. `merchant_logo_lookup_attempts` rows for P2P merchants deleted.
  A `merchant_ownership_migration_log(merchant_id, previous_owner, new_owner,
  cloned_from)` table records what moved (reversal path). The two
  `transactions` triggers are disabled around the repoint and
  `recurring_series` is repointed first (A.4 §1). Pre/post assertions per
  Part B.3. The ownership triggers below are created after this block.
- `merchant_confirmations(merchant_id → global_merchants cascade, user_id →
  users cascade, verdict check in ('CONFIRM','DISSENT'), created_at,
  updated_at, pk (merchant_id, user_id))`. Backfill: `confirmed = true and
  source = 'AI'` → one CONFIRM row for the admin user; `dissent_count > 0` →
  one DISSENT row. `confirmed`/`dissent_count` stay as derived cache columns.

Code:
- `routes/merchants.ts` + `merchants.service.ts`: `listGlobalMerchants(sql,
  userId, query)` filters `owner is null or owner = user`; every write that
  accepts a `merchant_id` (review, transaction PATCH/create, recurring-series
  confirm, both category-rule writers) checks visibility the same way and
  returns 404 for a foreign owned merchant.
- Seam for two sessions (A.2 §11): 4a = everything above plus the mapper
  recording P2P attempts without persisting; 4b = the mapper fan-out and
  voting below.
- `lib/identity.ts`: `loadGlobalAliasesForLookup(sql, searchKeys, userId)`
  with the owner filter; an owned alias outranks a shared one on the same key.
- `workers/merchant-mapper.ts`: candidates carry `user_ids[]` per key; a key is
  a candidate when no shared alias exists and at least one of its users lacks
  an owned alias; `persistMappings` fans P2P out per user (owned merchant +
  owned alias), brands stay shared; `relinkTransactions` joins aliases with
  `owner is null or owner = t.user_id`, owned first (`distinct on`);
  `merchant_mapping_attempts.last_resolution` + `person_display_name` let a
  known person key fan out to a new user without a model call or cooldown
  (A.4 §4); preview/apply unchanged in shape.
- `services/transactions.service.ts` review: alias lookup with owner filter;
  confirm/dissent become `merchant_confirmations` upserts; `confirmed` is
  recomputed as `distinct CONFIRM users >= threshold and distinct DISSENT users
  < threshold`; threshold from env `MERCHANT_CONFIRMATION_THRESHOLD` (default
  1). Owned merchants are only ever reviewed by their owner.
- `ai-usage.service` dashboard: mapping tab shows owner (Person/own) — small.
- Frontend: no change required (search now returns brands + own payees).

Tests: harness matrix for merchants (A cannot see, rule on, or list B's
payees); mapper spec for P2P fan-out and shared brands; review spec for
voting; migration rehearsal with a two-user P2P fixture.

Not changed for Vasu: his P2P merchant ids, feed names, person artwork,
category rules, recurring series. Threshold stays 1.

#### Task 4a — delivered (September 18, 2026)

Scope per A.2 §11: ownership schema, data migration, every read/write owner
filter, DB triggers, `GET /merchants` filter; the mapper records P2P
attempts without persisting them. `merchant_confirmations` and the mapper
fan-out are Task 4b.

Files: `supabase/migrations/20260918120000_owned_merchants.sql`;
`lib/identity.ts` (`loadGlobalAliasesForLookup(sql, keys, userId)`, owned
alias outranks shared), `workers/email-ai-normalizer.ts` (passes the user),
`services/merchants.service.ts` (`assertMerchantVisibleToUser`,
`listGlobalMerchants(sql, userId, …)`, both rule writers check visibility),
`routes/merchants.ts`, `services/transactions.service.ts` (create, PATCH and
review reject a foreign merchant with 404 before writing; probation lookup
owner-scoped), `workers/merchant-mapper.ts` (candidates are keys some user
has no visible alias for; brand writes use `on conflict (…) where
owner_user_id is null`; P2P results counted in `p2p_deferred_count` and
never written; `map-apply` rejects P2P proposals with a reason; relink joins
visible aliases owned-first; category backfill shared brands only),
`services/ai-usage.service.ts` (mapping rows carry `owner_user_id`),
`shared/types.ts` (row types; `AiMerchantMapping.owner_user_id`). Frontend
untouched: the review picker now simply receives brands plus the caller's own
payees.

Tests: `backend/test/tenancy/merchant-ownership-migration.spec.ts` rehearses
the data migration on the pre-4a schema with a payee referenced by two users
(clone + repoint, lowest user id keeps the row), one referenced by one user,
one by nobody (deleted), a brand (stays shared), logo lookups dropped for
people, log rows, reactive triggers back on, idempotent re-run, and the
old-code `on conflict (canonical_name)` failure the runbook warns about.
`merchant-ownership.spec.ts` proves the same alias key resolves per user and
that search never returns another user's payee. Constraints gained seven
cases (cross-owner references on all four tables, alias owner mirror, owner
hand-over refused while others reference, own/shared accepted); the matrix
gained seven (`GET /merchants`, both rule writers, merchant transactions,
create/PATCH/review with B's payee). Unit specs updated for the new
signatures and the P2P deferral.

Schema details beyond the spec text: `global_merchant_aliases` gets a
surrogate `id` primary key (search_key is unique per scope, not globally);
`propagate_global_merchant_owner` carries an owner change on a merchant to
its aliases and refuses while other users' rows reference it (operator SQL
to re-own a mistyped person is therefore a single `update`); the log table
records `canonical_name` for readability. After this file `supabase/seed.sql`
is not re-runnable (its `on conflict (canonical_name)` no longer infers), and
neither is any Worker build older than Task 4a for mapper writes.

Runbook (production). Vasu applies migrations per task and deploys once at
the end, so step 3 matters: with this migration applied and pre-4a code
deployed, the nightly mapper's merchant upsert errors (no data damage; new
payees stay unnamed) until the deploy. Everything else in the old code keeps
working: reads carry no owner filter, and the only tenant owns every payee.

1. **Pre-flight, read-only.** `npm run db:status` lists one pending file
   (`20260918120000_owned_merchants.sql`). Confirm the Supabase backup
   timestamp or take a `pg_dump` through the session pooler.
2. **Snapshot before and after** (SQL editor). The Task 2 counts query, plus:
   ```sql
   select type, count(*), count(*) filter (where owner_user_id is not null) as owned
   from public.global_merchants group by type order by type;
   select count(*) as aliases, count(*) filter (where owner_user_id is not null) as owned
   from public.global_merchant_aliases;
   -- P2P rows by number of referencing users (single tenant: only 0 and 1)
   select referencing_users, count(*) from (
     select gm.id, count(distinct refs.user_id) as referencing_users
     from public.global_merchants gm
     left join (
       select merchant_id, user_id from public.transactions
       union select merchant_id, user_id from public.user_merchant_rules
       union select merchant_id, user_id from public.user_merchant_category_rules
       union select merchant_id, user_id from public.recurring_series
     ) refs on refs.merchant_id = gm.id
     where gm.type = 'P2P' group by gm.id
   ) x group by 1 order by 1;
   ```
   Expected after: brand and P2P counts unchanged except P2P rows with zero
   referencing users (deleted); every P2P row owned; every alias of an owned
   merchant owned; the referencing-users histogram unchanged for 1.
3. **Pause the mapper if its schedule is on** (AI usage dashboard → merchant
   mapping schedule off, or `cd backend && npx wrangler secret put
   MERCHANT_MAPPING_ENABLED` with value `off`; neither needs a deploy).
   Re-enable after the Worker deploy that carries Task 4a.
4. **Apply.** `npm run db:migrate`. The file's own invariant block and the
   backstop assert both run inside the transaction. Then:
   ```sql
   select action, count(*) from public.merchant_ownership_migration_log group by 1;
   select count(*) from public.global_merchants
   where type = 'P2P' and owner_user_id is distinct from 'a661a921-cdb0-503b-a810-4b25e4f15ca0';
   ```
   Expect OWNED = P2P rows that had references, DELETED = the rest, CLONED =
   0, and the second query 0 (every payee is yours).
5. **Deploy** whenever the end-of-plan deploy happens; the Task 4a code
   needs this schema, and this schema needs the Task 4a code for mapper
   writes. Re-enable the mapper afterwards.
6. **Rollback.** Code: `wrangler rollback` (reads keep working; keep the
   mapper paused while pre-4a code runs). Data, only if ever needed, from
   the log:
   ```sql
   -- clones: repoint back and remove
   update public.transactions t set merchant_id = l.cloned_from_merchant_id
   from public.merchant_ownership_migration_log l
   where l.action = 'CLONED' and t.merchant_id = l.merchant_id;
   -- same shape for recurring_series, user_merchant_rules, user_merchant_category_rules
   delete from public.global_merchants where id in
     (select merchant_id from public.merchant_ownership_migration_log where action = 'CLONED');
   -- originals: back to shared (the trigger carries aliases along)
   update public.global_merchants set owner_user_id = null where id in
     (select merchant_id from public.merchant_ownership_migration_log where action = 'OWNED');
   ```
   DELETED rows had no references, so nothing points at them; the log keeps
   their names.

Operator note, mistyped people: a person the mapper typed `MERCHANT` is a
shared row and therefore visible to everyone. To make it private:
`update public.global_merchants set type = 'P2P', owner_user_id = '<user>'
where id = '<merchant>';` — the trigger moves its aliases and refuses if any
other user's rows reference it.

#### Task 4b — delivered (September 18, 2026)

Files: `supabase/migrations/20260918150000_merchant_confirmations.sql`;
`services/merchants.service.ts` (`recordMerchantConfirmation`: upsert this
user's verdict, recompute `confirmed = confirms >= threshold and dissents <
threshold` and `dissent_count = dissents`), `services/transactions.service.ts`
(review CONFIRM/DISSENT go through it; `reviewTransaction` takes
`{ merchantConfirmationThreshold }`), `routes/transactions.ts` +
`lib/config.ts` (`MERCHANT_CONFIRMATION_THRESHOLD`, default 1),
`workers/merchant-mapper.ts` (candidates carry `user_ids`; a person answer is
written as one owned merchant + owned alias per paying user, a brand once
shared; `fanOutKnownPersons()` runs first on every job and gives a new payer
of an already-resolved key the payee with no model call and no cooldown;
attempts record `last_resolution` and `person_display_name`; `map-apply`
accepts person proposals and fans them out to users lacking a visible
alias; result gains `owned_alias_created_count` and `person_fanout_count`),
`workers/queue.worker.ts` (logs them), `AiUsageDashboard.tsx` (a "Private"
badge on owned mappings).

Backfill rules (rehearsed in `merchant-confirmations-migration.spec.ts`):
only AI-sourced merchants; voter = owner for a private payee, else the user
with most transactions on the merchant; `confirmed` → CONFIRM, else
`dissent_count > 0` → DISSENT; merchants nobody references get no row.
Every owned P2P alias becomes a `P2P` attempt row with the payee's name;
attempt rows behind a shared alias become `MERCHANT`.

Tests: `merchant-confirmations.spec.ts` (threshold 1 reproduces today's
behaviour through the real review service, a changed mind replaces the vote,
threshold 2 needs two confirmations, a private payee rejects a stranger's
verdict), `merchant-fanout.spec.ts` (person once per payer, brand once,
relink gives each user their own merchant, fan-out to a new payer then
stops, never past a shared alias), constraints (+1), mapper / review /
config unit specs updated.

Runbook (production; migration now, deploy at the end, per Vasu's plan):

1. **Pre-flight.** `npm run db:status` lists one pending file
   (`20260918150000_merchant_confirmations.sql`). Confirm a backup.
2. **Snapshot before and after** (SQL editor):
   ```sql
   select source, confirmed, count(*), sum(dissent_count) as dissents
   from public.global_merchants group by 1, 2 order by 1, 2;
   select count(*) from public.merchant_mapping_attempts;
   ```
   Expected after: the first result unchanged (the backfill reads the caches,
   never writes them); the attempts count grows by the number of owned
   alias keys that had no attempt row.
3. **Apply** with `npm run db:migrate` (not the SQL editor, so the
   bookkeeping row lands with it). Then:
   ```sql
   select verdict, count(*) from public.merchant_confirmations group by 1;
   select last_resolution, count(*) from public.merchant_mapping_attempts group by 1;
   ```
   Expect CONFIRM = AI merchants with `confirmed = true` that some
   transaction references, DISSENT = AI merchants with `confirmed = false`
   and `dissent_count > 0`; `P2P` = number of distinct owned alias keys (your
   payees), `MERCHANT` = attempt rows behind shared aliases, null = keys the
   model was unsure about.
4. **Between migration and deploy** nothing new degrades. Old-code reviews
   keep flipping the cache columns without writing a verdict; the first
   new-code review of the same merchant recomputes the caches from verdicts,
   so any drift self-heals and nothing needs a manual fix.
5. **Deploy** at the end. `MERCHANT_CONFIRMATION_THRESHOLD` is optional;
   unset means 1, which is today's behaviour. Re-enable the mapper schedule
   afterwards (paused since Task 4a).
6. **Rollback.** Code: `wrangler rollback`. The table and columns are
   harmless to older code.

### Task 3 — Templates: provenance, private→shared promotion, guards

Migration `…_email_templates_provenance.sql`:
- `learned_from_user_id uuid references users(id) on delete cascade` (null
  only for SHARED), `scope text not null default 'PRIVATE' check (scope in
  ('PRIVATE','SHARED'))`, check `(scope = 'SHARED') = (learned_from_user_id is
  null)`; drop `email_templates_sender_skeleton_unique`; unique index
  `(from_address, skeleton_hash, coalesce(learned_from_user_id, zero-uuid))`
  (A.2 §2); index `(from_address, status, scope, learned_from_user_id)`.
- Backfill `learned_from_user_id` from `raw_emails.user_id` via
  `learned_from_raw_email_id`; rows whose source email is gone → the admin
  user if that user exists, else deleted (A.2 §7). All rows stay `PRIVATE`.

Code (`lib/email-templates.ts`, `workers/email-ai-normalizer.ts`):
- `{{S}}` marker: pre-pass wildcarding the greeting span after
  `dear|hi|hello|namaste` up to `,`/`!`/`:`/newline in both induce paths;
  compiles to a bounded non-captured span; self-verification unchanged.
- `saveTemplate(params + learned_from_user_id)` in one transaction: insert the
  learner's PRIVATE row (`on conflict do nothing` on the per-owner key); then,
  whether or not that inserted (A.4 §3), select other owners' `ACTIVE` PRIVATE
  rows with the same `(from_address, skeleton_hash)`; if any has an equal
  *structural* slot_map (A.2 §1, a `structuralSlotMap()` helper strips
  `confidence`), insert one SHARED row (owner null, `hit_count` summed, higher
  confidence, `on conflict do nothing`) and delete those PRIVATE rows. Returns
  inserted / promoted / skipped. Pruning to the cap within `(from_address,
  scope, owner)`.
- `loadTemplatesForSenders(sql, senders, userIds)` loads `SHARED` plus the
  batch users' `PRIVATE` rows; `applyTemplatesToEmail` filters per email.
- FIXED-merchant guard inside `resolveMerchantSpan`: zero body occurrences of
  the counterparty text → `{ ok: false, reason: 'merchant not anchored in
  body' }`; two-plus occurrences stay FIXED (A.3 §2). No dependency on Task 4.
- `EMAIL_TEMPLATE_PRIVATE_CAP` / `EMAIL_TEMPLATE_SHARED_CAP` env (default 40).

Tests: promotion on second-user induction; no promotion on differing slot_map;
PRIVATE invisible to other users; salutation scrub; FIXED guard; all existing
template specs green.

Not changed for Vasu: every existing template stays applicable to his mail;
new templates from his mail store `Dear {{S}},` instead of his name.

#### Task 3 — delivered (September 18, 2026)

Files: `supabase/migrations/20260918170000_email_templates_provenance.sql`;
`lib/email-templates.ts` (`{{S}}` marker in both induce paths, compiled to a
bounded non-captured span; `resolveMerchantSpan` refuses zero body
occurrences with `merchant not anchored in body`; `structuralSlotMap()`;
`EmailTemplate.scope` / `learned_from_user_id` + `isTemplateVisibleToUser()`;
`loadTemplatesForSenders(sql, senders, userIds)`; `saveTemplate` rewritten as
one transaction returning `{ inserted, promoted }`; `resolveTemplateCaps()`),
`workers/email-ai-normalizer.ts` (passes the batch's users to the loader,
filters templates per email by visibility, saves with the learner's id and
the env caps, counts `template_promoted_count`), `queue.worker.ts` (logs
it), `env.bindings.d.ts` (`EMAIL_TEMPLATE_PRIVATE_CAP`,
`EMAIL_TEMPLATE_SHARED_CAP`).

Deviations from the spec text, all in the same spirit:
- Uniqueness is two partial unique indexes (`(from_address, skeleton_hash)
  where scope = 'SHARED'`, `(from_address, skeleton_hash,
  learned_from_user_id) where scope = 'PRIVATE'`) instead of a `coalesce`
  expression index: same guarantees, and `on conflict … where scope = …`
  infers them exactly like the merchant tables.
- Ownerless legacy rows go to the admin UUID when that user exists, else to
  the sole user when exactly one exists, else are deleted.
- A PRIVATE row inserted while an equal SHARED twin already exists is deleted
  as redundant and reported as not learned.
- The salutation must start within the first 60 characters of the body (a
  short header line may precede it); a greeting word inside prose stays
  literal.

Tests: `email-templates.spec.ts` (+5: not-anchored refusal, salutation
wildcard + apply to another addressee, prose greeting untouched, bounded
`{{S}}` compile, structural comparison; the volatile-digits case now
exercises the referenced-token FIXED path), `tenancy/template-provenance.spec.ts`
(PRIVATE invisible to the other user, promotion with summed hits and the
higher confidence, no promotion on a differing structure, promotion on a
no-op insert, redundant private under a shared twin, per-partition pruning,
scope invariant), `tenancy/template-provenance-migration.spec.ts` (three
backfill shapes, constraint/index changes, idempotence), e2e fake and
normalizer spec updated for the new query shapes.

Runbook (production; migration now, deploy at the end):

1. **Pre-flight.** `npm run db:status` lists one pending file
   (`20260918170000_email_templates_provenance.sql`). Confirm a backup.
2. **Snapshot before and after** (SQL editor):
   ```sql
   select count(*) as templates,
     count(*) filter (where learned_from_raw_email_id is null) as without_source_email
   from public.email_templates;
   ```
   Expected after: the same count (your user exists, so ownerless rows are
   attributed, never deleted), and
   ```sql
   select scope, learned_from_user_id, count(*) from public.email_templates group by 1, 2;
   ```
   shows every row `PRIVATE` with your user id.
3. **Apply** with `npm run db:migrate`. The migration raises a `notice` with
   how many ownerless rows it attributed.
4. **Between migration and deploy**, template matching keeps working (the
   old loader ignores the new columns and every row is yours). Template
   *learning* stops until the deploy: the old insert's `on conflict
   (from_address, skeleton_hash)` no longer infers, the normalizer logs
   `AI_NORMALIZER_TEMPLATE_LEARN_ERROR` and carries on, so a new email
   format costs one AI call per email until the new code is up. No data is
   affected.
5. **Deploy** at the end. `EMAIL_TEMPLATE_PRIVATE_CAP` /
   `EMAIL_TEMPLATE_SHARED_CAP` are optional (default 40, today's cap).
6. **Rollback.** Code: `wrangler rollback` (matching keeps working, learning
   stops as in step 4). Schema stays.

Optional add-on (A.2 §7, cost only, not safety): an ops endpoint that
re-applies the `{{S}}` pass to existing PRIVATE skeletons so your legacy
templates can promote once a second user induces the same format. Not built;
new templates already learn with the marker.

### Task 5 — Account lifecycle and body retention (needs Task 2)

Migration `…_account_lifecycle_and_retention.sql`:
- `raw_emails.body_purged_at timestamptz`; partial index on `(status,
  processing_completed_at) where clean_text <> ''`.
- `job_schedules` check extended with `raw_email_body_purge`; seeded disabled
  at 03:30 IST.
- `user_deletion_log(user_id uuid pk, deleted_at, clerk_deleted boolean,
  google_revoked boolean)` — no PII.

Code:
- `DELETE /account` (body `{ "confirm": "DELETE" }`): revoke Google refresh
  tokens (`oauth2.googleapis.com/revoke`, best-effort, decrypt first); delete
  `email_templates where learned_from_user_id = :id and scope = 'PRIVATE'`;
  `delete from users where id = :id` (cascades all user-owned tables); delete
  the Clerk user via the Backend API using `auth_subject` and a new
  `CLERK_SECRET_KEY` secret (best-effort, logged in the tombstone); write the
  tombstone; 204. If Clerk deletion fails the user can sign in again and gets
  a fresh empty account — acceptable, flagged in the log.
- Before the cascade: delete `merchant_mapping_attempts` rows whose search
  keys occur only in this user's `financial_events`. Fetcher and normalizer
  treat a missing user as `PoisonMessageError` (ack, no DLQ). The nightly
  purge job retries Clerk deletions with `clerk_deleted = false` (A.2 §10).
- `DELETE /oauth/google/connection` also revokes at Google (best-effort).
- Purge job (queue job type `RAW_EMAIL_BODY_PURGE`, claimed from
  `job_schedules`): `set clean_text = '', body_purged_at = now()` for
  `PROCESSED | IGNORED | UNRECOGNIZED` rows older than
  `RAW_EMAIL_BODY_RETENTION_DAYS` (default 90) in bounded loops, age from
  `coalesce(processing_completed_at, created_at)`; `FAILED` rows keep bodies
  (reprocessable). `runStatementHarvest`, the sender-classifier candidate and
  backfill queries, the fetcher's backfill job and the retroactive gate all
  add `and body_purged_at is null` (A.4 §2). The legacy pre-gate purge
  (FUTURE_FIXES #16) is the first run of this job.
- Frontend: Settings → "Delete account" with typed confirmation (small screen
  addition, the only FE change in the plan).
- CONTRACT.md: deletion and retention semantics.

Tests: deletion cascade on the harness (every user-owned table empty, SHARED
templates survive, PRIVATE gone, other user untouched); purge predicate;
harvest skips purged rows.

Not changed for Vasu until he enables the purge job; he picks the retention
window (decision D6).

#### Task 5 — delivered (September 18, 2026)

Files: `supabase/migrations/20260918200000_account_lifecycle_and_retention.sql`;
`services/account.service.ts` (`parseDeleteAccountRequest`, `deleteAccount`),
`routes/account.ts` (`DELETE /account`, mounted with auth),
`lib/clerk-admin.ts` (`deleteClerkUser` against the Clerk Backend API,
`CLERK_SECRET_KEY`), `lib/db/errors.ts` (`isMissingUserReferenceError`),
`services/oauth.service.ts` (`revokeGoogleOAuthTokens`; `disconnectGoogleOAuthConnection`
revokes at Google when given the env), `workers/raw-email-body-purge.ts`
(`purgeExpiredRawEmailBodies` in bounded batches of 500 × 40,
`retryPendingClerkDeletions`, `runRawEmailBodyPurgeJob`),
`queue.messages.ts` / `queue.worker.ts` / `scheduled.worker.ts`
(`RAW_EMAIL_BODY_PURGE` job, claimed like the other nightly jobs),
`email-sync-fetcher.ts` (backfill, retro gate and statement harvest add
`body_purged_at is null`; user job and sender backfill ack a missing user as
poison), `sender-classifier.ts` (both gated-row queries), `email-ai-normalizer.ts`
(skips a deleted user's remaining emails, other users' continue),
`lib/config.ts` (`rawEmailBodyRetentionDays`, env
`RAW_EMAIL_BODY_RETENTION_DAYS`, default 90), `env.bindings.d.ts`,
`shared/types.ts` (`SCHEDULED_JOB_NAMES` + purge payload,
`DELETE_ACCOUNT_CONFIRMATION`). Frontend: `DeleteAccountSettings.tsx`
(typed `DELETE`, then clears the query cache and signs out), Settings hub
row, `/settings/delete-account` route, `apiClient.deleteCurrentUserAccount()`,
and a "Nightly email body purge" schedule card in the admin dashboard (the
switch that enables the job).

Deviations from the spec text, all in the same spirit:
- The tombstone also carries `auth_provider`, `auth_subject`,
  `clerk_delete_attempts`, `clerk_last_attempt_at`, `clerk_last_error`: the
  nightly retry needs the provider subject (`users.id` cannot be reversed
  into it), so it is kept only while `clerk_deleted = false` and a check
  constraint forces it null once the deletion succeeds. Still no email or
  name.
- The retry stands down for a user who signed in again before it ran (the
  middleware re-provisions the same id): it drops the tombstone rather than
  deleting a live sign-in, and logs `ACCOUNT_DELETION_CLERK_RETRY_USER_RETURNED`.
- The scan index is on `(status, coalesce(processing_completed_at,
  created_at)) where clean_text <> ''`, the expression the purge filters
  on, instead of the plain column.
- The normalizer does not poison the whole message when one user is gone
  (a job can carry several users' emails); it skips that user's remaining
  emails and acks. The fetcher and sender backfill ack as poison, as
  specified.
- Harness finding, fixed in the service: `delete from users` does **not**
  cascade cleanly on the real schema. `user_merchant_rule_categories` has
  no `user_id` (it hangs off `user_merchant_rules`) and its `category_id`
  FK is `NO ACTION`; the categories cascade runs before the rules' children
  are removed and the check fails. `deleteAccount` deletes those rows first
  inside the same transaction; no FK semantics changed (category deletion
  behaves as before). Noted in `supabase/README.md` for anyone deleting a
  user by hand.
- The retroactive gate's predicate is a no-op (a PENDING row is never
  purged) but is added for uniformity with the other gated-row queries.
- Pending Clerk retries run inside the purge job, as specified: until the
  purge schedule is switched on, a failed Clerk deletion waits in
  `user_deletion_log` (visible through the OPERATIONS.md panel).

Tests: `account.service.spec.ts` (order Google → transaction → Clerk,
tombstone bookkeeping on success / failure / 404 / missing secret / missing
subject, confirmation word), `raw-email-body-purge.spec.ts` (bounded
batches, backlog flag, retry outcomes), `db-errors.spec.ts`,
`oauth.service.spec.ts` (+2: revoke before forgetting, Google unreachable),
`scheduled-worker.spec.ts`, `queue-worker.spec.ts`, `job-schedules.service.spec.ts`,
`config.spec.ts`; `tenancy/account-deletion.spec.ts` (every user-owned table
empty for the deleted tenant, other tenant byte-identical, SHARED template
survives, PRIVATE gone, private payee gone, only single-payer mapping keys
forgotten, Clerk failure → retry → cleared, returned user → tombstone
dropped), `tenancy/body-retention.spec.ts` (statuses, `created_at`
fallback, FAILED/PENDING/gated untouched, idempotent, backfill and harvest
skip purged rows), `tenancy/account-lifecycle-migration.spec.ts` (column,
index, schedule seed, check constraint, hardened tombstone, idempotence).

Runbook (production; migration now, deploy at the end):

1. **Pre-flight.** `npm run db:status` lists the pending files in order
   (`…150000_merchant_confirmations`, `…170000_email_templates_provenance`
   if not yet applied, then `20260918200000_account_lifecycle_and_retention.sql`).
   Confirm a backup.
2. **Snapshot before and after** (SQL editor):
   ```sql
   select count(*) as total, count(*) filter (where clean_text <> '') as with_body
   from public.raw_emails;
   select job, enabled, to_char(run_at_local, 'HH24:MI') as run_at from public.job_schedules order by job;
   ```
   Expected after: identical `raw_emails` counts (the migration purges
   nothing) and a third schedule row `raw_email_body_purge`, `enabled =
   false`, `03:30`.
3. **Apply** with `npm run db:migrate`.
4. **Between migration and deploy** nothing degrades: the old code never
   reads `body_purged_at`, only offers its two job names to the claim (the
   new row stays disabled anyway) and answers `DELETE /account` with 404.
5. **Deploy** at the end. Secrets: `CLERK_SECRET_KEY` (Clerk dashboard →
   API keys → secret key) so a deletion removes the Clerk user at request
   time; without it deletions still erase the data and the retry waits.
   `RAW_EMAIL_BODY_RETENTION_DAYS` is optional (default 90, decision D6).
6. **Switch the purge on** from Settings → AI operations → "Nightly email
   body purge" (this also enables the Clerk retry). Its first run is the
   FUTURE_FIXES #16 purge. Size it first:
   ```sql
   select count(*) from public.raw_emails
   where status in ('PROCESSED','IGNORED','UNRECOGNIZED') and clean_text <> ''
     and coalesce(processing_completed_at, created_at) < now() - interval '90 days';
   ```
   One run purges at most 20,000 rows (40 batches × 500) and logs
   `RAW_EMAIL_BODY_PURGE_BACKLOG` while more remain; the backlog drains over
   consecutive nights.
7. **After the first run**:
   ```sql
   select count(*) filter (where body_purged_at is not null) as purged,
     count(*) filter (where body_purged_at is not null and status in ('FAILED','PENDING_EXTRACTION')) as must_be_zero
   from public.raw_emails;
   ```
8. **Rollback.** Code: `wrangler rollback`; the schedule row and the
   stamps are harmless to old code. One caveat once bodies have been purged:
   the old sender backfill treats a purged `IGNORED` row as gated, so a
   newly allowlisted sender would re-download those bodies (cost only, no
   data risk). Schema stays.

### Task 6 — Quotas and fairness

Migration `…_user_quotas.sql`:
- `user_usage_daily(user_id, day, messages_listed, bodies_fetched, pk
  (user_id, day))` — AI calls are read from `ai_usage_events` (A.2 §9); `day`
  is the IST date.
- `user_limits(user_id pk, daily_email_fetch_cap int null, daily_ai_call_cap
  int null, updated_at)` — per-user overrides live in the DB; global defaults
  from env `USER_DAILY_EMAIL_FETCH_CAP` (500) and `USER_DAILY_AI_CALL_CAP` (200).

Code:
- Fetcher: increments `messages_listed` / `bodies_fetched` per persisted page;
  checks the caps before each page; over cap → stop, keep cursor, persist the
  Gmail page token on the connection (`email_sync_continuation_token`, A.3
  §3), release lease, log `EMAIL_SYNC_QUOTA_DEFERRED`; no continuation
  enqueued; the next job resumes from the stored token.
- Dispatcher: skips users already over cap today (one join).
- Normalizer: per-user AI-call cap; over cap → emails stay PENDING, logged.
- Self-heal: each `EMAIL_SYNC_USER` job re-enqueues that user's PENDING rows
  older than 30 minutes (≤ 250), at most once per hour per user and never
  while the user is over the AI cap (A.2 §8) — also closes the
  lost-normalize-job gap.
- Nightly caps: `MAX_MERCHANTS_PER_RUN` / `MAX_SENDERS_PER_RUN` from env,
  backlog-scaled `min(cap, backlog)` in the existing 40-item chunks.
- Admin dashboard: per-user daily rollup endpoint (`/ai-usage/users`).

Tests: fetcher deferral keeps the cursor; dispatcher skip; normalizer cap;
requeue bounded; mapper backlog scaling.

Not changed for Vasu: caps default far above his volume; verify from
`raw_emails` counts before choosing defaults (decision D7).

#### Task 6 — delivered (September 19, 2026)

Completed the existing partial implementation and corrected the boundary cases.
Migration: `20260918220000_user_quotas.sql` (two hardened, user-owned quota
tables, persisted Gmail continuation state and a stale-PENDING scan index).
Code: `services/user-quotas.service.ts`, `lib/quotas.ts`, configuration/env
bindings, dispatcher, fetcher/backfill, normalizer, mapper and sender classifier;
admin `/ai-usage/users` GET and `/ai-usage/users/:userId/limits` PUT, shared
response types, API client, hooks and the AI operations per-user card.

Details and corrections:
- One email ceiling independently bounds listed IDs and downloaded bodies
  (default 500 each). Metadata-only/gated mail cannot bypass the quota. Both
  counters appear in the admin card, with per-user overrides.
- Gmail page size shrinks to the remaining allowance. A page that reaches
  the cap parks its *next* token immediately without another queue hop;
  consuming an older parked token does not erase the replacement. The query
  window and high watermark travel with it. Page counters and raw rows commit
  in one transaction before normalization enqueue, so queue failure does not
  make downloaded bodies disappear from accounting.
- Sender backfill also counts bodies and defers remaining rows with a
  delayed message until the next IST day.
- Each AI retry/fallback consumes a budget unit, not just each logical batch.
  Stopping inside a retry leaves single and batched emails PENDING, not FAILED.
  Templates are still attempted first. Usage remains sourced from the existing
  AI ledger, checked before each attempt, plus an in-job attempt budget.
- Self-heal is capped at 250 rows and its rolling-hour claim spans day
  boundaries. Failed enqueue releases the claim.
- Nightly sender rechecks share the sender cap; sender and merchant model
  batches contain at most 40 items. Invalid calendar dates in admin queries
  return 400, rather than a database error.
- Operational limitation: these are committed-usage fair-use checks, not
  distributed reservations. Concurrent in-flight jobs can overshoot. No
  strict billing ceiling is claimed. Recovery uses existing active-user /
  connection eligibility, and historical dashboard caps are today's settings.

Validation: **67 backend test files / 645 tests passed**, including **19
tenancy test files / 158 tests**; backend type checking and frontend production
build passed; 47 frontend tests passed. The Worker integration command has no
test files configured, so no runtime integration pass is claimed. The tenancy harness rehearses schema hardening, migration
idempotence, ownership, day counters, override behavior, dispatcher skipping,
rollup and hourly recovery. Regression tests cover the last available Gmail
slot, parked resumption, metadata cap, sender-backfill deferral, single/batch
AI retries at the cap and capped sender chunks/rechecks.

Runbook (production; migration now, deploy at the end):

1. **Pre-flight.** `npm run db:status` must list this file after any unapplied
   Task 4b / 3 / 5 migrations. Confirm a backup. Review the pending plan with
   `npm run db:migrate -- --to 20260918220000_user_quotas.sql --dry-run`.
2. **Size the defaults before deployment.** This historical count is a lower
   bound (old code did not record repeated listings or body refetches):
   ```sql
   select user_id, (created_at at time zone 'Asia/Kolkata')::date as day,
     count(*) as stored_messages,
     count(*) filter (where clean_text <> '') as stored_bodies
   from public.raw_emails
   where created_at >= now() - interval '30 days'
   group by 1, 2 order by stored_messages desc;
   select user_id, (created_at at time zone 'Asia/Kolkata')::date as day,
     count(*) as ai_attempts
   from public.ai_usage_events
   where user_id is not null and created_at >= now() - interval '30 days'
   group by 1, 2 order by ai_attempts desc;
   ```
   Raise the env defaults or insert per-user overrides after migration if
   needed. Production volumes were not accessed during implementation.
3. **Snapshot before and after:** use Part B.3 / Task 2's invariant queries
   and `select count(*) from public.oauth_connections;`. All existing row
   counts and ownership must remain unchanged.
4. **Apply:** `npm run db:migrate -- --to 20260918220000_user_quotas.sql`.
   Afterwards both new tables are empty, every connection's parked fields
   are null, and `select public.assert_tenancy_backstops();` succeeds.
5. **Between migration and deploy** old code ignores the additions; no data
   is changed and quotas are not yet active. Deploy only after all required
   task migrations. Optional env defaults: `USER_DAILY_EMAIL_FETCH_CAP=500`,
   `USER_DAILY_AI_CALL_CAP=200`, `MAX_MERCHANTS_PER_RUN=200`,
   `MAX_SENDERS_PER_RUN=200`. No new secrets or bindings are needed.
6. **Verify after deploy:** Settings -> AI operations -> Per-user fair use
   shows today's IST counters and caps. A controlled low-cap test should
   park the Gmail continuation with its cursor unchanged; increasing the
   override should permit the next scheduled sync. Restore intended caps.
   Check `EMAIL_SYNC_QUOTA_DEFERRED`, `AI_NORMALIZER_QUOTA_DEFERRED`,
   `EMAIL_SYNC_PENDING_REQUEUED`, and the nightly backlog/cap logs.
7. **Rollback:** roll back the Worker to the prior build; retain the additive
   schema. Older code ignores quotas and parked tokens and resumes from the
   durable cursor, potentially repeating Gmail reads. Bodies and transactions
   are not removed by this migration.

**Production migration and deployment have not been performed by this task.**

### Task 7 — Perimeter and operations hardening

- `wrangler.jsonc` `ratelimits` bindings: per-user 300 req/min after auth,
  per-IP 60 req/min on `/oauth/*` and unauthenticated paths; middleware returns
  429 with `Retry-After`.
- `Idempotency-Key` on `POST /transactions`: `idempotency_keys(user_id, key,
  request_hash, response_status, response_body jsonb, created_at, pk (user_id,
  key))`; same key + same body → replay; same key + different body → 409;
  rows older than 24 h purged by the nightly purge job.
- DLQ consumer binding for `money-manager-email-sync-dlq`: logs `DLQ_MESSAGE`
  (job type + ids only) and acks; redrive documented via `wrangler queues`.
- Cron tick logs `STALE_PENDING_EXTRACTION` when the MV has rows (alert source).
- `azp` validation against `CLERK_AUTHORIZED_PARTIES` (frontend origins),
  enforced when set; `aud` stays optional (A.3 §1). Secret set before deploy.
- Secret rotation runbook: `AI_REQUIRES_WEBHOOK_SECRET`,
  `GOOGLE_OAUTH_STATE_SECRET`; `AI_USAGE_ADMIN_USER_ID` moved to a secret.
- Log audit: grep every `console.*` for content fields; `EMAIL_PIPELINE_TRACE`
  stays dev-only.
- Docs: CONTRACT.md gains the ownership-class rule for new tables (user-owned
  vs shared, composite-FK discipline, RLS on), `sender_rules` USER = admin-only,
  privacy notes for sender/subject AI transit; OPERATIONS.md updated.

Not changed for Vasu: limits are generous; idempotency is opt-in by header.

#### Task 7 — delivered (September 19, 2026)

Migration: `20260919090000_perimeter_hardening.sql` (`idempotency_keys`, the
whole database footprint of the task). Code: `lib/http/rate-limit.ts`,
`services/idempotency.service.ts`, `lib/http/errors.ts` (429 helper +
error-carried headers), `app.ts`, `lib/auth.ts` (`azp`, both limiter hooks),
`routes/transactions.ts`, `workers/queue.worker.ts` (DLQ consumer),
`workers/scheduled.worker.ts` (stale-extraction alert),
`workers/raw-email-body-purge.ts` (24-hour ledger purge), `lib/ai.ts`
(production trace refusal), `wrangler.jsonc`, `env.bindings.d.ts`,
`.dev.vars.example`. Frontend untouched: nothing in the UI sends an
`Idempotency-Key` today, and the limits are far above its traffic.

Details and deviations:
- **Rate limiting.** 300/min per user, charged after the token verifies and
  *before* the provisioning upsert, so a flood costs one JWT verification
  rather than a database round trip. The IP limiter's scope is decided by
  **path, not by the Authorization header**: every `/oauth/*` call, every
  path that is not an authenticated mount (health, version, the secret-gated
  `/internal` endpoints, any unmatched path), and any authenticated route
  reached without a token. Deviation from the spec text, and the reason it
  matters: scoping on "has a bearer token" leaves `/health` and `/internal/*`
  with a junk `Authorization` header metered by nothing at all, which is
  worst exactly where it matters most — `/internal` is guarded only by a
  guessable secret. A token that fails verification on an authenticated route
  is also charged to the IP limiter, so token-shaped garbage is not a free
  pass either. The authenticated-mount list is compared against the real
  router by a test, so adding a route cannot silently change its class.
  Both limiters fail open — missing binding, throwing binding, or a request
  object without headers all allow the request — and a request with no
  `CF-Connecting-IP` is *not* limited rather than bucketed with everyone
  else under one key, which one caller could otherwise use to lock the rest
  out. `X-Forwarded-For` is deliberately ignored: a caller could name a
  victim there and spend the victim's window.
- **Idempotency.** Implemented as `withIdempotency(...)` wrapping the handler
  rather than as generic middleware: every ledger write is then awaited
  before the response is sent, which a `res.on('finish')` hook cannot promise
  in a Worker. Claim → run → complete, with the body pinned by SHA-256 over
  canonical JSON (key order irrelevant, array order significant). Reuse with
  a different body is 409; a claim still running is 409; a claim abandoned by
  a dead worker is reclaimable after 5 minutes, which a 24-hour purge alone
  would leave wedged. A failing handler releases the key. Only 2xx responses
  are stored, so a 500 is never pinned to a key.
- **`azp`.** Enforced only when `CLERK_AUTHORIZED_PARTIES` is set; a token
  with no `azp` is then rejected. `aud` stays optional (A.3 §1).
- **DLQ.** Consumer with `max_retries: 0`, handled before the normal path in
  `handleQueue` and written so it cannot throw: there is no queue behind a
  DLQ, so a failure there loses the message.
- **Stale extraction.** The tick reads `mv_stale_pending_extraction` in
  `waitUntil`; a failed read logs and costs the tick nothing. The view is
  refreshed by pg_cron, not by the Worker, and the Worker cannot refresh it
  (`app_backend` does not own it), so a frozen view would otherwise raise
  alerts about counts from an arbitrary earlier moment. A snapshot older than
  an hour therefore logs `STALE_PENDING_EXTRACTION_SNAPSHOT_STALE` instead —
  fix the refresh, not the pipeline.
- **`AI_USAGE_ADMIN_USER_ID`** left `wrangler.jsonc`. `keep_vars: true` means
  the deployed value survives the deploy, so this cannot lock the admin out
  mid-flight; the secret should still be set explicitly (runbook step 2).
- **Log audit** (all 115 `console.*` sites, keys extracted mechanically):
  one real finding. `AI_NORMALIZER_EMPTY_RESPONSE` logged
  `reasoning_content.slice(-300)` — the model's reasoning about the user's
  bank mail — with no flag guarding it; it now logs the length only. Every
  other site carries identifiers, counts, lengths or enums. Sender addresses
  stay (bank senders, needed for triage). `PIPELINE_TRACE_RESOLVED` was
  already flag-gated and is now additionally refused when
  `NODE_ENV=production`, flag or no flag.
- **A third pass found two wrong statements in the documentation itself,
  both in `CONTRACT.md` and both mine.** The privacy paragraph claimed "no
  account number, card number, balance or user email address is sent" to the
  AI provider. That guarantee belongs to `AI_NORMALIZER_MASK_NUMBERS`, which
  is a default and not a constant — with it off the body goes verbatim — and
  masking replaces **digits only**, so a name or an email address the bank
  prints in its own copy reaches the provider in either mode. The paragraph
  now states each of the three provider paths separately with what it
  actually guarantees (the sender classifier's digit-masking, by contrast,
  is unconditional). The ownership-class rule said a shared table must hold
  no personal data — a rule the codebase already broke twice, deliberately:
  `merchant_mapping_attempts` is shared and stores `person_display_name`
  (sound because the search key it is keyed by already identifies that
  person), and `user_deletion_log` is a tombstone that cannot carry an FK to
  the user it outlives. The rule now has three classes and names both
  exceptions with their arguments, so the next table is measured against
  something true. Verified in passing: my three interval-parameter patterns
  match nine existing call sites, several already in production, and no
  checked-in script calls the Worker over HTTP, so IP-limiting `/internal/*`
  breaks no tooling.
- **A second review pass found one more gap and two latent faults.** The
  gap: the stale-extraction query was the only new SQL no test ever executed
  — every spec mocked the client — so a wrong column name would have passed
  the suite, thrown in production, and been swallowed by the caller's own
  catch, leaving the alert permanently silent.
  `test/tenancy/stale-extraction.spec.ts` now runs it against the real view.
  The faults: the ledger purge took a single batch, so a backlog larger than
  one batch could never drain (it now loops, like the body purge beside it);
  and `DLQ_MESSAGE` logged `attempts` as if it were the job's attempt count,
  when a DLQ delivery starts its own count — renamed `dlqDeliveryAttempts`.
  The runbook also now says to confirm the DLQ exists before deploying a
  consumer for it. `wrangler.jsonc` was validated offline against wrangler's
  own `config-schema.json` (both limiter bindings, both consumers).
- **Two bugs found by review after the first pass, both now fixed and
  covered.** (1) The IP limiter's original scope test keyed off the
  Authorization header, leaving every unauthenticated path unmetered when a
  junk token was attached — the `/internal` brute-force surface. (2) The
  auth middleware decided "did the middleware already charge this?" from
  `req.path`, but express rewrites the path when it dispatches into a mounted
  router, so inside `requireAuthenticatedUser` a `/transactions` request
  reads as `/` and the auth-failure charge never fired. The request now
  carries a marker instead of the decision being re-derived. The second bug
  was invisible to unit tests with hand-built request objects and only
  surfaced against a real express server, which is why
  `test/perimeter.e2e.spec.ts` exists.
- **Also corrected in passing:** `OPERATIONS.md` still told the operator to
  build dashboards and alerts for `money-manager-ai-classification`, a queue
  retired with the triage worker in July 2026, and `backend/README.md`'s
  wired-bindings list still described that queue, a `* * * * *` cron and
  `max_batch_size=10`. Both now match `wrangler.jsonc`.
- **Not done, deliberately:** no rate-limit headers on success responses
  (Cloudflare's binding reports no remaining count), and no idempotency on
  `PATCH`/`DELETE` — those are already idempotent by shape, and the plan
  named `POST /transactions` only.

Validation: **72 backend test files / 734 tests passed**, including **21
tenancy files / 182 tests**; backend typecheck clean; frontend 47 tests and
production build clean. New: `test/rate-limit.spec.ts` (20),
`test/idempotency.service.spec.ts` (11),
`test/tenancy/stale-extraction.spec.ts` (4, the alert query against the real
materialized view: silent when empty, silent below the 30-minute threshold,
real counts once mail is stale, quiet again once it clears),
`test/perimeter.e2e.spec.ts` (7,
a real express server: 429 and its Retry-After on the wire, an
unauthenticated path charged despite a junk token, an authenticated route
left to the user limiter, one charge per OAuth request, rejection before the
body is read, no binding means no limiting), `test/tenancy/idempotency.spec.ts`
(18, real schema: replay, reuse conflict, in-flight, reclaim, per-user
isolation, release, purge, cascade, migration re-run, exactly-once across a
reordered retry, and a 201 still returned when the ledger write itself
fails).
Extended: auth (azp accepted/rejected/missing, 429 + Retry-After, no DB
touch when limited, junk token charged to the IP limiter), queue worker (DLQ
logged and acked, unreadable dead letter acked), scheduled worker (alert
fires, stays quiet, survives a failed read), purge (ledger purge as the third
duty), ai-config (production trace refusal), infra contract (DLQ consumer,
both limiter bindings, admin id no longer a committed var). The Worker
integration command still has no test files configured, so no runtime
integration pass is claimed.

Runbook (production; migration now, deploy at the end):

1. **Pre-flight.** `npm run db:status` lists this file after the unapplied
   Task 4b / 3 / 5 / 6 migrations. Confirm a backup. The file creates one
   empty table and reads nothing.
2. **Set the secrets before the deploy, not after.**
   ```bash
   cd backend
   npx wrangler secret put AI_USAGE_ADMIN_USER_ID   # a661a921-…
   npx wrangler secret put CLERK_AUTHORIZED_PARTIES # frontend origins, comma-separated
   ```
   `CLERK_AUTHORIZED_PARTIES` must list **every** origin the app is served
   from (the production origin, and `http://localhost:5173` if you sign in
   locally against production). Wrong or missing entries 401 every request
   the moment the new code is live. Leaving it unset is a valid choice: the
   check is then not enforced and behaviour is exactly as today.
3. **Apply.** `npm run db:migrate -- --to 20260919090000_perimeter_hardening.sql`.
   Afterwards:
   ```sql
   select count(*) from public.idempotency_keys;            -- 0
   select public.assert_tenancy_backstops();                -- succeeds
   ```
4. **Between migration and deploy** nothing changes: no old code path reads
   the table, and rate limiting, `azp` and the DLQ consumer all arrive with
   the deploy.
5. **Deploy** (the single end-of-plan deploy). The deploy creates the two
   rate-limit bindings and registers the DLQ consumer, so
   `money-manager-email-sync-dlq` must already exist as a queue — confirm
   with `npx wrangler queues list` first, and
   `npx wrangler queues create money-manager-email-sync-dlq` if it is
   missing, or the deploy fails. Verify:
   - `/health` responds; the app loads and one sync runs.
   - Worker logs show no `RATE_LIMIT_BINDING_ERROR`.
   - A tick logs either nothing about stale extraction or a
     `STALE_PENDING_EXTRACTION` line that matches the MV.
   - Optional idempotency check, safe on production because a replay writes
     nothing — send the same `POST /transactions` twice with one key and
     confirm the second response carries `Idempotent-Replay: true` and that
     only one transaction exists. Delete it afterwards.
6. **Rollback.** `wrangler rollback` removes the limits, the `azp` check and
   the DLQ consumer; the table is inert for older code. Re-put a previous
   secret value to undo step 2. `CLERK_AUTHORIZED_PARTIES` can also be
   neutralised without a deploy by putting an empty value (unset = not
   enforced), which is the fastest way out of a lockout.

**Production migration and deployment have not been performed by this task.**

### Task 8 — Scale runway (operator-led, start the Google track now)

- Google OAuth app verification + CASA for `gmail.readonly` (longest lead
  time; blocking above 100 users); Gmail API quota increase.
- Cloudflare Hyperdrive in front of Supabase (config; `config.ts` accepts the
  Hyperdrive host, port check relaxed for it).
- Split nightly jobs onto their own queue; tune consumer concurrency.
- PITR on Supabase + one restore-to-staging drill; error reporting hook.

**Delivered (September 19, 2026).** No migration; this is the only task in
the plan whose largest part cannot be written in code. Each bullet was taken
as far as the repo can take it, and the remainder is stated as an operator
step rather than quietly dropped.

- **The Google track is entirely operator work** — a verification
  submission, a demo video, a CASA assessment with months of lead time, a
  quota form. The runbook for it is in `OPERATIONS.md`; what the code now
  contributes is a tripwire, because the failure mode is silent and remote:
  user 101 registers successfully here and only then fails at Google's
  consent step. Once a day the tick counts distinct users holding a Google
  connection and logs `GOOGLE_TEST_USER_CAPACITY`, escalating to
  `GOOGLE_TEST_USER_CAP_WARNING` (and a webhook report) from 80 of 100.
  The count is deliberately a **lower bound**: Google counts entries on the
  consent screen's test-user list, and a deleted account's row cascades away
  here while its entry there survives, so this can only under-report. Said
  so in the log field (`countIsLowerBound`), the contract and the runbook,
  because a tripwire nobody distrusts is worse than none.
- **Hyperdrive is wired but not enabled.** `config.ts` now resolves the
  connection from `HYPERDRIVE.connectionString` when the binding exists and
  from `SUPABASE_POOLER_URL` otherwise, reporting which in
  `AppConfig.dbConnectionSource` and logging it once per isolate as
  `DB_CONNECTION_SOURCE`. The PgBouncer port check is skipped for the
  Hyperdrive string and kept for the pooler: Hyperdrive listens on a local
  port it chooses, so the check is meaningless against it — but the check it
  replaces (that the resource itself was created against 6543, not the
  direct 5432 that exhausts connections from a Worker) is unverifiable from
  inside the Worker, and is written down as an operator guarantee instead.
  The `hyperdrive` block sits commented in `wrangler.jsonc` with the create
  command, including `--caching-disabled`: Hyperdrive caches reads by
  default and this app reads its own writes. Rollback is to unbind and
  redeploy.
- **Nightly work moved to `money-manager-nightly`** with its own DLQ, so a
  batch job that runs for minutes never sits in front of a user's email
  sync. Four job types move: the three claimed from `job_schedules` plus the
  `SENDER_BACKFILL` fan-out the classifier produces during its own run. Two
  decisions make this a one-step, reversible deploy: the consumer dispatches
  on `job_type` and **both work queues accept every type**, so messages
  enqueued before the split drain normally; and an absent `NIGHTLY_QUEUE`
  binding falls back to the sync queue (logged once) rather than failing the
  send, which covers local dev, the unit suite and the window between
  creating the queue and deploying. A misrouted job still runs, and says so
  once via `QUEUE_ROUTING_UNEXPECTED`.
- **Consumer concurrency is now explicit**: 10 for sync, 2 for nightly,
  mirrored in `QUEUE_MAX_CONCURRENCY` and asserted against `wrangler.jsonc`
  by the infra contract test. Unset means Cloudflare auto-scales, which is
  the thing being prevented — the real ceiling is pooler slots, roughly
  `concurrency x DB_MAX_CONNECTIONS`. **These numbers come from arithmetic,
  not from a measurement**, and the runbook says so and says what to watch
  before raising them.
- **The error reporting hook is a webhook, deliberately not a vendor.**
  `ERROR_REPORTING_WEBHOOK_URL` is unset by default and the module is inert
  without it. It posts a fixed envelope for eight events (5xx, dead letters,
  queue threshold breaches, the two scheduled-job failures, stale
  extraction, snapshot staleness, the Google cap warning). Constraints it
  holds itself to: never throws, never retries, 3-second timeout, https only
  (localhost excepted), at most 10 sends per minute per isolate, message
  truncated to 500 characters and context to 20 keys of 200 characters. A
  delivery failure is a `console.warn` and nothing more — reporting a failed
  report would recurse. Throttling lives at the call sites, not in the sink:
  a dead-letter batch of ten is one report with a count, stale extraction
  pushes at most hourly while still logging every tick, and the capacity
  warning is daily.
- **The webhook is a new outbound data path, so it is in the contract.** A
  postgres error message can quote a constraint name or a failing value, so
  the honest statement — made in `CONTRACT.md` next to the AI-provider and
  logging paragraphs — is that the sink can receive fragments of user data
  and must be kept private. Request reports carry `req.path` and never the
  query string.
- **PITR is a Supabase setting and stays an operator step**, with the drill
  written down. One correction while writing it: the restore-check command
  is `npm run db:status`, and `scripts/db-migrate.mjs` reads
  `SUPABASE_POOLER_URL` from the environment *first, then
  `backend/.dev.vars`* — so a forgotten variable is not an error but a
  silent run against production. That is the Task 1 incident, and the step
  now sets the variable inline and says why.

Tests: `test/error-reporting.spec.ts` (16), `test/nightly-queue.spec.ts`
(8), `test/tenancy/google-capacity.spec.ts` (4, real schema), plus new cases
in `config.spec.ts`, `infra-contract.spec.ts` and `scheduled-worker.spec.ts`.
The capacity query gets a PGlite test for the reason Task 7's MV query did:
every other test of that path mocks the SQL client, so a wrong column name
would pass the suite and then throw once a day into a caller's own catch —
a tripwire that never trips. It also pins the semantics worth arguing about:
one user with two mailboxes is one test user, and a revoked connection still
counts because the slot on Google's list is still spent.

**Operator runbook (in order).**

1. **Start the Google verification track now.** Nothing else here depends on
   it and it is the only item with a lead time measured in months. Until it
   lands, keep the Clerk allowlist mirrored against the Google test-user
   list (D10).
2. **Create the nightly queues before the deploy**, or the deploy is
   rejected for an unknown queue:
   `wrangler queues create money-manager-nightly` and
   `wrangler queues create money-manager-nightly-dlq`.
3. **Deploy.** Verify: a nightly job appears on the new queue (or logs
   `NIGHTLY_QUEUE_BINDING_MISSING` if a binding was missed), and
   `QUEUE_BATCH_SUMMARY` names both queues over a day.
4. **Optional, any time after:** `wrangler secret put
   ERROR_REPORTING_WEBHOOK_URL`. Test it by watching for the first
   `UNHANDLED_REQUEST_ERROR`; no deploy needed.
5. **Optional, measured:** create the Hyperdrive resource, uncomment the
   block, deploy, and confirm `DB_CONNECTION_SOURCE` reads `hyperdrive`.
6. **Enable PITR** and do the restore drill once, against a new project,
   with `SUPABASE_POOLER_URL` set inline on the command.
7. **Rollback.** `wrangler rollback` returns to the single-queue Worker:
   the older code has no `NIGHTLY_QUEUE` binding, so it produces nightly
   jobs onto the sync queue again, and it declares no consumer for
   `money-manager-nightly` — so anything still sitting there stalls until
   Cloudflare's retention expires. That costs a day, not data: these are
   daily jobs claimed from `job_schedules`, and the next tick re-runs
   whatever the stranded message was carrying. Let the nightly queue drain
   first if that day matters. Unsetting `ERROR_REPORTING_WEBHOOK_URL`
   disables reporting with no deploy; commenting the `hyperdrive` block out
   returns to the pooler.

**Production migration and deployment have not been performed by this task.**

---

## Part D — Decisions Vasu must confirm before the relevant task

| # | Decision | Recommendation | Blocks |
|---|----------|----------------|--------|
| D1 | Templates: private→shared promotion + `{{S}}` + FIXED guard, no wipe | Approve (replaces profile-signal scrub) | Task 3 |
| D2 | P2P: `owner_user_id` on merchants/aliases, existing rows become his | Approve (replaces rules-with-display-name) | Task 4 |
| D3 | Confirmation threshold | 1 now via env; raise to 2 past ~10 users | Task 4 |
| D4 | DB: `app_backend` role + RLS deny-all + blanket policies; Data API off in dashboard | Approve | Task 2 |
| D5 | Drop `users.email` uniqueness | Yes | Task 2 |
| D6 | Deletion keeps SHARED templates, deletes PRIVATE; retention window | Keep shared; 90 days; FAILED rows keep bodies (built as recommended; `RAW_EMAIL_BODY_RETENTION_DAYS` changes the window) | Task 5 |
| D7 | Quota defaults | 500 emails/day, 200 AI calls/day per user, overridable per user in DB | Task 6 |
| D8 | Task order 1, 2, 4, 3, 5, 6, 7, 8 | As listed | — |
| D9 | Is logo batch 5 migration already applied to production? | Needed for the baseline step | Task 1 |
| D10 | Signup gating until Google verification: Clerk allowlist mirrors the Google test-user list | Yes, before the first invite | Task 8 (operator) |
| Q5/Q11 | Google verification timing; DPDP/data-residency needs | Vasu's call; not blocking | Task 8 |
