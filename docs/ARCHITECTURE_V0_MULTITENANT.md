# Money Manager — v0 Multi-Tenant (User-as-Tenant) Architecture Proposal

> Status: **v0 for review — no implementation yet.** Every recommendation below is a proposal until confirmed in the Decision Log (§16).
> Prepared: August 9, 2026. Based on direct inspection of the codebase, all 31 live tables, migrations, workers, routes, and the production database (2 users, 722 transactions, 23 MB).

---

## 1. Executive Summary

The good news: **the hardest multi-tenant foundations are already in place.** Every user-facing table carries a `user_id`, the schema enforces same-user integrity with composite `(id, user_id)` foreign keys and triggers, every API route resolves the user from a verified Clerk JWT (never from a client-supplied identifier), and all service queries I inspected filter by the authenticated `user_id`. There is no per-tenant redesign needed at the ownership-model level. The `user_id`-as-tenant model is correct for this product and should be kept.

The real multi-tenant gaps are concentrated in the **shared/global learning layer** — exactly where you suspected:

1. **`email_templates` are global and can bake one user's personal text (e.g. their name) into skeletons** that then live in a shared table, and the 40-per-sender cap is shared across all users. This is a known, documented single-user trade-off that must be fixed before launch.
2. **The merchant mapper promotes personal P2P payees into `global_merchants`** — user A's "RAHUL SHARMA" becomes a globally searchable merchant that user B can see via `GET /merchants`. This is the single worst cross-user PII exposure in the system today.
3. **Any single user's review confirms or demotes a global AI merchant mapping for everyone** (dissent threshold = 1, explicitly marked "revisit for multi-tenant" in the code).
4. **The backend connects to Postgres as the `postgres` superuser** with RLS effectively decorative, and Supabase's Data API surface is an unaudited second door.
5. **No per-user quotas or rate limits** — one user with a huge mailbox consumes the shared AI budget, Tavily quota, and queue throughput.
6. **Go-to-market gate, not code:** Gmail's `gmail.readonly` is a *restricted* OAuth scope. Beyond 100 test users, Google requires app verification and (typically) an annual CASA security assessment. This is the actual blocker between today and 10,000 users and needs to be planned like a feature.

My core recommendations: keep templates **global but PII-scrubbed at induction with per-user provenance**; move **P2P mappings out of the global merchant table into user-owned rows**; keep sender rules and true-brand merchants global; add a **non-superuser DB role + default-deny RLS as a safety net** (not as the primary enforcement); add per-user quotas and an isolation test suite. Nothing here requires a tenants table, per-tenant schemas, or any org/workspace machinery.

---

## 2. Existing Architecture (as verified from the implementation)

**Topology.** One npm-workspaces monorepo. A single Cloudflare Worker serves both the React 19 SPA (static assets, SPA fallback) and the Express 5 API under `/api/*` (`run_worker_first`). Supabase Postgres is reached through PgBouncer (transaction pooling, port 6543) using the `postgres` role. Cloudflare Queues (`money-manager-email-sync` + DLQ, `max_batch_size: 1`, 5 retries) carries all async jobs. One cron trigger (`*/10 * * * *`) drives email-sync dispatch and claims nightly jobs from the `job_schedules` table (`for update skip locked`).

**Auth.** `requireAuthenticatedUser` (backend/src/lib/auth.ts) verifies the Clerk RS256 JWT against the JWKS (cached, kid-rotation aware), checks issuer/audience/expiry, derives the app user UUID **deterministically (UUIDv5-style, SHA-1 over a fixed namespace + Clerk `sub`)**, and upserts the `users` row on every request. The email comes only from the verified token (the `x-user-email` header path was removed). `req.auth.userId` is the sole identity input to every service. The raw Clerk `sub` is *not* stored anywhere.

**Data access.** No client-side Supabase: the frontend calls only the backend API with a Clerk bearer token. The backend creates a fresh `postgres.js` client per HTTP request (`runWithSqlClientLifecycle`, max 5 connections, `prepare: false`) and tears it down at response end; queue/cron paths reuse a module-cached client.

**Database.** 31 tables. User-owned tables all have `user_id uuid not null references users(id) on delete cascade`. The baseline schema (March 2026) plus the hardening migration built genuinely unusual (in a good way) tenant discipline:

- Composite uniques `(id, user_id)` on `oauth_connections`, `accounts`, `raw_emails`, `financial_events`, `transactions`, `user_credit_cards`.
- Composite FKs so a transaction's `financial_event_id`, `account_id`, `credit_card_id`, and a raw email's `oauth_connection_id` **cannot reference another user's row by construction**.
- Triggers enforcing category scope (`category_id` must be the same user's or a system category) on `transactions` and `user_merchant_rules`, card/account consistency, and card account-type.
- Sensible per-user leading indexes (`idx_transactions_dashboard (user_id, txn_date desc)` etc.).
- `raw_emails` uniqueness is already per-mailbox: `(user_id, source_id)`.

**Global tables:** system `categories` (user_id null), `global_merchants` (+ aliases + per-type category defaults), `sender_rules`, `email_templates`, `merchant_logos` (+ aliases + lookup attempts), `merchant_mapping_attempts`, `usage_counters`, `job_schedules`.

**Pipeline.** Cron → dispatcher scans ACTIVE/DORMANT connections (keyset pagination, page 1000, per-connection sync leases with expiry) → per-user fetch jobs → metadata-first Gmail fetch with a DB-exhaustive sender gate (bodies of non-allowlisted senders are never downloaded) → normalizer (learned template match first, else masked/batched AI, then template induction with self-verification) → immutable `financial_events` + mutable `transactions`. Cross-email dedup uses `pg_advisory_xact_lock` on a per-user dedup key. Nightly AI jobs: sender classification and merchant mapping, both global, both capped (~200 items/day), both filtered so the model cannot inject rows outside the input set.

**Secrets & crypto.** Google OAuth tokens AES-GCM encrypted at rest with a dedicated 32-byte key; OAuth `state` HMAC-signed; card PDF passwords encrypted; internal ops endpoints on the public Worker URL guarded by a shared secret header with a timing-safe compare. Secrets deploy via `wrangler secret put`.

**Observability.** Cloudflare Workers observability enabled; structured console logs; `ai_usage_events`/`ai_call_runs` (metadata-only AI ledger, user-scoped); `usage_counters` for daily mapper activity; admin dashboard gated to one hardcoded user UUID (`AI_USAGE_ADMIN_USER_ID` in wrangler.jsonc); non-admins get 404.

**What does not exist:** server-side caching, API rate limiting, per-user quotas, account-deletion endpoint, RLS enforcement (backend is superuser), audit trail beyond AI ledgers, error-reporting service.

---

## 3. Important Risks in the Current System

Ordered by severity for a multi-user launch.

| # | Risk | Where | Severity |
|---|------|-------|----------|
| R1 | Personal P2P payee names become globally visible merchants (searchable by all users via `GET /merchants`) | `merchant-mapper.ts` (no user filter on key collection; P2P type creates `global_merchants` rows) | **Critical (PII)** |
| R2 | Template skeletons can embed a user's personal text (name in bank-boilerplate greetings); stored globally; shared 40/sender cap and 3-strikes counters | `lib/email-templates.ts`, `email_templates` | **Critical (PII) / High (correctness)** |
| R3 | Single-user confirm/dissent flips global merchant mappings for everyone (threshold 1; code comment says "revisit as voting for multi-tenant") | `transactions.service.ts:1754–1791` | High |
| R4 | Backend runs as `postgres` superuser; RLS flags are inconsistent (enabled on ~14 tables, off on the rest) and bypassed anyway; Supabase Data API (PostgREST) surface not audited | DB config | High (defense-in-depth gap) |
| R5 | No per-user quotas/fairness: mailbox size, AI spend, Tavily lookups, queue time are first-come-first-served; global nightly caps (~200/day) starve at scale | workers | High at scale |
| R6 | Gmail restricted-scope verification (CASA) not planned; hard cap of 100 users until done | Google Cloud project | **Blocking for >100 users** |
| R7 | `raw_emails.clean_text` keeps full financial-alert bodies in plaintext indefinitely for PROCESSED rows; pre-gate legacy bodies still present (documented gap) | `raw_emails` | Medium (data-at-rest exposure surface) |
| R8 | `users.email` is `unique not null` and upserted on every request: a Clerk email collision/change to an existing address makes provisioning 500 for that user | `auth.ts`, `users` | Medium |
| R9 | New `postgres.js` client per HTTP request (TLS + auth handshake per request; max 5 conns each) — latency and pooler pressure at scale | `app.ts`, `db/client.ts` | Medium at scale |
| R10 | Account deletion: no endpoint; cascades exist, but globally learned artifacts derived from a user's data (templates, P2P merchants) survive deletion | — | Medium (privacy/compliance) |
| R11 | Sender-classifier sends digit-masked *subjects* of gated (unclassified-sender) personal mail to the AI provider — fine single-user, needs privacy-policy cover multi-tenant | `sender-classifier.ts` | Medium (policy, not code) |
| R12 | No API rate limiting on a public Worker URL; auth is the only gate | routes | Medium |
| R13 | Migration bookkeeping is manual (`psql -f` untracked; one provenance migration is unsafe to rerun — documented) | ops | Medium |
| R14 | Admin = one hardcoded UUID in deploy config; job schedules and internal ops affect all users | wrangler.jsonc | Low (fine now, formalize later) |
| R15 | Manual `POST /transactions` has no idempotency key (client retry can duplicate) | transactions route | Low |

Not risks (verified fine): IDOR on CRUD routes (all queries scope by authenticated `user_id`; IDs are UUIDv4 — unguessable and ownership-checked anyway), SQL injection (postgres.js tagged templates everywhere), token handling, dedup/lease/job-claim race handling, queue idempotency (status checks + unique constraints), frontend cache isolation (per-browser, per-session).

---

## 4. The Tenant Model: One User = One Tenant

**Definition.** A tenant is exactly one row in `public.users`. Its identity is the deterministic UUID derived from the Clerk `sub`. Every piece of user-created or user-derived data hangs off `users.id` via `on delete cascade`. There is no organization, workspace, team, membership, or sharing concept, and none should be added.

**Is a separate `tenants` table needed? No.** A `tenants` table with a 1:1 `users` join would add a join to every query and a concept with no behavior. Direct `user_id` ownership is the right call at this scale and product shape.

**What `user_id`-as-tenant costs you later (eyes-open assessment).** If the product ever needs households/shared budgets (the only plausible evolution for a personal-finance app), the migration is mechanical precisely *because* ownership is uniform: introduce `tenants(id)`, backfill one tenant per user, add `tenant_id` alongside `user_id` (or rename semantically). The things that would make that migration painful are the things to avoid now:

- **Never embed `user_id` in externally visible, durable identifiers** (URLs, export filenames, storage paths that outlive the account). Currently fine.
- **Keep identity resolution in exactly one place** (the auth middleware). Currently true — keep it that way.
- **Store the raw auth-provider subject.** Today the Clerk `sub` is hashed into the UUID and discarded. Add `users.auth_subject text unique` (plus `auth_provider` if you like). Without it, migrating off Clerk, debugging auth issues, or proving which Clerk account owns which row requires reverse-engineering a SHA-1 derivation. This is the one genuinely foundational addition to the identity model. The UUID namespace constant (`2d0ec934-…`) must be treated as **permanently frozen** — changing it orphans every row in the database; add a code comment saying so.

---

## 5. Data Ownership & Isolation Model

Two ownership classes only. Every table must be explicitly in one, and every *new* table must declare its class in the migration comment.

**User-owned (tenant column `user_id not null`, cascade delete, every query filters by it):** `accounts`, `oauth_connections`, `user_credit_cards`, `card_statements`, `raw_emails`, `financial_events`, `transactions`, `transaction_relationships`, `user_merchant_rules`, `user_merchant_rule_categories`, `user_merchant_category_rules`, `user_hidden_categories`, `monthly_budgets`, `user_category_budget_rules`, `recurring_series`, `budget_suggestion_dismissals`, `ai_usage_events`, `ai_call_runs`, user categories (`categories` with `user_id`).

**System/global (no tenant column; users read, only the system or an admin writes):** system `categories`, `global_merchants` + `global_merchant_aliases` + `global_merchant_category_defaults` (**minus P2P — see §5.2**), `sender_rules`, `email_templates` (**with provenance + scrub — see §5.1**), `merchant_logos` + `merchant_logo_aliases` + `merchant_logo_lookup_attempts`, `merchant_mapping_attempts`, `usage_counters`, `job_schedules`, `users` itself (the registry).

The mixed `categories` table (system rows `user_id is null`, user rows `user_id not null`, `categories_scope_chk` enforcing the pairing, per-user hide overlay in `user_hidden_categories`) is a **good pattern — keep it.** It is exactly the "system defaults + user overrides" shape, already enforced by constraint and by the scope triggers.

### 5.1 Templates — the decision you asked about

**How it works today.** A template is a masked-email skeleton keyed by exact sender address, learned automatically from each successful AI answer, self-verified against its source email, capped at 40 per `from_address` with least-recently-useful eviction, disabled after 3 invalid matches. Templates never store amounts, dates, categories, or digits (everything numeric is masked to placeholders before the model ever sees it, and resolution happens per-email). The documented known gap: **alpha-only personal text — most concretely the account holder's name in "Dear Vasu," boilerplate — bakes into the skeleton.**

**Evaluation of your four options:**

| Option | Security | AI cost | Storage | Query simplicity | Maintainability |
|---|---|---|---|---|---|
| A. System-provided (hand-curated, global) | Clean | Zero marginal | Tiny | Same as today | **Bad** — you'd hand-maintain hundreds of bank formats; the whole point of the system is that it learns them |
| B. User-owned templates | Clean by construction | **Bad** — every user re-pays one AI call per format for the same HDFC/ICICI emails; at 10k users that multiplies the model bill by roughly the user count for the long tail | ~40/user/sender worst case | One extra filter | Fine |
| C. System + user copy/customize | N/A — users never see or edit templates; there is nothing to customize | — | — | — | N/A for auto-learned artifacts |
| D. **Global templates + PII-safe induction + provenance (recommended)** | Clean after scrub | **Optimal** — one AI call per format *ever, across the whole user base*; this is the economic engine of the product | Tiny (≈ true format count per sender) | Same as today | Good |

**Recommendation (D), concretely:**

1. **Scrub personal alpha text at induction.** Before a skeleton is saved: wildcard any span matching the learning user's profile signals (name parts from the Clerk token, email local-part) — a new anonymous word wildcard (reuse the `{{W}}` machinery for alpha spans). Additionally, apply a targeted salutation rule ("Dear X," / "Hi X," patterns → wildcard X), since greeting lines are where names live in bank mail. Self-verification already guarantees a scrubbed template still reproduces the answer.
2. **Refuse-to-learn as the fallback**: if a candidate skeleton still contains the user's profile tokens after scrubbing, don't save it — the format simply stays on the AI path (the system is fail-open by design; this costs pennies, not correctness).
3. **Add `learned_from_user_id uuid null references users(id) on delete set null`** (provenance, audit, and deletion policy). Keep `learned_from_raw_email_id` as-is.
4. **Keep the 40-per-sender cap global.** Your worry that global caps become invalid as users grow is correct *only while names bake into skeletons* (each user mints private variants of the same format and they fight over 40 slots). Once induction is PII-clean, per-sender variant count converges to the sender's true format count (empirically ~1–10; today's whole table is 201 rows across ~68 senders). The cap is then a genuine per-sender bound, not a per-user quota in disguise. Raise it to 60 if you want headroom; do not make it per-user.
5. **Keep hit/failure counters global** — a template that mis-extracts for anyone should be disabled for everyone (the email falls through to AI either way; there is no user-visible failure mode). This shared-fate is a feature, not a bug: 3 bad matches out of thousands of hits is signal regardless of which mailbox produced it.
6. **One-time cleanup:** delete (or re-verify against scrub rules) all 201 existing templates before launch. They will relearn within one AI call per format. Cheap, and it retires the entire legacy-PII question.

This decision is **foundational** (the cost model, privacy posture, and cap semantics all hang off it); the scrub heuristics themselves are implementation details that can evolve.

### 5.2 The same problem elsewhere — full sweep of shared state

I checked every global table for the same "one user's data leaks into or constrains everyone" shape:

- **`global_merchants` type `P2P` — must become user-owned.** Today the mapper collects unmapped `financial_events.search_key`s across *all* users, and when the model says "this is a person," it creates a global merchant named after that person plus a global alias. Any user can then see the name via merchant search, and a second user whose transactions hit the same key gets auto-linked to it. Personal payees are tenant data, full stop. **Recommendation:** the mapper still classifies P2P (it needs to, to avoid mis-branding people as companies), but the *write* changes: instead of a global merchant, write a `user_merchant_rules` row (display name = the person's name) for **each user who actually has events with that key**, keyed per user. Exclude type `P2P` from `GET /merchants` search results immediately (one-line mitigation), and migrate/delete existing P2P global rows. Note this is also *more correct*: `rahulsharma@okicici` is one person, but "RAHUL SHARMA" as a display name maps to different people for different users, and `canonical_name unique` would otherwise collide.
- **`global_merchants` confirm/dissent (R3).** Replace threshold-1 with proportional friction: confirmation requires reviews from **N distinct users** (N=2 is enough at this scale; keep N=1 until user count > ~10 via config), dissent demotes only when dissents from distinct users reach a similar bar, and track `confirmed_by_user_ids`/dissent counts per user (a tiny `merchant_confirmations(merchant_id, user_id, verdict)` table, unique per pair, replaces the raw counters). A single user — malicious or just wrong — must not be able to flip global auto-verify behavior. Their *own* view is already protected: a user's correction always writes their `user_merchant_rules` row, which outranks the global mapping for them.
- **`sender_rules` — keep global.** Bank/fintech sender identity is genuinely universal, AI rules are exact-address only, and users cannot write rules directly, so the poisoning surface is the AI classifier, which is input-filtered and re-asks DENYs. One user's mail *can* cause an ALLOW that affects everyone — but an ALLOW only admits mail from that sender into each user's own pipeline, gated per-mailbox; worst case is a wasted body fetch that downstream rejects. Acceptable.
- **`merchant_mapping_attempts` / `merchant_logo_lookup_attempts` / `usage_counters` — keep global.** Pure cost-control/ops state about *keys and brands*, not users. (Search keys do transit here for P2P counterparties; once P2P keys are excluded from the mapper's global path per above, exclude them from attempts too.)
- **`merchant_logos` — keep global.** Public brand artwork in a public-read bucket; no tenant data. Note the bucket write path requires the Storage API key, which lives only in scripts today — keep `SUPABASE_SECRET_KEY` out of the Worker env if the Worker doesn't need it.
- **`job_schedules` — keep global, admin-only.** Correct as-is.
- **`ai_usage_events`/`ai_call_runs`** are already user-scoped with an admin-only read path. Fine.
- **Cross-tenant AI batching:** the nightly jobs aggregate multiple users' keys/senders into one prompt. With P2P removed and digits masked, remaining payloads are brand names and sender addresses + masked subjects (R11) — defensible, but the privacy policy must say that sender addresses/subjects of unrecognized senders are processed by an AI provider. The per-email normalizer batches are already **grouped per user** (verified) — keep that invariant; never mix two users' email content in one model call.

---

## 6. Proposed Database Design

**Principle: the current schema is the design.** Changes are additive and small:

1. `users.auth_subject text unique` (+ backfill from Clerk for the 2 existing users) — §4.
2. Drop the `users.email` unique constraint (keep an index) **or** handle the conflict path explicitly (§3 R8). Recommendation: drop uniqueness — email is a display/contact attribute here, not an identity key; identity is `auth_subject`/`id`. Foundational-ish because provisioning 500s are lockouts.
3. `email_templates.learned_from_user_id uuid` (§5.1).
4. `merchant_confirmations(merchant_id, user_id, verdict, created_at, unique(merchant_id, user_id))`; migrate `confirmed`/`dissent_count` to derived values (§5.2).
5. P2P migration: delete/convert existing P2P `global_merchants` + aliases into per-user `user_merchant_rules`; add a check or partial index making the intent explicit (e.g. mapper inserts `type <> 'P2P'` only).
6. **Keep** composite-FK discipline as the standing rule for every future user-owned table (write it into CONTRACT.md): `user_id not null → users cascade`, `(id, user_id)` unique when other user-owned tables will reference it, cross-references via composite FKs, per-user leading columns on hot indexes.
7. **Indexes for scale:** the ones that matter at 10k users are already user-leading. Two to add when raw volume grows (not now): `raw_emails(user_id, status)` partial for per-user pipeline queries, and a BRIN or partitioning story for `raw_emails`/`financial_events` after ~50M rows (§9). Explicitly *not now*: partitioning, at 23 MB, would be pure ceremony.
8. **Retention column instead of body forever:** `raw_emails.body_purged_at timestamptz` + a scheduled purge setting `clean_text=''` for PROCESSED/terminal rows older than N days (recommend 90; templates learn at processing time, so bodies are only needed for reprocessing windows and debugging). Also run the documented one-time purge for pre-gate IGNORED/UNRECOGNIZED rows. This converts the biggest data-at-rest liability (R7) and the biggest storage line into a bounded window.

**Identifiers.** UUIDs everywhere (v4 `gen_random_uuid()` for rows, deterministic UUID for users) — correct; keep. No sequential IDs anywhere user-visible, so no enumeration surface. Money stays `bigint` paise. Timestamps `timestamptz`. All good, no changes.

**Quota/limit data.** Per-user quotas (§9) get one small table: `user_usage_daily(user_id, day, emails_fetched, ai_calls, ai_tokens, tavily_lookups, primary key (user_id, day))` — upserted from the pipeline, read by dispatch to defer over-quota users. Mirrors the existing `usage_counters` pattern, but tenant-scoped.

---

## 7. Security Model

**Layer 1 — API authorization (primary, exists).** Clerk JWT → `req.auth.userId` → every query filters by it. This remains the enforcement layer. Two hardening rules to adopt as convention: (a) services must take `userId` as an explicit parameter and include it in the `where` of every statement touching a user-owned table — already true everywhere I checked (57 usages in transactions.service alone); make it a review checklist item and a test obligation (§12); (b) no endpoint may accept a `user_id` in path/query/body — currently none do; keep it that way.

**Layer 2 — Database (defense-in-depth, mostly missing).**
- **Stop connecting as `postgres`.** Create an `app_backend` role with CRUD on `public` tables + `usage` on needed schemas, no DDL, no bypass. Cost: one migration + connection-string change. This bounds SQL-injection blast radius (even though none exists today), bad-migration blast radius, and accidental cross-schema writes.
- **Close the second door.** The Worker uses only the pooler; PostgREST/Data API and the anon key are unused. Either disable the Data API for the project (Supabase dashboard) or make RLS default-deny meaningful: today 14 tables have RLS enabled (blocking anon by default) and 17 don't — including `transactions`, `raw_emails`, `accounts`, `users`. **Enable RLS on every table with no public policies** (deny-all for API roles), and grant the backend role either `bypassrls` or blanket policies. This is the cheap version of RLS: it protects against the *other* access paths without threading `set local app.user_id` through PgBouncer transaction pooling.
- **Full per-user RLS (policies keyed on a per-transaction GUC) — evaluated, not recommended now.** It would make the DB enforce tenancy even against backend bugs, but with transaction pooling every request would need `BEGIN; set local…; …; COMMIT` wrapping across ~20 service files, meaningfully complicating a codebase whose app-layer scoping is disciplined and testable. Revisit if the team grows or if a scoping bug ever actually ships. This is an implementation detail that can evolve — the deny-all groundwork above is the part worth doing now.

**Layer 3 — Transport/perimeter.** Add Cloudflare rate-limiting rules on `/api/*` (zero code): e.g. 300 req/min/IP general, 20 req/min on `/oauth/*`. Keep internal ops secret-gated (already timing-safe); rotate the secret at launch and move `AI_USAGE_ADMIN_USER_ID` from a committed var to a secret if you consider admin identity sensitive.

**Secrets.** Already good (wrangler secrets, AES-GCM for tokens, HMAC state, encrypted PDF passwords). Two notes: `backend/.dev.vars` holds production-shaped secrets on a dev machine — ensure it stays gitignored (it is) and rotate anything ever pasted into docs; `SUPABASE_SECRET_KEY` should not ship to the Worker unless a Worker path needs Storage writes.

**Sensitive-data handling.** Body-retention purge (§6.8); logs already avoid content (verified changelog + spot checks; `EMAIL_PIPELINE_TRACE` gates verbose tracing — ensure it stays off in production and never logs bodies even when on); AI ledger is metadata-only by design; masking keeps digits from providers. The remaining flows to *document for users* rather than change: sender addresses + masked subjects to the AI classifier (R11), Gmail `snippet` transit (documented gap #3).

**Session/authn.** Clerk handles sessions; the backend is stateless bearer-token verification with issuer/audience/exp/nbf/skew checks — solid. `CLERK_JWT_AUDIENCE` is configurable but make sure it's actually set in production so tokens minted for another Clerk app of yours can't cross.

---

## 8. Safe Handling of Defaults, Settings, Templates, Quotas, Overrides

The system already has the right layered-resolution pattern in its identity graph — typed user rule > AI pick > typed global alias default > system generic — and per-user hides over system categories. Generalize the principle: **system data is never mutated per user; user intent is always an overlay row in a user-owned table.** Checklist for every future feature: default lives in a global table or code constant; user override lives in a `user_*` table keyed `(user_id, subject)`; resolution reads override-then-default; deletion of the user removes only overlays. Every current feature (categories, merchant categories, budget rules, hidden categories) already follows this. Limits move from global constants to per-user where they govern user resources (template cap stays global per §5.1; sync lookback, AI daily spend, mailbox fetch caps become per-user-configurable constants with a global default).

---

## 9. Performance & Scalability to ~10,000 Users

Back-of-envelope at 10k active users: ~30 emails/user/day → **300k raw_emails/day (~110M rows/yr)**, of which maybe 10–20% become transactions (~15–20M transactions/yr). Dispatch = 10k sync jobs/10min ≈ 17 msg/s sustained — well inside Cloudflare Queues limits. Postgres handles this volume comfortably on a mid Supabase tier *if* the retention purge (§6.8) keeps `clean_text` bounded; the row counts alone are fine with the existing user-leading indexes, with table partitioning as a >50M-row deferred option.

Actual bottlenecks, in the order they'll bite:

1. **Google OAuth verification cap (R6)** — at 100 users, not a performance issue but the binding constraint. Start the verification/CASA process months before you need it.
2. **Gmail API quota** — ~5–10 quota units per message get; 300k emails/day ≈ 1.5–3M units/day plus metadata fetches. Default per-project quota requires an increase request; per-user rate limits (250 units/s/user) are fine. Plan the quota request with the verification work.
3. **AI spend and nightly caps** — the 200-key/day mapper and 200-sender/day classifier caps are global; at 10k users the unknown-key arrival rate exceeds them and the system "heals" too slowly. Make caps scale with backlog (e.g. min(2000, backlog)) and batch into multiple calls; the input-filtering design already supports it. Add the per-user AI quota table (§6) so one huge mailbox can't starve others — dispatcher defers over-quota users to the next day rather than failing them.
4. **DB connections (R9)** — per-request client creation costs a TLS handshake and pooler auth per request (tens of ms of tail latency) and multiplies concurrent pooler slots. At 10k users' interactive load this is the first *latency* item. Options: (a) module-cached client for HTTP like the queue path already does (small code change; Workers isolates make lifetime management the risk the current design avoids); (b) **Cloudflare Hyperdrive** in front of Supabase (built for exactly this; no code-shape change; recommended); (c) raise `DB_MAX_CONNECTIONS` carefully within pooler limits. Evolve-safely item — measure first.
5. **Queue shape** — single queue, `max_batch_size 1`, all job types multiplexed means a burst of email-sync jobs delays nightly jobs behind it. At 10k users, split queues by class (sync vs nightly vs backfill) or at least raise consumer concurrency; Cloudflare handles this in config, not code. Per-connection leases already prevent duplicate concurrent syncs.
6. **Worker limits** — queue consumers get generous wall-clock but bounded CPU; the AI normalizer's 45s request timeout and batch-of-250 processing fit today; watch CPU-time metrics as batches grow.

Caching: none needed server-side at this scale (reads are per-user and indexed; the client caches aggressively). If analytics queries get hot later, materialized per-user monthly rollups are the first move, not a cache tier. Pagination: transactions are paged (100/page); keep keyset-style ordering as row counts grow (current `txn_date desc` + offset pages are fine into the millions per user, which no user will reach).

---

## 10. Reliability & Failure Handling

Existing and good (verified): queue retries (5×) + DLQ; poison/transient error taxonomy; per-email failure isolation in AI batches (omitted email → FAILED visibly, never silently dropped); AI tier fallback with 402/429-triggered rotation and cooldown; per-connection sync leases with expiry (crash recovery); `for update skip locked` job claiming (double-fire safe); idempotent ingestion (`(user_id, source_id)` unique, `(raw_email_id, extraction_index)` unique, statement-cycle unique indexes); status-guard idempotency on reprocessing; advisory-lock dedup making concurrent duplicate alerts deterministic; dormancy guard; kill switches (`NORMALIZATION_KILL_SWITCH`, merchant-mapping flag, `EMAIL_TEMPLATES` off-switch).

Gaps to close (all small):

- **DLQ handling is a dead end today** — nothing consumes `money-manager-email-sync-dlq`. Add an alert (queue depth > 0) and a documented redrive path (the requeue endpoint pattern already exists).
- **Idempotency for manual writes (R15):** accept an optional `Idempotency-Key` on `POST /transactions` (unique per user+key, 24h) — cheap, and mobile networks will retry.
- **Backpressure to Gmail/AI:** per-user quota deferral (§9.3) doubles as backpressure. The fetcher should also honor Gmail 429/`rateLimitExceeded` with exponential backoff at the job level (retry via queue is already the mechanism; verify the error is classified transient, not poison).
- **Backups/restore:** Supabase daily backups exist on paid tiers; for financial data at launch enable **PITR** and write down the restore runbook (target: restore-to-staging drill once). At 23 MB this is a checkbox; decide before data grows. Restoration granularity is whole-DB — per-user "undelete" is served by the soft-delete design (`user_deleted_at`, REVERSED events), not backups.
- **Deploy safety:** migrations are hand-run via pooler with no tracked history (R13). Adopt `supabase migration up` (or a one-table `schema_migrations` you own) so applied-set is queryable; expand-migrate-contract for breaking changes; feature flags already exist as env switches — fine at this team size.

---

## 11. Account Lifecycle, Deletion, Retention

- **Signup:** auto-provisioning on first authenticated request (exists). Add `auth_subject` capture.
- **Deletion (new endpoint, required for launch):** `DELETE /account` → (1) revoke Google token at Google + delete `oauth_connections`; (2) `delete from users where id = …` — cascades wipe all 19 user-owned tables by construction (this is where the composite-FK discipline pays off); (3) delete `email_templates where learned_from_user_id = …` **for pre-scrub templates only** (post-scrub templates carry no personal text; keeping them is fine and beneficial — decide in §17 Q6); (4) delete the Clerk user via backend API; (5) write a minimal tombstone log (user UUID + timestamp, no PII) for support disputes. Export-before-delete (CSV of transactions) is a nice-to-have, not v0.
- **Retention:** email bodies purged per §6.8; metadata rows retained (they're the dedup/audit spine); AI ledger retained (metadata-only). Backups age out on the provider schedule — deletion is therefore "live-system deletion"; state that plainly in the privacy policy.

---

## 12. Testing & Validation Strategy

The suite (205+ backend tests) covers pipeline logic well. What's missing is **adversarial tenancy proof**:

1. **Cross-tenant matrix test (the important one):** a fixture creating users A and B with full object graphs, then for *every* authenticated route: call as A with B's resource IDs in every ID-shaped slot (path, query, body FKs like `account_id`, `category_id`, `credit_card_id`, `relationship targets`, `restore`, `review`) and assert 404/400 and, crucially, **no row mutation** (snapshot B's rows before/after). Table-driven so a new route fails CI until it's enrolled.
2. **Constraint tests:** attempted cross-user inserts at the SQL layer (bypassing services) must be rejected by the composite FKs/triggers — proves the DB backstop actually backstops.
3. **Pipeline isolation tests:** two users' pending emails in one normalizer run → assert per-user AI batch grouping, per-user persistence, template learn/apply across users only via scrubbed skeletons (a fixture email with "Dear TESTNAME" must not save TESTNAME into a skeleton).
4. **Global-influence tests:** P2P key never creates a global merchant; single-user dissent doesn't demote below threshold; hidden-category resolution stays per-user (some of this exists — extend).
5. **Quota tests:** over-quota user deferred, others unaffected.

---

## 13. Observability & Operations

Keep it proportional: Cloudflare observability + structured logs + the existing user-scoped AI ledger are 80% of what 10k users need. Add: (a) log discipline — every log line that concerns a tenant carries `user_id` (UUID only, never email/subject/body); grep-audit existing `console.*` calls once; (b) three alerts — DLQ depth > 0, `mv_stale_pending_extraction` non-empty (the MV + cron already exist, nothing reads it), and daily AI spend > threshold (data already in `ai_usage_events`); (c) an error-reporting hook (Sentry or Workers Analytics Engine) when you have real users — optional now; (d) the admin dashboard already gives funnel/spend/retry health — point it at per-user rollups when quotas land. Audit records: reviews/rule-writes are implicitly audited by row timestamps + classification_source; a dedicated audit table is enterprise ceremony this product doesn't need.

---

## 14. Alternatives Considered (and rejected)

| Alternative | Why not |
|---|---|
| Separate `tenants` table now | A join and a concept with zero behavior; 1:1 backfill later is mechanical if ever needed (§4) |
| Per-tenant schemas / databases | Absurd at 10k individual users; kills migrations, pooling, cross-user learning (which is the product's cost engine) |
| Per-user templates | Multiplies the AI bill by user count for identical bank formats; the global+scrub design gets isolation without paying that (§5.1) |
| Full per-user RLS as primary enforcement | `set local` threading through transaction pooling across every service for marginal gain over a disciplined, tested app layer; deny-all RLS net + non-superuser role capture most of the value (§7) |
| Supabase Auth instead of Clerk | Migration cost with no tenancy benefit; Clerk JWT verification is already clean and stateless |
| Moving email bodies to object storage | Purge-after-processing (§6.8) removes the need; revisit only if reprocessing windows must grow |
| API gateway / microservices / server cache tier | The Worker+Express monolith with per-user indexed reads is the right size; nothing here is CPU- or fan-out-bound at 10k users |

---

## 15. Risk Register

| ID | Risk | Likelihood | Impact | Mitigation | Owner decision needed |
|----|------|-----------|--------|------------|----------------------|
| R1 | P2P payee PII in global merchants | Certain (already happening) | High | §5.2 P2P user-scoping + search filter + data migration | Q2 |
| R2 | Name-baked global templates | Certain for name-bearing formats | High | §5.1 scrub + provenance + relearn wipe | Q1 |
| R3 | Single-user global mapping flips | Medium | Medium | §5.2 per-user confirmations table + threshold config | Q3 |
| R4 | Superuser DB access / open second door | Latent | High if ever exercised | §7 role + deny-all RLS + Data API off | Q4 |
| R5 | Resource starvation by heavy users | High at scale | Medium | §9 per-user quotas + backlog-scaled caps | Q8 |
| R6 | Google restricted-scope cap at 100 users | Certain | Blocking | Start verification/CASA early; budget it | Q5 |
| R7 | Plaintext bodies accumulate | Certain | Medium | §6.8 purge policy | Q7 |
| R8 | users.email unique lockout | Low | Medium (user lockout) | §6.2 | Q9 |
| R9 | Per-request DB handshake latency | High at scale | Medium | §9.4 Hyperdrive (preferred) | — (evolvable) |
| R10 | Deletion leaves derived artifacts | Certain until built | Medium | §11 deletion endpoint | Q6 |
| R13 | Untracked migrations | Medium | Medium | §10 migration bookkeeping | — (evolvable) |
| R15 | Duplicate manual transactions on retry | Low | Low | §10 idempotency key | — (evolvable) |

---

## 16. Decision Log

**Confirmed requirements (from your brief):** one user = one tenant; no orgs/teams/workspaces/memberships/enterprise IdP; ~10k-user comfort target; durable ownership/isolation foundations prioritized over code churn; no implementation until this document is approved.

**Recommendations (mine, awaiting your confirmation):** keep `user_id`-as-tenant with no tenants table; keep templates global with PII-scrubbed induction + provenance + pre-launch relearn wipe; move P2P mappings to user-owned rows and filter P2P from merchant search; per-user merchant confirmations with configurable thresholds; non-superuser DB role + deny-all RLS + Data API disabled; per-user daily quotas with deferral; body purge at 90 days; account-deletion endpoint per §11; Hyperdrive for connections; cross-tenant test matrix as a CI gate; `users.auth_subject` added and email uniqueness dropped.

**Assumptions (minor, reversible — flag if wrong):** (a) the product remains India-focused (INR, Indian bank formats) through this horizon; (b) Gmail remains the only mail source near-term; (c) "admin" remains you alone; (d) no regulatory regime beyond standard privacy expectations applies yet (no RBI/PCI-style obligations since you store alerts, not credentials/card numbers — last4/first4 only); (e) Supabase and Cloudflare remain the platforms; (f) the two current users' data may be freely migrated/cleaned by you.

**Unresolved — see §17.** Nothing in §17 has been decided; where the roadmap references them, the phase is contingent.

---

## 17. Questions Requiring Your Input

1. **Templates (Q1):** Approve *global + scrub + provenance + wipe-and-relearn* (§5.1)? The alternative worth naming is per-user templates at a multiplied AI cost; I recommend against it.
2. **P2P (Q2):** Approve moving P2P payees to user-owned rules and out of `global_merchants` (including deleting existing P2P rows)? Any objection to the interim one-liner (exclude P2P from merchant search) shipping first?
3. **Merchant voting (Q3):** OK with confirmations/dissents becoming per-user rows with a config threshold (1 today, 2 at >10 users)?
4. **DB hardening depth (Q4):** Approve non-superuser role + RLS-deny-all + Data API disabled? Or do you want full per-user RLS policies now despite the pooling complexity?
5. **Google verification (Q5):** When do you expect to exceed 100 users? Verification/CASA has real lead time and possibly real cost — is that acceptable, or should v0 explicitly target the ≤100-user test tier while this runs?
6. **Deletion policy (Q6):** On account deletion, do we also delete *post-scrub* templates learned from that user's mail (maximal position), or keep them (they contain no personal text and benefit everyone)? I recommend keep, with pre-scrub templates deleted.
7. **Body retention (Q7):** Is 90 days of `clean_text` retention after processing acceptable, or do you want shorter/longer (affects reprocessing/debug windows)?
8. **Quotas (Q8):** What's an acceptable per-user daily ceiling — emails fetched (e.g. 500/day) and AI spend (e.g. ₹2–5/day equivalent)? Rough numbers are enough; they become config.
9. **users.email (Q9):** OK to drop the unique constraint (identity moves to `auth_subject`)?
10. **Admin (Q10):** Stay with the single env-configured admin UUID through 10k users, or add a `users.role` column now? (I recommend the env var until a second operator exists.)
11. **Compliance posture (Q11):** Any jurisdiction/regulatory requirement you're aware of (data residency, DPDP Act obligations for India) that should shape retention/deletion beyond §11?

---

## 18. Phased Implementation Roadmap

Each phase is independently shippable; nothing starts until you answer §17.

**Phase 0 — Stop-the-bleed (days, before any new users):**
P2P filter on merchant search; wipe existing P2P global merchants (2-user data, trivial); template wipe-and-relearn *after* Phase 1 scrub lands (or immediately with `EMAIL_TEMPLATES=off` until then); rotate internal secret; disable Data API / verify anon paths blocked; run the legacy body purge.

**Phase 1 — Foundations (1–2 weeks of focused work):**
Template PII scrub + refuse-to-learn + `learned_from_user_id`; P2P → user-owned mapper write path; `users.auth_subject` + email-uniqueness decision; non-superuser role + deny-all RLS; `merchant_confirmations` table; migration bookkeeping. *Everything here is the "expensive to change later" set.*

**Phase 2 — Multi-user hardening (1–2 weeks):**
Cross-tenant test matrix in CI; per-user quota table + dispatcher deferral; backlog-scaled nightly caps; account-deletion endpoint; body-retention purge job; Cloudflare rate-limit rules; DLQ alert + redrive runbook; idempotency key on manual transactions.

**Phase 3 — Scale runway (as user count approaches 3 digits):**
Google verification/CASA process (start earliest — longest lead time; arguably Phase 1 in calendar terms); Gmail quota increase request; Hyperdrive; queue split/concurrency tuning; PITR + restore drill; error reporting; per-user rollups on the admin dashboard.

**Explicit non-goals at every phase:** orgs/teams/sharing, per-tenant infra, enterprise RBAC, full per-user RLS, partitioning, server cache tier — each has a named trigger condition in this doc rather than a place on the roadmap.

---

*Verification basis: baseline + all 30 subsequent migrations read; live DB inspected read-only (roles, RLS flags, row counts, key table definitions); auth middleware, DB client lifecycle, route table, and the transactions/merchants/oauth service scoping read in full or spot-checked; dispatcher/scheduler/normalizer/mapper worker paths inspected for tenancy behavior; wrangler config and env inventory reviewed (names only).*
