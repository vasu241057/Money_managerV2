# Money Manager — Context Transfer

> Last updated: July 10, 2026

---

## 1. Project Overview

Money Manager is a **personal finance management app** that automatically imports transactions from Gmail, extracts financial data, and presents a dashboard for tracking income, expenses, and account balances.

**Stack:** React 19 (Vite) + Cloudflare Workers (Express) + Supabase PostgreSQL + Clerk Auth + Google OAuth/Gmail API + OpenCode Go AI

**Design constraints that drive everything:** privacy (the user's mailbox must not leak to third parties) and AI cost. Personal email bodies are never downloaded from Gmail (sender gate, §6), digits never reach the AI provider (placeholder masking, §7), every AI extraction teaches a local template so a format only ever costs one model call (self-learning templates, §7), and per-item AI calls are replaced by daily batch jobs wherever possible (§8).

---

## 2. Repository Structure

```
money-manager/
├── frontend/          # React 19 + Vite + Tailwind CSS v4
├── backend/           # Cloudflare Workers (Express 5 + wrangler)
├── shared/            # Canonical types shared between frontend & backend
├── supabase/          # PostgreSQL migrations + seed data
├── docs/              # CONTRACT.md (data contract) + OPERATIONS.md (ops runbook)
└── package.json       # npm workspaces monorepo root
```

---

## 3. Infrastructure

| Service | Purpose | Details |
|---------|---------|---------|
| **Supabase** | PostgreSQL database | PgBouncer at port 6543. Project: `sfucyhewgqyawrsfuyfa`, region: ap-southeast-1 |
| **Cloudflare Workers** | Backend API + queue workers | Wrangler 4.69.0, Express 5 inside worker |
| **Clerk** | Authentication | JWT-based. JWKS: `big-mosquito-76.clerk.accounts.dev` |
| **Cloudflare Queues** | Async job processing | 1 queue: `money-manager-email-sync` (DLQ: `money-manager-email-sync-dlq`) |
| **Google OAuth** | Gmail access | Client ID: `611255820334-d25utocn5qima99l4o8hhe2346c732ee.apps...` |
| **OpenCode Go** | AI model access | `https://opencode.ai/zen/go/v1/chat/completions`. Email extraction + daily sender/merchant classification |

Cron triggers (wrangler.jsonc): `*/10 * * * *` (email sync dispatch) and `30 21 * * *` (= 03:00 IST — daily sender classification + merchant mapping).

---

## 4. Domain Model (Database Tables)

| Table | Purpose | Key Notes |
|-------|---------|-----------|
| `users` | App users, synced from Clerk | `id` = UUIDv5 from Clerk `sub` |
| `oauth_connections` | Google OAuth tokens | Tokens AES-GCM encrypted at rest. Status: ACTIVE/DORMANT/AUTH_REVOKED/ERROR_PAUSED |
| `accounts` | User bank/card accounts | Types: cash/bank/card/other |
| `categories` | Income/expense categories | `user_id` null = system category. Parent-child tree, RESTRICT delete |
| `user_credit_cards` | Card identity (first4 + last4 + label) | Links to `card` account |
| `global_merchants` | Canonical merchant name + type | Types: MERCHANT/TRANSFER_INSTITUTION/AGGREGATOR/P2P. Grown by the daily merchant-mapping job |
| `global_merchant_aliases` | Search key → merchant mapping | `search_key` is PK, canonical uppercase-alphanumeric format |
| `user_merchant_rules` | Per-user classification overrides | Unique per `(user_id, search_key)`. Written on review with `apply_rule`; consulted by BOTH extraction engines |
| `sender_rules` | **Exhaustive** sender allowlist | pattern + ALLOW/DENY + BANK/FINTECH/WALLET/OTHER + SEED/AI/USER. Seeded by migration; grown by daily AI job. **No inline fallback in code** — a load failure retries the sync |
| `raw_emails` | Gmail email metadata + (gated) text | Unique per `(user_id, source_id)`. Has `from_address` + `subject`. Status: PENDING_EXTRACTION/PROCESSED/IGNORED/UNRECOGNIZED/FAILED. Sender-gated rows are metadata-only (`clean_text=''`, IGNORED) |
| `financial_events` | **Immutable** transaction facts | Linked to raw_email. Status: ACTIVE/REVERSED |
| `transactions` | **Mutable** user-facing projection | Linked 1:1 to financial_event. Status: VERIFIED/NEEDS_REVIEW |

**Key rule:** `transactions` is the canonical entity name. The term "expenses" is deprecated.

---

## 5. API Routes

All routes under `/api/*`, authenticated via Clerk JWT (`Authorization: Bearer <token>`).

### Accounts
| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/accounts` | List user accounts |
| POST | `/accounts` | Create account |
| PUT | `/accounts/:id` | Update account |
| DELETE | `/accounts/:id` | Delete account |

### Categories
| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/categories` | List categories (user + system) |
| POST | `/categories` | Create category |
| PUT | `/categories/:id` | Update category |
| DELETE | `/categories/:id` | Delete category |

### Transactions
| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/transactions` | List with filters (date range, type, status, account, category) |
| POST | `/transactions` | Create manual transaction |
| PUT | `/transactions/:id` | Update transaction |
| POST | `/transactions/:id/review` | Review + classify. `apply_rule: true` writes a `user_merchant_rules` row that teaches both extraction engines |
| DELETE | `/transactions/:id` | Soft delete (status change) |

### OAuth (Google)
| Method | Route | Purpose |
|--------|-------|---------|
| POST | `/oauth/google/start` | Get Google consent URL (signed state) |
| POST | `/oauth/google/callback` | Exchange code for tokens, upsert connection |
| GET | `/oauth/google/connection` | Get connection status + email processing stats |
| DELETE | `/oauth/google/connection` | Revoke connection (clear tokens) |

### Internal
| Method | Route | Purpose |
|--------|-------|---------|
| POST | `/internal/requeue/pending` | Re-enqueue PENDING_EXTRACTION emails (secret-guarded: `AI_REQUIRES_WEBHOOK_SECRET` via `x-ai-webhook-secret` header; `?limit=`, default 1000, max 10,000) |

---

## 6. Email Sync Pipeline

### Phase 1: Dispatch (`email-sync-dispatcher.ts`)
- **Triggered by:** Cloudflare cron (`*/10 * * * *`), or manually via `curl http://localhost:8787/cdn-cgi/handler/scheduled`
- Scans all users with ACTIVE Google OAuth connections
- Dormancy guard: marks connections DORMANT if `last_app_open_date > 45 days`
- Enqueues per-user `EMAIL_SYNC_USER` jobs in batches of 100

### Phase 2: Fetch + Sender Gate (`email-sync-fetcher.ts`)
Two-phase, metadata-first fetch — personal email bodies never leave Gmail:

```
messages.list (IDs only; first sync capped to EMAIL_SYNC_INITIAL_LOOKBACK_DAYS, default 90)
   → messages.get?format=metadata&metadataHeaders=From&metadataHeaders=Subject
   → sender gate decision on the From header
       ├─ denied  → persist metadata-only row (from_address, subject, clean_text='', IGNORED)
       │            body is NEVER downloaded; row remains for dedupe + audit + later backfill
       └─ allowed → messages.get?format=full → sanitize body (max 50K chars) → PENDING_EXTRACTION
   → enqueue NORMALIZE_RAW_EMAILS for admitted rows
```

**Sender gate** (`lib/sender-gate.ts`): entries come from the `sender_rules` table (ALLOW rows) plus the `EMAIL_SENDER_ALLOWLIST_EXTRA` env escape hatch. Entry kinds by shape:
- contains `@` → exact address (`payments-noreply@google.com`)
- contains `.` → domain/subdomain suffix (`fi.money`)
- otherwise → **brand token, substring of the entire From header** (`federal` matches `federal.bank`, `federalbank.co.in`, and display name "Federal Bank")

Matching is deliberately permissive: a lookalike (`hdfc@gmail.com`) passes the gate, costs one body fetch, and is rejected downstream. A `sender_rules` load failure throws a transient error (retry) — there is **no inline fallback**; the DB is exhaustive.

### Phase 3: Normalize (`email-ai-normalizer.ts` — the single engine)
- Receives up to 250 raw email IDs per job
- **Single path: template match → else AI (masked) → learn.** Learned templates for the sender are tried first and resolve locally with zero AI cost; misses go to the model, and each successful single-transaction answer is induced back into a template (§7)
- Creates `financial_events` (immutable facts) + `transactions` (mutable projection)
- NEEDS_REVIEW transactions wait for human review (the per-transaction AI triage worker was retired July 9 — the daily merchant mapper + human review supersede it)

---

## 7. Normalization Engine (template → AI → learn)

The deterministic/heuristic engine was scrapped July 10 in favor of one
self-learning path in `email-ai-normalizer.ts`:

```
pending email
   → try sender's learned templates (most-hit first)   [zero AI cost]
       ├─ EXTRACT hit → resolve amount/date/last4/merchant locally → persist
       ├─ IGNORE hit  → mark IGNORED (recurring OTP/notification formats)
       └─ miss        → masked, batched AI call → persist
                          → induce a template from the answer → self-verify → save
```

### Self-learning templates (`lib/email-templates.ts` + `email_templates` table)

A template is the masked email generalized into a **skeleton** keyed by the
exact sender address. Marker language: `{{N}}` = number the AI referenced
(amount/date part/instrument; captured positionally), `{{n}}` = anonymous
number beside a referenced one, `{{W}}` = whole word containing unreferenced
numbers (refs, balances, phone numbers — wildcarded so varying digit runs
can't shift slot ordinals), `{{M}}` = the merchant span (wildcarded only when
it occurs exactly once at word boundaries; otherwise the AI's normalized name
is stored as a FIXED merchant). Static bank boilerplate stays literal — that
is what anchors the match. Whitespace is collapsed identically at learn and
match time.

Safeguards:
- **Self-verification:** before saving, the induced template is replayed
  against the very email it came from and must reproduce the AI's exact
  amount, timestamp, last4, and merchant. A template can never be worse than
  the answer that taught it.
- **IGNORE learning is guarded:** only from a well-formed empty answer AND
  only when the email contains no monetary language (₹/Rs/INR/debit/credit…).
- **Multi-transaction emails are never learned** (AI-only); a single-txn
  template can't mis-match them because matching is full-text anchored.
- **3-strikes disable:** a structural match that yields invalid values
  (`matched_invalid`) increments a failure counter; 3 failures → DISABLED.
  The email falls through to the AI path either way.
- **Per-sender cap 40** with least-recently-useful eviction; concurrent
  learners collapse via unique `(from_address, skeleton_hash)`.
- **Fail-open:** a template-table outage just means everything goes to AI.
- Template hits use the **same persist path** as AI answers: identity graph,
  dedup window, identity-based auto-verify all unchanged. Category on a
  template hit comes from the identity graph (user rule > alias default >
  system default) — never stored in the template.

Known trade-offs: month-name dates ("05-Jul-26") lock a template to that
literal month (an off-month email misses → AI → learns that variant; ≤12,
self-limiting); alpha-only personal text (e.g. the account holder's name in
boilerplate) bakes into skeletons — acceptable single-user, revisit before
multi-tenant.

### AI path privacy: indexed-placeholder masking (`lib/masking.ts`, default ON)

Before the model call, every numeric token becomes `<N1>`, `<N2>`, … — the provider never sees amounts, balances, account numbers, OTPs, or phone numbers, and uniform placeholder width hides magnitudes. The model answers with token references (`amount_token: "N1"`, `date_text: "<N3>-<N4>-<N5>"`, `instrument_token: "N2"`); everything is resolved locally:
- amounts via the deterministic engine's paise parser (rupees→paise is never the model's job)
- dates via a local parser for Indian bank formats (DD-MM-YY, DD-Mon-YYYY, "Mar 04, 2026", ISO; IST timezone)
- last4 from the token's digits

### Batching (masked mode only)

Pending emails are grouped per user and sent up to `AI_NORMALIZER_BATCH_SIZE` (default 4, max 10) per call as `=== EMAIL k ===` sections with **globally unique placeholder ids across the batch**. Response shape: `{ "emails": [ { "email": 1, "transactions": [...] } ] }`. Failure isolation: transient → whole queue message retries (processed emails protected by status checks); permanent → batch's emails marked FAILED; an email omitted from the response is failed visibly, never treated as "no transactions".

### Tier fallback

`AI_NORMALIZER_TIERS` (default `deepseek-v4-flash,deepseek-v4-pro,qwen3.7-plus`). HTTP 429 triggers **immediate** tier cooldown + rotation (`RateLimitedModelError`); other failures rotate after `AI_NORMALIZER_TIER_ROTATE_FAILURES`. Max `AI_NORMALIZER_MAX_RETRIES` total attempts.

### Identity graph (`lib/identity.ts`, shared by template + AI paths and review)

Search keys are canonical uppercase-alphanumeric (`toSearchKey`), identical across the template path, the AI path, and the review service — so `user_merchant_rules` and `global_merchant_aliases` written anywhere resolve everywhere. Category precedence in the AI path: user rule > AI's per-email pick (validated against the user's real category IDs; hallucinated IDs fall back to Miscellaneous) > global-alias default > system default.

### Auto-verify: identity-based, NOT confidence-based

A transaction auto-verifies only on identity signals — a `user_merchant_rules` hit or a `global_merchant_aliases` hit with a resolved category. Model self-reported confidence is stored (`ai_confidence_score`) for review-inbox triage ordering only. `TRANSFER_INSTITUTION` alias hits flip the type to `transfer`.

### Cross-email dedup (`lib/txn-dedup.ts`)

One payment often produces two alerts (bank + payment app). At persist time (template and AI hits alike): if another raw email already produced an ACTIVE event with the same user + amount + direction within ±`TXN_DEDUP_WINDOW_MINUTES` (default 10), the new transaction is forced to NEEDS_REVIEW — even a user-rule hit doesn't override it.

---

## 8. Daily AI Batch Jobs (03:00 IST cron)

Pattern: batch the day's unknowns into ONE AI call each, persist results, self-heal retroactively. Both jobs filter model output against the input set, so the model cannot inject arbitrary rows.

### Sender classification (`sender-classifier.ts`)
- Collects distinct gated sender addresses (last 14 days, max 200) not covered by any rule
- Sends **addresses only** (no subjects, no content) → classifies BANK/FINTECH/WALLET/OTHER
- Upserts `sender_rules`: AI rules are always **exact addresses** (a misclassification can never open a whole domain); DENY rules mean a sender is asked about once, ever
- For newly ALLOWed senders, enqueues `SENDER_BACKFILL` jobs: refetches full bodies by stored Gmail ID, flips rows to PENDING_EXTRACTION, enqueues normalization. A missed bank is a ≤24h delay, not a permanent hole

### Merchant mapping (`merchant-mapper.ts`)
- Collects distinct `financial_events.search_key`s (last 14 days, max 200) with no `global_merchant_aliases` entry
- Sends `{key, name}` pairs (names only, no amounts) → canonical brand + type ("FSN E-COMMERCE VENTURES" → Nykaa, "BUNDL TECHNOLOGIES" → Swiggy)
- Upserts `global_merchants` + aliases, then **relinks** recent merchant-less non-USER transactions so yesterday's extractions get named too

---

## 9. Environment Variables

All in `backend/.dev.vars` (see `.dev.vars.example`). Secrets for deployment go via `wrangler secret put`.

### Required (always)
| Variable | Purpose |
|----------|---------|
| `SUPABASE_POOLER_URL` | PostgreSQL connection (PgBouncer port 6543) |
| `CLERK_JWKS_URL` | Clerk JWT verification endpoint |
| `CLERK_JWT_ISSUER` | Clerk issuer URL |

### Required for Gmail
| Variable | Purpose |
|----------|---------|
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | OAuth client |
| `GOOGLE_OAUTH_REDIRECT_URI` | Must match Google Console exactly |
| `GOOGLE_OAUTH_STATE_SECRET` | HMAC-SHA256 key for state signing |
| `GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY` | 32-byte base64 AES-GCM key for token encryption |

### AI (OpenCode Go)
| Variable | Default | Purpose |
|----------|---------|---------|
| `OPENCODE_GO_API_KEY` | — | Required for all AI paths; missing key = skip with error log |
| `OPENCODE_GO_MODEL` | `deepseek-v4-flash` | Daily jobs model (sender classifier + merchant mapper) |

### Sender gate & sync
| Variable | Default | Purpose |
|----------|---------|---------|
| `EMAIL_SENDER_GATE` | on | `off` disables the gate (not recommended) |
| `EMAIL_SENDER_ALLOWLIST_EXTRA` | — | Comma-separated extra entries (token/domain/address shapes) |
| `EMAIL_SYNC_INITIAL_LOOKBACK_DAYS` | `90` | First-sync backfill cap |

### Normalization engine
| Variable | Default | Purpose |
|----------|---------|---------|
| `EMAIL_TEMPLATES` | on | `off` disables template matching + learning (debug/rollback only — everything goes to AI) |
| `AI_NORMALIZER_TIERS` | `deepseek-v4-flash,deepseek-v4-pro,qwen3.7-plus` | Comma-separated model chain |
| `AI_NORMALIZER_TIER_ROTATE_FAILURES` | `3` | Fails before rotating tier |
| `AI_NORMALIZER_TIER_COOLDOWN_MS` | `300000` | Cooldown on drained tier (5 min) |
| `AI_NORMALIZER_MAX_RETRIES` | `6` | Total retries across all tiers |
| `AI_NORMALIZER_TIMEOUT_MS` | `45000` | Per-request timeout |
| `AI_NORMALIZER_TEMPERATURE` | `0` | Deterministic output |
| `AI_NORMALIZER_MAX_TOKENS` | `0` (unset) | Max AI response length |
| `AI_NORMALIZER_MIN_CONFIDENCE` | `0.30` | Below this → item dropped |
| `AI_NORMALIZER_MASK_NUMBERS` | `true` | `<N#>` placeholder masking (set false only for debugging) |
| `AI_NORMALIZER_BATCH_SIZE` | `4` | Emails per model call in masked mode (max 10; 1 disables) |
| `AI_NORMALIZER_AUTO_VERIFY_CONFIDENCE` | `0.85` | Legacy; auto-verify is identity-based now |

### Dedup & safety switches
| Variable | Default | Purpose |
|----------|---------|---------|
| `TXN_DEDUP_WINDOW_MINUTES` | `10` | Cross-email duplicate window (0 disables) |
| `NORMALIZATION_KILL_SWITCH` | — | `true` skips all normalization |
| `AI_REQUIRES_WEBHOOK_SECRET` | — | Secret guarding the internal requeue endpoint |

---

## 10. How to Run Locally

```bash
npm run dev             # frontend (Vite :5173) + backend (wrangler :8787)
npm run dev:frontend    # individually
npm run dev:backend

# Trigger email sync manually (cron doesn't auto-fire in wrangler dev)
curl "http://localhost:8787/cdn-cgi/handler/scheduled"

npm run test            # frontend tests
npm run test:backend    # backend tests (vitest; 159 tests, all green as of Jul 10 2026)
cd backend && npm run typecheck
```

---

## 11. Key Files Reference

### Backend Workers
| File | Purpose |
|------|---------|
| `backend/src/index.ts` | Worker entry point (fetch, scheduled, queue handlers) |
| `backend/src/workers/scheduled.worker.ts` | Cron router: email-sync dispatch + daily sender/merchant jobs |
| `backend/src/workers/email-sync-dispatcher.ts` | Scan users, create EMAIL_SYNC_USER jobs |
| `backend/src/workers/email-sync-fetcher.ts` | Two-phase Gmail fetch, sender gate, raw email persistence, `runSenderBackfillJob` |
| `backend/src/workers/email-ai-normalizer.ts` | **The normalization engine**: template match → masked/batched AI → template learning; tier fallback, identity graph, persistence |
| `backend/src/workers/sender-classifier.ts` | Daily AI sender classification → sender_rules + backfill enqueue |
| `backend/src/workers/merchant-mapper.ts` | Daily AI merchant mapping → global_merchants/aliases + relink |
| `backend/src/workers/queue.worker.ts` | Queue message router + batch metrics |
| `backend/src/workers/queue.messages.ts` | Job validation + builders |
| `backend/src/workers/queue.errors.ts` | Poison/Transient error types |

### Backend Infrastructure
| File | Purpose |
|------|---------|
| `backend/src/lib/sender-gate.ts` | Sender allowlist matching + sender_rules loading |
| `backend/src/lib/masking.ts` | `<N#>` placeholder masking + local date/token resolution |
| `backend/src/lib/email-templates.ts` | Self-learning templates: skeleton induction, matching, self-verification, persistence |
| `backend/src/lib/identity.ts` | Shared identity graph: search keys, VPA/aggregator detection, rule/alias lookups, paise parser |
| `backend/src/lib/txn-dedup.ts` | Cross-email duplicate probe |
| `backend/src/lib/ai.ts` | AI config resolvers (normalizer tiers, engine, masking, batching) |
| `backend/src/lib/config.ts` | AppConfig from env vars |
| `backend/src/lib/auth.ts` | Clerk JWT verification + user auto-provisioning |
| `backend/src/lib/db/client.ts` | Request-scoped PostgreSQL client lifecycle |
| `backend/src/lib/http/json-body.ts` | JSON body middleware (limitBytes, INVALID_JSON/PAYLOAD_TOO_LARGE HttpErrors) |
| `backend/src/lib/infra.ts` | Queue names + cron constants |

### Database Migrations (apply in order; the seed is load-bearing)
| File | Purpose |
|------|---------|
| `20260308170000_baseline_schema.sql` | All tables, indexes, constraints |
| `20260308171000_seed_system_data.sql` | System categories + merchants + aliases |
| `20260308172000_schema_hardening.sql` | Same-user FK constraints |
| `20260308221000_raw_emails_user_source_uniqueness.sql` | Uniqueness → (user_id, source_id) |
| `20260309083000_dispatcher_index.sql` | Dispatcher scan index |
| `20260704090000_raw_emails_sender_metadata.sql` | `from_address` + `subject` on raw_emails |
| `20260705100000_sender_rules.sql` | sender_rules table |
| `20260707090000_seed_sender_rules.sql` | **Load-bearing seed** — without it the allowlist is empty and everything is gated |
| `20260707120000_daily_job_indexes.sql` | Indexes for sender-classifier / backfill / merchant-mapper scans |
| `20260710090000_email_templates.sql` | Self-learning template store (sender-keyed skeletons + slot maps) |

---

## 12. Known Gaps & TODOs

1. **Phase E superseded (July 10):** templates are now learned automatically from AI answers (§7) instead of being derived offline from accumulated mail. Remaining template follow-ups: consider a `<MON>` month wildcard for month-name dates; template-vs-AI agreement as an additional auto-verify signal; PII scrub of skeletons before any multi-tenant launch. Do **not** seed templates from `docs/EXHAUSTIVE_INDIAN_BANKING_ALGO.md` — it is untrusted AI-generated brainstorming, kept only as inspiration.

2. **Old data isn't retroactively cleaned.** Emails ingested before the sender gate still have bodies in `raw_emails.clean_text`. Conservative purge: `update public.raw_emails set clean_text = '' where status in ('IGNORED','UNRECOGNIZED') and clean_text <> '';`

3. **Gmail metadata responses include a ~100-char `snippet`** which we don't store or read for gated mail, but it does transit. No way to suppress server-side.

4. **No per-request training opt-out** — the dead `AI_NORMALIZER_NO_TRAINING` plumbing was removed (it was never sent). Data retention/training opt-out must be configured at the OpenCode Go account level; verify before commercial launch.

5. **Manual cron trigger in local dev** — Cloudflare Workers don't auto-trigger cron in `wrangler dev`. Run: `curl "http://localhost:8787/cdn-cgi/handler/scheduled"`

6. **Google OAuth tokens invalidate after password change** — connection goes AUTH_REVOKED and must be re-authorized.

7. **No rate limit dashboard** — queue monitoring logs exist but no dashboard (see `docs/OPERATIONS.md`).

---

## 13. Change Log (July 4–10, 2026 overhaul)

1. **Correctness:** hybrid engine no-op fixed (deferred terminal statuses); 429 → immediate tier rotation; AI category IDs validated against the user's real categories.
2. **Privacy/cost:** sender gate with metadata-first two-phase fetch (personal bodies never downloaded); `sender_rules` DB-exhaustive allowlist; content logging removed; `<N#>` placeholder masking (no digits reach the provider); masked-mode batching (default 4 emails/call).
3. **Learning loop:** canonical search keys unified across engines + review service; identity graph (user rules + global aliases) wired into the AI path; daily sender-classification and merchant-mapping AI jobs with backfill/relink self-healing.
4. **Robustness:** identity-based auto-verify (confidence is triage-only); cross-email dedup window; 90-day first-sync cap; per-email failure isolation in batches.
5. **Tests:** suite went from 118/176 green to all-green (revived the dead normalizer suites, implemented the real json-body middleware); 185/185 after the triage removal.
6. **Triage worker retired (July 9):** the per-transaction review-triage classifier (`ai-classifier.ts`), its `money-manager-ai-classification` queue, the `/internal/ai/requires` webhook route, and the `AI_AUTO_VERIFY_MIN_CONFIDENCE`/`AI_QUEUE_DELAY_SECONDS` env vars were all removed. NEEDS_REVIEW now resolves via human review + the daily merchant mapper. The webhook secret guard survives only to protect `/internal/requeue/pending`.
7. **Deterministic engine scrapped, self-learning templates added (July 10):** the heuristic engine and the hybrid orchestrator were deleted; identity helpers moved to `lib/identity.ts`. New `lib/email-templates.ts` + `email_templates` table: every masked AI answer is induced into a sender-keyed skeleton (self-verified against its source email before saving); matching runs before any model call, so each email format costs at most one AI call ever. IGNORE templates gate recurring OTP/notification mail. `NORMALIZATION_ENGINE` env replaced by `EMAIL_TEMPLATES` (on/off). Suite: 159 tests green.

---

## 14. End-to-End Flow (Happy Path)

```
1. User signs in via Clerk
2. User connects Google via OAuth (POST /oauth/google/start → consent → callback)
3. OAuth connection created with encrypted tokens, status ACTIVE
4. Cron triggers EMAIL_SYNC_DISPATCH → dispatcher enqueues EMAIL_SYNC_USER
5. Fetcher refreshes Google token, lists message IDs (first sync: last 90 days)
6. Fetcher gets headers only; sender gate admits bank/fintech senders
7. Admitted mail: full body fetched, sanitized, persisted PENDING_EXTRACTION
   Gated mail: metadata-only row, IGNORED, body never downloaded
8. Fetcher enqueues NORMALIZE_RAW_EMAILS
9. Engine tries the sender's learned templates first (local, zero AI cost);
   misses go to the model (masked, batched) and each single-transaction
   answer teaches a self-verified template for next time;
   financial_events + transactions created
10. Auto-verify on user-rule/alias hits; everything else NEEDS_REVIEW
    (cross-email duplicates always NEEDS_REVIEW)
11. User reviews; apply_rule teaches user_merchant_rules (both engines)
12. 03:00 IST: sender classifier widens the allowlist (+ backfills missed mail);
    merchant mapper names unknown counterparties (+ relinks recent transactions)
13. User sees transactions in dashboard
```
