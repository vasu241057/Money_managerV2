# Money Manager — Context Transfer

> Last updated: May 28, 2026

---

## 1. Project Overview

Money Manager is a **personal finance management app** that automatically imports transactions from Gmail, extracts financial data, and presents a dashboard for tracking income, expenses, and account balances.

**Stack:** React 19 (Vite) + Cloudflare Workers (Express) + Supabase PostgreSQL + Clerk Auth + Google OAuth/Gmail API + OpenRouter AI

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
| **Cloudflare Queues** | Async job processing | 2 queues: `money-manager-email-sync`, `money-manager-ai-classification` |
| **Google OAuth** | Gmail access | Client ID: `611255820334-d25utocn5qima99l4o8hhe2346c732ee.apps...` |
| **OpenRouter** | AI model access | For email extraction + transaction classification |

---

## 4. Domain Model (Database Tables)

| Table | Purpose | Key Notes |
|-------|---------|-----------|
| `users` | App users, synced from Clerk | `id` = UUIDv5 from Clerk `sub` |
| `oauth_connections` | Google OAuth tokens | Tokens AES-GCM encrypted at rest. Status: ACTIVE/DORMANT/AUTH_REVOKED/ERROR_PAUSED |
| `accounts` | User bank/card accounts | Types: cash/bank/card/other |
| `categories` | Income/expense categories | `user_id` null = system category. Parent-child tree, RESTRICT delete |
| `user_credit_cards` | Card identity (first4 + last4 + label) | Links to `card` account |
| `global_merchants` | Canonical merchant name + type | Types: MERCHANT/TRANSFER_INSTITUTION/AGGREGATOR/P2P |
| `global_merchant_aliases` | Search key → merchant mapping | `search_key` is PK |
| `user_merchant_rules` | Per-user classification overrides | Unique per `(user_id, search_key)` |
| `raw_emails` | Gmail email text, pending extraction | Unique per `(user_id, source_id)`. Status: PENDING_EXTRACTION/PROCESSED/IGNORED/UNRECOGNIZED/FAILED |
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
| POST | `/transactions/:id/review` | Review + classify (user action) |
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
| POST | `/internal/ai/requires` | AI webhook for async classification (secret-guarded) |

---

## 6. Email Sync Pipeline (3 Phases)

### Phase 1: Dispatch (`email-sync-dispatcher.ts`)
- **Triggered by:** Cloudflare cron (`* * * * *`), or manually via `curl http://localhost:8787/cdn-cgi/handler/scheduled`
- Scans all users with ACTIVE Google OAuth connections
- Dormancy guard: marks connections DORMANT if `last_app_open_date > 45 days`
- Enqueues per-user `EMAIL_SYNC_USER` jobs in batches of 100

### Phase 2: Fetch (`email-sync-fetcher.ts`)
- For each user: refreshes OAuth token with OCC (optimistic concurrency control)
- Calls Gmail API: `GET /gmail/v1/users/me/messages?q=after:{timestamp}`
- Deduplicates against existing `raw_emails` (skips non-FAILED duplicates)
- Fetches full message body for new messages
- Extracts clean text (plaintext > HTML > snippet, max 50K chars)
- Persists to `raw_emails` with status `PENDING_EXTRACTION`
- **IMPORTANT:** After persisting raw emails, it now enqueues `NORMALIZE_RAW_EMAILS` jobs (this was the gap that was fixed)

### Phase 3: Normalize (`email-normalizer.ts` orchestrator)
- Receives up to 250 raw email IDs per job
- **NEW:** Routes to AI engine or deterministic engine based on `NORMALIZATION_ENGINE` env var
- Creates `financial_events` (immutable facts) + `transactions` (mutable projection)
- Marks raw_emails as PROCESSED/IGNORED/UNRECOGNIZED/FAILED
- Enqueues `AI_CLASSIFICATION` jobs for NEEDS_REVIEW transactions

---

## 7. Normalization Engine System (NEW — May 2026)

### Architecture

```
NORMALIZE_RAW_EMAILS queue message
         │
         ▼
email-normalizer.ts (orchestrator)
         │
    reads NORMALIZATION_ENGINE
         │
    ┌────┼────────────┐
    │    │            │
    ▼    ▼            ▼
   AI   DETERMINISTIC HYBRID
```

### Engines

| Engine | File | How it works |
|--------|------|--------------|
| **AI** (`"ai"`) | `email-ai-normalizer.ts` | Sends raw email text to AI model via OpenRouter. Validates JSON response. Persists extracted transactions. | 
| **Deterministic** (`"deterministic"`) | `email-deterministic-normalizer.ts` | Original regex-based extraction engine. Parses INR amounts, keywords, payment methods from raw email text. | 
| **Hybrid** (`"hybrid"`) | `email-normalizer.ts` | Runs deterministic first. Collects FAILED + UNRECOGNIZED raw email IDs. Retries only those with AI engine. |

### AI Tier Fallback System

The AI engine uses a multi-tier model chain:

```
Tier 1: google/gemini-2.0-flash-001:free     (FREE, 200 req/day)
   ↓ 429 or 3 fails
Tier 2: deepseek/deepseek-chat-v3-0324       (PAID, $0.20/M input)
   ↓ 429 or 3 fails
Tier 3: meta-llama/llama-4-maverick          (PAID, $0.15/M input)
   ↓
Email marked FAILED (all tiers exhausted)
```

- Failed tiers go into **5-minute cooldown** before retrying
- Max **6 total attempts** across all tiers per email

### AI Prompt

The system prompt teaches the AI to extract:
- `direction`: debit/credit
- `amount_in_paise`: rupees converted to paise (₹100.50 = 10050)
- `txn_timestamp`: ISO-8601
- `payment_method`: upi/credit_card/debit_card/netbanking/cash/unknown
- `counterparty_raw`: merchant/person/bank name
- `instrument_last4`: last 4 digits of card/account
- `category_guess`: food/transport/shopping/utilities/entertainment/health/education/rent/salary/investment/transfer/insurance/travel/subscription/other
- `confidence`: 0.0-1.0

### Confidence → Status Mapping

| AI Confidence | Transaction Status | 
|---------------|-------------------|
| ≥ 0.85 | VERIFIED |
| 0.30 - 0.84 | NEEDS_REVIEW (→ AI classification queue) |
| < 0.30 | Raw email marked UNRECOGNIZED |

### Switching Engines

Change `NORMALIZATION_ENGINE` in `.dev.vars`:
```env
NORMALIZATION_ENGINE=ai            # AI only (default, current)
NORMALIZATION_ENGINE=deterministic  # Regex engine only
NORMALIZATION_ENGINE=hybrid         # Deterministic first, AI fallback
```

No restart needed if using `wrangler dev --live-reload`.

---

## 8. Environment Variables

All in `backend/.dev.vars`. Secrets for deployment go via `wrangler secret put`.

### Required (always)
| Variable | Purpose |
|----------|---------|
| `SUPABASE_POOLER_URL` | PostgreSQL connection (PgBouncer port 6543) |
| `CLERK_JWKS_URL` | Clerk JWT verification endpoint |
| `CLERK_JWT_ISSUER` | Clerk issuer URL |

### Required for Gmail
| Variable | Purpose |
|----------|---------|
| `GOOGLE_CLIENT_ID` | OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | OAuth client secret |
| `GOOGLE_OAUTH_REDIRECT_URI` | Must match Google Console exactly |
| `GOOGLE_OAUTH_STATE_SECRET` | HMAC-SHA256 key for state signing |
| `GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY` | 32-byte base64 AES-GCM key for token encryption |

### Required for AI
| Variable | Purpose |
|----------|---------|
| `OPENROUTER_API_KEY` | OpenRouter API key (currently empty — needs to be set) |
| `OPENROUTER_MODEL` | AI classifier model (default: `openai/gpt-4o-mini`) |

### Normalization Engine (NEW)
| Variable | Default | Purpose |
|----------|---------|---------|
| `NORMALIZATION_ENGINE` | `ai` | ai / deterministic / hybrid |
| `AI_NORMALIZER_TIERS` | `google/gemini-2.0-flash-001:free,deepseek/deepseek-chat-v3-0324,meta-llama/llama-4-maverick` | Comma-separated model list |
| `AI_NORMALIZER_TIER_ROTATE_FAILURES` | `3` | Fails before rotating tier |
| `AI_NORMALIZER_TIER_COOLDOWN_MS` | `300000` | Cooldown on drained tier (5 min) |
| `AI_NORMALIZER_MAX_RETRIES` | `6` | Total retries across all tiers |
| `AI_NORMALIZER_TIMEOUT_MS` | `15000` | Per-request timeout (15s) |
| `AI_NORMALIZER_TEMPERATURE` | `0` | Deterministic output |
| `AI_NORMALIZER_MAX_TOKENS` | `800` | Max AI response length |
| `AI_NORMALIZER_AUTO_VERIFY_CONFIDENCE` | `0.85` | Confidence threshold for auto-verify |
| `AI_NORMALIZER_MIN_CONFIDENCE` | `0.30` | Below this → UNRECOGNIZED |
| `AI_NORMALIZER_NO_TRAINING` | `true` | Opt out of AI training |

---

## 9. How to Run Locally

### 1. Start everything
```bash
npm run dev
# Starts both frontend (Vite :5173) and backend (wrangler :8787)
```

### 2. Or start individually
```bash
npm run dev:frontend    # http://localhost:5173
npm run dev:backend     # http://localhost:8787
```

### 3. Trigger email sync (manual — needed in local dev)
```bash
curl "http://localhost:8787/cdn-cgi/handler/scheduled"
```

### 4. Run tests
```bash
npm run test           # Frontend tests
npm run test:backend   # Backend tests
```

### 5. Typecheck
```bash
cd backend && npm run typecheck
```

---

## 10. Key Files Reference

### Backend Workers
| File | Purpose |
|------|---------|
| `backend/src/index.ts` | Worker entry point (fetch, scheduled, queue handlers) |
| `backend/src/app.ts` | Express app setup (middleware, routes, error handling) |
| `backend/src/workers/scheduled.worker.ts` | Cron → enqueue EMAIL_SYNC_DISPATCH |
| `backend/src/workers/email-sync-dispatcher.ts` | Scan users, create EMAIL_SYNC_USER jobs |
| `backend/src/workers/email-sync-fetcher.ts` | Gmail API calls, persist raw emails, enqueue normalize |
| `backend/src/workers/email-normalizer.ts` | **Orchestrator** — routes to AI or deterministic engine |
| `backend/src/workers/email-ai-normalizer.ts` | **NEW** — AI-based extraction with tier fallback |
| `backend/src/workers/email-deterministic-normalizer.ts` | Original regex extraction engine |
| `backend/src/workers/queue.worker.ts` | Queue message router + batch metrics |
| `backend/src/workers/queue.messages.ts` | Job validation + builders |
| `backend/src/workers/queue.errors.ts` | Poison/Transient error types |
| `backend/src/workers/ai-classifier.ts` | AI classification for NEEDS_REVIEW transactions |

### Backend Services
| File | Purpose |
|------|---------|
| `backend/src/services/oauth.service.ts` | Google OAuth start/callback/connection management |
| `backend/src/services/transactions.service.ts` | Transaction CRUD + review logic |
| `backend/src/services/accounts.service.ts` | Account CRUD |
| `backend/src/services/categories.service.ts` | Category CRUD |

### Backend Infrastructure
| File | Purpose |
|------|---------|
| `backend/src/lib/config.ts` | AppConfig from env vars |
| `backend/src/lib/ai.ts` | AI config resolvers (normalizer + classifier) |
| `backend/src/lib/auth.ts` | Clerk JWT verification + user auto-provisioning |
| `backend/src/lib/db/client.ts` | Request-scoped PostgreSQL client lifecycle |
| `backend/src/lib/db/serialization.ts` | Type-safe DB row → JS value coercion |
| `backend/src/lib/http/errors.ts` | HTTP error types + rendering |
| `backend/src/lib/http/validation.ts` | Request payload validation |
| `backend/src/lib/http/json-body.ts` | Express JSON body parser middleware |
| `backend/src/lib/infra.ts` | Queue names + cron constant |
| `backend/src/env.bindings.d.ts` | Non-generated env binding types |
| `backend/worker-configuration.d.ts` | Auto-generated Cloudflare types (queues, KV, etc.) |

### Shared
| File | Purpose |
|------|---------|
| `shared/types.ts` | Canonical types: DB rows, API DTOs, queue payloads, lifecycle guards |

### Config
| File | Purpose |
|------|---------|
| `backend/wrangler.jsonc` | Wrangler config: cron, queues, env vars |
| `backend/.dev.vars` | Local env vars + secrets (DO NOT COMMIT) |
| `backend/.dev.vars.example` | Template for .dev.vars |

### Database
| File | Purpose |
|------|---------|
| `supabase/migrations/20260308170000_baseline_schema.sql` | All tables, indexes, constraints |
| `supabase/migrations/20260308171000_seed_system_data.sql` | System categories + merchants + aliases |
| `supabase/migrations/20260308172000_schema_hardening.sql` | Same-user FK constraints |
| `supabase/migrations/20260308221000_raw_emails_user_source_uniqueness.sql` | Change uniqueness to (user_id, source_id) |
| `supabase/migrations/20260309083000_dispatcher_index.sql` | Dispatcher scan index |
| `supabase/seed.sql` | Initial seed data |
| `supabase/queries/` | Reference queries |

---

## 11. Known Gaps & TODOs

1. **OPENROUTER_API_KEY is empty** — must be set before AI normalizer works. If empty, normalization will skip with an error log.

2. **Gemini 2.0 Flash expires June 1, 2026** — the free tier model in the chain needs to be replaced with `google/gemini-2.5-flash:free` or another free model after that date.

3. **No AI zero-retention guarantee** — the `no-training` flag is set but OpenRouter logs cannot be guaranteed to be zero. Before commercial launch, consider a paid plan with confirmed zero-retention.

4. **Manual cron trigger in local dev** — Cloudflare Workers don't auto-trigger cron in `wrangler dev`. Must run: `curl "http://localhost:8787/cdn-cgi/handler/scheduled"`

5. **Supabase credentials exposed in repo** — `.dev.vars` has real database credentials. Ensure `.dev.vars` is in `.gitignore`. Rotate the password before going public.

6. **Google OAuth tokens invalidated after password change** — if the user changes their Google password, the OAuth connection goes AUTH_REVOKED and must be re-authorized.

7. **Deterministic engine merchants are limited** — the seed data has ~30 merchants. For broad coverage, the AI engine + gradual merchant database growth is the plan.

8. **No rate limit dashboard** — queue monitoring logs exist but no dashboard is set up (see `docs/OPERATIONS.md` for planned dashboards).

---

## 12. Recent Fixes (May 24-28, 2026)

1. **Empty `json-body.ts`** — was crashing the entire worker. Added `express.json()` middleware.

2. **`toNullableString` rejecting Date objects** — `max(re.internal_date)` returns a Date, not string. Bypassed the string coercion for datetime fields in `oauth.service.ts`.

3. **Manual cron trigger poisoning** — `controller.cron` is empty string when manually triggered. Added fallback to `EMAIL_SYNC_CRON` in `buildEmailSyncDispatchJob()`.

4. **Normalization pipeline gap** — raw emails were fetched but never normalized. Added `NORMALIZE_RAW_EMAILS` enqueue after raw email persistence in `email-sync-fetcher.ts`.

5. **Full AI normalizer system** — built the entire tier fallback, model calling, JSON validation, and persistence system (Steps 1-8 above).

---

## 13. End-to-End Flow (Happy Path)

```
1. User signs in via Clerk
2. User connects Google via OAuth (POST /oauth/google/start → consent → callback)
3. OAuth connection created with encrypted tokens, status ACTIVE
4. Cron triggers EMAIL_SYNC_DISPATCH
5. Dispatcher finds user, enqueues EMAIL_SYNC_USER
6. Fetcher refreshes Google token, calls Gmail API
7. Fetcher persists raw_emails (PENDING_EXTRACTION)
8. Fetcher enqueues NORMALIZE_RAW_EMAILS
9. Normalizer (AI or deterministic) extracts transactions
10. financial_events + transactions created
11. NEEDS_REVIEW transactions enqueued to AI_CLASSIFICATION_QUEUE
12. AI classifier processes them, updates status to VERIFIED
13. User sees transactions in dashboard
14. User can manually review, re-categorize, add notes
```
