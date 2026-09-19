# Money Manager Canonical Contract (Milestone 1)

Version: `v1.1`
Frozen on: `2026-03-08`

This document is the canonical contract for schema, enums, DTOs, and lifecycle behavior.
It is intentionally aligned to `shared/types.ts` and the SQL blueprint.

## 1. Naming Resolution (Final)

- Canonical mutable projection entity is **`transactions`**.
- The term **`expenses` is deprecated** and must not be used in new APIs, services, queues, or UI state models.
- Any legacy "expense" concept maps to `transactions`:
  - legacy expense row -> `transactions` row
  - legacy expense status -> `transactions.status`

## 2. Canonical Table Contracts

The following are source-of-truth entities and corresponding TS interfaces in `shared/types.ts`:

- `users` -> `UserRow`
- `oauth_connections` -> `OauthConnectionRow`
- `accounts` -> `AccountRow`
- `categories` -> `CategoryRow`
- `user_credit_cards` -> `UserCreditCardRow`
- `global_merchants` -> `GlobalMerchantRow` (`owner_user_id` null = shared brand, set = that user's private payee)
- `global_merchant_aliases` -> `GlobalMerchantAliasRow` (`id` primary key; `search_key` unique among shared aliases and per owner among owned ones; `owner_user_id` mirrors the merchant's owner)
- `global_merchant_category_defaults` -> `GlobalMerchantCategoryDefaultRow`
- `user_merchant_rules` -> `UserMerchantRuleRow`
- `user_merchant_rule_categories` -> `UserMerchantRuleCategoryRow`
- `raw_emails` -> `RawEmailRow`
- `mv_stale_pending_extraction` -> `StalePendingExtractionRow`
- `financial_events` -> `FinancialEventRow`
- `transactions` -> `TransactionRow`
- `v_credit_card_transactions` -> analytics-friendly card transaction view

Column names and enum values are intentionally identical to SQL.

## 3. Enum Contract (DB-Exact)

- `OAuthSyncStatus`: `ACTIVE | DORMANT | AUTH_REVOKED | ERROR_PAUSED`
- `AccountType`: `cash | bank | card | other`
- `CategoryType`: `income | expense | transfer`
- `MerchantType`: `MERCHANT | TRANSFER_INSTITUTION | AGGREGATOR | P2P`
- `RawEmailStatus`: `PENDING_EXTRACTION | PROCESSED | IGNORED | UNRECOGNIZED | FAILED`
- `EventDirection`: `debit | credit`
- `PaymentMethod`: `upi | credit_card | debit_card | netbanking | cash | unknown`
- `FinancialEventStatus`: `ACTIVE | REVERSED`
- `TransactionType`: `income | expense | transfer`
- `TransactionStatus`: `VERIFIED | NEEDS_REVIEW`
- `ClassificationSource`: `USER | SYSTEM_DEFAULT | HEURISTIC | AI`
- `CardNetwork`: `visa | mastercard | rupay | amex | diners | other`

## 4. Status Lifecycle (Operational Rules)

Implemented as transition guards in `shared/types.ts`:

- `OAUTH_SYNC_STATUS_TRANSITIONS`
- `RAW_EMAIL_STATUS_TRANSITIONS`
- `FINANCIAL_EVENT_STATUS_TRANSITIONS`
- `TRANSACTION_STATUS_TRANSITIONS`

### Key lifecycle decision

The earlier phrase `REQUIRES_AI` is **not** a DB transaction status in v1 schema.
Unknown classifications are stored as `transactions.status = 'NEEDS_REVIEW'` and routed to AI asynchronously via queue (`AiClassificationJobPayload`).

Materialized view refresh for stale extraction alerts is scheduled via `pg_cron` every 10 minutes:
- `refresh-stale-mv` -> `SELECT public.refresh_mv_stale_pending_extraction();`

## 5. API DTO Contract (v1)

Declared in `shared/types.ts`:

- Pagination: `PageRequest`, `PaginatedResponse<T>`
- Listing: `ListTransactionsQuery`, `TransactionFeedItem`
- Account CRUD: `CreateAccountRequest`, `UpdateAccountRequest`
- Category CRUD: `CreateCategoryRequest`, `UpdateCategoryRequest`
- Manual writes: `CreateManualTransactionRequest`, `UpdateTransactionRequest`
- Review loop: `ReviewTransactionRequest`, `ReviewTransactionResponse`
- Card mapping writes: `CreateUserCreditCardRequest`, `UpdateUserCreditCardRequest`
- Card resolver: `ResolveUserCreditCardRequest`, `ResolveUserCreditCardResponse`
- Merchant overrides: `UpsertUserMerchantRuleRequest`
- OAuth: `GoogleOAuthStartResponse`, `GoogleOAuthCallbackRequest`, `GoogleOAuthConnectionResponse`
  - `GoogleOAuthConnectionStatusResponse`
- Admin AI operations: `AiUsageSummaryResponse`, `AiUsageDashboardResponse`, `AiUsageEvent`, `AiUsageModelBreakdown`, `AiProcessingSummary`, `AiProcessingRun`, `AiProcessingAttempt`
- Internal AI webhook: `AiRequiresWebhookRequest`, `AiRequiresWebhookResponse`

Transaction classification invariants:

- Money direction is immutable after creation. A `debit` financial event can
  be `expense` or `transfer`; a `credit` event can be `income` or `transfer`.
- `transactions.category_id`, when present, must reference a category whose
  `type` equals `transactions.type`. A category type change is rejected when
  it would invalidate a transaction or typed merchant default.
- Manual creation accepts an optional `direction`; it defaults to `credit` for
  income and `debit` otherwise. Transfer creation should send the user's
  explicit Money out (`debit`) or Money in (`credit`) choice.
- PATCH and review accept `type`, but cannot cross the direction matrix above.
- Review rules keep one merchant/search identity and store category defaults
  separately per transaction type.

### Merchant visibility (multi-tenant, Task 4a)

- A merchant is visible to a user when `global_merchants.owner_user_id` is
  null (shared brand) or equals that user (their private payee).
- `GET /merchants` returns only visible merchants. `GET /merchants/mine` and
  `GET /merchants/:merchantId/transactions` are scoped by the caller's own
  transactions and therefore never show another user's payee.
- Every write that accepts a `merchant_id` rejects an invisible merchant with
  `404 MERCHANT_NOT_FOUND` before writing: `POST /transactions`,
  `PATCH /transactions/:transactionId`, `POST /transactions/:transactionId/review`,
  `PUT /merchants/:merchantId/category`, `PUT /merchants/:merchantId/descriptor-category`.
- Database triggers back this up: `transactions`, `user_merchant_rules`,
  `user_merchant_category_rules` and `recurring_series` refuse a
  `merchant_id` owned by another user; an alias always carries its
  merchant's owner.
- Alias resolution during extraction and review filters the same way and
  prefers the user's own alias over a shared one on the same key.
- The mapper writes brands once as shared rows and a person once per user who
  pays that key, as that user's private payee (owned merchant + owned alias).
  A key one user already resolved as a person is given to a new payer without
  a model call (`merchant_mapping_attempts.last_resolution = 'P2P'` +
  `person_display_name`).

### Email template scope (multi-tenant, Task 3)

- `email_templates.scope` is `PRIVATE` (applies only to
  `learned_from_user_id`'s mail) or `SHARED` (owner null, applies to
  everyone). A template starts PRIVATE to the user whose email taught it and
  is promoted to SHARED only when a different user's email independently
  induces the same skeleton with the same structural slot map (slot map
  minus `confidence`); the PRIVATE rows are then replaced by one SHARED row.
- Uniqueness: one SHARED row per `(from_address, skeleton_hash)`, one
  PRIVATE row per `(from_address, skeleton_hash, learned_from_user_id)`.
- Skeleton marker `{{S}}`: the addressee span of a leading greeting
  (Dear/Hi/Hello/Namaste … up to `,` `!` `:`), matched as a bounded span and
  never stored. A FIXED merchant name is learned only when the counterparty
  text occurs in the body (two-plus occurrences); zero occurrences refuse
  to learn (`merchant not anchored in body`).
- Caps: `EMAIL_TEMPLATE_PRIVATE_CAP` per (sender, owner) and
  `EMAIL_TEMPLATE_SHARED_CAP` per sender, default 40.

### Merchant confirmation voting (multi-tenant, Task 4b)

- `merchant_confirmations(merchant_id, user_id, verdict CONFIRM|DISSENT)`
  holds one verdict per user per AI-sourced merchant. A review that keeps the
  alias-suggested merchant is that user's CONFIRM; one that replaces or clears
  it is their DISSENT; a later review by the same user replaces their row.
- `global_merchants.confirmed` and `dissent_count` are caches recomputed on
  every verdict: `confirmed = distinct CONFIRM users >= threshold and distinct
  DISSENT users < threshold`, `dissent_count = distinct DISSENT users`.
- Threshold: `MERCHANT_CONFIRMATION_THRESHOLD` (positive integer, default 1).
- A private payee only takes its owner's verdict (DB trigger).

### Account deletion and email body retention (multi-tenant, Task 5)

- `DELETE /account` with body `{ "confirm": "DELETE" }` (exact word, else
  `400 CONFIRMATION_REQUIRED`) erases the caller's account and answers `204`.
  Order: revoke the Google grant at `oauth2.googleapis.com/revoke` for every
  stored token (best-effort); then in one transaction delete
  `merchant_mapping_attempts` rows whose `search_key` occurs only in this
  user's `financial_events`, delete the user's `email_templates` with
  `scope = 'PRIVATE'` (SHARED rows carry no owner and stay), delete the
  `users` row so every user-owned table cascades (including their private
  payees and aliases), and write the tombstone; finally delete the Clerk user
  through the Backend API with `CLERK_SECRET_KEY` (best-effort).
- `user_deletion_log(user_id pk, deleted_at, clerk_deleted, google_revoked,
  auth_provider, auth_subject, clerk_delete_attempts, clerk_last_attempt_at,
  clerk_last_error)` holds no email or name. `auth_subject` (the opaque
  provider id) is kept only while `clerk_deleted = false` and is nulled the
  moment the provider deletion succeeds (check constraint).
- Pending provider deletions are retried by the nightly `raw_email_body_purge`
  job. If the person signed in again before the retry (the auth middleware
  re-provisions the same id), the tombstone is dropped instead of deleting
  their live sign-in.
- A queue job that started before a deletion and now hits a foreign key into
  `users` acks: the fetcher and sender backfill as a poison message, the
  normalizer by skipping that user's remaining emails.
- Retention: the `raw_email_body_purge` job (queue type
  `RAW_EMAIL_BODY_PURGE`, scheduled in `job_schedules`, ships disabled at
  03:30 IST) sets `clean_text = ''` and `body_purged_at = now()` on
  `PROCESSED | IGNORED | UNRECOGNIZED` rows whose
  `coalesce(processing_completed_at, created_at)` is older than
  `RAW_EMAIL_BODY_RETENTION_DAYS` (default 90), in bounded batches. `FAILED`
  rows keep their body so they stay reprocessable. Every query that
  recognises a sender-gated row by `status = 'IGNORED' and clean_text = ''`
  (sender-classifier candidates and backfill, the fetcher's backfill job, the
  retroactive gate, the statement harvest) also requires
  `body_purged_at is null`, so a purged body is never mistaken for a
  never-fetched one.

### Quotas and fairness (multi-tenant, Task 6)

- `user_usage_daily` records message IDs listed and bodies fetched by user and
  `Asia/Kolkata` calendar day. `USER_DAILY_EMAIL_FETCH_CAP` (default 500)
  independently caps both counters, so sender-gated mail cannot create an
  unlimited metadata scan. `user_limits.daily_email_fetch_cap` overrides that
  ceiling for one user. Counts include repeated listings; counters and raw
  rows commit together before downstream enqueue.
- The fetcher checks before each Gmail page and shrinks `maxResults` to the
  remaining allowance. At a cap it saves the next page token, original query
  window and highest timestamp on the connection; the timestamp cursor stays
  put, no continuation is enqueued, and the lease is released. The dispatcher
  skips capped users until the next IST day (or an increased override).
  Reconnection/disconnection clears parked state. Invalid parked tokens are
  cleared and the job retries from the durable timestamp cursor.
- Sender backfill uses the same fetch allowance and counters. Capped rows
  stay gated and a delayed queue message retries them after midnight IST.
- `USER_DAILY_AI_CALL_CAP` (default 200), overridden by
  `user_limits.daily_ai_call_cap`, counts attempts in `ai_usage_events`,
  including retries and fallback models. Every attempt checks the ledger;
  a local attempt budget also bounds a job if telemetry writes fail. No
  second AI counter is maintained. Deferred emails remain `PENDING_EXTRACTION`;
  local template matches can still finish without an AI call.
- These are fair-use guardrails checked against committed usage. Concurrent
  jobs already in flight can overshoot by their in-flight work; strict
  distributed reservation/billing enforcement is not provided.
- Fresh `EMAIL_SYNC_USER` jobs requeue at most 250 pending rows older than
  30 minutes, oldest first, at most once per rolling hour per user (including
  across midnight), and never while AI usage is capped. Failed enqueue
  releases the claim. Recovery follows the existing dispatcher eligibility
  rules: active users with eligible Google connections.
- `MAX_MERCHANTS_PER_RUN` / `MAX_SENDERS_PER_RUN` default to 200, each taking
  `min(cap, backlog)` candidates in batches of at most 40. Sender rechecks
  share the same cap with new senders. Known-person fan-out is separately
  bounded by the merchant cap and does not call a model.
- Admin-only `GET /ai-usage/users?day=YYYY-MM-DD` returns up to 200 users,
  ordered by activity, with daily counters, cost, effective caps and current
  pending/deferred counts. Historical days use current cap settings.
  Admin-only `PUT /ai-usage/users/:userId/limits` accepts positive integers
  up to 1,000,000 or null for either cap; null restores the default and omitted
  fields remain unchanged. Other authenticated users receive 404. Settings
  -> AI operations exposes today's rollup and the override editor.
- Both quota tables cascade on account deletion and use the standard RLS /
  backend-role hardening. The migration seeds no usage or overrides.

### Perimeter and operations hardening (multi-tenant, Task 7)

- **Rate limits.** 300 requests per minute per authenticated user, charged
  right after the token verifies and before the provisioning upsert; 60 per
  minute per client IP for requests without a bearer token, for every
  `/oauth/*` call, and for a request whose token fails verification. Over the
  limit is `429 RATE_LIMITED` with `Retry-After: 60`. Both limiters fail
  open: a missing or failing binding never rejects a request. Cloudflare
  counts per colo, not globally, so these are abuse controls rather than
  exact ceilings.
- **Idempotency.** `POST /transactions` accepts an optional
  `Idempotency-Key` header (printable ASCII, at most 255 characters). The key
  is scoped to the caller: the same key from two users is two independent
  keys. The same key with the same body replays the original status and body
  with `Idempotent-Replay: true` and writes nothing. The same key with a
  different body is `409 IDEMPOTENCY_KEY_REUSED`; a key whose first request
  is still running is `409 IDEMPOTENCY_KEY_IN_FLIGHT`. Bodies are compared by
  SHA-256 over canonical JSON, so field order does not matter. A failed
  handler releases the key so the caller can retry. A claim abandoned by a
  dying worker is reclaimable after 5 minutes; every row is purged 24 hours
  after it was created. No header means today's behaviour exactly.
- **Authorized parties.** When `CLERK_AUTHORIZED_PARTIES` is set
  (comma-separated origins), the token's `azp` claim must be one of them, and
  a token with no `azp` is rejected. Unset means not enforced.
  `CLERK_JWT_AUDIENCE` stays optional: Clerk's default session tokens carry
  `azp`, not `aud`.
- **Dead letters.** `money-manager-email-sync-dlq` has a consumer that logs
  `DLQ_MESSAGE` (queue, message id, attempts, job type and identifiers — never
  payload content) and acks. It never retries: there is no queue behind it.
- **Stale extraction alerting.** Every 10-minute tick reads
  `mv_stale_pending_extraction` and logs `STALE_PENDING_EXTRACTION` when any
  user has rows. A failed read logs `STALE_PENDING_EXTRACTION_CHECK_FAILED`
  and never costs the tick its dispatch.
- **Ownership class for every new table.** Every table is one of three
  classes, chosen and stated in its migration:
  - **user-owned** — `user_id uuid not null references public.users(id) on
    delete cascade`. Every FK to another user-owned table is composite
    (`(id, user_id)`) or carries a same-user trigger, and every read filters
    by the authenticated user. This is the default; pick it unless one of
    the other two clearly applies.
  - **shared** — no `user_id`, readable by every tenant, written only by
    backend jobs or an operator. A shared table must hold **no personal
    data**: anything keyed to a person belongs in a user-owned table, or in
    an optionally-owned one (`global_merchants.owner_user_id` null = shared
    brand, set = that user's private payee — the Task 4a pattern).
  - **tombstone** — survives the user row, so it cannot carry an FK to it
    (`user_deletion_log`). It must hold the minimum needed for the job it
    finishes and drop that as soon as the job succeeds.
  - Every class: the migration calls
    `public.grant_app_backend('public.<table>')` and ends with
    `select public.assert_tenancy_backstops();`. A table that skips the
    helper has RLS without a policy and reads zero rows as `app_backend` —
    the assert fails the migration instead of shipping it (A.2 §6).
  - **Two deliberate exceptions exist; do not copy them without the same
    argument.** `merchant_mapping_attempts` is shared and, since Task 4b,
    stores `person_display_name` — a person's name on a shared row. It is
    keyed by the search key, and that key *already* identifies the person to
    anyone who can read the table, so caching the resolved name beside it
    adds no exposure that the key did not already carry; it buys a new payer
    of the same key their payee with no model call and no cooldown (A.4 §4).
    Account deletion removes the row when its last referencing user goes.
    `user_deletion_log` holds `auth_subject` until the provider-side delete
    succeeds, then nulls it (a check constraint enforces that), because
    `users.id` cannot be reversed into the subject. Neither is reachable
    from any user-facing endpoint; the admin mapping tab is the only place a
    person name from the first one is visible, which is acceptable while
    the admin is the sole operator and must be stated in the privacy policy
    (A.2 §12).
- **`sender_rules.source = 'USER'` is admin-only.** No user-facing endpoint
  writes `sender_rules`; the table is global, so a user-supplied allowlist
  entry would change every tenant's gating. Entries are written by the
  nightly classifier (`source = 'AI'`) or by an operator in SQL. A per-user
  allowlist, if ever wanted, is a new user-owned table, not a column here.
- **What reaches the AI provider.** Three paths, with different guarantees —
  stated exactly, because the privacy policy is written from this list.
  - *Normalization* sends the email `subject` and `clean_text`. While
    `AI_NORMALIZER_MASK_NUMBERS` is on (the default), every digit run is
    replaced by an `<N#>` placeholder of uniform width first, so no amount,
    balance, account number, card number, OTP or phone number leaves us, and
    magnitudes are hidden too; the model answers with token references that
    are resolved locally. Turning that flag off sends the body verbatim,
    digits included — it is a debugging switch, not a supported mode.
    **Masking removes digits and nothing else**: non-numeric text in the
    body still goes as written, so a name or an email address the bank
    prints in its own copy does reach the provider in either mode.
  - *Sender classification* sends the sender address and up to a few recent
    subject lines, always digit-masked (`maskSubjectForClassifier` replaces
    every digit with `#`) and never conditionally. Bodies are never sent on
    this path.
  - *Merchant mapping* sends the merchant search key, which for a
    person-to-person payment can be a person's name or a VPA. When
    `TAVILY_API_KEY` is set the same payee string alone is sent to web
    search — never amounts, users or email content.
  - Provider calls are ledgered in `ai_usage_events` with token counts and
    latency, never content.
- **What reaches the logs.** Identifiers, counts, lengths and enum values
  only. Sender addresses (`fromAddress`) are logged because they are bank
  senders and drive triage; counterparty names, amounts, subjects and email
  bodies are not. The one content-bearing log, `PIPELINE_TRACE_RESOLVED`,
  requires `EMAIL_PIPELINE_TRACE` and is refused outright when
  `NODE_ENV=production`.

### Auth context for backend CRUD (Milestone 4)

- Authenticated API calls use Clerk bearer tokens:
  - `Authorization: Bearer <session_jwt>` (required)
  - The user's email comes only from the verified token's email claim; tokens
    without one get a synthetic `<user-id>@users.clerk.local` address. The
    old `x-user-email` bootstrap header is gone — a client-supplied address
    would let any caller write someone else's email onto their own row.
- Backend verification env:
  - `CLERK_JWKS_URL` (required)
  - `CLERK_JWT_ISSUER` (required)
  - `CLERK_JWT_AUDIENCE` (optional, comma-separated)
  - `CLERK_AUTHORIZED_PARTIES` (optional, comma-separated origins; enforced
    against `azp` when set — Task 7)
- Internal persistence maps Clerk `sub` -> deterministic UUIDv5 for `users.id`.

### OAuth API surface (Milestone 7)

- `POST /oauth/google/start` -> returns Google consent URL with signed state
- `POST /oauth/google/callback` -> exchanges code, upserts `oauth_connections`, returns redacted connection
- `GET /oauth/google/connection` -> returns latest Google connection status or `null`
- `DELETE /oauth/google/connection` -> revokes the grant at Google (best-effort), then clears tokens and sets `sync_status = 'AUTH_REVOKED'`
- Backend stores OAuth tokens encrypted at rest in `oauth_connections` (`access_token`, `refresh_token`).

## 6. Queue Payload Contract (Phase 1/2/3)

Declared in `shared/types.ts`:

- `EMAIL_SYNC_QUEUE` uses discriminated union `EmailSyncJobPayload`:
  - `EmailSyncDispatchJobPayload`
    - `{ job_type: 'EMAIL_SYNC_DISPATCH', scheduled_time, triggered_at, cron }`
    - optional continuation fields:
      - `start_offset` (non-negative integer)
      - `scan_upper_user_id` (UUID anchor)
  - `EmailSyncUserJobPayload`
    - `{ job_type: 'EMAIL_SYNC_USER', user_id, last_sync_timestamp }`
  - `NormalizeRawEmailsJobPayload`
    - `{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids }`
  - `RawEmailBodyPurgeJobPayload`
    - `{ job_type: 'RAW_EMAIL_BODY_PURGE', triggered_at }` (nightly, claimed from `job_schedules`)
- Async AI handoff queue: `AiClassificationJobPayload`
  - `{ job_type: 'AI_CLASSIFICATION', transaction_id, requested_at }`
  - consumed on dedicated low-priority queue settings with Cloudflare DLQ

### Milestone 11 review + async AI loop

- `NEEDS_REVIEW` transactions enqueue `AI_CLASSIFICATION` asynchronously; main normalization pipeline does not block on AI completion.
- AI queue consumer writes `transactions.ai_confidence_score` and may transition `transactions.status` to `VERIFIED` when confidence threshold policy allows.
- User review action (`POST /transactions/:transactionId/review`) always sets `classification_source = 'USER'`, marks transaction `VERIFIED`, and can upsert `user_merchant_rules`.
- Internal webhook `POST /internal/ai/requires` accepts secret-guarded enqueue requests and maps them to `AI_CLASSIFICATION` jobs.

### Phase 1 dispatcher behavior (Milestone 8)

- Cron remains `* * * * *` and enqueues `EMAIL_SYNC_DISPATCH`.
- Dispatcher scans Google connections via paginated loop (`LIMIT 1000 OFFSET n`) with deterministic ordering.
- Dispatcher may emit continuation `EMAIL_SYNC_DISPATCH` jobs with `start_offset` and `scan_upper_user_id` for bounded per-invocation runtime.
- Dormancy guard:
  - if `users.last_app_open_date` is older than 45 days, mark matching Google connections as `DORMANT` and skip enqueue.
  - otherwise ensure Google connections are `ACTIVE` and enqueue one `EMAIL_SYNC_USER` job per eligible user.
- Queue throughput guard:
  - `EMAIL_SYNC_USER` jobs are sent in batches of 100 messages using `sendBatch`.

## 7. Financial Event Mapping Clarification

The extractor concept may include fields like `channel` and `source_id`.
In v1 persistence contract:

- `source_id` is stored on `raw_emails.source_id`.
- Raw email idempotency is scoped as `(user_id, source_id)` (not global `source_id` uniqueness).
- `channel` is inferred by provenance (e.g., row linked to `raw_emails` from Gmail ingestion), not as a `financial_events` column.
- `financial_events` only stores immutable transaction facts per SQL.

## 8. Category Tree Deletion Policy

- `categories.parent_id` uses `ON DELETE RESTRICT` (not cascade).
- Parent categories cannot be deleted while children exist. This is intentional to prevent silent subtree data loss.

## 9. Done Criteria for Milestone 1

Milestone 1 baseline is complete when:

- `shared/types.ts` is 1:1 with SQL schema + Phase payload contracts.
- Contract naming is frozen on `transactions` (not `expenses`).
- Lifecycle transitions are explicit and centralized.
- System seed data is deterministic and idempotent.
- Stale extraction alerting query/materialized view exists and has an automatic refresh mechanism.

## 10. Credit Card Subcategorization Contract (new)

- `user_credit_cards` stores user-defined card identity:
  - `card_label` (user-facing custom name, e.g., "Kotak Myntra Card")
  - `first4` + `last4` (both required in v1.1 prototype)
  - `account_id` linked to a `card` account
- `transactions.credit_card_id` links any matched card transaction to that specific card.
- `transactions.financial_event_id` is mandatory (`NOT NULL`) to preserve immutable-fact linkage.
- Same-user linkage is DB-enforced with composite FKs (no cross-user joins by mistake):
  - `raw_emails(oauth_connection_id, user_id)` -> `oauth_connections(id, user_id)` with `ON DELETE SET NULL (oauth_connection_id)`
  - `financial_events(raw_email_id, user_id)` -> `raw_emails(id, user_id)`
  - `transactions(financial_event_id, user_id)` -> `financial_events(id, user_id)`
- `transactions.credit_card_id` enforces account consistency:
  - if `credit_card_id` is set and `account_id` is null, DB auto-fills `account_id` from the card mapping
  - if both are set and mismatch, DB rejects the write
- Category scoping is DB-enforced:
  - `transactions.category_id` must be either a system category or a category owned by `transactions.user_id`
  - `user_merchant_rules.custom_category_id` must be either a system category or a category owned by `user_merchant_rules.user_id`
- Analytics can aggregate by `credit_card_id` to show spend by card.
- `v_credit_card_transactions` is a comprehensive projection with category/merchant/classification/note fields.
- `v_credit_card_transactions` includes only rows whose linked `financial_events.status = 'ACTIVE'`.
- Matching helper function: `resolve_user_credit_card(user_id, first4, last4)`.
- Resolver ambiguity policy: if only `last4` is provided and multiple active cards share that `last4`, no match is returned.

## 11. Milestone 12 Operations Contract (new)

- Queue DLQ authority remains Cloudflare-managed; app handlers only `ack()` poison payloads and retry transient failures.
- Queue worker emits structured per-batch metrics:
  - `QUEUE_BATCH_SUMMARY`
  - `QUEUE_ALERT_THRESHOLD_BREACH`
- Alert threshold env contract:
  - `QUEUE_ALERT_RETRY_RATE_PERCENT` (default `15`)
  - `QUEUE_ALERT_POISON_ACK_COUNT` (default `3`)
  - `QUEUE_ALERT_FINAL_RETRY_COUNT` (default `1`)
- Operational dashboards and SLO runbook are documented in `docs/OPERATIONS.md`.

## 12. Monthly Spending Plan Contract

- Cash flow and the spending plan are separate views. Analytics keeps showing
  real income, expense, and transfer movement; the budget answers how much of a
  user-defined flexible limit remains.
- Resolution order is fixed and centralized:
  `transaction override > refund/reimbursement relationship > recurring rule > category rule > system default`.
- System defaults count expenses and leave ordinary income/transfers outside.
  An explicitly included credit is a budget adjustment.
- Initial setup saves the monthly limit and all fixed-cost category choices in
  one database transaction. Parent fixed-cost choices explicitly cover their
  child categories, so the displayed total and saved treatment cannot diverge.
- A linked credit inherits the treatment of its expense. Multiple partial
  credits may link to one expense, but their total adjustment is capped at the
  purchase amount. Cross-month credits are attributed back to the purchase
  month, so the original month recalculates when the relationship is created.
- Automatic refund linking is restricted to exactly one capable purchase on
  the same card with the same merchant identity, within 120 days. Ambiguous
  matches remain user-confirmed suggestions.
- Recurring suggestions require at least three consistent debit expenses with
  the same merchant/search identity and category, weekly or monthly timing,
  and no more than 15% observed amount variation. Nothing changes until the
  user confirms the suggestion. Confirmation must exactly match the pattern
  revalidated by the server; unrelated submitted transactions are rejected.
- User deletion sets `transactions.user_deleted_at`; it never deletes the
  immutable `financial_events` row. Restore clears the timestamp.
