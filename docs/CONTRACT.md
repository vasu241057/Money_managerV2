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
- `global_merchants` -> `GlobalMerchantRow`
- `global_merchant_aliases` -> `GlobalMerchantAliasRow`
- `user_merchant_rules` -> `UserMerchantRuleRow`
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
- Manual writes: `CreateManualTransactionRequest`, `UpdateTransactionRequest`
- Card mapping writes: `CreateUserCreditCardRequest`, `UpdateUserCreditCardRequest`
- Card resolver: `ResolveUserCreditCardRequest`, `ResolveUserCreditCardResponse`
- Merchant overrides: `UpsertUserMerchantRuleRequest`
- OAuth: `GoogleOAuthStartResponse`, `GoogleOAuthCallbackRequest`, `GoogleOAuthConnectionResponse`

## 6. Queue Payload Contract (Phase 1/2/3)

Declared in `shared/types.ts`:

- `EMAIL_SYNC_QUEUE` uses discriminated union `EmailSyncJobPayload`:
  - `EmailSyncDispatchJobPayload`
    - `{ job_type: 'EMAIL_SYNC_DISPATCH', scheduled_time, triggered_at, cron }`
  - `EmailSyncUserJobPayload`
    - `{ job_type: 'EMAIL_SYNC_USER', user_id, last_sync_timestamp }`
- Phase 3 optional queue mode: `NormalizeRawEmailsJobPayload`
  - `{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids }`
- Async AI handoff queue: `AiClassificationJobPayload`
  - `{ job_type: 'AI_CLASSIFICATION', transaction_id, requested_at }`

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
