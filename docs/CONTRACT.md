# Money Manager Canonical Contract (Milestone 0)

Version: `v1`
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
- `global_merchants` -> `GlobalMerchantRow`
- `global_merchant_aliases` -> `GlobalMerchantAliasRow`
- `user_merchant_rules` -> `UserMerchantRuleRow`
- `raw_emails` -> `RawEmailRow`
- `financial_events` -> `FinancialEventRow`
- `transactions` -> `TransactionRow`

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

## 4. Status Lifecycle (Operational Rules)

Implemented as transition guards in `shared/types.ts`:

- `OAUTH_SYNC_STATUS_TRANSITIONS`
- `RAW_EMAIL_STATUS_TRANSITIONS`
- `FINANCIAL_EVENT_STATUS_TRANSITIONS`
- `TRANSACTION_STATUS_TRANSITIONS`

### Key lifecycle decision

The earlier phrase `REQUIRES_AI` is **not** a DB transaction status in v1 schema.
Unknown classifications are stored as `transactions.status = 'NEEDS_REVIEW'` and routed to AI asynchronously via queue (`AiClassificationJobPayload`).

## 5. API DTO Contract (v1)

Declared in `shared/types.ts`:

- Pagination: `PageRequest`, `PaginatedResponse<T>`
- Listing: `ListTransactionsQuery`, `TransactionFeedItem`
- Manual writes: `CreateManualTransactionRequest`, `UpdateTransactionRequest`
- Merchant overrides: `UpsertUserMerchantRuleRequest`
- OAuth: `GoogleOAuthStartResponse`, `GoogleOAuthCallbackRequest`, `GoogleOAuthConnectionResponse`

## 6. Queue Payload Contract (Phase 1/2/3)

Declared in `shared/types.ts`:

- Phase 1 dispatcher -> fetcher: `EmailSyncJobPayload`
  - `{ user_id, last_sync_timestamp }`
- Phase 3 optional queue mode: `NormalizeRawEmailsJobPayload`
  - `{ raw_email_ids }`
- Async AI handoff: `AiClassificationJobPayload`
  - `{ transaction_id }`

## 7. Financial Event Mapping Clarification

The extractor concept may include fields like `channel` and `source_id`.
In v1 persistence contract:

- `source_id` is stored on `raw_emails.source_id`.
- `channel` is inferred by provenance (e.g., row linked to `raw_emails` from Gmail ingestion), not as a `financial_events` column.
- `financial_events` only stores immutable transaction facts per SQL.

## 8. Done Criteria for Milestone 0

Milestone 0 is complete when:

- `shared/types.ts` is 1:1 with SQL schema + Phase payload contracts.
- Contract naming is frozen on `transactions` (not `expenses`).
- Lifecycle transitions are explicit and centralized.
- This document exists and is treated as a required reference for Milestone 1+ implementation.
