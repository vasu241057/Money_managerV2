// ============================================================
// Money Manager Canonical Data + API Contract (Milestone 1)
// Source of truth date: 2026-03-08
//
// This file is intentionally aligned 1:1 with the agreed SQL schema
// and queue payloads for Phase 1 / 2 / 3.
// ============================================================

// ── Common primitives ────────────────────────────────────────

export type UUID = string;
export type ISODateTimeString = string; // timestamptz serialized as ISO-8601

// ── Enumerations (DB-aligned) ───────────────────────────────

export type OAuthProvider = 'google';

export type OAuthSyncStatus =
  | 'ACTIVE'
  | 'DORMANT'
  | 'AUTH_REVOKED'
  | 'ERROR_PAUSED';

export type AccountType = 'cash' | 'bank' | 'card' | 'other';

export type CategoryType = 'income' | 'expense' | 'transfer';

export type CardNetwork =
  | 'visa'
  | 'mastercard'
  | 'rupay'
  | 'amex'
  | 'diners'
  | 'other';

export type MerchantType =
  | 'MERCHANT'
  | 'TRANSFER_INSTITUTION'
  | 'AGGREGATOR'
  | 'P2P';

export type RawEmailStatus =
  | 'PENDING_EXTRACTION'
  | 'PROCESSED'
  | 'IGNORED'
  | 'UNRECOGNIZED'
  | 'FAILED';

export type EventDirection = 'debit' | 'credit';

export type PaymentMethod =
  | 'upi'
  | 'credit_card'
  | 'debit_card'
  | 'netbanking'
  | 'cash'
  | 'unknown';

export type FinancialEventStatus = 'ACTIVE' | 'REVERSED';

export type TransactionType = 'income' | 'expense' | 'transfer';

export type TransactionStatus = 'VERIFIED' | 'NEEDS_REVIEW';

export type ClassificationSource =
  | 'USER'
  | 'SYSTEM_DEFAULT'
  | 'HEURISTIC'
  | 'AI';

// ── Canonical naming decision ───────────────────────────────

/**
 * Canonical mutable projection entity.
 * NOTE: Legacy term "expenses" is deprecated; use "transactions".
 */
export const CANONICAL_MUTABLE_ENTITY = 'transactions' as const;

// ── SQL row contracts (1:1 column mapping) ──────────────────

export interface UserRow {
  id: UUID;
  email: string;
  last_app_open_date: ISODateTimeString;
  created_at: ISODateTimeString;
}

export interface OauthConnectionRow {
  id: UUID;
  user_id: UUID;
  provider: OAuthProvider;
  email_address: string;
  access_token: string | null;
  refresh_token: string | null;
  last_sync_timestamp: number; // BIGINT
  sync_status: OAuthSyncStatus;
  created_at: ISODateTimeString;
  updated_at: ISODateTimeString;
}

export interface AccountRow {
  id: UUID;
  user_id: UUID;
  name: string;
  type: AccountType;
  instrument_last4: string | null;
  initial_balance_in_paise: number; // BIGINT
  created_at: ISODateTimeString;
  updated_at: ISODateTimeString;
}

export interface UserCreditCardRow {
  id: UUID;
  user_id: UUID;
  account_id: UUID;
  card_label: string;
  issuer_name: string | null;
  network: CardNetwork | null;
  first4: string;
  last4: string;
  is_active: boolean;
  created_at: ISODateTimeString;
  updated_at: ISODateTimeString;
}

export interface CategoryRow {
  id: UUID;
  user_id: UUID | null; // null = system/global category
  parent_id: UUID | null;
  name: string;
  type: CategoryType;
  icon: string | null;
  is_system: boolean;
  created_at: ISODateTimeString;
}

export interface GlobalMerchantRow {
  id: UUID;
  canonical_name: string;
  default_category_id: UUID | null;
  type: MerchantType;
  is_context_required: boolean;
  created_at: ISODateTimeString;
  updated_at: ISODateTimeString;
}

export interface GlobalMerchantAliasRow {
  search_key: string; // PRIMARY KEY
  merchant_id: UUID;
  created_at: ISODateTimeString;
  updated_at: ISODateTimeString;
}

export interface UserMerchantRuleRow {
  id: UUID;
  user_id: UUID;
  search_key: string;
  merchant_id: UUID | null;
  custom_category_id: UUID | null;
  created_at: ISODateTimeString;
  updated_at: ISODateTimeString;
}

export interface RawEmailRow {
  id: UUID;
  user_id: UUID;
  oauth_connection_id: UUID | null;
  source_id: string; // unique per user mailbox (user_id + source_id)
  internal_date: ISODateTimeString;
  clean_text: string;
  status: RawEmailStatus;
  created_at: ISODateTimeString;
}

export interface StalePendingExtractionRow {
  user_id: UUID;
  stale_count: number;
  oldest_created_at: ISODateTimeString;
  newest_created_at: ISODateTimeString;
  snapshot_at: ISODateTimeString;
}

export interface CreditCardTransactionViewRow {
  transaction_id: UUID;
  user_id: UUID;
  txn_date: ISODateTimeString;
  amount_in_paise: number;
  type: TransactionType;
  status: TransactionStatus;
  account_id: UUID | null;
  category_id: UUID | null;
  category_name: string | null;
  category_icon: string | null;
  merchant_id: UUID | null;
  merchant_name: string | null;
  financial_event_status: FinancialEventStatus;
  classification_source: ClassificationSource;
  user_note: string | null;
  ai_confidence_score: number | null;
  credit_card_id: UUID;
  card_label: string;
  issuer_name: string | null;
  network: CardNetwork | null;
  first4: string;
  last4: string;
}

export interface FinancialEventRow {
  id: UUID;
  user_id: UUID;
  raw_email_id: UUID | null;
  extraction_index: number;
  direction: EventDirection;
  amount_in_paise: number; // BIGINT
  currency: string;
  txn_timestamp: ISODateTimeString;
  payment_method: PaymentMethod;
  instrument_id: string | null;
  counterparty_raw: string | null;
  search_key: string | null;
  status: FinancialEventStatus;
  created_at: ISODateTimeString;
}

export interface TransactionRow {
  id: UUID;
  user_id: UUID;
  financial_event_id: UUID;
  account_id: UUID | null;
  category_id: UUID | null;
  merchant_id: UUID | null;
  credit_card_id: UUID | null;
  amount_in_paise: number; // BIGINT
  type: TransactionType;
  txn_date: ISODateTimeString;
  user_note: string | null;
  status: TransactionStatus;
  classification_source: ClassificationSource;
  ai_confidence_score: number | null; // NUMERIC(3,2)
  created_at: ISODateTimeString;
  updated_at: ISODateTimeString;
}

// ── API DTO contracts ────────────────────────────────────────

export interface PageRequest {
  page?: number;
  limit?: number;
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  limit: number;
  has_more: boolean;
}

export interface ListTransactionsQuery extends PageRequest {
  from?: ISODateTimeString;
  to?: ISODateTimeString;
  status?: TransactionStatus;
  type?: TransactionType;
  account_id?: UUID;
  category_id?: UUID;
  credit_card_id?: UUID;
}

export interface ListGlobalMerchantsQuery {
  q?: string;
  limit?: number;
}

export type GlobalMerchantListItem = Pick<GlobalMerchantRow, 'id' | 'canonical_name' | 'type'>;

export interface CreateAccountRequest {
  name: string;
  type: AccountType;
  instrument_last4?: string | null;
  initial_balance_in_paise?: number;
}

export interface UpdateAccountRequest {
  name?: string;
  type?: AccountType;
  instrument_last4?: string | null;
  initial_balance_in_paise?: number;
}

export interface CreateCategoryRequest {
  name: string;
  type: CategoryType;
  icon?: string | null;
  parent_id?: UUID | null;
}

export interface UpdateCategoryRequest {
  name?: string;
  type?: CategoryType;
  icon?: string | null;
  parent_id?: UUID | null;
}

export interface TransactionFeedItem {
  transaction: TransactionRow;
  financial_event: FinancialEventRow;
  account: Pick<AccountRow, 'id' | 'name' | 'type'> | null;
  credit_card: Pick<UserCreditCardRow, 'id' | 'card_label' | 'first4' | 'last4'> | null;
  category: Pick<CategoryRow, 'id' | 'name' | 'type' | 'icon'> | null;
  merchant: Pick<GlobalMerchantRow, 'id' | 'canonical_name' | 'type'> | null;
  raw_email: Pick<RawEmailRow, 'id' | 'source_id' | 'internal_date' | 'status'> | null;
}

/**
 * Manual user entry. Backend creates BOTH:
 * 1) financial_events row (immutable fact)
 * 2) transactions row (mutable projection)
 */
export interface CreateManualTransactionRequest {
  amount_in_paise: number;
  type: TransactionType;
  txn_date: ISODateTimeString;
  account_id?: UUID | null;
  category_id?: UUID | null;
  merchant_id?: UUID | null;
  user_note?: string | null;
  payment_method?: PaymentMethod;
  instrument_id?: string | null;
  counterparty_raw?: string | null;
}

export interface UpdateTransactionRequest {
  account_id?: UUID | null;
  category_id?: UUID | null;
  merchant_id?: UUID | null;
  credit_card_id?: UUID | null;
  user_note?: string | null;
  status?: TransactionStatus;
  classification_source?: ClassificationSource;
  ai_confidence_score?: number | null;
}

export interface CreateUserCreditCardRequest {
  account_id: UUID;
  card_label: string;
  issuer_name?: string | null;
  network?: CardNetwork | null;
  first4: string;
  last4: string;
  is_active?: boolean;
}

export interface UpdateUserCreditCardRequest {
  account_id?: UUID;
  card_label?: string;
  issuer_name?: string | null;
  network?: CardNetwork | null;
  first4?: string;
  last4?: string;
  is_active?: boolean;
}

export interface ResolveUserCreditCardRequest {
  user_id: UUID;
  first4?: string | null;
  last4: string;
}

export interface ResolveUserCreditCardResponse {
  credit_card_id: UUID;
  account_id: UUID;
  card_label: string;
}

export interface UpsertUserMerchantRuleRequest {
  search_key: string;
  merchant_id?: UUID | null;
  custom_category_id?: UUID | null;
}

export interface ReviewTransactionRequest {
  category_id?: UUID | null;
  merchant_id?: UUID | null;
  user_note?: string | null;
  apply_rule?: boolean;
  rule_search_key?: string | null;
}

export interface ReviewTransactionResponse {
  transaction: TransactionFeedItem;
  rule_applied: boolean;
  applied_search_key: string | null;
}

export interface GoogleOAuthStartResponse {
  auth_url: string;
}

export interface GoogleOAuthCallbackRequest {
  code: string;
  state: string;
}

export interface GoogleOAuthConnectionResponse {
  connection: OauthConnectionRow;
}

export interface GoogleOAuthConnectionStatusResponse {
  connection: OauthConnectionRow | null;
}

// ── Queue + worker payload contracts (Phase 1/2/3) ──────────

/**
 * Phase 1 cron control-plane envelope.
 * Scheduled handler enqueues this job first.
 */
export interface EmailSyncDispatchJobPayload {
  job_type: 'EMAIL_SYNC_DISPATCH';
  scheduled_time: number;
  triggered_at: ISODateTimeString;
  cron: string;
  /**
   * Optional continuation offset used when dispatcher slices long runs.
   * Defaults to 0 when omitted.
   */
  start_offset?: number;
  /**
   * Optional stable scan anchor to reduce OFFSET drift from concurrent inserts.
   * When set, dispatcher scans users with id <= scan_upper_user_id.
   */
  scan_upper_user_id?: UUID;
}

/**
 * Phase 1 dispatcher -> Phase 2 fetcher per-user sync job.
 */
export interface EmailSyncUserJobPayload {
  job_type: 'EMAIL_SYNC_USER';
  user_id: UUID;
  last_sync_timestamp: number;
  /**
   * Optional continuation fields used when Phase 2 fetcher slices a deep Gmail
   * pagination chain across multiple queue jobs.
   */
  continuation_connection_id?: UUID;
  continuation_page_token?: string;
  continuation_after_seconds?: number;
  continuation_max_internal_timestamp_seen?: number;
}

/**
 * Optional Phase 3 queue mode: normalize a bounded set of raw emails.
 * Phase 3 may also run via cron scanning PENDING_EXTRACTION rows.
 */
export const NORMALIZE_RAW_EMAILS_MAX_IDS = 250;

export interface NormalizeRawEmailsJobPayload {
  job_type: 'NORMALIZE_RAW_EMAILS';
  /**
   * Bounded to NORMALIZE_RAW_EMAILS_MAX_IDS for predictable worker latency.
   */
  raw_email_ids: UUID[];
}

/**
 * Full payload contract for EMAIL_SYNC_QUEUE.
 * A single queue handles control-plane + per-user + normalization jobs,
 * discriminated by job_type.
 */
export type EmailSyncJobPayload =
  | EmailSyncDispatchJobPayload
  | EmailSyncUserJobPayload
  | NormalizeRawEmailsJobPayload;

/**
 * Async AI handoff queue payload for unknown classifications.
 */
export interface AiClassificationJobPayload {
  job_type: 'AI_CLASSIFICATION';
  transaction_id: UUID;
  requested_at: ISODateTimeString;
}

export interface AiRequiresWebhookRequest {
  job_type?: 'REQUIRES_AI';
  transaction_id: UUID;
  requested_at?: ISODateTimeString;
}

export interface AiRequiresWebhookResponse {
  accepted: boolean;
  queued_at: ISODateTimeString;
}

// ── Extractor contracts (Phase 3 deterministic engine) ──────

/**
 * In-memory extraction result used before persistence.
 * Notes:
 * - channel/source_id live on raw_emails and are joined when needed.
 * - financial_events table intentionally stores immutable transaction facts.
 */
export interface ExtractedFact {
  raw_email_id: UUID;
  extraction_index: number;
  direction: EventDirection;
  amount_in_paise: number;
  currency: 'INR';
  txn_timestamp: ISODateTimeString;
  payment_method: PaymentMethod;
  instrument_id: string | null;
  counterparty_raw: string | null;
  search_key: string | null;
}

// ── Lifecycle contracts (status transition guards) ───────────

export const OAUTH_SYNC_STATUS_TRANSITIONS: Record<
  OAuthSyncStatus,
  readonly OAuthSyncStatus[]
> = {
  ACTIVE: ['DORMANT', 'AUTH_REVOKED', 'ERROR_PAUSED'],
  DORMANT: ['ACTIVE', 'AUTH_REVOKED', 'ERROR_PAUSED'],
  AUTH_REVOKED: ['ACTIVE'],
  ERROR_PAUSED: ['ACTIVE', 'AUTH_REVOKED'],
};

export const RAW_EMAIL_STATUS_TRANSITIONS: Record<
  RawEmailStatus,
  readonly RawEmailStatus[]
> = {
  PENDING_EXTRACTION: ['PROCESSED', 'IGNORED', 'UNRECOGNIZED', 'FAILED'],
  PROCESSED: [],
  IGNORED: [],
  UNRECOGNIZED: [],
  FAILED: ['PENDING_EXTRACTION'],
};

export const FINANCIAL_EVENT_STATUS_TRANSITIONS: Record<
  FinancialEventStatus,
  readonly FinancialEventStatus[]
> = {
  ACTIVE: ['REVERSED'],
  REVERSED: [],
};

export const TRANSACTION_STATUS_TRANSITIONS: Record<
  TransactionStatus,
  readonly TransactionStatus[]
> = {
  VERIFIED: ['NEEDS_REVIEW'],
  NEEDS_REVIEW: ['VERIFIED'],
};
