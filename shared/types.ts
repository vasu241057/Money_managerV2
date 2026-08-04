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

export type BudgetTreatment = 'AUTO' | 'INCLUDE' | 'EXCLUDE';

export type EffectiveBudgetTreatment = Exclude<BudgetTreatment, 'AUTO'>;

export type BudgetTreatmentSource =
  | 'TRANSACTION_OVERRIDE'
  | 'REFUND_RELATIONSHIP'
  | 'RECURRING_RULE'
  | 'CATEGORY_RULE'
  | 'SYSTEM_DEFAULT';

export type BudgetEffectKind = 'SPEND' | 'ADJUSTMENT' | 'NONE';

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
  // Optional: public BIN data, display-only — matching uses last4.
  first4: string | null;
  last4: string;
  is_active: boolean;
  /** Day-of-month the billing cycle closes; auto-derived from statements. */
  billing_cycle_day: number | null;
  /** Whether a statement-PDF password is stored (the password itself never leaves the server). */
  has_pdf_password: boolean;
  /** Character count of the stored password — display-only (renders as that many dots). */
  pdf_password_length: number | null;
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
  // Per-user hide of a shared system category ("delete" semantics for the
  // UI). Only meaningful when is_system; surfaced with include_hidden=true.
  is_hidden?: boolean;
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

export interface MerchantLogoAsset {
  public_url: string;
  brand_color: string | null;
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

export interface UserMerchantRuleCategoryRow {
  user_merchant_rule_id: UUID;
  transaction_type: TransactionType;
  category_id: UUID;
  created_at: ISODateTimeString;
  updated_at: ISODateTimeString;
}

export interface GlobalMerchantCategoryDefaultRow {
  merchant_id: UUID;
  transaction_type: TransactionType;
  category_id: UUID;
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
  from_address: string | null;
  subject: string | null;
  status: RawEmailStatus;
  processing_route: 'TEMPLATE' | 'AI' | null;
  ai_call_id: UUID | null;
  processing_completed_at: ISODateTimeString | null;
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
  first4: string | null;
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
  budget_treatment: BudgetTreatment;
  recurring_series_id: UUID | null;
  user_deleted_at: ISODateTimeString | null;
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
	/** Searches merchant/counterparty, note, and partial rupee amount. */
	q?: string;
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
  merchant:
    | (Pick<GlobalMerchantRow, 'id' | 'canonical_name' | 'type'> & {
        logo: MerchantLogoAsset | null;
      })
    | null;
  raw_email: Pick<RawEmailRow, 'id' | 'source_id' | 'internal_date' | 'status'> | null;
  budget: TransactionBudgetInfo;
}

/**
 * Manual user entry. Backend creates BOTH:
 * 1) financial_events row (immutable fact)
 * 2) transactions row (mutable projection)
 */
export interface CreateManualTransactionRequest {
  amount_in_paise: number;
  type: TransactionType;
  /** Required to distinguish incoming vs outgoing manual transfers. */
  direction?: EventDirection;
  txn_date: ISODateTimeString;
  account_id?: UUID | null;
  category_id?: UUID | null;
  merchant_id?: UUID | null;
  user_note?: string | null;
  payment_method?: PaymentMethod;
  instrument_id?: string | null;
  counterparty_raw?: string | null;
  /** Preserves an explicit plan choice when a manual fact is recreated. */
  budget_treatment?: BudgetTreatment;
}

export interface UpdateTransactionRequest {
  type?: TransactionType;
  account_id?: UUID | null;
  category_id?: UUID | null;
  merchant_id?: UUID | null;
  credit_card_id?: UUID | null;
  user_note?: string | null;
  status?: TransactionStatus;
  classification_source?: ClassificationSource;
  ai_confidence_score?: number | null;
  budget_treatment?: BudgetTreatment;
}

export interface TransactionBudgetRelationship {
  id: UUID;
  kind: 'REFUND' | 'REIMBURSEMENT';
  role: 'SOURCE' | 'TARGET';
  related_transaction_id: UUID;
  related_amount_in_paise: number;
  related_txn_date: ISODateTimeString;
  related_label: string;
  created_by: 'AUTO' | 'USER';
  confidence: number;
  /** Multiple refund credits may point at one purchase. */
  related_count: number;
}

export interface TransactionBudgetInfo {
  treatment: BudgetTreatment;
  effective_treatment: EffectiveBudgetTreatment;
  treatment_source: BudgetTreatmentSource;
  effect: BudgetEffectKind;
  effect_in_paise: number;
  relationship: TransactionBudgetRelationship | null;
}

export interface CreateUserCreditCardRequest {
  // Omitted → the backend adopts the unregistered same-last4 card account
  // if exactly one exists, else creates a fresh backing account.
  account_id?: UUID | null;
  card_label: string;
  issuer_name?: string | null;
  network?: CardNetwork | null;
  first4?: string | null;
  last4: string;
  is_active?: boolean;
  /** Statement-PDF password; encrypted at rest, never returned. */
  pdf_password?: string | null;
  billing_cycle_day?: number | null;
}

export interface UpdateUserCreditCardRequest {
  account_id?: UUID;
  card_label?: string;
  issuer_name?: string | null;
  network?: CardNetwork | null;
  first4?: string | null;
  last4?: string;
  is_active?: boolean;
  /** Statement-PDF password; encrypted at rest, never returned. Null clears. */
  pdf_password?: string | null;
  /** Day-of-month the billing cycle closes; normally auto-derived. */
  billing_cycle_day?: number | null;
}

export interface UpsertUserMerchantRuleRequest {
  search_key: string;
  merchant_id?: UUID | null;
  custom_category_id?: UUID | null;
}

export interface ReviewTransactionRequest {
  type?: TransactionType;
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

export interface GoogleOAuthProcessingStats {
  raw_email_total_count: number;
  raw_email_pending_count: number;
  raw_email_processed_count: number;
  raw_email_failed_count: number;
  raw_email_unrecognized_count: number;
  raw_email_ignored_count: number;
  latest_email_internal_date: ISODateTimeString | null;
  latest_email_created_at: ISODateTimeString | null;
}

export interface GoogleOAuthConnectionStatusResponse {
  connection: OauthConnectionRow | null;
  processing_stats: GoogleOAuthProcessingStats;
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
   * Stable id for one connection-sync chain. Queue retries and Gmail page
   * continuations retain it so a later cron cannot overlap the same mailbox.
   */
  sync_run_id?: UUID;
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
 * Daily sender-classification job: sends the distinct unknown gated sender
 * addresses to AI once, persists ALLOW/DENY rules in sender_rules.
 */
export interface SenderClassificationJobPayload {
  job_type: 'SENDER_CLASSIFICATION';
  triggered_at: ISODateTimeString;
}

export const SENDER_BACKFILL_MAX_IDS = 100;

/**
 * Daily merchant-mapping job: sends the distinct unmapped counterparty names
 * to AI once, persists canonical merchants + aliases, and re-links recent
 * transactions to the new merchants.
 */
export interface MerchantMappingJobPayload {
  job_type: 'MERCHANT_MAPPING';
  triggered_at: ISODateTimeString;
}

/**
 * Backfill job for a newly allowlisted sender: refetches full bodies for the
 * user's metadata-only gated raw emails and re-enters them into extraction.
 */
export interface SenderBackfillJobPayload {
  job_type: 'SENDER_BACKFILL';
  user_id: UUID;
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
  | NormalizeRawEmailsJobPayload
  | SenderClassificationJobPayload
  | SenderBackfillJobPayload
  | MerchantMappingJobPayload;


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

export interface AiNormalizedTransactionItem {
  direction: 'debit' | 'credit';
  amount_in_paise: number;
  txn_timestamp: string;
  payment_method: PaymentMethod;
  counterparty_raw: string | null;
  instrument_last4: string | null;
  category_guess: string | null;
  category_id: string | null;
  sub_category_id: string | null;
  account_id: string | null;
  credit_card_id: string | null;
  confidence: number;
}

export interface AiNormalizationResponse {
  transactions: AiNormalizedTransactionItem[];
  /** Present when the email is a credit-card statement notification. */
  statement?: unknown;
}

export interface NormalizerTierConfig {
  models: string[];
  /** Attempts allowed per tier before rotating (index-aligned with models). */
  tierAttempts: number[];
  cooldownMs: number;
  maxRetries: number;
  timeoutMs: number;
  temperature: number;
  maxTokens: number;
  autoVerifyConfidence: number;
  minConfidence: number;
  /** Mask numeric tokens as <N#> before the AI call (privacy). Default on. */
  maskNumbers: boolean;
  /** Emails per model call in masked mode (1 disables batching). */
  batchSize: number;
}

// ---- Analytics summary (computed server-side; months bucket by Asia/Kolkata) ----

export interface MonthlyCategorySlice {
  category_id: UUID | null;
  category_name: string | null;
  amount_in_paise: number;
  txn_count: number;
}

export interface MonthlyAccountSlice {
  account_id: UUID | null;
  /** Gross expense transactions assigned to the account. */
  expense_in_paise: number;
  /** Incoming credits on card-type accounts (refunds, reversals, cashback). */
  credit_in_paise: number;
  /** expense_in_paise - credit_in_paise; may be negative in a refund-heavy month. */
  net_spend_in_paise: number;
  txn_count: number;
}

export interface MonthlySummary {
  /** 'YYYY-MM' in Asia/Kolkata */
  month: string;
  income_in_paise: number;
  expense_in_paise: number;
  transfer_in_paise: number;
  txn_count: number;
  expense_by_category: MonthlyCategorySlice[];
  expense_by_account: MonthlyAccountSlice[];
}

export interface AccountAllTimeSummary {
  account_id: UUID;
  income_in_paise: number;
  expense_in_paise: number;
}

export interface AnalyticsSummaryResponse {
  /** Ascending by month */
  months: MonthlySummary[];
  all_time: { income_in_paise: number; expense_in_paise: number };
  accounts: AccountAllTimeSummary[];
  review: { total: number; last_3_days: number };
}

// ---- Monthly spending plan ----

export interface BudgetCategoryRule {
  category_id: UUID;
  category_name: string;
  category_icon: string | null;
  treatment: EffectiveBudgetTreatment;
}

export interface BudgetCategorySummary {
  category_id: UUID | null;
  category_name: string;
  category_icon: string | null;
  spent_in_paise: number;
  adjustment_in_paise: number;
  net_in_paise: number;
  transaction_count: number;
}

export interface BudgetMonthSummary {
  /** YYYY-MM in Asia/Kolkata. */
  month: string;
  limit_in_paise: number | null;
  /** YYYY-MM of the saved limit row; differs from month when the plan is inherited. */
  limit_source_month: string | null;
  counted_spending_in_paise: number;
  adjustments_in_paise: number;
  used_in_paise: number;
  remaining_in_paise: number | null;
  outside_plan_in_paise: number;
  categories: BudgetCategorySummary[];
}

export interface RefundMatchSuggestion {
  type: 'REFUND_MATCH';
  fingerprint: string;
  confidence: number;
  source_transaction_id: UUID;
  source_amount_in_paise: number;
  source_txn_date: ISODateTimeString;
  target_transaction_id: UUID;
  target_amount_in_paise: number;
  target_remaining_in_paise: number;
  target_txn_date: ISODateTimeString;
  label: string;
  can_auto_link: boolean;
  /** How much linking would reduce the target purchase month's used budget. */
  budget_impact_in_paise: number;
}

export interface RecurringPatternSuggestion {
  type: 'RECURRING_PATTERN';
  fingerprint: string;
  confidence: number;
  label: string;
  transaction_ids: UUID[];
  occurrence_count: number;
  cadence: 'WEEKLY' | 'MONTHLY';
  expected_day: number;
  amount_min_in_paise: number;
  amount_max_in_paise: number;
  suggested_treatment: EffectiveBudgetTreatment;
}

export type BudgetSuggestion = RefundMatchSuggestion | RecurringPatternSuggestion;

export interface BudgetOverviewResponse {
  summary: BudgetMonthSummary;
  category_rules: BudgetCategoryRule[];
  recurring_series: RecurringSeriesSummary[];
  suggestions: BudgetSuggestion[];
}

export interface RecurringSeriesSummary {
  id: UUID;
  fingerprint: string;
  label: string;
  cadence: 'WEEKLY' | 'MONTHLY';
  expected_day: number;
  amount_min_in_paise: number;
  amount_max_in_paise: number;
  budget_treatment: BudgetTreatment;
  status: 'ACTIVE' | 'PAUSED';
  last_matched_at: ISODateTimeString | null;
}

export interface UpsertMonthlyBudgetRequest {
  limit_in_paise: number;
}

export interface BudgetPlanCategoryRuleInput {
  category_id: UUID;
  treatment: EffectiveBudgetTreatment | null;
}

export interface UpsertBudgetPlanRequest extends UpsertMonthlyBudgetRequest {
  /** Only listed categories are changed; null removes that category's rule. */
  category_rules: BudgetPlanCategoryRuleInput[];
}

export interface SetCategoryBudgetRuleRequest {
  treatment: EffectiveBudgetTreatment;
}

export interface CreateTransactionRelationshipRequest {
  source_transaction_id: UUID;
  target_transaction_id: UUID;
  kind: 'REFUND' | 'REIMBURSEMENT';
}

export interface RefundTargetOption {
  transaction_id: UUID;
  label: string;
  amount_in_paise: number;
  remaining_in_paise: number;
  txn_date: ISODateTimeString;
}

export interface ConfirmRecurringPatternRequest {
  fingerprint: string;
  transaction_ids: UUID[];
  treatment: EffectiveBudgetTreatment;
}

export interface UpdateRecurringSeriesRequest {
  budget_treatment?: BudgetTreatment;
  status?: 'ACTIVE' | 'PAUSED';
}

export interface DismissBudgetSuggestionRequest {
  type: 'REFUND_MATCH' | 'RECURRING_PATTERN';
  fingerprint: string;
}

// ---- Admin AI usage ledger ----

export type AiUsageProvider = 'opencode' | 'openrouter';
export type AiUsageOperation =
  | 'EMAIL_NORMALIZATION'
  | 'SENDER_CLASSIFICATION'
  | 'MERCHANT_MAPPING';
export type AiUsageStatus = 'SUCCESS' | 'ERROR';

export interface AiUsageAccountSnapshot {
  provider: 'openrouter';
  /** Account usage when available; otherwise usage on the configured key. */
  all_time_cost_usd: number;
  /** Usage on the configured OpenRouter key in the current month. */
  current_month_cost_usd: number;
  purchased_credits_usd: number | null;
  credits_remaining_usd: number | null;
  limit_usd: number | null;
  remaining_usd: number | null;
  fetched_at: ISODateTimeString;
}

export interface AiUsageSummaryResponse {
  account: AiUsageAccountSnapshot | null;
  tracked_since: ISODateTimeString | null;
  tracked_all_time_cost_usd: number;
  tracked_current_month_cost_usd: number;
  tracked_all_time_request_count: number;
  tracked_current_month_request_count: number;
}

export interface AiUsageMonthSummary {
  /** YYYY-MM in Asia/Kolkata. */
  month: string;
  request_count: number;
  success_count: number;
  cost_usd: number;
  item_count: number;
  average_latency_ms: number;
  total_tokens: number;
}

export interface AiUsageModelBreakdown {
  provider: AiUsageProvider;
  model: string;
  request_count: number;
  success_count: number;
  cost_usd: number;
  prompt_tokens: number;
  completion_tokens: number;
  reasoning_tokens: number;
  cached_tokens: number;
  total_tokens: number;
  item_count: number;
  average_latency_ms: number;
}

export interface AiUsageEvent {
  id: UUID;
  call_id: UUID | null;
  attempt_number: number | null;
  provider: AiUsageProvider;
  model: string;
  operation: AiUsageOperation;
  status: AiUsageStatus;
  http_status: number | null;
  item_count: number;
  prompt_tokens: number;
  completion_tokens: number;
  reasoning_tokens: number;
  cached_tokens: number;
  total_tokens: number;
  cost_usd: number | null;
  latency_ms: number;
  error_code: string | null;
  generation_id: string | null;
  created_at: ISODateTimeString;
}

export interface AiProcessingAttempt {
  id: UUID;
  attempt_number: number;
  model: string;
  status: AiUsageStatus;
  http_status: number | null;
  error_code: string | null;
  cost_usd: number | null;
  latency_ms: number;
}

export interface AiProcessingRun {
  id: UUID;
  item_count: number;
  status: 'RUNNING' | 'SUCCESS' | 'FAILED';
  attempt_count: number;
  started_at: ISODateTimeString;
  completed_at: ISODateTimeString | null;
  attempts: AiProcessingAttempt[];
}

export interface AiProcessingSummary {
  emails_handled: number;
  template_hits: number;
  ai_routed_emails: number;
  email_final_failures: number;
  transactions_created: number;
  logical_calls: number;
  provider_attempts: number;
  first_attempt_successes: number;
  recovered_calls: number;
  final_failures: number;
  in_progress_calls: number;
  recent_runs: AiProcessingRun[];
}

export interface AiUsageDashboardResponse {
  selected_month: string;
  page: number;
  page_size: number;
  total_event_count: number;
  months: AiUsageMonthSummary[];
  models: AiUsageModelBreakdown[];
  events: AiUsageEvent[];
  processing: AiProcessingSummary;
}

// ---- Card statements (billing-cycle capture from statement emails) ----

export type CardStatementSource = 'EMAIL' | 'USER';

export interface CardStatementRow {
  id: UUID;
  user_id: UUID;
  /** Null while parked: last4 matched no registered card yet. */
  credit_card_id: UUID | null;
  card_last4: string | null;
  period_start: string | null; // date (YYYY-MM-DD)
  period_end: string | null;
  statement_date: string | null;
  total_due_in_paise: number | null;
  min_due_in_paise: number | null;
  due_date: string | null;
  source: CardStatementSource;
  source_raw_email_id: UUID | null;
  created_at: ISODateTimeString;
  updated_at: ISODateTimeString;
}
