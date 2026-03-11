import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import type { ReviewTransactionRequest } from '../../../shared/types';
import type { Transaction } from '../hooks/useTransactions';
import { useCategories } from '../hooks/useCategories';
import { apiClient } from '../lib/api-client';
import { ensureLegacyLocalDataMigrated } from '../lib/legacy-local-migration';
import '../styles/review-inbox.css';

interface ReviewInboxProps {
  items: Transaction[];
  onReview: (params: {
    transactionId: string;
    payload: ReviewTransactionRequest;
  }) => Promise<unknown>;
}

interface ReviewDraft {
  categoryId: string;
  merchantId: string;
  userNote: string;
  applyRule: boolean;
  ruleSearchKey: string;
}

interface CategoryOption {
  id: string;
  label: string;
}

const pad2 = (value: number) => value.toString().padStart(2, '0');

function formatDate(date: Date): string {
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
}

function formatConfidence(confidence: number | null | undefined): string {
  if (confidence === null || confidence === undefined) {
    return 'n/a';
  }

  return `${Math.round(confidence * 100)}%`;
}

function buildInitialDraft(transaction: Transaction): ReviewDraft {
  return {
    categoryId: transaction.persistedCategoryId ?? '',
    merchantId: transaction.merchantId ?? '',
    userNote: transaction.description ?? '',
    applyRule: false,
    ruleSearchKey: transaction.searchKey ?? '',
  };
}

function normalizeOptionalString(value: string): string | null {
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

export function ReviewInbox({ items, onReview }: ReviewInboxProps) {
  const { categories } = useCategories();
  const [merchantSearchInput, setMerchantSearchInput] = useState('');
  const [merchantQuery, setMerchantQuery] = useState('');
  useEffect(() => {
    const timeoutId = window.setTimeout(() => {
      setMerchantQuery(merchantSearchInput.trim());
    }, 250);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [merchantSearchInput]);

  const merchantsQuery = useQuery({
    queryKey: ['global-merchants', 'review-inbox', merchantQuery],
    enabled: items.length > 0,
    staleTime: 5 * 60 * 1000,
    placeholderData: previousData => previousData,
    queryFn: async () => {
      await ensureLegacyLocalDataMigrated();
      return apiClient.listGlobalMerchants({
        q: merchantQuery.length > 0 ? merchantQuery : undefined,
        limit: merchantQuery.length > 0 ? 200 : 300,
      });
    },
  });
  const [drafts, setDrafts] = useState<Record<string, ReviewDraft>>({});
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [errors, setErrors] = useState<Record<string, string>>({});

  useEffect(() => {
    setDrafts((prev) => {
      const next: Record<string, ReviewDraft> = {};

      items.forEach((item) => {
        next[item.id] = prev[item.id] ?? buildInitialDraft(item);
      });

      return next;
    });
  }, [items]);

  const optionsByType = useMemo(() => {
    const grouped: Record<Transaction['type'], CategoryOption[]> = {
      expense: [],
      income: [],
      transfer: [],
    };

    categories.forEach((category) => {
      grouped[category.type].push({
        id: category.id,
        label: category.name,
      });

      (category.subCategories ?? []).forEach((subCategoryName) => {
        const subCategoryId = category.subCategoryIds?.[subCategoryName];
        if (!subCategoryId) {
          return;
        }

        grouped[category.type].push({
          id: subCategoryId,
          label: `${category.name} / ${subCategoryName}`,
        });
      });
    });

    return grouped;
  }, [categories]);

  const updateDraft = (transactionId: string, patch: Partial<ReviewDraft>) => {
    setDrafts((prev) => ({
      ...prev,
      [transactionId]: {
        ...(prev[transactionId] ?? {
          categoryId: '',
          merchantId: '',
          userNote: '',
          applyRule: false,
          ruleSearchKey: '',
        }),
        ...patch,
      },
    }));
  };

  const merchantOptions = merchantsQuery.data ?? [];
  const merchantHint = useMemo(() => {
    if (merchantsQuery.isError) {
      return 'Could not load merchants right now. Try again.';
    }

    if (merchantsQuery.isFetching) {
      return 'Loading merchants...';
    }

    if (merchantQuery.length > 0 && merchantOptions.length === 0) {
      return 'No merchants found for this search.';
    }

    if (merchantQuery.length === 0) {
      return 'Showing common merchants. Search to find others.';
    }

    return null;
  }, [merchantOptions.length, merchantQuery.length, merchantsQuery.isError, merchantsQuery.isFetching]);

  const merchantNameById = useMemo(
    () =>
      new Map(
        merchantOptions.map((merchant) => [
          merchant.id,
          `${merchant.canonical_name} (${merchant.type.toLowerCase()})`,
        ]),
      ),
    [merchantOptions],
  );

  const submitReview = async (transaction: Transaction) => {
    const draft = drafts[transaction.id] ?? buildInitialDraft(transaction);
    const nextCategoryId = normalizeOptionalString(draft.categoryId);
    const initialCategoryId = transaction.persistedCategoryId ?? null;
    const nextMerchantId = normalizeOptionalString(draft.merchantId);
    const initialMerchantId = transaction.merchantId ?? null;
    const nextUserNote = normalizeOptionalString(draft.userNote);
    const initialUserNote = normalizeOptionalString(transaction.description ?? '');

    if (draft.applyRule && !nextCategoryId && !nextMerchantId) {
      setErrors((prev) => ({
        ...prev,
        [transaction.id]: 'Rule requires category or merchant selection.',
      }));
      return;
    }

    const payload: ReviewTransactionRequest = {};

    if (nextCategoryId !== initialCategoryId) {
      payload.category_id = nextCategoryId;
    }

    if (nextMerchantId !== initialMerchantId) {
      payload.merchant_id = nextMerchantId;
    }

    if (nextUserNote !== initialUserNote) {
      payload.user_note = nextUserNote;
    }

    if (draft.applyRule) {
      payload.apply_rule = true;
      const normalizedRuleSearchKey = normalizeOptionalString(draft.ruleSearchKey);
      if (normalizedRuleSearchKey) {
        payload.rule_search_key = normalizedRuleSearchKey;
      }
    }

    setPendingId(transaction.id);
    setErrors((prev) => {
      const next = { ...prev };
      delete next[transaction.id];
      return next;
    });

    try {
      await onReview({
        transactionId: transaction.id,
        payload,
      });
    } catch (error) {
      setErrors((prev) => ({
        ...prev,
        [transaction.id]: error instanceof Error ? error.message : 'Failed to submit review',
      }));
    } finally {
      setPendingId(null);
    }
  };

  if (items.length === 0) {
    return null;
  }

  return (
    <section className="review-inbox">
      <div className="review-inbox-header">
        <h3>Review Inbox</h3>
        <span>{items.length} pending</span>
      </div>
      <div className="review-inbox-toolbar">
        <label className="review-field-label">
          Merchant Search
          <input
            type="search"
            value={merchantSearchInput}
            onChange={(event) => {
              setMerchantSearchInput(event.target.value);
            }}
            placeholder="Search merchant name"
          />
        </label>
        {merchantHint && <p className="review-meta-text">{merchantHint}</p>}
      </div>

      <div className="review-inbox-list">
        {items.map((transaction) => {
          const draft = drafts[transaction.id] ?? buildInitialDraft(transaction);
          const options = optionsByType[transaction.type] ?? [];
          const amountPrefix =
            transaction.type === 'expense'
              ? '-'
              : transaction.type === 'income'
                ? '+'
                : '';

          return (
            <article key={transaction.id} className="review-card">
              <div className="review-card-header">
                <div>
                  <div className="review-card-title">{transaction.category}</div>
                  <div className="review-card-meta">
                    {formatDate(transaction.date)} · AI confidence {formatConfidence(transaction.aiConfidenceScore)}
                  </div>
                </div>
                <div className={`review-card-amount ${transaction.type}`}>
                  {amountPrefix}₹{transaction.amount.toFixed(2)}
                </div>
              </div>

              {transaction.searchKey && (
                <div className="review-card-search-key">Search Key: {transaction.searchKey}</div>
              )}
              {(transaction.merchantName || transaction.merchantId) && (
                <div className="review-card-search-key">
                  Detected merchant:{' '}
                  {transaction.merchantName ??
                    merchantNameById.get(transaction.merchantId ?? '') ??
                    transaction.merchantId}
                </div>
              )}

              <label className="review-field-label">
                Merchant
                <select
                  value={draft.merchantId}
                  onChange={(event) => {
                    updateDraft(transaction.id, { merchantId: event.target.value });
                  }}
                  disabled={pendingId === transaction.id}
                >
                  <option value="">Unknown / no merchant</option>
                  {draft.merchantId &&
                    !merchantNameById.has(draft.merchantId) && (
                      <option value={draft.merchantId}>
                        {draft.merchantId === transaction.merchantId && transaction.merchantName
                          ? `${transaction.merchantName} (current)`
                          : `Selected (${draft.merchantId.slice(0, 8)}...)`}
                      </option>
                    )}
                  {merchantOptions.map((merchant) => (
                    <option key={merchant.id} value={merchant.id}>
                      {merchant.canonical_name}
                    </option>
                  ))}
                </select>
              </label>

              <label className="review-field-label">
                Category
                <select
                  value={draft.categoryId}
                  onChange={(event) => {
                    updateDraft(transaction.id, { categoryId: event.target.value });
                  }}
                  disabled={pendingId === transaction.id}
                >
                  <option value="">No category</option>
                  {options.map((option) => (
                    <option key={option.id} value={option.id}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label className="review-field-label">
                Note
                <textarea
                  rows={2}
                  value={draft.userNote}
                  onChange={(event) => {
                    updateDraft(transaction.id, { userNote: event.target.value });
                  }}
                  disabled={pendingId === transaction.id}
                  placeholder="Add a note for this transaction"
                />
              </label>

              <label className="review-checkbox">
                <input
                  type="checkbox"
                  checked={draft.applyRule}
                  disabled={pendingId === transaction.id}
                  onChange={(event) => {
                    updateDraft(transaction.id, { applyRule: event.target.checked });
                  }}
                />
                <span>Apply for similar transactions</span>
              </label>

              {draft.applyRule && (
                <label className="review-field-label">
                  Rule Search Key (optional)
                  <input
                    type="text"
                    value={draft.ruleSearchKey}
                    onChange={(event) => {
                      updateDraft(transaction.id, { ruleSearchKey: event.target.value });
                    }}
                    disabled={pendingId === transaction.id}
                    placeholder="Default uses detected search key"
                  />
                </label>
              )}

              {errors[transaction.id] && (
                <p className="review-error-text">{errors[transaction.id]}</p>
              )}

              <button
                className="review-approve-btn"
                type="button"
                onClick={() => {
                  void submitReview(transaction);
                }}
                disabled={pendingId === transaction.id}
              >
                {pendingId === transaction.id ? 'Saving...' : 'Approve'}
              </button>
            </article>
          );
        })}
      </div>
    </section>
  );
}
