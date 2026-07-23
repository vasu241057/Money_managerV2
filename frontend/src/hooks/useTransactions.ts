import { useEffect, useMemo } from 'react';
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  AccountRow,
  CategoryRow,
  PaymentMethod,
  ReviewTransactionRequest,
  TransactionFeedItem,
} from '../../../shared/types';
import { apiClient, toErrorMessage } from '../lib/api-client';
import { paiseToRupees, rupeesToPaise } from '../lib/money';
import type { Transaction } from '../types/domain';
import {
  areImmutableFieldsChanged,
  normalizeOptionalId,
  normalizeOptionalText,
  resolvePersistedCategoryId,
} from '../lib/transaction-adapter';

interface UseTransactionsResult {
  transactions: Transaction[];
  /** True once every feed page has been fetched (deep links can trust a miss). */
  isFeedComplete: boolean;
  reviewInbox: Transaction[];
  addTransaction: (transaction: Omit<Transaction, 'id'> | Transaction) => Promise<Transaction>;
  reviewTransaction: (params: {
    transactionId: string;
    payload: ReviewTransactionRequest;
  }) => Promise<Transaction>;
  deleteTransaction: (id: string) => Promise<void>;
  isLoading: boolean;
  error: string | null;
}

const TRANSACTIONS_QUERY_KEY = ['transactions'] as const;
const ACCOUNTS_QUERY_KEY = ['accounts'] as const;
const CATEGORIES_QUERY_KEY = ['categories'] as const;
const ANALYTICS_SUMMARY_QUERY_KEY = ['analytics-summary'] as const;
const FEED_STALE_TIME_MS = 30_000;

function mapFeedItemToUiTransaction(
  item: TransactionFeedItem,
  accountsById: Map<string, AccountRow>,
  categoriesById: Map<string, CategoryRow>,
): Transaction {
  const transaction = item.transaction;
  const accountId = transaction.account_id;
  const accountName = accountId ? accountsById.get(accountId)?.name ?? item.account?.name : undefined;

  let categoryName = 'Uncategorized';
  let categoryId: string | null | undefined = transaction.category_id;
  let categoryIcon: string | null | undefined = item.category?.icon;
  let subCategory: string | undefined;
  let subCategoryId: string | null | undefined;

  if (transaction.category_id) {
    const matchedCategory = categoriesById.get(transaction.category_id);
    if (matchedCategory?.parent_id) {
      const parent = categoriesById.get(matchedCategory.parent_id);
      // If parent is a root (parent_id === null), the matched category IS the parent
      // (e.g., "Food" under "Expense" root) — show it directly, not the root name
      if (parent && parent.parent_id === null) {
        categoryName = matchedCategory.name;
        categoryId = matchedCategory.id;
        categoryIcon = matchedCategory.icon;
      } else if (parent) {
        // Normal 3-level: parent → sub (e.g., Food → Groceries)
        categoryName = parent.name;
        categoryId = parent.id;
        subCategory = matchedCategory.name;
        subCategoryId = matchedCategory.id;
        categoryIcon = matchedCategory.icon ?? parent.icon;
      } else {
        categoryName = matchedCategory.name;
        categoryId = matchedCategory.id;
        categoryIcon = matchedCategory.icon;
      }
    } else if (matchedCategory) {
      categoryName = matchedCategory.name;
      categoryId = matchedCategory.id;
      categoryIcon = matchedCategory.icon;
    } else if (item.category) {
      categoryName = item.category.name;
      categoryId = item.category.id;
      categoryIcon = item.category.icon;
    }
  } else if (item.category) {
    categoryName = item.category.name;
    categoryId = item.category.id;
    categoryIcon = item.category.icon;
  }

  return {
    id: transaction.id,
    amount: paiseToRupees(transaction.amount_in_paise),
    type: transaction.type,
    category: categoryName,
    categoryId,
    categoryIcon,
    subCategory,
    subCategoryId,
    date: new Date(transaction.txn_date),
    description: transaction.user_note ?? undefined,
    accountId,
    accountName,
    isManual: item.financial_event.raw_email_id === null,
    persistedCategoryId: transaction.category_id,
    merchantId: transaction.merchant_id,
    merchantName: item.merchant?.canonical_name ?? null,
    merchantType: item.merchant?.type ?? null,
    searchKey: item.financial_event.search_key,
    counterpartyRaw: item.financial_event.counterparty_raw,
    paymentMethod: item.financial_event.payment_method,
    instrumentLast4: item.credit_card?.last4 ?? item.financial_event.instrument_id ?? null,
    creditCardId: item.credit_card?.id ?? null,
    creditCardLabel: item.credit_card?.card_label ?? null,
    status: transaction.status,
    classificationSource: transaction.classification_source,
    aiConfidenceScore: transaction.ai_confidence_score,
  };
}

const FEED_PAGE_LIMIT = 100;

function useRemoteTransactions(): UseTransactionsResult {
  const queryClient = useQueryClient();

  const accountsQuery = useQuery({
    queryKey: ACCOUNTS_QUERY_KEY,
    queryFn: async () => {
      return apiClient.listAccounts();
    },
    staleTime: FEED_STALE_TIME_MS,
  });

  const categoriesQuery = useQuery({
    queryKey: CATEGORIES_QUERY_KEY,
    queryFn: async () => {
      // include_hidden must match useCategories: both hooks share this query
      // key, and hidden rows are needed so transactions pointing at a hidden
      // category still resolve its name.
      return apiClient.listCategories(undefined, true);
    },
    staleTime: FEED_STALE_TIME_MS,
  });

  // Paged so the first 100 rows render immediately; the effect below pumps
  // the remaining pages in the background instead of blocking first paint.
  const transactionsQuery = useInfiniteQuery({
    queryKey: TRANSACTIONS_QUERY_KEY,
    initialPageParam: 1,
    queryFn: async ({ pageParam }) => {
      return apiClient.listTransactions({ page: pageParam, limit: FEED_PAGE_LIMIT });
    },
    getNextPageParam: (lastPage, _pages, lastPageParam) =>
      lastPage.has_more ? lastPageParam + 1 : undefined,
    staleTime: FEED_STALE_TIME_MS,
  });

  const { hasNextPage, isFetchingNextPage, isError: feedHasError, fetchNextPage } = transactionsQuery;
  useEffect(() => {
    if (hasNextPage && !isFetchingNextPage && !feedHasError) {
      void fetchNextPage();
    }
  }, [hasNextPage, isFetchingNextPage, feedHasError, fetchNextPage]);

  const accountsById = useMemo(() => {
    const rows = accountsQuery.data ?? [];
    return new Map(rows.map(row => [row.id, row]));
  }, [accountsQuery.data]);

  const categoriesById = useMemo(() => {
    const rows = categoriesQuery.data ?? [];
    return new Map(rows.map(row => [row.id, row]));
  }, [categoriesQuery.data]);

  const transactions = useMemo(() => {
    const rows = transactionsQuery.data?.pages.flatMap(page => page.data) ?? [];
    return rows.map(item => mapFeedItemToUiTransaction(item, accountsById, categoriesById));
  }, [transactionsQuery.data, accountsById, categoriesById]);

  const transactionsById = useMemo(
    () => new Map(transactions.map(transaction => [transaction.id, transaction])),
    [transactions],
  );
  const reviewInbox = useMemo(
    () =>
      transactions
        .filter(transaction => transaction.status === 'NEEDS_REVIEW')
        .sort((left, right) => {
          const leftConfidence = left.aiConfidenceScore ?? Number.POSITIVE_INFINITY;
          const rightConfidence = right.aiConfidenceScore ?? Number.POSITIVE_INFINITY;
          return leftConfidence - rightConfidence;
        }),
    [transactions],
  );

  const upsertTransactionMutation = useMutation({
    mutationFn: async (transactionInput: Omit<Transaction, 'id'> | Transaction) => {
      const accountIdToPersist = normalizeOptionalId(transactionInput.accountId);

      if ('id' in transactionInput && transactionInput.id) {
        const existing = transactionsById.get(transactionInput.id);
        if (!existing) {
          throw new Error('Transaction not found. Refresh and try again.');
        }

        // The editor manages parent + sub selection explicitly now, so the
        // nested-path restore options are no longer wanted: a cleared sub
        // must stay cleared.
        const categoryIdToPersist = resolvePersistedCategoryId(transactionInput);

        if (areImmutableFieldsChanged(existing, transactionInput)) {
          if (!existing.isManual) {
            throw new Error('Amount, type, and date cannot be changed for bank-detected transactions.');
          }

          const recreated = await apiClient.createManualTransaction({
            amount_in_paise: rupeesToPaise(transactionInput.amount),
            type: transactionInput.type,
            txn_date: transactionInput.date.toISOString(),
            account_id: accountIdToPersist,
            category_id: categoryIdToPersist,
            merchant_id: transactionInput.merchantId ?? null,
            user_note: normalizeOptionalText(transactionInput.description),
            payment_method: (transactionInput.paymentMethod ?? undefined) as PaymentMethod | undefined,
            instrument_id: transactionInput.instrumentLast4 ?? null,
            counterparty_raw: transactionInput.counterpartyRaw ?? null,
          });

          try {
            await apiClient.deleteTransaction(transactionInput.id);
          } catch (deleteError) {
            await queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });

            const detail =
              deleteError instanceof Error ? deleteError.message : 'Please refresh and retry.';
            throw new Error(
              `Updated transaction was created, but old transaction could not be removed. The new transaction was kept to prevent data loss. ${detail}`,
            );
          }

          return mapFeedItemToUiTransaction(recreated, accountsById, categoriesById);
        }

        const updated = await apiClient.updateTransaction(transactionInput.id, {
          account_id: accountIdToPersist,
          category_id: categoryIdToPersist,
          user_note: normalizeOptionalText(transactionInput.description),
        });

        return mapFeedItemToUiTransaction(updated, accountsById, categoriesById);
      }

      const categoryIdToPersist = resolvePersistedCategoryId(transactionInput);
      const created = await apiClient.createManualTransaction({
        amount_in_paise: rupeesToPaise(transactionInput.amount),
        type: transactionInput.type,
        txn_date: transactionInput.date.toISOString(),
        account_id: accountIdToPersist,
        category_id: categoryIdToPersist,
        user_note: normalizeOptionalText(transactionInput.description),
      });

      return mapFeedItemToUiTransaction(created, accountsById, categoriesById);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
    },
  });

  const deleteTransactionMutation = useMutation({
    mutationFn: async (transactionId: string) => {
      return apiClient.deleteTransaction(transactionId);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
    },
  });

  const reviewTransactionMutation = useMutation({
    mutationFn: async ({
      transactionId,
      payload,
    }: {
      transactionId: string;
      payload: ReviewTransactionRequest;
    }) => {
      const reviewed = await apiClient.reviewTransaction(transactionId, payload);
      return mapFeedItemToUiTransaction(reviewed.transaction, accountsById, categoriesById);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
    },
    onError: async () => {
      await queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
    },
  });

  const firstError =
    transactionsQuery.error ??
    accountsQuery.error ??
    categoriesQuery.error ??
    upsertTransactionMutation.error ??
    deleteTransactionMutation.error ??
    reviewTransactionMutation.error;

  return {
    transactions,
    isFeedComplete: transactionsQuery.isSuccess && !transactionsQuery.hasNextPage,
    reviewInbox,
    addTransaction: async transaction => upsertTransactionMutation.mutateAsync(transaction),
    reviewTransaction: async params => reviewTransactionMutation.mutateAsync(params),
    deleteTransaction: async id => {
      await deleteTransactionMutation.mutateAsync(id);
    },
    isLoading:
      transactionsQuery.isLoading ||
      accountsQuery.isLoading ||
      categoriesQuery.isLoading ||
      upsertTransactionMutation.isPending ||
      deleteTransactionMutation.isPending ||
      reviewTransactionMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
  };
}

export { type Transaction };

export function useTransactions(): UseTransactionsResult {
  return useRemoteTransactions();
}
