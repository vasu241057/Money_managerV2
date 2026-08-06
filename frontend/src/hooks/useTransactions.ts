import { useEffect, useMemo } from 'react';
import {
  type InfiniteData,
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import type {
  AccountRow,
  CategoryRow,
  PaginatedResponse,
  PaymentMethod,
  ReviewTransactionRequest,
  BudgetTreatment,
  TransactionFeedItem,
} from '../../../shared/types';
import { apiClient, toErrorMessage } from '../lib/api-client';
import { paiseToRupees, rupeesToPaise } from '../lib/money';
import type { Transaction } from '../types/domain';
import {
  areImmutableSourceFieldsChanged,
  normalizeOptionalId,
  normalizeOptionalText,
  resolvePersistedCategoryId,
  transactionDirection,
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
  }) => Promise<{ transaction: Transaction; backfilledCount: number }>;
  deleteTransaction: (id: string) => Promise<void>;
  restoreTransaction: (id: string) => Promise<void>;
  setBudgetTreatment: (id: string, treatment: BudgetTreatment) => Promise<Transaction>;
	loadMore: () => Promise<void>;
	hasMore: boolean;
	isFetchingMore: boolean;
	isInitialLoading: boolean;
  isLoading: boolean;
  error: string | null;
}

interface UseTransactionsOptions {
	search?: string;
	from?: string;
	to?: string;
	autoLoadAll?: boolean;
}

const TRANSACTIONS_QUERY_KEY = ['transactions'] as const;
const ACCOUNTS_QUERY_KEY = ['accounts'] as const;
const CATEGORIES_QUERY_KEY = ['categories'] as const;
const ANALYTICS_SUMMARY_QUERY_KEY = ['analytics-summary'] as const;
const FEED_STALE_TIME_MS = 30_000;
type TransactionFeedCache = InfiniteData<PaginatedResponse<TransactionFeedItem>, number>;

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
        // Ledger artwork intentionally follows the parent category. A
        // subcategory still appears in metadata, but does not multiply the
        // visual vocabulary until explicitly curated in a future pass.
        categoryIcon = parent.icon;
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
    direction: item.financial_event.direction,
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
    merchantLogoUrl: item.merchant?.logo?.public_url ?? null,
    merchantBrandColor: item.merchant?.logo?.brand_color ?? null,
    searchKey: item.financial_event.search_key,
    counterpartyRaw: item.financial_event.counterparty_raw,
    paymentMethod: item.financial_event.payment_method,
    instrumentLast4: item.credit_card?.last4 ?? item.financial_event.instrument_id ?? null,
    creditCardId: item.credit_card?.id ?? null,
    creditCardLabel: item.credit_card?.card_label ?? null,
    status: transaction.status,
    classificationSource: transaction.classification_source,
    aiConfidenceScore: transaction.ai_confidence_score,
    budgetTreatment: item.budget.treatment,
    effectiveBudgetTreatment: item.budget.effective_treatment,
    budgetTreatmentSource: item.budget.treatment_source,
    budgetEffect: item.budget.effect,
    budgetEffectAmount: paiseToRupees(item.budget.effect_in_paise),
    budgetRelationship: item.budget.relationship,
  };
}

const FEED_PAGE_LIMIT = 100;

function useRemoteTransactions(options: UseTransactionsOptions = {}): UseTransactionsResult {
  const queryClient = useQueryClient();
	const search = options.search?.trim() ?? '';
	const from = options.from?.trim() ?? '';
	const to = options.to?.trim() ?? '';
	const transactionsQueryKey = search || from || to
		? [...TRANSACTIONS_QUERY_KEY, {
			q: search || undefined,
			from: from || undefined,
			to: to || undefined,
		}] as const
		: TRANSACTIONS_QUERY_KEY;

  const updateCachedTransaction = (
    transactionId: string,
    update: (current: TransactionFeedItem) => TransactionFeedItem,
  ): TransactionFeedItem | null => {
    let previous: TransactionFeedItem | null = null;

	queryClient.setQueriesData<TransactionFeedCache>({ queryKey: TRANSACTIONS_QUERY_KEY }, current => {
      if (!current) {
        return current;
      }

	  let cacheChanged = false;
      const pages = current.pages.map(page => {
        let pageChanged = false;
        const data = page.data.map(item => {
          if (item.transaction.id !== transactionId) {
            return item;
          }

		  previous ??= item;
		  cacheChanged = true;
          pageChanged = true;
          return update(item);
        });

        return pageChanged ? { ...page, data } : page;
      });

	  return cacheChanged ? { ...current, pages } : current;
    });

    return previous;
  };

  const replaceCachedTransaction = (updated: TransactionFeedItem): boolean => {
    return updateCachedTransaction(updated.transaction.id, () => updated) !== null;
  };

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
	queryKey: transactionsQueryKey,
    initialPageParam: 1,
    queryFn: async ({ pageParam }) => {
		return apiClient.listTransactions({
			page: pageParam,
			limit: FEED_PAGE_LIMIT,
			q: search || undefined,
			from: from || undefined,
			to: to || undefined,
		});
    },
    getNextPageParam: (lastPage, _pages, lastPageParam) =>
      lastPage.has_more ? lastPageParam + 1 : undefined,
    staleTime: FEED_STALE_TIME_MS,
  });

  const { hasNextPage, isFetchingNextPage, isError: feedHasError, fetchNextPage } = transactionsQuery;
  useEffect(() => {
	if (options.autoLoadAll !== false && hasNextPage && !isFetchingNextPage && !feedHasError) {
      void fetchNextPage();
    }
	}, [options.autoLoadAll, hasNextPage, isFetchingNextPage, feedHasError, fetchNextPage]);

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
          const newestFirst = right.date.getTime() - left.date.getTime();
          return newestFirst !== 0 ? newestFirst : right.id.localeCompare(left.id);
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

        if (areImmutableSourceFieldsChanged(existing, transactionInput)) {
          if (!existing.isManual) {
            throw new Error('Amount and date cannot be changed for bank-detected transactions.');
          }

          const recreated = await apiClient.createManualTransaction({
            amount_in_paise: rupeesToPaise(transactionInput.amount),
            type: transactionInput.type,
            direction: transactionDirection(transactionInput),
            txn_date: transactionInput.date.toISOString(),
            account_id: accountIdToPersist,
            category_id: categoryIdToPersist,
            merchant_id: transactionInput.merchantId ?? null,
            user_note: normalizeOptionalText(transactionInput.description),
            payment_method: (transactionInput.paymentMethod ?? undefined) as PaymentMethod | undefined,
            instrument_id: transactionInput.instrumentLast4 ?? null,
            counterparty_raw: transactionInput.counterpartyRaw ?? null,
            budget_treatment: transactionInput.budgetTreatment,
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

          return recreated;
        }

        const updated = await apiClient.updateTransaction(transactionInput.id, {
          type: transactionInput.type === existing.type ? undefined : transactionInput.type,
          account_id: accountIdToPersist,
          category_id: categoryIdToPersist,
          user_note: normalizeOptionalText(transactionInput.description),
          budget_treatment:
            transactionInput.budgetTreatment === existing.budgetTreatment
              ? undefined
              : transactionInput.budgetTreatment,
        });

        return updated;
      }

      const categoryIdToPersist = resolvePersistedCategoryId(transactionInput);
      const created = await apiClient.createManualTransaction({
        amount_in_paise: rupeesToPaise(transactionInput.amount),
        type: transactionInput.type,
        direction: transactionDirection(transactionInput),
        txn_date: transactionInput.date.toISOString(),
        account_id: accountIdToPersist,
        category_id: categoryIdToPersist,
        user_note: normalizeOptionalText(transactionInput.description),
      });

      return created;
    },
    onMutate: async transactionInput => {
      if (!('id' in transactionInput) || !transactionInput.id) {
        return { previous: null };
      }

      const existing = transactionsById.get(transactionInput.id);
      if (!existing || areImmutableSourceFieldsChanged(existing, transactionInput)) {
        return { previous: null };
      }

      await queryClient.cancelQueries({ queryKey: TRANSACTIONS_QUERY_KEY });

      const accountId = normalizeOptionalId(transactionInput.accountId);
      const categoryId = resolvePersistedCategoryId(transactionInput);
      const userNote = normalizeOptionalText(transactionInput.description);
      const previous = updateCachedTransaction(transactionInput.id, item => ({
        ...item,
        transaction: {
          ...item.transaction,
          type: transactionInput.type,
          account_id: accountId,
          category_id: categoryId,
          user_note: userNote,
        },
        account:
          accountId === item.account?.id
            ? item.account
            : null,
        category:
          categoryId === item.category?.id
            ? item.category
            : null,
      }));

      return { previous };
    },
    onSuccess: updated => {
      // The mutation response already contains the fully joined feed row.
      // Patch that one cached row instead of blocking on a sequential refetch
      // of every loaded infinite-query page. New/recreated rows are uncommon;
      // refresh those feeds in the background because their page position may
      // have changed.
      if (!replaceCachedTransaction(updated)) {
        void queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      }
	  void queryClient.invalidateQueries({
		  queryKey: TRANSACTIONS_QUERY_KEY,
		  predicate: query => query.queryKey.length > TRANSACTIONS_QUERY_KEY.length,
	  });
      void queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ['budget'] });
    },
    onError: (_error, _transactionInput, context) => {
      if (context?.previous) {
        replaceCachedTransaction(context.previous);
      }
      void queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ['budget'] });
    },
  });

  const deleteTransactionMutation = useMutation({
    mutationFn: async (transactionId: string) => {
      return apiClient.deleteTransaction(transactionId);
    },
    onMutate: async transactionId => {
	  await queryClient.cancelQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
	  const snapshots = queryClient.getQueriesData<TransactionFeedCache>({
		  queryKey: TRANSACTIONS_QUERY_KEY,
	  });
	  queryClient.setQueriesData<TransactionFeedCache>({ queryKey: TRANSACTIONS_QUERY_KEY }, current => {
		  if (!current) return current;
		  let removedCount = 0;
		  const pages = current.pages.map(page => {
			  const data = page.data.filter(item => {
				  const keep = item.transaction.id !== transactionId;
				  if (!keep) removedCount += 1;
				  return keep;
			  });
			  return data.length === page.data.length ? page : { ...page, data };
		  });
		  if (removedCount === 0) return current;
		  const total = Math.max(0, (current.pages[0]?.total ?? 0) - removedCount);
		  return {
			  ...current,
			  pages: pages.map(page => ({
				  ...page,
				  total,
				  has_more: page.page * page.limit < total,
			  })),
		  };
	  });
	  return { snapshots };
    },
	onError: (_error, _transactionId, context) => {
	  for (const [queryKey, data] of context?.snapshots ?? []) {
		  queryClient.setQueryData(queryKey, data);
	  }
    },
	onSettled: () => {
	  void queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ['budget'] });
    },
  });

  const restoreTransactionMutation = useMutation({
    mutationFn: async (transactionId: string) => apiClient.restoreTransaction(transactionId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ['budget'] });
    },
  });

  const budgetTreatmentMutation = useMutation({
    mutationFn: async ({
      transactionId,
      treatment,
    }: {
      transactionId: string;
      treatment: BudgetTreatment;
    }) => apiClient.updateTransaction(transactionId, { budget_treatment: treatment }),
    onSuccess: updated => {
      if (!replaceCachedTransaction(updated)) {
        void queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      }
      void queryClient.invalidateQueries({ queryKey: ['budget'] });
    },
    onError: () => {
      void queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ['budget'] });
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
      return apiClient.reviewTransaction(transactionId, payload);
    },
    onMutate: async ({ transactionId, payload }) => {
      await queryClient.cancelQueries({ queryKey: TRANSACTIONS_QUERY_KEY });

      const previous = updateCachedTransaction(transactionId, item => {
        const categoryId =
          payload.category_id === undefined
            ? item.transaction.category_id
            : payload.category_id;
        const merchantId =
          payload.merchant_id === undefined
            ? item.transaction.merchant_id
            : payload.merchant_id;

        return {
          ...item,
          transaction: {
            ...item.transaction,
            type: payload.type ?? item.transaction.type,
            category_id: categoryId,
            merchant_id: merchantId,
            user_note:
              payload.user_note === undefined
                ? item.transaction.user_note
                : payload.user_note,
            status: 'VERIFIED',
            classification_source: 'USER',
          },
          category:
            categoryId === item.category?.id
              ? item.category
              : null,
          merchant:
            merchantId === item.merchant?.id
              ? item.merchant
              : null,
        };
      });

      return { previous };
    },
    onSuccess: reviewed => {
      // A merchant backfill re-categorized and verified other transactions the
      // cache still holds at their old values, so the whole feed has to reload
      // rather than just the row that was reviewed.
      if (reviewed.backfilled_count > 0) {
        void queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
        void queryClient.invalidateQueries({ queryKey: ['budget'] });
        void queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
        return;
      }
      if (!replaceCachedTransaction(reviewed.transaction)) {
        void queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      }
	  void queryClient.invalidateQueries({
		  queryKey: TRANSACTIONS_QUERY_KEY,
		  predicate: query => query.queryKey.length > TRANSACTIONS_QUERY_KEY.length,
	  });
      void queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
    },
    onError: (_error, _variables, context) => {
      if (context?.previous) {
        replaceCachedTransaction(context.previous);
      }
      void queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: ANALYTICS_SUMMARY_QUERY_KEY });
    },
  });

  const firstError =
    transactionsQuery.error ??
    accountsQuery.error ??
    categoriesQuery.error ??
    upsertTransactionMutation.error ??
    deleteTransactionMutation.error ??
    restoreTransactionMutation.error ??
    budgetTreatmentMutation.error ??
    reviewTransactionMutation.error;

  return {
    transactions,
    isFeedComplete: transactionsQuery.isSuccess && !transactionsQuery.hasNextPage,
    reviewInbox,
    addTransaction: async transaction => {
      const saved = await upsertTransactionMutation.mutateAsync(transaction);
      return mapFeedItemToUiTransaction(saved, accountsById, categoriesById);
    },
    reviewTransaction: async params => {
      const reviewed = await reviewTransactionMutation.mutateAsync(params);
      return {
        transaction: mapFeedItemToUiTransaction(reviewed.transaction, accountsById, categoriesById),
        backfilledCount: reviewed.backfilled_count,
      };
    },
    deleteTransaction: async id => {
      await deleteTransactionMutation.mutateAsync(id);
    },
    restoreTransaction: async id => {
      await restoreTransactionMutation.mutateAsync(id);
    },
    setBudgetTreatment: async (id, treatment) => {
      const updated = await budgetTreatmentMutation.mutateAsync({ transactionId: id, treatment });
      return mapFeedItemToUiTransaction(updated, accountsById, categoriesById);
    },
	loadMore: async () => {
		if (transactionsQuery.hasNextPage && !transactionsQuery.isFetchingNextPage) {
			await transactionsQuery.fetchNextPage();
		}
	},
	hasMore: Boolean(transactionsQuery.hasNextPage),
	isFetchingMore: transactionsQuery.isFetchingNextPage,
	isInitialLoading:
	  transactionsQuery.isLoading || accountsQuery.isLoading || categoriesQuery.isLoading,
    isLoading:
      transactionsQuery.isLoading ||
      accountsQuery.isLoading ||
      categoriesQuery.isLoading ||
      upsertTransactionMutation.isPending ||
      deleteTransactionMutation.isPending ||
      restoreTransactionMutation.isPending ||
      budgetTreatmentMutation.isPending ||
      reviewTransactionMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
  };
}

export { type Transaction };

export function useTransactions(options: UseTransactionsOptions = {}): UseTransactionsResult {
  return useRemoteTransactions(options);
}
