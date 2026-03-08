import { useMemo } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { AccountRow, CategoryRow, TransactionFeedItem } from '../../../shared/types';
import { isRemoteDataEnabled } from '../config/data-source';
import { apiClient, toErrorMessage } from '../lib/api-client';
import { paiseToRupees, rupeesToPaise } from '../lib/money';
import { useLocalTransactions, type Transaction } from './local/useLocalTransactions';

interface UseTransactionsResult {
  transactions: Transaction[];
  addTransaction: (transaction: Omit<Transaction, 'id'> | Transaction) => Promise<Transaction>;
  deleteTransaction: (id: string) => Promise<void>;
  isLoading: boolean;
  error: string | null;
}

const REMOTE_DATA_ENABLED = isRemoteDataEnabled();
const TRANSACTIONS_QUERY_KEY = ['transactions'] as const;
const ACCOUNTS_QUERY_KEY = ['accounts'] as const;
const CATEGORIES_QUERY_KEY = ['categories'] as const;

function normalizeOptionalId(value: string | null | undefined): string | null {
  if (!value || value.trim() === '') {
    return null;
  }

  return value;
}

function normalizeOptionalText(value: string | undefined): string | null {
  if (!value) {
    return null;
  }

  const normalized = value.trim();
  return normalized === '' ? null : normalized;
}

function areImmutableFieldsChanged(current: Transaction, next: Transaction): boolean {
  if (current.amount !== next.amount) {
    return true;
  }

  if (current.type !== next.type) {
    return true;
  }

  return current.date.getTime() !== next.date.getTime();
}

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
  let subCategory: string | undefined;
  let subCategoryId: string | null | undefined;

  if (transaction.category_id) {
    const matchedCategory = categoriesById.get(transaction.category_id);
    if (matchedCategory?.parent_id) {
      const parent = categoriesById.get(matchedCategory.parent_id);
      if (parent) {
        categoryName = parent.name;
        categoryId = parent.id;
        subCategory = matchedCategory.name;
        subCategoryId = matchedCategory.id;
      } else {
        categoryName = matchedCategory.name;
        categoryId = matchedCategory.id;
      }
    } else if (matchedCategory) {
      categoryName = matchedCategory.name;
      categoryId = matchedCategory.id;
    } else if (item.category) {
      categoryName = item.category.name;
      categoryId = item.category.id;
    }
  } else if (item.category) {
    categoryName = item.category.name;
    categoryId = item.category.id;
  }

  return {
    id: transaction.id,
    amount: paiseToRupees(transaction.amount_in_paise),
    type: transaction.type,
    category: categoryName,
    categoryId,
    subCategory,
    subCategoryId,
    date: new Date(transaction.txn_date),
    description: transaction.user_note ?? undefined,
    accountId,
    accountName,
  };
}

async function listAllTransactions(): Promise<TransactionFeedItem[]> {
  const limit = 200;
  let page = 1;
  const allItems: TransactionFeedItem[] = [];

  while (true) {
    const response = await apiClient.listTransactions({ page, limit });
    allItems.push(...response.data);

    if (!response.has_more) {
      break;
    }

    page += 1;
  }

  return allItems;
}

function useLocalTransactionFallback(): UseTransactionsResult {
  const local = useLocalTransactions();

  return {
    transactions: local.transactions,
    addTransaction: async transaction => local.addTransaction(transaction),
    deleteTransaction: async id => {
      local.deleteTransaction(id);
    },
    isLoading: false,
    error: null,
  };
}

function useRemoteTransactions(): UseTransactionsResult {
  const queryClient = useQueryClient();

  const accountsQuery = useQuery({
    queryKey: ACCOUNTS_QUERY_KEY,
    queryFn: () => apiClient.listAccounts(),
  });

  const categoriesQuery = useQuery({
    queryKey: CATEGORIES_QUERY_KEY,
    queryFn: () => apiClient.listCategories(),
  });

  const transactionsQuery = useQuery({
    queryKey: TRANSACTIONS_QUERY_KEY,
    queryFn: listAllTransactions,
  });

  const accountsById = useMemo(() => {
    const rows = accountsQuery.data ?? [];
    return new Map(rows.map(row => [row.id, row]));
  }, [accountsQuery.data]);

  const categoriesById = useMemo(() => {
    const rows = categoriesQuery.data ?? [];
    return new Map(rows.map(row => [row.id, row]));
  }, [categoriesQuery.data]);

  const transactions = useMemo(() => {
    const rows = transactionsQuery.data ?? [];
    return rows.map(item => mapFeedItemToUiTransaction(item, accountsById, categoriesById));
  }, [transactionsQuery.data, accountsById, categoriesById]);

  const transactionsById = useMemo(
    () => new Map(transactions.map(transaction => [transaction.id, transaction])),
    [transactions],
  );

  const upsertTransactionMutation = useMutation({
    mutationFn: async (transactionInput: Omit<Transaction, 'id'> | Transaction) => {
      const categoryIdToPersist = normalizeOptionalId(
        transactionInput.subCategoryId ?? transactionInput.categoryId,
      );
      const accountIdToPersist = normalizeOptionalId(transactionInput.accountId);

      if ('id' in transactionInput && transactionInput.id) {
        const existing = transactionsById.get(transactionInput.id);
        if (!existing) {
          throw new Error('Transaction not found. Refresh and try again.');
        }

        if (areImmutableFieldsChanged(existing, transactionInput)) {
          await apiClient.deleteTransaction(transactionInput.id);
          const recreated = await apiClient.createManualTransaction({
            amount_in_paise: rupeesToPaise(transactionInput.amount),
            type: transactionInput.type,
            txn_date: transactionInput.date.toISOString(),
            account_id: accountIdToPersist,
            category_id: categoryIdToPersist,
            user_note: normalizeOptionalText(transactionInput.description),
          });

          return mapFeedItemToUiTransaction(recreated, accountsById, categoriesById);
        }

        const updated = await apiClient.updateTransaction(transactionInput.id, {
          account_id: accountIdToPersist,
          category_id: categoryIdToPersist,
          user_note: normalizeOptionalText(transactionInput.description),
        });

        return mapFeedItemToUiTransaction(updated, accountsById, categoriesById);
      }

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
    },
  });

  const deleteTransactionMutation = useMutation({
    mutationFn: (transactionId: string) => apiClient.deleteTransaction(transactionId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: TRANSACTIONS_QUERY_KEY });
    },
  });

  const firstError =
    transactionsQuery.error ??
    accountsQuery.error ??
    categoriesQuery.error ??
    upsertTransactionMutation.error ??
    deleteTransactionMutation.error;

  return {
    transactions,
    addTransaction: async transaction => upsertTransactionMutation.mutateAsync(transaction),
    deleteTransaction: async id => {
      await deleteTransactionMutation.mutateAsync(id);
    },
    isLoading:
      transactionsQuery.isLoading ||
      accountsQuery.isLoading ||
      categoriesQuery.isLoading ||
      upsertTransactionMutation.isPending ||
      deleteTransactionMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
  };
}

export { type Transaction };

export function useTransactions(): UseTransactionsResult {
  if (REMOTE_DATA_ENABLED) {
    return useRemoteTransactions();
  }

  return useLocalTransactionFallback();
}
