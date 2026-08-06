import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  TransactionType,
  UserMerchantListItem,
  UserMerchantTransactionItem,
} from '../../../shared/types';
import { apiClient, toErrorMessage } from '../lib/api-client';

export const USER_MERCHANTS_QUERY_KEY = ['user-merchants'] as const;

interface SetCategoryInput {
  merchantId: string;
  transactionType: TransactionType;
  categoryId: string;
  /** True rewrites every existing transaction for this merchant and type. */
  backfill: boolean;
  /** Set to scope the rule to one raw descriptor instead of the merchant. */
  searchKey?: string;
}

interface UseUserMerchantsResult {
  merchants: UserMerchantListItem[];
  isLoading: boolean;
  isSaving: boolean;
  error: string | null;
  retry: () => Promise<unknown>;
  /** Resolves with how many existing transactions the save rewrote. */
  setCategory: (input: SetCategoryInput) => Promise<number>;
}

export function useUserMerchants(): UseUserMerchantsResult {
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: USER_MERCHANTS_QUERY_KEY,
    queryFn: () => apiClient.listUserMerchants(),
    staleTime: 30_000,
  });

  const setCategoryMutation = useMutation({
    mutationFn: ({ merchantId, transactionType, categoryId, backfill, searchKey }: SetCategoryInput) =>
      searchKey === undefined
        ? apiClient.setUserMerchantCategory(merchantId, {
            transaction_type: transactionType,
            category_id: categoryId,
            backfill,
          })
        : apiClient.setUserDescriptorCategory(merchantId, {
            search_key: searchKey,
            transaction_type: transactionType,
            category_id: categoryId,
            backfill,
          }),
    onSuccess: async () => {
      // A backfill rewrites categories, review status and therefore budget
      // totals, so every surface reading transactions has to refetch.
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: USER_MERCHANTS_QUERY_KEY }),
        queryClient.invalidateQueries({ queryKey: ['transactions'] }),
        queryClient.invalidateQueries({ queryKey: ['budget'] }),
        queryClient.invalidateQueries({ queryKey: ['analytics-summary'] }),
      ]);
    },
  });

  return {
    merchants: query.data ?? [],
    isLoading: query.isLoading,
    isSaving: setCategoryMutation.isPending,
    error: query.error ? toErrorMessage(query.error) : null,
    retry: () => query.refetch(),
    setCategory: async (input) => {
      const response = await setCategoryMutation.mutateAsync(input);
      return response.backfilled_count;
    },
  };
}

/** Transaction history for one merchant. Fetches only while a merchant is open. */
export function useUserMerchantTransactions(merchantId: string | null): {
  transactions: UserMerchantTransactionItem[];
  isLoading: boolean;
  error: string | null;
} {
  const query = useQuery({
    queryKey: [...USER_MERCHANTS_QUERY_KEY, 'transactions', merchantId],
    queryFn: () => apiClient.listUserMerchantTransactions(merchantId as string),
    enabled: merchantId !== null,
    staleTime: 30_000,
  });

  return {
    transactions: query.data ?? [],
    isLoading: query.isLoading,
    error: query.error ? toErrorMessage(query.error) : null,
  };
}
