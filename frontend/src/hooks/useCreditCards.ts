import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { CreateUserCreditCardRequest, UpdateUserCreditCardRequest, UserCreditCardRow } from '../../../shared/types';
import { apiClient, toErrorMessage } from '../lib/api-client';

interface UseCreditCardsResult {
  creditCards: UserCreditCardRow[];
  addCreditCard: (payload: CreateUserCreditCardRequest) => Promise<{ relinkedTransactionCount: number }>;
  updateCreditCard: (id: string, payload: UpdateUserCreditCardRequest) => Promise<{ relinkedTransactionCount: number }>;
  deleteCreditCard: (id: string) => Promise<void>;
  isLoading: boolean;
  error: string | null;
}

const CREDIT_CARDS_QUERY_KEY = ['credit-cards'] as const;

function useRemoteCreditCards(): UseCreditCardsResult {
  const queryClient = useQueryClient();

  const creditCardsQuery = useQuery({
    queryKey: CREDIT_CARDS_QUERY_KEY,
    queryFn: async () => {
      return apiClient.listCreditCards();
    },
  });

  const addCreditCardMutation = useMutation({
    mutationFn: async (payload: CreateUserCreditCardRequest) => {
      const result = await apiClient.createCreditCard(payload);
      return { relinkedTransactionCount: result.relinkedTransactionCount };
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CREDIT_CARDS_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ['transactions'] });
    },
  });

  const updateCreditCardMutation = useMutation({
    mutationFn: async ({ id, payload }: { id: string; payload: UpdateUserCreditCardRequest }) => {
      const result = await apiClient.updateCreditCard(id, payload);
      return { relinkedTransactionCount: result.relinkedTransactionCount };
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CREDIT_CARDS_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ['transactions'] });
    },
  });

  const deleteCreditCardMutation = useMutation({
    mutationFn: async (creditCardId: string) => {
      return apiClient.deleteCreditCard(creditCardId);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CREDIT_CARDS_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ['transactions'] });
    },
  });

  const firstError =
    creditCardsQuery.error ??
    addCreditCardMutation.error ??
    updateCreditCardMutation.error ??
    deleteCreditCardMutation.error;

  return {
    creditCards: creditCardsQuery.data ?? [],
    addCreditCard: async payload => {
      const result = await addCreditCardMutation.mutateAsync(payload);
      return { relinkedTransactionCount: result.relinkedTransactionCount };
    },
    updateCreditCard: async (id, payload) => {
      const result = await updateCreditCardMutation.mutateAsync({ id, payload });
      return { relinkedTransactionCount: result.relinkedTransactionCount };
    },
    deleteCreditCard: async id => {
      await deleteCreditCardMutation.mutateAsync(id);
    },
    isLoading:
      creditCardsQuery.isLoading ||
      addCreditCardMutation.isPending ||
      updateCreditCardMutation.isPending ||
      deleteCreditCardMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
  };
}

export { type UserCreditCardRow };

export function useCreditCards(): UseCreditCardsResult {
  return useRemoteCreditCards();
}
