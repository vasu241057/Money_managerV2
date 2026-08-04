import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { BudgetPlanCategoryRuleInput, EffectiveBudgetTreatment } from '../../../shared/types';
import { apiClient, toErrorMessage } from '../lib/api-client';

export const BUDGET_QUERY_KEY = ['budget'] as const;

export function useBudget(month: string) {
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: [...BUDGET_QUERY_KEY, month],
    queryFn: () => apiClient.getBudgetOverview(month),
    staleTime: 30_000,
  });

  const savePlanMutation = useMutation({
    mutationFn: ({
      limitInPaise,
      categoryRules,
    }: {
      limitInPaise: number;
      categoryRules: BudgetPlanCategoryRuleInput[];
    }) => apiClient.upsertBudgetPlan(month, {
      limit_in_paise: limitInPaise,
      category_rules: categoryRules,
    }),
    onSuccess: overview => {
      queryClient.setQueryData([...BUDGET_QUERY_KEY, month], overview);
      void queryClient.invalidateQueries({ queryKey: ['transactions'] });
    },
  });

  const linkRefundMutation = useMutation({
    mutationFn: ({ sourceId, targetId }: { sourceId: string; targetId: string }) =>
      apiClient.createTransactionRelationship({
        source_transaction_id: sourceId,
        target_transaction_id: targetId,
        kind: 'REFUND',
      }),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: BUDGET_QUERY_KEY }),
        queryClient.invalidateQueries({ queryKey: ['transactions'] }),
      ]);
    },
  });

  const dismissSuggestionMutation = useMutation({
    mutationFn: ({ type, fingerprint }: { type: 'REFUND_MATCH' | 'RECURRING_PATTERN'; fingerprint: string }) =>
      apiClient.dismissBudgetSuggestion({ type, fingerprint }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: BUDGET_QUERY_KEY });
    },
  });

  const confirmRecurringMutation = useMutation({
    mutationFn: ({
      fingerprint,
      transactionIds,
      treatment,
    }: {
      fingerprint: string;
      transactionIds: string[];
      treatment: EffectiveBudgetTreatment;
    }) => apiClient.confirmRecurringPattern({
      fingerprint,
      transaction_ids: transactionIds,
      treatment,
    }),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: BUDGET_QUERY_KEY }),
        queryClient.invalidateQueries({ queryKey: ['transactions'] }),
      ]);
    },
  });

  const updateRecurringMutation = useMutation({
    mutationFn: ({
      seriesId,
      treatment,
      status,
    }: {
      seriesId: string;
      treatment?: 'AUTO' | EffectiveBudgetTreatment;
      status?: 'ACTIVE' | 'PAUSED';
    }) => apiClient.updateRecurringSeries(seriesId, {
      budget_treatment: treatment,
      status,
    }),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: BUDGET_QUERY_KEY }),
        queryClient.invalidateQueries({ queryKey: ['transactions'] }),
      ]);
    },
  });

  const firstError = query.error
    ?? savePlanMutation.error
    ?? linkRefundMutation.error
    ?? dismissSuggestionMutation.error
    ?? confirmRecurringMutation.error
    ?? updateRecurringMutation.error;
  return {
    overview: query.data ?? null,
    isLoading: query.isLoading,
    isSaving: savePlanMutation.isPending
      || linkRefundMutation.isPending
      || dismissSuggestionMutation.isPending
      || confirmRecurringMutation.isPending
      || updateRecurringMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
    retry: async () => {
      savePlanMutation.reset();
      linkRefundMutation.reset();
      dismissSuggestionMutation.reset();
      confirmRecurringMutation.reset();
      updateRecurringMutation.reset();
      await query.refetch();
    },
    savePlan: (limitInPaise: number, categoryRules: BudgetPlanCategoryRuleInput[]) =>
      savePlanMutation.mutateAsync({ limitInPaise, categoryRules }),
    linkRefund: (sourceId: string, targetId: string) =>
      linkRefundMutation.mutateAsync({ sourceId, targetId }),
    dismissSuggestion: (type: 'REFUND_MATCH' | 'RECURRING_PATTERN', fingerprint: string) =>
      dismissSuggestionMutation.mutateAsync({ type, fingerprint }),
    confirmRecurring: (
      fingerprint: string,
      transactionIds: string[],
      treatment: EffectiveBudgetTreatment,
    ) => confirmRecurringMutation.mutateAsync({ fingerprint, transactionIds, treatment }),
    updateRecurring: (
      seriesId: string,
      input: { treatment?: 'AUTO' | EffectiveBudgetTreatment; status?: 'ACTIVE' | 'PAUSED' },
    ) => updateRecurringMutation.mutateAsync({ seriesId, ...input }),
  };
}

export function useBudgetRelationshipActions() {
  const queryClient = useQueryClient();
  const unlinkMutation = useMutation({
    mutationFn: (relationshipId: string) => apiClient.deleteTransactionRelationship(relationshipId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: BUDGET_QUERY_KEY }),
        queryClient.invalidateQueries({ queryKey: ['transactions'] }),
      ]);
    },
  });
	const linkMutation = useMutation({
		mutationFn: ({
			sourceId,
			targetId,
			kind,
		}: {
			sourceId: string;
			targetId: string;
			kind: 'REFUND' | 'REIMBURSEMENT';
		}) => apiClient.createTransactionRelationship({
			source_transaction_id: sourceId,
			target_transaction_id: targetId,
			kind,
		}),
		onSuccess: async () => {
			await Promise.all([
				queryClient.invalidateQueries({ queryKey: BUDGET_QUERY_KEY }),
				queryClient.invalidateQueries({ queryKey: ['transactions'] }),
			]);
		},
	});
  return {
    unlinkRelationship: (relationshipId: string) => unlinkMutation.mutateAsync(relationshipId),
    linkRelationship: (sourceId: string, targetId: string, kind: 'REFUND' | 'REIMBURSEMENT') =>
		linkMutation.mutateAsync({ sourceId, targetId, kind }),
    isUnlinking: unlinkMutation.isPending,
    isLinking: linkMutation.isPending,
    error: unlinkMutation.error
		? toErrorMessage(unlinkMutation.error)
		: linkMutation.error ? toErrorMessage(linkMutation.error) : null,
  };
}

export function useRefundTargets(sourceTransactionId: string | null, enabled: boolean) {
	const query = useQuery({
		queryKey: ['budget', 'relationship-targets', sourceTransactionId],
		queryFn: () => apiClient.listRelationshipTargets(sourceTransactionId ?? ''),
		enabled: enabled && sourceTransactionId !== null,
		staleTime: 30_000,
	});
	return {
		targets: query.data ?? [],
		isLoading: query.isLoading,
		error: query.error ? toErrorMessage(query.error) : null,
	};
}
