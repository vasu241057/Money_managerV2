import { useMemo } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { CardStatementRow } from '../../../shared/types';
import { apiClient, toErrorMessage } from '../lib/api-client';

const CARD_STATEMENTS_QUERY_KEY = ['card-statements'] as const;

export interface UpsertStatementInput {
  credit_card_id: string;
  total_due_in_paise?: number | null;
  min_due_in_paise?: number | null;
  due_date?: string | null;
  period_start?: string | null;
  period_end?: string | null;
}

interface UseCardStatementsResult {
  statements: CardStatementRow[];
  /** Latest statement per linked card (by due date, then recency). */
  latestByCard: Map<string, CardStatementRow>;
  saveStatement: (input: UpsertStatementInput) => Promise<CardStatementRow>;
  isLoading: boolean;
  error: string | null;
}

export function useCardStatements(): UseCardStatementsResult {
  const queryClient = useQueryClient();

  const statementsQuery = useQuery({
    queryKey: CARD_STATEMENTS_QUERY_KEY,
    queryFn: async () => apiClient.listCardStatements(),
    staleTime: 60_000,
  });

  const saveMutation = useMutation({
    mutationFn: async (input: UpsertStatementInput) => apiClient.upsertCardStatement(input),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CARD_STATEMENTS_QUERY_KEY });
    },
  });

  const latestByCard = useMemo(() => {
    const map = new Map<string, CardStatementRow>();
    // The list arrives due_date-desc (nulls last), so first hit per card wins.
    for (const statement of statementsQuery.data ?? []) {
      if (!statement.credit_card_id) continue;
      if (!map.has(statement.credit_card_id)) {
        map.set(statement.credit_card_id, statement);
      }
    }
    return map;
  }, [statementsQuery.data]);

  const firstError = statementsQuery.error ?? saveMutation.error;

  return {
    statements: statementsQuery.data ?? [],
    latestByCard,
    saveStatement: async input => saveMutation.mutateAsync(input),
    isLoading: statementsQuery.isLoading || saveMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
  };
}
