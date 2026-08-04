import { useAuth } from '@clerk/react';
import { useQuery } from '@tanstack/react-query';
import type {
  AiUsageDashboardResponse,
  AiUsageSummaryResponse,
} from '../../../shared/types';
import { ApiError, apiClient, toErrorMessage } from '../lib/api-client';

export const AI_USAGE_SUMMARY_QUERY_KEY = ['ai-usage-summary'] as const;

export function currentMonthIST(): string {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' }).slice(0, 7);
}

interface UseAiUsageSummaryResult {
  summary: AiUsageSummaryResponse | null;
  isLoading: boolean;
  isRefreshing: boolean;
  isAdmin: boolean;
  error: string | null;
  refetch: () => Promise<void>;
}

export function useAiUsageSummary(): UseAiUsageSummaryResult {
  const { userId } = useAuth();
  const query = useQuery({
    queryKey: [...AI_USAGE_SUMMARY_QUERY_KEY, userId],
    queryFn: async () => apiClient.getAiUsageSummary(),
    enabled: Boolean(userId),
    staleTime: 60_000,
    retry: (failureCount, error) =>
      !(error instanceof ApiError && error.status === 404) && failureCount < 1,
  });

  const denied = query.error instanceof ApiError && query.error.status === 404;
  return {
    summary: query.data ?? null,
    isLoading: query.isLoading,
    isRefreshing: query.isFetching && !query.isLoading,
    isAdmin: Boolean(query.data) && !denied,
    error: query.error && !denied ? toErrorMessage(query.error) : null,
    refetch: async () => { await query.refetch(); },
  };
}

interface UseAiUsageDashboardResult {
  dashboard: AiUsageDashboardResponse | null;
  isLoading: boolean;
  isRefreshing: boolean;
  error: string | null;
  refetch: () => Promise<void>;
}

export function useAiUsageDashboard(month: string, page: number): UseAiUsageDashboardResult {
  const { userId } = useAuth();
  const query = useQuery({
    queryKey: ['ai-usage-dashboard', userId, month, page],
    queryFn: async () => apiClient.getAiUsageDashboard(month, page),
    enabled: Boolean(userId),
    staleTime: 30_000,
  });

  return {
    dashboard: query.data ?? null,
    isLoading: query.isLoading,
    isRefreshing: query.isFetching && !query.isLoading,
    error: query.error ? toErrorMessage(query.error) : null,
    refetch: async () => { await query.refetch(); },
  };
}
