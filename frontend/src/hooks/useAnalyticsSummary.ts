import { useQuery } from '@tanstack/react-query';
import type { AnalyticsSummaryResponse } from '../../../shared/types';
import { apiClient, toErrorMessage } from '../lib/api-client';

export const ANALYTICS_SUMMARY_QUERY_KEY = ['analytics-summary'] as const;

interface UseAnalyticsSummaryResult {
  summary: AnalyticsSummaryResponse | null;
  isLoading: boolean;
  error: string | null;
}

// One compact server-computed payload feeds the Home hero, the Analytics
// screen, and account/card balances — the full transaction feed is only
// needed for row-level lists.
export function useAnalyticsSummary(): UseAnalyticsSummaryResult {
  const query = useQuery({
    queryKey: ANALYTICS_SUMMARY_QUERY_KEY,
    queryFn: async () => apiClient.getAnalyticsSummary(),
    staleTime: 30_000,
  });

  return {
    summary: query.data ?? null,
    isLoading: query.isLoading,
    error: query.error ? toErrorMessage(query.error) : null,
  };
}

// Month key helper matching the backend's Asia/Kolkata bucketing exactly.
export function monthKeyIST(date: Date): string {
  return date.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' }).slice(0, 7);
}
