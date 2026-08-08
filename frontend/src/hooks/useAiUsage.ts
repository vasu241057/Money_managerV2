import { useAuth } from '@clerk/react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  AiTavilyKeyCredits,
  AiUsageDashboardResponse,
  AiUsageSummaryResponse,
  JobSchedule,
  JobScheduleUpdateRequest,
  ScheduledJobName,
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

interface UseTavilyKeysResult {
  keys: AiTavilyKeyCredits[];
  isLoading: boolean;
  error: string | null;
}

/**
 * Reaches out to Tavily once per configured key, so it stays behind `enabled`
 * and only runs while the merchant mapping tab is actually open.
 */
export function useTavilyKeys(enabled: boolean): UseTavilyKeysResult {
  const { userId } = useAuth();
  const query = useQuery({
    queryKey: ['ai-usage-tavily-keys', userId],
    queryFn: async () => apiClient.getTavilyKeys(),
    enabled: Boolean(userId) && enabled,
    staleTime: 60_000,
  });

  return {
    keys: query.data?.keys ?? [],
    isLoading: query.isLoading,
    error: query.error ? toErrorMessage(query.error) : null,
  };
}

export const JOB_SCHEDULES_QUERY_KEY = ['ai-usage-job-schedules'] as const;

interface UseJobSchedulesResult {
  schedules: JobSchedule[];
  isLoading: boolean;
  error: string | null;
}

export function useJobSchedules(): UseJobSchedulesResult {
  const { userId } = useAuth();
  const query = useQuery({
    queryKey: [...JOB_SCHEDULES_QUERY_KEY, userId],
    queryFn: async () => apiClient.getJobSchedules(),
    enabled: Boolean(userId),
    staleTime: 30_000,
  });

  return {
    schedules: query.data?.schedules ?? [],
    isLoading: query.isLoading,
    error: query.error ? toErrorMessage(query.error) : null,
  };
}

interface UpdateJobScheduleVariables {
  job: ScheduledJobName;
  update: JobScheduleUpdateRequest;
}

/**
 * Refetches the dashboard too: its "nightly mapping is running" banner reads
 * the same schedule, so leaving it stale would contradict the toggle the user
 * just flipped.
 */
export function useUpdateJobSchedule() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ job, update }: UpdateJobScheduleVariables) =>
      apiClient.updateJobSchedule(job, update),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: JOB_SCHEDULES_QUERY_KEY }),
        queryClient.invalidateQueries({ queryKey: ['ai-usage-dashboard'] }),
      ]);
    },
  });
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
