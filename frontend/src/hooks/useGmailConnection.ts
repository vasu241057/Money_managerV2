import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { GoogleOAuthProcessingStats, OauthConnectionRow } from '../../../shared/types';
import { apiClient, toErrorMessage } from '../lib/api-client';
import { ensureLegacyLocalDataMigrated } from '../lib/legacy-local-migration';

interface UseGmailConnectionResult {
  connection: OauthConnectionRow | null;
  processingStats: GoogleOAuthProcessingStats;
  connectGmail: () => Promise<void>;
  disconnectGmail: () => Promise<void>;
  completeOAuthCallback: (code: string, state: string) => Promise<OauthConnectionRow>;
  isLoading: boolean;
  error: string | null;
}

const GMAIL_CONNECTION_QUERY_KEY = ['gmail-connection'] as const;
const EMPTY_GMAIL_PROCESSING_STATS: GoogleOAuthProcessingStats = {
  raw_email_total_count: 0,
  raw_email_pending_count: 0,
  raw_email_processed_count: 0,
  raw_email_failed_count: 0,
  raw_email_unrecognized_count: 0,
  raw_email_ignored_count: 0,
  latest_email_internal_date: null,
  latest_email_created_at: null,
};

function useRemoteGmailConnection(skipStatusQuery = false): UseGmailConnectionResult {
  const queryClient = useQueryClient();

  const connectionQuery = useQuery({
    queryKey: GMAIL_CONNECTION_QUERY_KEY,
    enabled: !skipStatusQuery,
    queryFn: async () => {
      await ensureLegacyLocalDataMigrated();
      const response = await apiClient.getGoogleOAuthConnectionStatus();
      return {
        connection: response.connection,
        processingStats: response.processing_stats,
      };
    },
  });

  const connectMutation = useMutation({
    mutationFn: async () => {
      await ensureLegacyLocalDataMigrated();
      const response = await apiClient.startGoogleOAuth();
      window.location.assign(response.auth_url);
    },
  });

  const callbackMutation = useMutation({
    mutationFn: async ({ code, state }: { code: string; state: string }) => {
      await ensureLegacyLocalDataMigrated();
      const response = await apiClient.completeGoogleOAuthCallback({ code, state });
      return response.connection;
    },
    onSuccess: async connection => {
      queryClient.setQueryData(
        GMAIL_CONNECTION_QUERY_KEY,
        (current:
          | { connection: OauthConnectionRow | null; processingStats: GoogleOAuthProcessingStats }
          | undefined) => ({
          connection,
          processingStats: current?.processingStats ?? EMPTY_GMAIL_PROCESSING_STATS,
        }),
      );
      await queryClient.invalidateQueries({ queryKey: GMAIL_CONNECTION_QUERY_KEY });
    },
  });

  const disconnectMutation = useMutation({
    mutationFn: async () => {
      await ensureLegacyLocalDataMigrated();
      await apiClient.disconnectGoogleOAuth();
    },
    onSuccess: async () => {
      queryClient.setQueryData(
        GMAIL_CONNECTION_QUERY_KEY,
        (current:
          | { connection: OauthConnectionRow | null; processingStats: GoogleOAuthProcessingStats }
          | undefined) => ({
          connection: null,
          processingStats: current?.processingStats ?? EMPTY_GMAIL_PROCESSING_STATS,
        }),
      );
      await queryClient.invalidateQueries({ queryKey: GMAIL_CONNECTION_QUERY_KEY });
    },
  });

  const firstError =
    connectionQuery.error ??
    connectMutation.error ??
    callbackMutation.error ??
    disconnectMutation.error;

  return {
    connection: connectionQuery.data?.connection ?? null,
    processingStats: connectionQuery.data?.processingStats ?? EMPTY_GMAIL_PROCESSING_STATS,
    connectGmail: async () => connectMutation.mutateAsync(),
    disconnectGmail: async () => disconnectMutation.mutateAsync(),
    completeOAuthCallback: async (code: string, state: string) =>
      callbackMutation.mutateAsync({ code, state }),
    isLoading:
      connectionQuery.isLoading ||
      connectMutation.isPending ||
      callbackMutation.isPending ||
      disconnectMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
  };
}

export function useGmailConnection(): UseGmailConnectionResult {
  return useRemoteGmailConnection(false);
}

export function useGmailConnectionForOAuthCallback(): UseGmailConnectionResult {
  return useRemoteGmailConnection(true);
}
