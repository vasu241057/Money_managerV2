import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { OauthConnectionRow } from '../../../shared/types';
import { isRemoteDataEnabled } from '../config/data-source';
import { apiClient, toErrorMessage } from '../lib/api-client';
import { ensureLegacyLocalDataMigrated } from '../lib/legacy-local-migration';

interface UseGmailConnectionResult {
  isAvailable: boolean;
  connection: OauthConnectionRow | null;
  connectGmail: () => Promise<void>;
  disconnectGmail: () => Promise<void>;
  completeOAuthCallback: (code: string, state: string) => Promise<OauthConnectionRow>;
  isLoading: boolean;
  error: string | null;
}

const REMOTE_DATA_ENABLED = isRemoteDataEnabled();
const GMAIL_CONNECTION_QUERY_KEY = ['gmail-connection'] as const;

function useLocalFallback(): UseGmailConnectionResult {
  return {
    isAvailable: false,
    connection: null,
    connectGmail: async () => undefined,
    disconnectGmail: async () => undefined,
    completeOAuthCallback: async () => {
      throw new Error('Gmail sync is available only when remote data mode is enabled.');
    },
    isLoading: false,
    error: null,
  };
}

function useRemoteGmailConnection(skipStatusQuery = false): UseGmailConnectionResult {
  const queryClient = useQueryClient();

  const connectionQuery = useQuery({
    queryKey: GMAIL_CONNECTION_QUERY_KEY,
    enabled: !skipStatusQuery,
    queryFn: async () => {
      await ensureLegacyLocalDataMigrated();
      const response = await apiClient.getGoogleOAuthConnectionStatus();
      return response.connection;
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
      queryClient.setQueryData(GMAIL_CONNECTION_QUERY_KEY, connection);
      await queryClient.invalidateQueries({ queryKey: GMAIL_CONNECTION_QUERY_KEY });
    },
  });

  const disconnectMutation = useMutation({
    mutationFn: async () => {
      await ensureLegacyLocalDataMigrated();
      await apiClient.disconnectGoogleOAuth();
    },
    onSuccess: async () => {
      queryClient.setQueryData(GMAIL_CONNECTION_QUERY_KEY, null);
      await queryClient.invalidateQueries({ queryKey: GMAIL_CONNECTION_QUERY_KEY });
    },
  });

  const firstError =
    connectionQuery.error ??
    connectMutation.error ??
    callbackMutation.error ??
    disconnectMutation.error;

  return {
    isAvailable: true,
    connection: connectionQuery.data ?? null,
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

const useGmailConnectionImpl: () => UseGmailConnectionResult = REMOTE_DATA_ENABLED
  ? () => useRemoteGmailConnection(false)
  : useLocalFallback;

const useGmailConnectionCallbackImpl: () => UseGmailConnectionResult = REMOTE_DATA_ENABLED
  ? () => useRemoteGmailConnection(true)
  : useLocalFallback;

export function useGmailConnection(): UseGmailConnectionResult {
  return useGmailConnectionImpl();
}

export function useGmailConnectionForOAuthCallback(): UseGmailConnectionResult {
  return useGmailConnectionCallbackImpl();
}
