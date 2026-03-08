import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { isRemoteDataEnabled } from '../config/data-source';
import { apiClient, toErrorMessage } from '../lib/api-client';
import { paiseToRupees, rupeesToPaise } from '../lib/money';
import { useLocalAccounts, type Account } from './local/useLocalAccounts';

interface UseAccountsResult {
  accounts: Account[];
  addAccount: (account: Omit<Account, 'id'>) => Promise<void>;
  updateAccount: (id: string, updates: Partial<Omit<Account, 'id'>>) => Promise<void>;
  deleteAccount: (id: string) => Promise<void>;
  isLoading: boolean;
  error: string | null;
}

const REMOTE_DATA_ENABLED = isRemoteDataEnabled();
const ACCOUNTS_QUERY_KEY = ['accounts'] as const;

function useLocalAccountFallback(): UseAccountsResult {
  const local = useLocalAccounts();

  return {
    accounts: local.accounts,
    addAccount: async account => {
      local.addAccount(account);
    },
    updateAccount: async (id, updates) => {
      local.updateAccount(id, updates);
    },
    deleteAccount: async id => {
      local.deleteAccount(id);
    },
    isLoading: false,
    error: null,
  };
}

function useRemoteAccounts(): UseAccountsResult {
  const queryClient = useQueryClient();

  const accountsQuery = useQuery({
    queryKey: ACCOUNTS_QUERY_KEY,
    queryFn: () => apiClient.listAccounts(),
  });

  const addAccountMutation = useMutation({
    mutationFn: (account: Omit<Account, 'id'>) =>
      apiClient.createAccount({
        name: account.name,
        type: account.type,
        initial_balance_in_paise:
          account.balance === undefined ? undefined : rupeesToPaise(account.balance),
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ACCOUNTS_QUERY_KEY });
    },
  });

  const updateAccountMutation = useMutation({
    mutationFn: ({ id, updates }: { id: string; updates: Partial<Omit<Account, 'id'>> }) =>
      apiClient.updateAccount(id, {
        name: updates.name,
        type: updates.type,
        initial_balance_in_paise:
          updates.balance === undefined ? undefined : rupeesToPaise(updates.balance),
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ACCOUNTS_QUERY_KEY });
    },
  });

  const deleteAccountMutation = useMutation({
    mutationFn: (accountId: string) => apiClient.deleteAccount(accountId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ACCOUNTS_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ['transactions'] });
    },
  });

  const accounts: Account[] = (accountsQuery.data ?? []).map(row => ({
    id: row.id,
    name: row.name,
    type: row.type,
    balance: paiseToRupees(row.initial_balance_in_paise),
  }));

  const firstError =
    accountsQuery.error ??
    addAccountMutation.error ??
    updateAccountMutation.error ??
    deleteAccountMutation.error;

  return {
    accounts,
    addAccount: async account => {
      await addAccountMutation.mutateAsync(account);
    },
    updateAccount: async (id, updates) => {
      await updateAccountMutation.mutateAsync({ id, updates });
    },
    deleteAccount: async id => {
      await deleteAccountMutation.mutateAsync(id);
    },
    isLoading:
      accountsQuery.isLoading ||
      addAccountMutation.isPending ||
      updateAccountMutation.isPending ||
      deleteAccountMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
  };
}

export { type Account };

const useAccountsImpl: () => UseAccountsResult = REMOTE_DATA_ENABLED
  ? useRemoteAccounts
  : useLocalAccountFallback;

export function useAccounts(): UseAccountsResult {
  return useAccountsImpl();
}
