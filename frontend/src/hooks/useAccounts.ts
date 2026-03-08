import { useLocalStorage } from './useLocalStorage';

export interface Account {
  id: string;
  name: string;
  type: 'cash' | 'bank' | 'card' | 'other';
  balance?: number; // Optional initial balance, can be calculated from transactions
}

const DEFAULT_ACCOUNTS: Account[] = [
  { id: '1', name: 'Cash', type: 'cash' },
  { id: '2', name: 'Bank Account', type: 'bank' },
];

export const useAccounts = () => {
  const [accounts, setAccounts] = useLocalStorage<Account[]>('accounts', DEFAULT_ACCOUNTS);

  const addAccount = (account: Omit<Account, 'id'>) => {
    const newAccount = { ...account, id: crypto.randomUUID() };
    setAccounts(prev => [...prev, newAccount]);
  };

  const updateAccount = (id: string, updates: Partial<Omit<Account, 'id'>>) => {
    setAccounts(prev => prev.map(acc => acc.id === id ? { ...acc, ...updates } : acc));
  };

  const deleteAccount = (id: string) => {
    // Prevent deleting the last account
    if (accounts.length <= 1) {
      alert("You must have at least one account.");
      return;
    }
    setAccounts(prev => prev.filter(acc => acc.id !== id));
  };

  return { accounts, addAccount, updateAccount, deleteAccount };
};
