import { useLocalStorage } from './useLocalStorage';

export interface Transaction {
  id: string;
  amount: number;
  type: 'income' | 'expense';
  category: string;
  subCategory?: string;
  date: Date;
  description?: string;
  accountId?: string; // Optional, for future use
}

export const useTransactions = () => {
  // We store dates as strings in localStorage, so we need to parse them back
  const [storedTransactions, setStoredTransactions] = useLocalStorage<Transaction[]>('transactions', []);

  // Convert string dates back to Date objects
  const transactions = storedTransactions.map(t => ({
    ...t,
    date: new Date(t.date)
  }));

  const addTransaction = (transaction: Omit<Transaction, 'id'> | Transaction) => {
    if ('id' in transaction && transaction.id) {
      // Update existing
      setStoredTransactions(prev => prev.map(t => t.id === transaction.id ? transaction as Transaction : t));
    } else {
      // Create new
      const newTransaction = { ...transaction, id: crypto.randomUUID() } as Transaction;
      setStoredTransactions(prev => [newTransaction, ...prev]);
    }
  };

  const deleteTransaction = (id: string) => {
    setStoredTransactions(prev => prev.filter(t => t.id !== id));
  };

  return { transactions, addTransaction, deleteTransaction };
};
