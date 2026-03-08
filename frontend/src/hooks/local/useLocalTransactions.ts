import { useLocalStorage } from '../useLocalStorage';

export interface Transaction {
  id: string;
  amount: number;
  type: 'income' | 'expense' | 'transfer';
  category: string;
  categoryId?: string | null;
  subCategory?: string;
  subCategoryId?: string | null;
  date: Date;
  description?: string;
  accountId?: string | null;
  accountName?: string;
  isManual?: boolean;
}

export function useLocalTransactions() {
  const [storedTransactions, setStoredTransactions] = useLocalStorage<Transaction[]>('transactions', []);

  const transactions = storedTransactions.map(t => ({
    ...t,
    date: new Date(t.date),
  }));

  const addTransaction = (transaction: Omit<Transaction, 'id'> | Transaction): Transaction => {
    if ('id' in transaction && transaction.id) {
      setStoredTransactions(prev =>
        prev.map(t => (t.id === transaction.id ? (transaction as Transaction) : t)),
      );
      return transaction as Transaction;
    }

    const newTransaction = {
      ...transaction,
      id: crypto.randomUUID(),
      isManual: transaction.isManual ?? true,
    } as Transaction;
    setStoredTransactions(prev => [newTransaction, ...prev]);
    return newTransaction;
  };

  const deleteTransaction = (id: string) => {
    setStoredTransactions(prev => prev.filter(t => t.id !== id));
  };

  return { transactions, addTransaction, deleteTransaction };
}
