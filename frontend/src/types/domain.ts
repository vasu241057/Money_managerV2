import type { ClassificationSource, MerchantType, TransactionStatus } from '../../../shared/types';

// No balance field: account balances are derived from transactions
// (income − expense per account), never stored or entered.
export interface Account {
  id: string;
  name: string;
  type: 'cash' | 'bank' | 'card' | 'other';
  instrument_last4?: string | null;
}

export interface Category {
  id: string;
  name: string;
  icon: string;
  type: 'income' | 'expense' | 'transfer';
  subCategories: string[];
  subCategoryIds?: Record<string, string>;
  subCategorySystem?: Record<string, boolean>;
  isSystem?: boolean;
}

export interface Transaction {
  id: string;
  amount: number;
  type: 'income' | 'expense' | 'transfer';
  category: string;
  categoryId?: string | null;
  categoryIcon?: string | null;
  subCategory?: string;
  subCategoryId?: string | null;
  date: Date;
  description?: string;
  accountId?: string | null;
  accountName?: string;
  isManual?: boolean;
  persistedCategoryId?: string | null;
  merchantId?: string | null;
  merchantName?: string | null;
  merchantType?: MerchantType | null;
  searchKey?: string | null;
  counterpartyRaw?: string | null;
  paymentMethod?: string | null;
  instrumentLast4?: string | null;
  creditCardId?: string | null;
  creditCardLabel?: string | null;
  status?: TransactionStatus;
  classificationSource?: ClassificationSource;
  aiConfidenceScore?: number | null;
}
