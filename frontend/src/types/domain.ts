import type { ClassificationSource, TransactionStatus } from '../../../shared/types';

export interface Account {
  id: string;
  name: string;
  type: 'cash' | 'bank' | 'card' | 'other';
  balance?: number;
}

export interface Category {
  id: string;
  name: string;
  icon: string;
  type: 'income' | 'expense' | 'transfer';
  subCategories: string[];
  subCategoryIds?: Record<string, string>;
  isSystem?: boolean;
}

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
  persistedCategoryId?: string | null;
  merchantId?: string | null;
  merchantName?: string | null;
  searchKey?: string | null;
  status?: TransactionStatus;
  classificationSource?: ClassificationSource;
  aiConfidenceScore?: number | null;
}
