import type { Transaction } from '../hooks/local/useLocalTransactions';

export function normalizeOptionalId(value: string | null | undefined): string | null {
  if (!value || value.trim() === '') {
    return null;
  }

  return value;
}

export function normalizeOptionalText(value: string | undefined): string | null {
  if (!value) {
    return null;
  }

  const normalized = value.trim();
  return normalized === '' ? null : normalized;
}

export function resolvePersistedCategoryId(
  transaction: Omit<Transaction, 'id'> | Transaction,
): string | null {
  return normalizeOptionalId(transaction.subCategoryId ?? transaction.categoryId);
}

export function areImmutableFieldsChanged(current: Transaction, next: Transaction): boolean {
  if (current.amount !== next.amount) {
    return true;
  }

  if (current.type !== next.type) {
    return true;
  }

  return current.date.getTime() !== next.date.getTime();
}
