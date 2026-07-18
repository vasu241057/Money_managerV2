import type { Transaction } from '../types/domain';

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

// The sub id wins when set; the editor manages parent/sub selection
// explicitly, so a cleared sub deliberately falls back to the parent.
export function resolvePersistedCategoryId(
  transaction: Omit<Transaction, 'id'> | Transaction,
): string | null {
  return (
    normalizeOptionalId(transaction.subCategoryId) ?? normalizeOptionalId(transaction.categoryId)
  );
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
