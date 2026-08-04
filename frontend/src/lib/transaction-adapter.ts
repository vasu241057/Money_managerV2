import type { EventDirection, TransactionType } from '../../../shared/types';
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

export function areImmutableSourceFieldsChanged(current: Transaction, next: Transaction): boolean {
	if (current.amount !== next.amount) {
		return true;
	}

	return current.date.getTime() !== next.date.getTime();
}

export function transactionDirection(transaction: Pick<Transaction, 'direction' | 'type'>): EventDirection {
	return transaction.direction ?? (transaction.type === 'income' ? 'credit' : 'debit');
}

export function allowedTransactionTypes(direction: EventDirection): readonly TransactionType[] {
	return direction === 'debit' ? ['expense', 'transfer'] : ['income', 'transfer'];
}
