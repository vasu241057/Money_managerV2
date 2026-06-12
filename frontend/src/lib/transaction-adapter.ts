import type { CategoryRow } from '../../../shared/types';
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

export function resolvePersistedCategoryId(
  transaction: Omit<Transaction, 'id'> | Transaction,
  options?: {
    current?: Pick<Transaction, 'type' | 'categoryId' | 'subCategoryId'> | null;
    categoriesById?: ReadonlyMap<string, Pick<CategoryRow, 'parent_id'>>;
  },
): string | null {
  const selectedSubCategoryId = normalizeOptionalId(transaction.subCategoryId);
  if (selectedSubCategoryId) {
    return selectedSubCategoryId;
  }

  const selectedCategoryId = normalizeOptionalId(transaction.categoryId);
  const current = options?.current;
  const categoryLookup = options?.categoriesById;
  const currentSubCategoryId = normalizeOptionalId(current?.subCategoryId);
  const currentCategoryId = normalizeOptionalId(current?.categoryId);

  if (
    selectedCategoryId &&
    current &&
    categoryLookup &&
    transaction.type === current.type &&
    currentSubCategoryId &&
    currentCategoryId === selectedCategoryId
  ) {
    const selectedCategory = categoryLookup.get(selectedCategoryId);
    const currentSubCategory = categoryLookup.get(currentSubCategoryId);

    const isUnsupportedNestedPath =
      selectedCategory?.parent_id !== null &&
      currentSubCategory?.parent_id === selectedCategoryId;

    if (isUnsupportedNestedPath) {
      return currentSubCategoryId;
    }
  }

  return selectedCategoryId;
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
