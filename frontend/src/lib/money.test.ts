import { describe, expect, it } from 'vitest';
import type { Transaction } from '../hooks/local/useLocalTransactions';
import { paiseToRupees, rupeesToPaise } from './money';
import {
  areImmutableFieldsChanged,
  normalizeOptionalId,
  normalizeOptionalText,
  resolvePersistedCategoryId,
} from './transaction-adapter';

const BASE_TRANSACTION: Transaction = {
  id: 'transaction-id',
  amount: 100,
  type: 'expense',
  category: 'Food',
  categoryId: 'category-id',
  date: new Date('2026-01-01T00:00:00.000Z'),
  description: 'Lunch',
  accountId: 'account-id',
};

describe('money utilities', () => {
  it('converts rupees to paise with rounding', () => {
    expect(rupeesToPaise(123.456)).toBe(12346);
  });

  it('converts paise to rupees', () => {
    expect(paiseToRupees(12345)).toBe(123.45);
  });
});

describe('transaction adapter helpers', () => {
  it('normalizes optional ids and text', () => {
    expect(normalizeOptionalId('')).toBeNull();
    expect(normalizeOptionalId('  ')).toBeNull();
    expect(normalizeOptionalId('uuid-value')).toBe('uuid-value');

    expect(normalizeOptionalText(undefined)).toBeNull();
    expect(normalizeOptionalText('  ')).toBeNull();
    expect(normalizeOptionalText(' note ')).toBe('note');
  });

  it('prefers sub-category id over category id for persistence', () => {
    expect(
      resolvePersistedCategoryId({
        ...BASE_TRANSACTION,
        subCategoryId: 'sub-category-id',
      }),
    ).toBe('sub-category-id');

    expect(
      resolvePersistedCategoryId({
        ...BASE_TRANSACTION,
        subCategoryId: null,
      }),
    ).toBe('category-id');
  });

  it('detects immutable field edits', () => {
    expect(
      areImmutableFieldsChanged(BASE_TRANSACTION, {
        ...BASE_TRANSACTION,
      }),
    ).toBe(false);

    expect(
      areImmutableFieldsChanged(BASE_TRANSACTION, {
        ...BASE_TRANSACTION,
        amount: 101,
      }),
    ).toBe(true);

    expect(
      areImmutableFieldsChanged(BASE_TRANSACTION, {
        ...BASE_TRANSACTION,
        date: new Date('2026-01-02T00:00:00.000Z'),
      }),
    ).toBe(true);
  });
});
