import { describe, expect, it, vi } from 'vitest';
import type {
  AccountRow,
  CategoryRow,
  CreateAccountRequest,
  CreateCategoryRequest,
  CreateManualTransactionRequest,
  PaginatedResponse,
  TransactionFeedItem,
} from '../../../shared/types';
import {
  buildLegacyTransactionMarker,
  buildMigrationMarkerKey,
  decodeJwtIdentity,
  parseLegacyTransactionMarker,
  readLegacySnapshotFromStorage,
  runLegacyLocalDataMigration,
} from './legacy-local-migration';

class MemoryStorage {
  private readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.has(key) ? this.values.get(key) ?? null : null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }
}

function toBase64Url(value: string): string {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function createJwt(payload: Record<string, unknown>): string {
  const header = toBase64Url(JSON.stringify({ alg: 'none', typ: 'JWT' }));
  const encodedPayload = toBase64Url(JSON.stringify(payload));
  return `${header}.${encodedPayload}.`;
}

function makeTransactionFeedItem(
  transactionId: string,
  instrumentId: string | null,
  accountId: string | null = null,
  categoryId: string | null = null,
): TransactionFeedItem {
  return {
    transaction: {
      id: transactionId,
      user_id: 'user-id',
      financial_event_id: `fe-${transactionId}`,
      account_id: accountId,
      category_id: categoryId,
      merchant_id: null,
      credit_card_id: null,
      amount_in_paise: 1000,
      type: 'expense',
      txn_date: '2026-01-01T00:00:00.000Z',
      user_note: null,
      status: 'VERIFIED',
      classification_source: 'USER',
      ai_confidence_score: null,
      created_at: '2026-01-01T00:00:00.000Z',
      updated_at: '2026-01-01T00:00:00.000Z',
    },
    financial_event: {
      id: `fe-${transactionId}`,
      user_id: 'user-id',
      raw_email_id: null,
      extraction_index: 0,
      direction: 'debit',
      amount_in_paise: 1000,
      currency: 'INR',
      txn_timestamp: '2026-01-01T00:00:00.000Z',
      payment_method: 'unknown',
      instrument_id: instrumentId,
      counterparty_raw: null,
      search_key: null,
      status: 'ACTIVE',
      created_at: '2026-01-01T00:00:00.000Z',
    },
    account:
      accountId === null
        ? null
        : {
            id: accountId,
            name: 'Account',
            type: 'cash',
          },
    credit_card: null,
    category:
      categoryId === null
        ? null
        : {
            id: categoryId,
            name: 'Category',
            type: 'expense',
            icon: null,
          },
    merchant: null,
    raw_email: null,
  };
}

function createMigrationApiFixture(options?: {
  existingTransactions?: TransactionFeedItem[];
  existingCategories?: CategoryRow[];
  existingAccounts?: AccountRow[];
}) {
  const accounts: AccountRow[] = [...(options?.existingAccounts ?? [])];
  const categories: CategoryRow[] = [...(options?.existingCategories ?? [])];
  const transactions: TransactionFeedItem[] = [...(options?.existingTransactions ?? [])];

  const listAccounts = vi.fn(async (): Promise<AccountRow[]> => accounts);
  const createAccount = vi.fn(async (payload: CreateAccountRequest): Promise<AccountRow> => {
    const row: AccountRow = {
      id: `account-${accounts.length + 1}`,
      user_id: 'user-id',
      name: payload.name,
      type: payload.type,
      instrument_last4: payload.instrument_last4 ?? null,
      initial_balance_in_paise: payload.initial_balance_in_paise ?? 0,
      created_at: '2026-01-01T00:00:00.000Z',
      updated_at: '2026-01-01T00:00:00.000Z',
    };
    accounts.push(row);
    return row;
  });

  const listCategories = vi.fn(async (): Promise<CategoryRow[]> => categories);
  const createCategory = vi.fn(async (payload: CreateCategoryRequest): Promise<CategoryRow> => {
    const row: CategoryRow = {
      id: `category-${categories.length + 1}`,
      user_id: 'user-id',
      parent_id: payload.parent_id ?? null,
      name: payload.name,
      type: payload.type,
      icon: payload.icon ?? null,
      is_system: false,
      created_at: '2026-01-01T00:00:00.000Z',
    };
    categories.push(row);
    return row;
  });

  const listTransactions = vi.fn(
    async ({
      page,
      limit,
    }: {
      page?: number;
      limit?: number;
    }): Promise<PaginatedResponse<TransactionFeedItem>> => {
      const resolvedPage = page ?? 1;
      const resolvedLimit = limit ?? 200;
      const start = (resolvedPage - 1) * resolvedLimit;
      const data = transactions.slice(start, start + resolvedLimit);

      return {
        data,
        total: transactions.length,
        page: resolvedPage,
        limit: resolvedLimit,
        has_more: start + resolvedLimit < transactions.length,
      };
    },
  );

  const createManualTransaction = vi.fn(
    async (payload: CreateManualTransactionRequest): Promise<TransactionFeedItem> => {
      const created = makeTransactionFeedItem(
        `tx-${transactions.length + 1}`,
        payload.instrument_id ?? null,
        payload.account_id ?? null,
        payload.category_id ?? null,
      );
      transactions.push(created);
      return created;
    },
  );

  return {
    api: {
      listAccounts,
      createAccount,
      listCategories,
      createCategory,
      listTransactions,
      createManualTransaction,
    },
    createdState: {
      accounts,
      categories,
      transactions,
    },
  };
}

describe('legacy local migration helpers', () => {
  it('decodes jwt identity and marker tokens', () => {
    const token = createJwt({
      sub: 'user_abc',
      iss: 'https://clerk.example.com',
    });

    const identity = decodeJwtIdentity(token);
    expect(identity).toEqual({
      sub: 'user_abc',
      iss: 'https://clerk.example.com',
    });

    const marker = buildLegacyTransactionMarker('legacy-tx-1');
    expect(parseLegacyTransactionMarker(marker)).toBe('legacy-tx-1');

    expect(parseLegacyTransactionMarker('not-a-marker')).toBeNull();
    expect(decodeJwtIdentity('not-a-jwt')).toBeNull();
  });

  it('parses legacy localStorage snapshot with sanitization', () => {
    const storage = new MemoryStorage();
    storage.setItem(
      'accounts',
      JSON.stringify([
        { id: 'a1', name: 'Cash', type: 'cash', balance: 123.45 },
        { id: '', name: '  ', type: 'bank' },
      ]),
    );
    storage.setItem(
      'categories',
      JSON.stringify([
        {
          id: 'c1',
          name: 'Food',
          icon: 'Utensils',
          type: 'expense',
          subCategories: ['Groceries'],
          subCategoryIds: { Groceries: 'sc1' },
        },
      ]),
    );
    storage.setItem(
      'transactions',
      JSON.stringify([
        {
          id: 't1',
          amount: 100,
          type: 'expense',
          category: 'Food',
          categoryId: 'c1',
          subCategory: 'Groceries',
          subCategoryId: 'sc1',
          date: '2026-01-01T00:00:00.000Z',
          accountId: 'a1',
          accountName: 'Cash',
        },
        {
          id: 't1',
          amount: 50,
          type: 'expense',
          category: 'Food',
          date: '2026-01-02T00:00:00.000Z',
        },
      ]),
    );

    const snapshot = readLegacySnapshotFromStorage(storage);
    expect(snapshot.accounts.length).toBe(2);
    expect(snapshot.categories.length).toBe(1);
    expect(snapshot.transactions.length).toBe(2);
    expect(snapshot.transactions[0].migrationKey).toBe('t1');
    expect(snapshot.transactions[1].migrationKey).toBe('t1__1');
  });
});

describe('runLegacyLocalDataMigration', () => {
  it('migrates accounts/categories/transactions and writes completion marker', async () => {
    const storage = new MemoryStorage();
    storage.setItem(
      'accounts',
      JSON.stringify([{ id: 'local-account-1', name: 'Cash Wallet', type: 'cash', balance: 123.45 }]),
    );
    storage.setItem(
      'categories',
      JSON.stringify([
        {
          id: 'local-category-food',
          name: 'Food',
          icon: 'Utensils',
          type: 'expense',
          subCategories: ['Groceries'],
          subCategoryIds: { Groceries: 'local-subcategory-groceries' },
        },
      ]),
    );
    storage.setItem(
      'transactions',
      JSON.stringify([
        {
          id: 'legacy-transaction-1',
          amount: 50.5,
          type: 'expense',
          category: 'Food',
          categoryId: 'local-category-food',
          subCategory: 'Groceries',
          subCategoryId: 'local-subcategory-groceries',
          date: '2026-01-02T10:00:00.000Z',
          description: 'Lunch',
          accountId: 'local-account-1',
          accountName: 'Cash Wallet',
        },
      ]),
    );

    const token = createJwt({ sub: 'user_1', iss: 'https://clerk.example.com' });
    const fixture = createMigrationApiFixture();

    await runLegacyLocalDataMigration({
      api: fixture.api,
      storage,
      getToken: async () => token,
      now: () => new Date('2026-03-09T00:00:00.000Z'),
    });

    expect(fixture.api.createAccount).toHaveBeenCalledTimes(1);
    expect(fixture.api.createCategory).toHaveBeenCalledTimes(2);
    expect(fixture.api.createManualTransaction).toHaveBeenCalledTimes(1);

    const createdTxPayload = fixture.api.createManualTransaction.mock.calls[0]?.[0];
    expect(createdTxPayload.amount_in_paise).toBe(5050);
    expect(createdTxPayload.account_id).toBe('account-1');
    expect(createdTxPayload.category_id).toBe('category-2');
    expect(createdTxPayload.instrument_id).toBe(buildLegacyTransactionMarker('legacy-transaction-1'));

    const markerKey = buildMigrationMarkerKey({
      sub: 'user_1',
      iss: 'https://clerk.example.com',
    });
    const markerRaw = storage.getItem(markerKey);
    expect(markerRaw).not.toBeNull();

    await runLegacyLocalDataMigration({
      api: fixture.api,
      storage,
      getToken: async () => token,
      now: () => new Date('2026-03-09T00:00:00.000Z'),
    });

    expect(fixture.api.createAccount).toHaveBeenCalledTimes(1);
    expect(fixture.api.createCategory).toHaveBeenCalledTimes(2);
    expect(fixture.api.createManualTransaction).toHaveBeenCalledTimes(1);
  });

  it('skips transaction create when legacy marker already exists remotely', async () => {
    const storage = new MemoryStorage();
    storage.setItem(
      'transactions',
      JSON.stringify([
        {
          id: 'legacy-existing',
          amount: 100,
          type: 'expense',
          category: 'Food',
          date: '2026-01-02T10:00:00.000Z',
        },
      ]),
    );

    const existingMarker = buildLegacyTransactionMarker('legacy-existing');
    const fixture = createMigrationApiFixture({
      existingTransactions: [makeTransactionFeedItem('tx-existing', existingMarker)],
    });

    await runLegacyLocalDataMigration({
      api: fixture.api,
      storage,
      getToken: async () => createJwt({ sub: 'user_2', iss: 'https://clerk.example.com' }),
      now: () => new Date('2026-03-09T00:00:00.000Z'),
    });

    expect(fixture.api.createManualTransaction).not.toHaveBeenCalled();
  });

  it('preserves distinct local accounts with duplicate names', async () => {
    const storage = new MemoryStorage();
    storage.setItem(
      'accounts',
      JSON.stringify([
        { id: 'local-account-1', name: 'Cash', type: 'cash', balance: 0 },
        { id: 'local-account-2', name: 'Cash', type: 'cash', balance: 0 },
      ]),
    );
    storage.setItem(
      'transactions',
      JSON.stringify([
        {
          id: 'legacy-transaction-1',
          amount: 10,
          type: 'expense',
          category: 'Food',
          date: '2026-01-01T00:00:00.000Z',
          accountId: 'local-account-1',
        },
        {
          id: 'legacy-transaction-2',
          amount: 20,
          type: 'expense',
          category: 'Food',
          date: '2026-01-02T00:00:00.000Z',
          accountId: 'local-account-2',
        },
      ]),
    );

    const fixture = createMigrationApiFixture();
    await runLegacyLocalDataMigration({
      api: fixture.api,
      storage,
      getToken: async () => createJwt({ sub: 'user_3', iss: 'https://clerk.example.com' }),
      now: () => new Date('2026-03-09T00:00:00.000Z'),
    });

    expect(fixture.api.createAccount).toHaveBeenCalledTimes(2);
    expect(fixture.api.createManualTransaction).toHaveBeenCalledTimes(2);

    const firstPayload = fixture.api.createManualTransaction.mock.calls[0]?.[0];
    const secondPayload = fixture.api.createManualTransaction.mock.calls[1]?.[0];

    expect(firstPayload.account_id).toBe('account-1');
    expect(secondPayload.account_id).toBe('account-2');
  });

  it('normalizes non-positive transaction amounts into valid backend transactions', async () => {
    const storage = new MemoryStorage();
    storage.setItem(
      'transactions',
      JSON.stringify([
        {
          id: 'legacy-invalid-zero',
          amount: 0,
          type: 'expense',
          category: 'Food',
          date: '2026-01-01T00:00:00.000Z',
        },
        {
          id: 'legacy-negative-expense',
          amount: -25,
          type: 'expense',
          category: 'Food',
          date: '2026-01-02T00:00:00.000Z',
        },
      ]),
    );

    const fixture = createMigrationApiFixture();
    const token = createJwt({ sub: 'user_4', iss: 'https://clerk.example.com' });

    await runLegacyLocalDataMigration({
      api: fixture.api,
      storage,
      getToken: async () => token,
      now: () => new Date('2026-03-09T00:00:00.000Z'),
    });

    expect(fixture.api.createManualTransaction).toHaveBeenCalledTimes(2);
    expect(fixture.api.createManualTransaction.mock.calls[0]?.[0].amount_in_paise).toBe(1);
    expect(fixture.api.createManualTransaction.mock.calls[0]?.[0].type).toBe('expense');
    expect(fixture.api.createManualTransaction.mock.calls[1]?.[0].amount_in_paise).toBe(2500);
    expect(fixture.api.createManualTransaction.mock.calls[1]?.[0].type).toBe('income');
    expect(fixture.api.createManualTransaction.mock.calls[1]?.[0].category_id).toBe('category-2');
    expect(fixture.api.createManualTransaction.mock.calls[1]?.[0].user_note).toContain(
      'Legacy negative amount',
    );

    const markerKey = buildMigrationMarkerKey({
      sub: 'user_4',
      iss: 'https://clerk.example.com',
    });
    expect(storage.getItem(markerKey)).not.toBeNull();
  });

  it('uses normalized type for category mapping when negative amount flips transaction type', async () => {
    const storage = new MemoryStorage();
    storage.setItem(
      'categories',
      JSON.stringify([
        {
          id: 'legacy-food-expense',
          name: 'Food',
          icon: 'Utensils',
          type: 'expense',
          subCategories: [],
          subCategoryIds: {},
        },
      ]),
    );
    storage.setItem(
      'transactions',
      JSON.stringify([
        {
          id: 'legacy-negative-food',
          amount: -10,
          type: 'expense',
          category: 'Food',
          categoryId: 'legacy-food-expense',
          date: '2026-01-02T10:00:00.000Z',
        },
      ]),
    );

    const fixture = createMigrationApiFixture();

    await runLegacyLocalDataMigration({
      api: fixture.api,
      storage,
      getToken: async () => createJwt({ sub: 'user_7', iss: 'https://clerk.example.com' }),
      now: () => new Date('2026-03-09T00:00:00.000Z'),
    });

    expect(fixture.api.createCategory).toHaveBeenCalledTimes(2);
    expect(fixture.api.createCategory.mock.calls[0]?.[0]).toMatchObject({
      name: 'Food',
      type: 'expense',
      parent_id: null,
    });
    expect(fixture.api.createCategory.mock.calls[1]?.[0]).toMatchObject({
      name: 'Food',
      type: 'income',
      parent_id: null,
    });

    expect(fixture.api.createManualTransaction).toHaveBeenCalledTimes(1);
    expect(fixture.api.createManualTransaction.mock.calls[0]?.[0]).toMatchObject({
      type: 'income',
      category_id: 'category-2',
    });
  });

  it('maps legacy category names to existing system child categories without creating duplicate roots', async () => {
    const storage = new MemoryStorage();
    storage.setItem(
      'categories',
      JSON.stringify([
        {
          id: 'legacy-food-root',
          name: 'Food',
          icon: 'Utensils',
          type: 'expense',
          subCategories: [],
          subCategoryIds: {},
        },
      ]),
    );
    storage.setItem(
      'transactions',
      JSON.stringify([
        {
          id: 'legacy-food-tx',
          amount: 100,
          type: 'expense',
          category: 'Food',
          categoryId: 'legacy-food-root',
          date: '2026-01-02T10:00:00.000Z',
        },
      ]),
    );

    const fixture = createMigrationApiFixture({
      existingCategories: [
        {
          id: 'system-expense-root',
          user_id: null,
          parent_id: null,
          name: 'Expense',
          type: 'expense',
          icon: 'ArrowUpCircle',
          is_system: true,
          created_at: '2026-01-01T00:00:00.000Z',
        },
        {
          id: 'system-food-child',
          user_id: null,
          parent_id: 'system-expense-root',
          name: 'Food',
          type: 'expense',
          icon: 'Utensils',
          is_system: true,
          created_at: '2026-01-01T00:00:00.000Z',
        },
      ],
    });

    await runLegacyLocalDataMigration({
      api: fixture.api,
      storage,
      getToken: async () => createJwt({ sub: 'user_5', iss: 'https://clerk.example.com' }),
      now: () => new Date('2026-03-09T00:00:00.000Z'),
    });

    expect(fixture.api.createCategory).not.toHaveBeenCalled();
    expect(fixture.api.createManualTransaction).toHaveBeenCalledTimes(1);
    expect(fixture.api.createManualTransaction.mock.calls[0]?.[0].category_id).toBe(
      'system-food-child',
    );
  });

  it('flattens legacy sub-categories to a top-level parent when only a nested parent name match exists', async () => {
    const storage = new MemoryStorage();
    storage.setItem(
      'categories',
      JSON.stringify([
        {
          id: 'legacy-food-root',
          name: 'Food',
          icon: 'Utensils',
          type: 'expense',
          subCategories: ['Groceries'],
          subCategoryIds: {
            Groceries: 'legacy-groceries-sub',
          },
        },
      ]),
    );
    storage.setItem(
      'transactions',
      JSON.stringify([
        {
          id: 'legacy-food-subcat-tx',
          amount: 150,
          type: 'expense',
          category: 'Food',
          categoryId: 'legacy-food-root',
          subCategory: 'Groceries',
          subCategoryId: 'legacy-groceries-sub',
          date: '2026-01-02T10:00:00.000Z',
        },
      ]),
    );

    const fixture = createMigrationApiFixture({
      existingCategories: [
        {
          id: 'system-expense-root',
          user_id: null,
          parent_id: null,
          name: 'Expense',
          type: 'expense',
          icon: 'ArrowUpCircle',
          is_system: true,
          created_at: '2026-01-01T00:00:00.000Z',
        },
        {
          id: 'system-food-child',
          user_id: null,
          parent_id: 'system-expense-root',
          name: 'Food',
          type: 'expense',
          icon: 'Utensils',
          is_system: true,
          created_at: '2026-01-01T00:00:00.000Z',
        },
      ],
    });

    await runLegacyLocalDataMigration({
      api: fixture.api,
      storage,
      getToken: async () => createJwt({ sub: 'user_6', iss: 'https://clerk.example.com' }),
      now: () => new Date('2026-03-09T00:00:00.000Z'),
    });

    expect(fixture.api.createCategory).toHaveBeenCalledTimes(2);
    expect(fixture.api.createCategory.mock.calls[0]?.[0]).toMatchObject({
      name: 'Food',
      parent_id: null,
    });
    expect(fixture.api.createCategory.mock.calls[1]?.[0]).toMatchObject({
      name: 'Groceries',
      parent_id: 'category-3',
    });
    expect(fixture.api.createManualTransaction.mock.calls[0]?.[0].category_id).toBe('category-4');
  });
});
