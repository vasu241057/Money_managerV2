import type { AccountType, CategoryType, TransactionType, TransactionFeedItem } from '../../../shared/types';
import { isRemoteDataEnabled } from '../config/data-source';
import { apiClient } from './api-client';
import { getAuthToken } from './auth-token';
import { rupeesToPaise } from './money';

const LEGACY_MIGRATION_VERSION = 1 as const;
const LEGACY_MIGRATION_MARKER_PREFIX = 'money-manager:legacy-migration:v1';
const LEGACY_TRANSACTION_MARKER_PREFIX = 'legacy-local:v1:';

const LEGACY_ACCOUNTS_KEY = 'accounts';
const LEGACY_CATEGORIES_KEY = 'categories';
const LEGACY_TRANSACTIONS_KEY = 'transactions';

const TRANSACTION_PAGE_LIMIT = 200;

type LegacyMigrationApiClient = Pick<
  typeof apiClient,
  | 'listAccounts'
  | 'createAccount'
  | 'listCategories'
  | 'createCategory'
  | 'listTransactions'
  | 'createManualTransaction'
>;

interface StorageLike {
  getItem: (key: string) => string | null;
  setItem: (key: string, value: string) => void;
}

interface LegacyMigrationDeps {
  api: LegacyMigrationApiClient;
  storage: StorageLike;
  getToken: () => Promise<string | null>;
  now: () => Date;
}

interface JwtIdentity {
  sub: string;
  iss: string | null;
}

interface LegacyAccount {
  id: string;
  name: string;
  type: AccountType;
  balance: number;
}

interface LegacyCategory {
  id: string;
  name: string;
  type: CategoryType;
  icon: string;
  subCategories: string[];
  subCategoryIds: Record<string, string>;
}

interface LegacyTransaction {
  migrationKey: string;
  amount: number;
  type: TransactionType;
  categoryName: string | null;
  categoryId: string | null;
  subCategoryName: string | null;
  subCategoryId: string | null;
  date: Date;
  description: string | null;
  accountId: string | null;
  accountName: string | null;
}

interface NormalizedLegacyTransaction {
  amountInPaise: number;
  type: TransactionType;
  userNote: string | null;
}

interface LegacySnapshot {
  accounts: LegacyAccount[];
  categories: LegacyCategory[];
  transactions: LegacyTransaction[];
}

interface RemoteAccount {
  id: string;
  name: string;
  type: AccountType;
}

interface RemoteCategory {
  id: string;
  parent_id: string | null;
  name: string;
  type: CategoryType;
  icon: string | null;
  is_system: boolean;
}

interface CategoryDraft {
  key: string;
  name: string;
  type: CategoryType;
  icon: string;
  localIds: Set<string>;
  subCategories: Map<string, SubCategoryDraft>;
}

interface SubCategoryDraft {
  key: string;
  name: string;
  localIds: Set<string>;
}

interface CategoryResolution {
  parentIdByKey: Map<string, string>;
  childIdByKey: Map<string, string>;
  categoryIdByLegacyId: Map<string, string>;
  subCategoryIdByLegacyId: Map<string, string>;
  categoryTypeById: Map<string, CategoryType>;
  createdParentCount: number;
  createdChildCount: number;
}

interface MigrationResult {
  accountsMigrated: number;
  parentCategoriesMigrated: number;
  subCategoriesMigrated: number;
  transactionsMigrated: number;
}

interface CompletedMigrationMarker {
  version: typeof LEGACY_MIGRATION_VERSION;
  status: 'completed';
  completed_at: string;
  source_counts: {
    accounts: number;
    categories: number;
    transactions: number;
  };
  migrated_counts: {
    accounts: number;
    parent_categories: number;
    sub_categories: number;
    transactions: number;
  };
}

const DEFAULT_DEPS: LegacyMigrationDeps = {
  api: apiClient,
  storage: undefined as unknown as StorageLike,
  getToken: () => getAuthToken(),
  now: () => new Date(),
};

let inFlightMigration: Promise<void> | null = null;

export function decodeJwtIdentity(token: string): JwtIdentity | null {
  const parts = token.split('.');
  if (parts.length < 2) {
    return null;
  }

  const payload = decodeJwtPayload(parts[1]);
  if (!payload) {
    return null;
  }

  const sub = normalizeNonEmptyString(payload.sub);
  if (!sub) {
    return null;
  }

  const iss = normalizeNonEmptyString(payload.iss);
  return { sub, iss };
}

export function buildMigrationMarkerKey(identity: JwtIdentity): string {
  const encodedIssuer = encodeURIComponent(identity.iss ?? 'unknown-issuer');
  const encodedSubject = encodeURIComponent(identity.sub);
  return `${LEGACY_MIGRATION_MARKER_PREFIX}:${encodedIssuer}:${encodedSubject}`;
}

export function buildLegacyTransactionMarker(localTransactionKey: string): string {
  return `${LEGACY_TRANSACTION_MARKER_PREFIX}${encodeURIComponent(localTransactionKey)}`;
}

export function parseLegacyTransactionMarker(instrumentId: string | null): string | null {
  if (!instrumentId || !instrumentId.startsWith(LEGACY_TRANSACTION_MARKER_PREFIX)) {
    return null;
  }

  const encoded = instrumentId.slice(LEGACY_TRANSACTION_MARKER_PREFIX.length);
  try {
    const decoded = decodeURIComponent(encoded);
    return decoded.trim() === '' ? null : decoded;
  } catch {
    return null;
  }
}

export function readLegacySnapshotFromStorage(storage: StorageLike): LegacySnapshot {
  const rawAccounts = readStorageArray(storage, LEGACY_ACCOUNTS_KEY);
  const rawCategories = readStorageArray(storage, LEGACY_CATEGORIES_KEY);
  const rawTransactions = readStorageArray(storage, LEGACY_TRANSACTIONS_KEY);

  const accounts = rawAccounts
    .map((value, index) => parseLegacyAccount(value, index))
    .filter((value): value is LegacyAccount => value !== null);

  const categories = rawCategories
    .map((value, index) => parseLegacyCategory(value, index))
    .filter((value): value is LegacyCategory => value !== null);

  const transactions = parseLegacyTransactions(rawTransactions);

  return {
    accounts,
    categories,
    transactions,
  };
}

export function hasLegacyData(snapshot: LegacySnapshot): boolean {
  return snapshot.accounts.length > 0 || snapshot.categories.length > 0 || snapshot.transactions.length > 0;
}

function isCompletedMigrationMarker(value: string | null): boolean {
  if (!value) {
    return false;
  }

  try {
    const parsed = JSON.parse(value) as Partial<CompletedMigrationMarker>;
    return parsed.version === LEGACY_MIGRATION_VERSION && parsed.status === 'completed';
  } catch {
    return false;
  }
}

function writeCompletedMigrationMarker(
  storage: StorageLike,
  markerKey: string,
  now: Date,
  snapshot: LegacySnapshot,
  result: MigrationResult,
): void {
  const marker: CompletedMigrationMarker = {
    version: LEGACY_MIGRATION_VERSION,
    status: 'completed',
    completed_at: now.toISOString(),
    source_counts: {
      accounts: snapshot.accounts.length,
      categories: snapshot.categories.length,
      transactions: snapshot.transactions.length,
    },
    migrated_counts: {
      accounts: result.accountsMigrated,
      parent_categories: result.parentCategoriesMigrated,
      sub_categories: result.subCategoriesMigrated,
      transactions: result.transactionsMigrated,
    },
  };

  storage.setItem(markerKey, JSON.stringify(marker));
}

export async function runLegacyLocalDataMigration(deps: LegacyMigrationDeps): Promise<void> {
  const token = await deps.getToken();
  if (!token) {
    return;
  }

  const identity = decodeJwtIdentity(token);
  if (!identity) {
    return;
  }

  const markerKey = buildMigrationMarkerKey(identity);
  if (isCompletedMigrationMarker(deps.storage.getItem(markerKey))) {
    return;
  }

  const snapshot = readLegacySnapshotFromStorage(deps.storage);
  if (!hasLegacyData(snapshot)) {
    writeCompletedMigrationMarker(
      deps.storage,
      markerKey,
      deps.now(),
      snapshot,
      {
        accountsMigrated: 0,
        parentCategoriesMigrated: 0,
        subCategoriesMigrated: 0,
        transactionsMigrated: 0,
      },
    );
    return;
  }

  const accountMigration = await migrateAccounts(deps.api, snapshot);
  const categoryMigration = await migrateCategories(deps.api, snapshot);
  const transactionsMigrated = await migrateTransactions(
    deps.api,
    snapshot.transactions,
    accountMigration.accountIdByLegacyId,
    accountMigration.accountIdByNameKey,
    categoryMigration,
  );

  writeCompletedMigrationMarker(deps.storage, markerKey, deps.now(), snapshot, {
    accountsMigrated: accountMigration.createdCount,
    parentCategoriesMigrated: categoryMigration.createdParentCount,
    subCategoriesMigrated: categoryMigration.createdChildCount,
    transactionsMigrated,
  });
}

export async function ensureLegacyLocalDataMigrated(
  overrides?: Partial<LegacyMigrationDeps>,
): Promise<void> {
  if (!isRemoteDataEnabled()) {
    return;
  }

  if (typeof window === 'undefined') {
    return;
  }

  if (inFlightMigration) {
    return inFlightMigration;
  }

  const deps: LegacyMigrationDeps = {
    api: overrides?.api ?? DEFAULT_DEPS.api,
    storage: overrides?.storage ?? window.localStorage,
    getToken: overrides?.getToken ?? DEFAULT_DEPS.getToken,
    now: overrides?.now ?? DEFAULT_DEPS.now,
  };

  inFlightMigration = runLegacyLocalDataMigration(deps).finally(() => {
    inFlightMigration = null;
  });

  return inFlightMigration;
}

function readStorageArray(storage: StorageLike, key: string): unknown[] {
  const raw = storage.getItem(key);
  if (!raw) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function parseLegacyAccount(value: unknown, index: number): LegacyAccount | null {
  if (!isRecord(value)) {
    return null;
  }

  const id = normalizeNonEmptyString(value.id) ?? `legacy-account-${index + 1}`;
  const name = normalizeNonEmptyString(value.name) ?? `Legacy Account ${index + 1}`;
  const type = normalizeAccountType(value.type);
  const balance = parseNumber(value.balance) ?? 0;

  return {
    id,
    name,
    type,
    balance,
  };
}

function parseLegacyCategory(value: unknown, index: number): LegacyCategory | null {
  if (!isRecord(value)) {
    return null;
  }

  const id = normalizeNonEmptyString(value.id) ?? `legacy-category-${index + 1}`;
  const name = normalizeNonEmptyString(value.name);
  if (!name) {
    return null;
  }

  const type = normalizeCategoryType(value.type);
  const icon = normalizeNonEmptyString(value.icon) ?? 'Circle';
  const subCategories = Array.isArray(value.subCategories)
    ? value.subCategories
        .map(item => normalizeNonEmptyString(item))
        .filter((item): item is string => item !== null)
    : [];

  const subCategoryIds: Record<string, string> = {};
  if (isRecord(value.subCategoryIds)) {
    Object.entries(value.subCategoryIds).forEach(([subCategoryName, subCategoryId]) => {
      const normalizedName = normalizeNonEmptyString(subCategoryName);
      const normalizedId = normalizeNonEmptyString(subCategoryId);
      if (!normalizedName || !normalizedId) {
        return;
      }

      subCategoryIds[normalizedName] = normalizedId;
    });
  }

  return {
    id,
    name,
    type,
    icon,
    subCategories,
    subCategoryIds,
  };
}

function parseLegacyTransactions(values: unknown[]): LegacyTransaction[] {
  const seenKeyCounts = new Map<string, number>();

  return values
    .map((value, index) => parseLegacyTransaction(value, index, seenKeyCounts))
    .filter((value): value is LegacyTransaction => value !== null);
}

function parseLegacyTransaction(
  value: unknown,
  index: number,
  seenKeyCounts: Map<string, number>,
): LegacyTransaction | null {
  if (!isRecord(value)) {
    return null;
  }

  const amount = parseNumber(value.amount);
  if (amount === null) {
    return null;
  }

  const parsedDate = parseDate(value.date);
  if (!parsedDate) {
    return null;
  }

  const baseKey = normalizeNonEmptyString(value.id) ?? `legacy-tx-${index + 1}`;
  const currentCount = seenKeyCounts.get(baseKey) ?? 0;
  seenKeyCounts.set(baseKey, currentCount + 1);
  const migrationKey = currentCount === 0 ? baseKey : `${baseKey}__${currentCount}`;

  return {
    migrationKey,
    amount,
    type: normalizeTransactionType(value.type),
    categoryName: normalizeNonEmptyString(value.category),
    categoryId: normalizeNonEmptyString(value.categoryId),
    subCategoryName: normalizeNonEmptyString(value.subCategory),
    subCategoryId: normalizeNonEmptyString(value.subCategoryId),
    date: parsedDate,
    description: normalizeOptionalText(value.description),
    accountId: normalizeNonEmptyString(value.accountId),
    accountName: normalizeNonEmptyString(value.accountName),
  };
}

function parseDate(value: unknown): Date | null {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }

  if (typeof value === 'string' || typeof value === 'number') {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  return null;
}

function parseNumber(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function normalizeAccountType(value: unknown): AccountType {
  if (value === 'cash' || value === 'bank' || value === 'card' || value === 'other') {
    return value;
  }

  return 'other';
}

function normalizeCategoryType(value: unknown): CategoryType {
  if (value === 'income' || value === 'expense' || value === 'transfer') {
    return value;
  }

  return 'expense';
}

function normalizeTransactionType(value: unknown): TransactionType {
  if (value === 'income' || value === 'expense' || value === 'transfer') {
    return value;
  }

  return 'expense';
}

function normalizeNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim();
  return normalized === '' ? null : normalized;
}

function normalizeOptionalText(value: unknown): string | null {
  const normalized = normalizeNonEmptyString(value);
  return normalized ?? null;
}

function normalizeLookupKey(value: string): string {
  return value.trim().toLowerCase();
}

function buildAccountMatchKey(name: string, type: AccountType): string {
  return `${normalizeLookupKey(name)}::${type}`;
}

function inferAccountType(name: string): AccountType {
  const key = normalizeLookupKey(name);
  if (key.includes('cash')) {
    return 'cash';
  }

  if (key.includes('card')) {
    return 'card';
  }

  if (key.includes('bank')) {
    return 'bank';
  }

  return 'other';
}

async function migrateAccounts(
  api: LegacyMigrationApiClient,
  snapshot: LegacySnapshot,
): Promise<{
  accountIdByLegacyId: Map<string, string>;
  accountIdByNameKey: Map<string, string>;
  createdCount: number;
}> {
  const localAccounts = buildAccountDrafts(snapshot.accounts, snapshot.transactions);
  const remoteAccounts = await api.listAccounts();

  const accountIdByLegacyId = new Map<string, string>();
  const accountIdByNameKey = new Map<string, string>();

  remoteAccounts.forEach((row) => {
    const nameKey = normalizeLookupKey(row.name);
    if (!accountIdByNameKey.has(nameKey)) {
      accountIdByNameKey.set(nameKey, row.id);
    }
  });

  const remoteAccountsByKey = new Map<string, RemoteAccount[]>();
  const assignedCountByKey = new Map<string, number>();
  remoteAccounts.forEach((row) => {
    const key = buildAccountMatchKey(row.name, row.type);
    const list = remoteAccountsByKey.get(key) ?? [];
    list.push({
      id: row.id,
      name: row.name,
      type: row.type,
    });
    remoteAccountsByKey.set(key, list);
  });

  let createdCount = 0;

  for (const draft of localAccounts) {
    const matchingKey = buildAccountMatchKey(draft.name, draft.type);
    const candidates = remoteAccountsByKey.get(matchingKey) ?? [];
    const consumed = assignedCountByKey.get(matchingKey) ?? 0;
    const existing = candidates[consumed];

    if (existing) {
      accountIdByLegacyId.set(draft.legacyId, existing.id);
      assignedCountByKey.set(matchingKey, consumed + 1);
      continue;
    }

    const created = await api.createAccount({
      name: draft.name,
      type: draft.type,
      initial_balance_in_paise: rupeesToPaise(draft.balance),
    });
    createdCount += 1;

    accountIdByLegacyId.set(draft.legacyId, created.id);
    const createdNameKey = normalizeLookupKey(created.name);
    if (!accountIdByNameKey.has(createdNameKey)) {
      accountIdByNameKey.set(createdNameKey, created.id);
    }

    const updatedCandidates = remoteAccountsByKey.get(matchingKey) ?? [];
    updatedCandidates.push({
      id: created.id,
      name: created.name,
      type: created.type,
    });
    remoteAccountsByKey.set(matchingKey, updatedCandidates);
    assignedCountByKey.set(matchingKey, consumed + 1);
  }

  return {
    accountIdByLegacyId,
    accountIdByNameKey,
    createdCount,
  };
}

function buildAccountDrafts(
  accounts: LegacyAccount[],
  transactions: LegacyTransaction[],
): Array<{
  legacyId: string;
  name: string;
  type: AccountType;
  balance: number;
}> {
  const drafts: Array<{ legacyId: string; name: string; type: AccountType; balance: number }> = [];
  const byLegacyId = new Map<string, number>();
  const byNameKey = new Map<string, string>();

  accounts.forEach((account) => {
    const normalizedName = normalizeNonEmptyString(account.name);
    if (!normalizedName) {
      return;
    }

    if (byLegacyId.has(account.id)) {
      return;
    }

    const draft = {
      legacyId: account.id,
      name: normalizedName,
      type: account.type,
      balance: account.balance,
    };
    byLegacyId.set(account.id, drafts.length);
    byNameKey.set(normalizeLookupKey(normalizedName), account.id);
    drafts.push(draft);
  });

  transactions.forEach((transaction) => {
    if (transaction.accountId && byLegacyId.has(transaction.accountId)) {
      return;
    }

    if (transaction.accountId) {
      const inferredName = transaction.accountName ?? `Legacy Account ${transaction.accountId}`;
      const draft = {
        legacyId: transaction.accountId,
        name: inferredName,
        type: inferAccountType(inferredName),
        balance: 0,
      };
      byLegacyId.set(transaction.accountId, drafts.length);
      byNameKey.set(normalizeLookupKey(inferredName), transaction.accountId);
      drafts.push(draft);
      return;
    }

    if (!transaction.accountName) {
      return;
    }

    const nameKey = normalizeLookupKey(transaction.accountName);
    if (byNameKey.has(nameKey)) {
      return;
    }

    const legacyId = `legacy-account-name:${nameKey}`;
    const draft = {
      legacyId,
      name: transaction.accountName,
      type: inferAccountType(transaction.accountName),
      balance: 0,
    };

    byLegacyId.set(legacyId, drafts.length);
    byNameKey.set(nameKey, legacyId);
    drafts.push(draft);
  });

  return drafts;
}

async function migrateCategories(
  api: LegacyMigrationApiClient,
  snapshot: LegacySnapshot,
): Promise<CategoryResolution> {
  const drafts = buildCategoryDrafts(snapshot.categories, snapshot.transactions);
  const remoteCategories = await api.listCategories();

  const parentIdByKey = new Map<string, string>();
  const childIdByKey = new Map<string, string>();
  const categoryIdByLegacyId = new Map<string, string>();
  const subCategoryIdByLegacyId = new Map<string, string>();

  const rows = [...remoteCategories] as RemoteCategory[];
  const categoryTypeById = new Map(rows.map(row => [row.id, row.type]));

  let createdParentCount = 0;
  let createdChildCount = 0;

  for (const draft of drafts) {
    let parent = findExistingParentCategory(rows, draft);
    if (draft.subCategories.size > 0 && parent?.parent_id !== null) {
      parent = findExistingTopLevelParentCategory(rows, draft);
    }

    let parentId = parent?.id;

    if (!parentId) {
      const created = await api.createCategory({
        name: draft.name,
        type: draft.type,
        icon: draft.icon,
        parent_id: null,
      });
      rows.push(created);
      parentId = created.id;
      categoryTypeById.set(created.id, created.type);
      createdParentCount += 1;
    }

    parentIdByKey.set(draft.key, parentId);
    draft.localIds.forEach((localId) => {
      categoryIdByLegacyId.set(localId, parentId);
    });

    const sortedChildren = [...draft.subCategories.values()].sort((a, b) =>
      a.name.localeCompare(b.name),
    );
    for (const childDraft of sortedChildren) {
      const existingChild = findExistingChildCategory(
        rows,
        parentId,
        draft.type,
        childDraft.name,
      );
      let childId = existingChild?.id;

      if (!childId) {
        const createdChild = await api.createCategory({
          name: childDraft.name,
          type: draft.type,
          icon: draft.icon,
          parent_id: parentId,
        });
        rows.push(createdChild);
        childId = createdChild.id;
        categoryTypeById.set(createdChild.id, createdChild.type);
        createdChildCount += 1;
      }

      childIdByKey.set(childDraft.key, childId);
      childDraft.localIds.forEach((localId) => {
        subCategoryIdByLegacyId.set(localId, childId);
      });
    }
  }

  return {
    parentIdByKey,
    childIdByKey,
    categoryIdByLegacyId,
    subCategoryIdByLegacyId,
    categoryTypeById,
    createdParentCount,
    createdChildCount,
  };
}

function buildCategoryDrafts(
  categories: LegacyCategory[],
  transactions: LegacyTransaction[],
): CategoryDraft[] {
  const parents = new Map<string, CategoryDraft>();

  const ensureParent = (type: CategoryType, name: string, icon: string): CategoryDraft => {
    const parentKey = buildParentCategoryKey(type, name);
    const existing = parents.get(parentKey);
    if (existing) {
      return existing;
    }

    const draft: CategoryDraft = {
      key: parentKey,
      name,
      type,
      icon,
      localIds: new Set<string>(),
      subCategories: new Map<string, SubCategoryDraft>(),
    };
    parents.set(parentKey, draft);
    return draft;
  };

  const ensureChild = (parent: CategoryDraft, subCategoryName: string): SubCategoryDraft => {
    const childKey = buildChildCategoryKey(parent.type, parent.name, subCategoryName);
    const existing = parent.subCategories.get(childKey);
    if (existing) {
      return existing;
    }

    const draft: SubCategoryDraft = {
      key: childKey,
      name: subCategoryName,
      localIds: new Set<string>(),
    };
    parent.subCategories.set(childKey, draft);
    return draft;
  };

  categories.forEach((category) => {
    const parent = ensureParent(category.type, category.name, category.icon);
    parent.localIds.add(category.id);

    category.subCategories.forEach((subCategoryName) => {
      const child = ensureChild(parent, subCategoryName);
      const localSubCategoryId = category.subCategoryIds[subCategoryName];
      if (localSubCategoryId) {
        child.localIds.add(localSubCategoryId);
      }
    });
  });

  transactions.forEach((transaction) => {
    if (!transaction.categoryName) {
      return;
    }

    const normalizedType = normalizeTransactionTypeForLegacyAmount(transaction.type, transaction.amount);
    const parent = ensureParent(normalizedType, transaction.categoryName, 'Circle');
    if (transaction.categoryId) {
      parent.localIds.add(transaction.categoryId);
    }

    if (!transaction.subCategoryName) {
      return;
    }

    const child = ensureChild(parent, transaction.subCategoryName);
    if (transaction.subCategoryId) {
      child.localIds.add(transaction.subCategoryId);
    }
  });

  return [...parents.values()].sort((a, b) => {
    if (a.type !== b.type) {
      return a.type.localeCompare(b.type);
    }

    return a.name.localeCompare(b.name);
  });
}

function buildParentCategoryKey(type: CategoryType, name: string): string {
  return `${type}::${normalizeLookupKey(name)}`;
}

function buildChildCategoryKey(type: CategoryType, parentName: string, childName: string): string {
  return `${buildParentCategoryKey(type, parentName)}::${normalizeLookupKey(childName)}`;
}

function findExistingParentCategory(
  rows: RemoteCategory[],
  draft: CategoryDraft,
): RemoteCategory | null {
  const normalizedName = normalizeLookupKey(draft.name);

  const userRootMatch = rows.find(
    row =>
      row.parent_id === null &&
      row.is_system === false &&
      row.type === draft.type &&
      normalizeLookupKey(row.name) === normalizedName,
  );
  if (userRootMatch) {
    return userRootMatch;
  }

  const userNestedMatch = rows.find(
    row =>
      row.parent_id !== null &&
      row.is_system === false &&
      row.type === draft.type &&
      normalizeLookupKey(row.name) === normalizedName,
  );
  if (userNestedMatch) {
    return userNestedMatch;
  }

  const systemRootMatch = rows.find(
    row =>
      row.parent_id === null &&
      row.is_system === true &&
      row.type === draft.type &&
      normalizeLookupKey(row.name) === normalizedName,
  );
  if (systemRootMatch) {
    return systemRootMatch;
  }

  const systemNestedMatch = rows.find(
    row =>
      row.parent_id !== null &&
      row.is_system === true &&
      row.type === draft.type &&
      normalizeLookupKey(row.name) === normalizedName,
  );

  return systemNestedMatch ?? null;
}

function findExistingTopLevelParentCategory(
  rows: RemoteCategory[],
  draft: CategoryDraft,
): RemoteCategory | null {
  const normalizedName = normalizeLookupKey(draft.name);

  const userRootMatch = rows.find(
    row =>
      row.parent_id === null &&
      row.is_system === false &&
      row.type === draft.type &&
      normalizeLookupKey(row.name) === normalizedName,
  );
  if (userRootMatch) {
    return userRootMatch;
  }

  const systemRootMatch = rows.find(
    row =>
      row.parent_id === null &&
      row.is_system === true &&
      row.type === draft.type &&
      normalizeLookupKey(row.name) === normalizedName,
  );
  return systemRootMatch ?? null;
}

function findExistingChildCategory(
  rows: RemoteCategory[],
  parentId: string,
  type: CategoryType,
  childName: string,
): RemoteCategory | null {
  const normalizedName = normalizeLookupKey(childName);

  const userMatch = rows.find(
    row =>
      row.parent_id === parentId &&
      row.is_system === false &&
      row.type === type &&
      normalizeLookupKey(row.name) === normalizedName,
  );
  if (userMatch) {
    return userMatch;
  }

  const systemMatch = rows.find(
    row =>
      row.parent_id === parentId &&
      row.is_system === true &&
      row.type === type &&
      normalizeLookupKey(row.name) === normalizedName,
  );
  return systemMatch ?? null;
}

async function migrateTransactions(
  api: LegacyMigrationApiClient,
  transactions: LegacyTransaction[],
  accountIdByLegacyId: Map<string, string>,
  accountIdByNameKey: Map<string, string>,
  categoryResolution: CategoryResolution,
): Promise<number> {
  const existingLegacyKeys = await listExistingLegacyTransactionKeys(api);
  const sortedTransactions = [...transactions].sort((a, b) => {
    if (a.date.getTime() !== b.date.getTime()) {
      return a.date.getTime() - b.date.getTime();
    }

    return a.migrationKey.localeCompare(b.migrationKey);
  });

  let migratedCount = 0;

  for (const transaction of sortedTransactions) {
    if (existingLegacyKeys.has(transaction.migrationKey)) {
      continue;
    }

    const normalizedTransaction = normalizeLegacyTransactionForCreate(transaction);

    const accountId = resolveAccountId(transaction, accountIdByLegacyId, accountIdByNameKey);
    const categoryId = resolveCategoryId(
      transaction,
      normalizedTransaction.type,
      categoryResolution,
    );

    try {
      await api.createManualTransaction({
        amount_in_paise: normalizedTransaction.amountInPaise,
        type: normalizedTransaction.type,
        txn_date: transaction.date.toISOString(),
        account_id: accountId,
        category_id: categoryId,
        user_note: normalizedTransaction.userNote,
        payment_method: 'unknown',
        instrument_id: buildLegacyTransactionMarker(transaction.migrationKey),
      });

      existingLegacyKeys.add(transaction.migrationKey);
      migratedCount += 1;
    } catch (error) {
      const reconciledKeys = await listExistingLegacyTransactionKeys(api);
      if (reconciledKeys.has(transaction.migrationKey)) {
        existingLegacyKeys.add(transaction.migrationKey);
        continue;
      }

      throw error;
    }
  }

  return migratedCount;
}

function resolveAccountId(
  transaction: LegacyTransaction,
  accountIdByLegacyId: Map<string, string>,
  accountIdByNameKey: Map<string, string>,
): string | null {
  if (transaction.accountId) {
    return accountIdByLegacyId.get(transaction.accountId) ?? null;
  }

  if (transaction.accountName) {
    return accountIdByNameKey.get(normalizeLookupKey(transaction.accountName)) ?? null;
  }

  return null;
}

function resolveCategoryId(
  transaction: LegacyTransaction,
  normalizedType: TransactionType,
  categoryResolution: CategoryResolution,
): string | null {
  if (transaction.subCategoryId) {
    const mapped = categoryResolution.subCategoryIdByLegacyId.get(transaction.subCategoryId);
    if (mapped && categoryResolution.categoryTypeById.get(mapped) === normalizedType) {
      return mapped;
    }
  }

  if (transaction.subCategoryName && transaction.categoryName) {
    const childKey = buildChildCategoryKey(
      normalizedType,
      transaction.categoryName,
      transaction.subCategoryName,
    );
    const mapped = categoryResolution.childIdByKey.get(childKey);
    if (mapped) {
      return mapped;
    }
  }

  if (transaction.categoryId) {
    const mapped = categoryResolution.categoryIdByLegacyId.get(transaction.categoryId);
    if (mapped && categoryResolution.categoryTypeById.get(mapped) === normalizedType) {
      return mapped;
    }
  }

  if (transaction.categoryName) {
    const parentKey = buildParentCategoryKey(normalizedType, transaction.categoryName);
    return categoryResolution.parentIdByKey.get(parentKey) ?? null;
  }

  return null;
}

function normalizeLegacyTransactionForCreate(transaction: LegacyTransaction): NormalizedLegacyTransaction {
  let amountInPaise = rupeesToPaise(transaction.amount);
  const normalizedType = normalizeTransactionTypeForLegacyAmount(transaction.type, transaction.amount);
  let normalizedNote = transaction.description;

  if (transaction.amount < 0) {
    amountInPaise = Math.abs(amountInPaise);
    normalizedNote = appendMigrationNote(
      normalizedNote,
      `Legacy negative amount (${transaction.amount.toFixed(2)}) normalized during migration.`,
    );
  }

  if (amountInPaise === 0) {
    amountInPaise = 1;
    normalizedNote = appendMigrationNote(
      normalizedNote,
      'Legacy near-zero amount normalized to 0.01 during migration.',
    );
  }

  return {
    amountInPaise,
    type: normalizedType,
    userNote: normalizedNote,
  };
}

function normalizeTransactionTypeForLegacyAmount(
  type: TransactionType,
  amount: number,
): TransactionType {
  if (amount >= 0) {
    return type;
  }

  if (type === 'expense') {
    return 'income';
  }

  if (type === 'income') {
    return 'expense';
  }

  return type;
}

function appendMigrationNote(baseNote: string | null, suffix: string): string {
  if (!baseNote || baseNote.trim() === '') {
    return suffix;
  }

  return `${baseNote.trim()} ${suffix}`;
}

async function listExistingLegacyTransactionKeys(api: LegacyMigrationApiClient): Promise<Set<string>> {
  const keys = new Set<string>();

  let page = 1;
  while (true) {
    const response = await api.listTransactions({
      page,
      limit: TRANSACTION_PAGE_LIMIT,
    });

    response.data.forEach((item: TransactionFeedItem) => {
      const marker = parseLegacyTransactionMarker(item.financial_event.instrument_id);
      if (marker) {
        keys.add(marker);
      }
    });

    if (!response.has_more) {
      break;
    }

    page += 1;
  }

  return keys;
}

function decodeJwtPayload(payloadSegment: string): Record<string, unknown> | null {
  const normalized = payloadSegment.replace(/-/g, '+').replace(/_/g, '/');
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=');

  try {
    const decoded = atob(padded);
    const parsed = JSON.parse(decoded) as unknown;
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
