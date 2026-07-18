import { useMemo } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { CategoryRow } from '../../../shared/types';
import { apiClient, toErrorMessage } from '../lib/api-client';
import type { Category } from '../types/domain';

export interface HiddenCategory {
  id: string;
  name: string;
  type: CategoryRow['type'];
  parentName: string | null;
}

interface UseCategoriesResult {
  categories: Category[];
  hiddenCategories: HiddenCategory[];
  addCategory: (category: Omit<Category, 'id'>) => Promise<void>;
  renameCategory: (id: string, name: string) => Promise<void>;
  deleteCategory: (id: string) => Promise<{ hidden: boolean }>;
  restoreCategory: (id: string) => Promise<void>;
  addSubCategory: (categoryId: string, subCategoryName: string) => Promise<void>;
  deleteSubCategory: (categoryId: string, subCategoryName: string) => Promise<void>;
  isLoading: boolean;
  error: string | null;
}

const CATEGORIES_QUERY_KEY = ['categories'] as const;

function mapCategoryRows(rows: CategoryRow[]): {
  categories: Category[];
  hidden: HiddenCategory[];
} {
  const rowsById = new Map(rows.map(row => [row.id, row]));

  const hidden = rows
    .filter(row => row.is_hidden === true)
    .sort((a, b) => a.name.localeCompare(b.name))
    .map(row => {
      const parent = row.parent_id ? rowsById.get(row.parent_id) : undefined;
      return {
        id: row.id,
        name: row.name,
        type: row.type,
        // Only meaningful for hidden subs; a hidden parent's parent is a
        // structural root, which would just be noise.
        parentName: parent && parent.parent_id !== null ? parent.name : null,
      };
    });

  // The visible tree ignores hidden rows entirely; user-owned children of a
  // hidden system parent drop out with it (they return on restore).
  const visibleRows = rows.filter(row => row.is_hidden !== true);

  // Root IDs (parent_id === null): Income, Expense, Transfer — structural, not assignable
  const rootIds = new Set(
    visibleRows.filter(row => row.parent_id === null).map(row => row.id),
  );

  // Children grouped by parent
  const childrenByParent = new Map<string, CategoryRow[]>();
  visibleRows.forEach(row => {
    if (!row.parent_id) return;
    const existing = childrenByParent.get(row.parent_id) ?? [];
    existing.push(row);
    childrenByParent.set(row.parent_id, existing);
  });

  // Parents: parent_id points to a root (not null, parent is a root)
  const categories = visibleRows
    .filter(row => row.parent_id !== null && rootIds.has(row.parent_id))
    .sort((a, b) => a.name.localeCompare(b.name))
    .map(parent => {
      const children = (childrenByParent.get(parent.id) ?? []).sort((a, b) =>
        a.name.localeCompare(b.name),
      );

      return {
        id: parent.id,
        name: parent.name,
        icon: parent.icon ?? 'Circle',
        type: parent.type,
        isSystem: parent.is_system,
        subCategories: children.map(child => child.name),
        subCategoryIds: Object.fromEntries(children.map(child => [child.name, child.id])),
        subCategorySystem: Object.fromEntries(children.map(child => [child.name, child.is_system])),
      };
    });

  return { categories, hidden };
}

function useRemoteCategories(): UseCategoriesResult {
  const queryClient = useQueryClient();

  const categoriesQuery = useQuery({
    queryKey: CATEGORIES_QUERY_KEY,
    queryFn: async () => {
      // Hidden rows are fetched too (flagged is_hidden) so the Settings
      // restore list works; the visible tree filters them out.
      return apiClient.listCategories(undefined, true);
    },
  });

  const addCategoryMutation = useMutation({
    mutationFn: async (category: Omit<Category, 'id'>) => {
      const rows = categoriesQuery.data ?? [];
      // Find the structural root for this type (Income/Expense/Transfer)
      const root = rows.find(row => row.parent_id === null && row.type === category.type);
      if (!root) {
        // Creating with parent_id null would make an invisible top-level
        // category the tree mapper treats as a root.
        throw new Error('Categories are still loading. Try again in a moment.');
      }
      const parentId = root.id;

      const parent = await apiClient.createCategory({
        name: category.name,
        type: category.type,
        icon: category.icon,
        parent_id: parentId,
      });

      if (!category.subCategories || category.subCategories.length === 0) {
        return parent;
      }

      await Promise.all(
        category.subCategories.map(subCategoryName =>
          apiClient.createCategory({
            name: subCategoryName,
            type: category.type,
            icon: category.icon,
            parent_id: parent.id,
          }),
        ),
      );

      return parent;
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CATEGORIES_QUERY_KEY });
    },
  });

  const deleteCategoryMutation = useMutation({
    mutationFn: async (categoryId: string): Promise<{ hidden: boolean }> => {
      const rows = categoriesQuery.data ?? [];
      const target = rows.find(row => row.id === categoryId);

      // System rows are hidden per-user server-side (cascading to their
      // direct system children) — never delete anything around them.
      if (target?.is_system) {
        return apiClient.deleteCategory(categoryId);
      }

      const children = rows.filter(row => row.parent_id === categoryId && !row.is_system);

      for (const child of children) {
        await apiClient.deleteCategory(child.id);
      }

      return apiClient.deleteCategory(categoryId);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CATEGORIES_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ['transactions'] });
    },
  });

  const restoreCategoryMutation = useMutation({
    mutationFn: async (categoryId: string) => {
      await apiClient.restoreCategory(categoryId);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CATEGORIES_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ['transactions'] });
    },
  });

  const renameCategoryMutation = useMutation({
    mutationFn: async ({ categoryId, name }: { categoryId: string; name: string }) => {
      await apiClient.updateCategory(categoryId, { name });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CATEGORIES_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ['transactions'] });
    },
  });

  const addSubCategoryMutation = useMutation({
    mutationFn: async ({
      categoryId,
      subCategoryName,
    }: {
      categoryId: string;
      subCategoryName: string;
    }) => {
      const rows = categoriesQuery.data ?? [];
      const parent = rows.find(row => row.id === categoryId);
      if (!parent) {
        throw new Error('Parent category was not found');
      }

      await apiClient.createCategory({
        name: subCategoryName,
        type: parent.type,
        icon: parent.icon,
        parent_id: parent.id,
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CATEGORIES_QUERY_KEY });
    },
  });

  const deleteSubCategoryMutation = useMutation({
    mutationFn: async ({
      categoryId,
      subCategoryName,
    }: {
      categoryId: string;
      subCategoryName: string;
    }) => {
      const rows = categoriesQuery.data ?? [];
      const child = rows.find(
        row => row.parent_id === categoryId && row.name === subCategoryName && !row.is_system,
      );

      if (!child) {
        throw new Error('Sub-category was not found');
      }

      await apiClient.deleteCategory(child.id);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CATEGORIES_QUERY_KEY });
      await queryClient.invalidateQueries({ queryKey: ['transactions'] });
    },
  });

  const mapped = useMemo(() => mapCategoryRows(categoriesQuery.data ?? []), [categoriesQuery.data]);

  const firstError =
    categoriesQuery.error ??
    addCategoryMutation.error ??
    renameCategoryMutation.error ??
    deleteCategoryMutation.error ??
    restoreCategoryMutation.error ??
    addSubCategoryMutation.error ??
    deleteSubCategoryMutation.error;

  return {
    categories: mapped.categories,
    hiddenCategories: mapped.hidden,
    addCategory: async category => {
      await addCategoryMutation.mutateAsync(category);
    },
    renameCategory: async (id, name) => {
      await renameCategoryMutation.mutateAsync({ categoryId: id, name });
    },
    deleteCategory: async id => deleteCategoryMutation.mutateAsync(id),
    restoreCategory: async id => {
      await restoreCategoryMutation.mutateAsync(id);
    },
    addSubCategory: async (categoryId, subCategoryName) => {
      await addSubCategoryMutation.mutateAsync({ categoryId, subCategoryName });
    },
    deleteSubCategory: async (categoryId, subCategoryName) => {
      await deleteSubCategoryMutation.mutateAsync({ categoryId, subCategoryName });
    },
    isLoading:
      categoriesQuery.isLoading ||
      addCategoryMutation.isPending ||
      renameCategoryMutation.isPending ||
      deleteCategoryMutation.isPending ||
      restoreCategoryMutation.isPending ||
      addSubCategoryMutation.isPending ||
      deleteSubCategoryMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
  };
}

export { type Category };

export function useCategories(): UseCategoriesResult {
  return useRemoteCategories();
}
