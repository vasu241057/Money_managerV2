import { useMemo } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { CategoryRow } from '../../../shared/types';
import { isRemoteDataEnabled } from '../config/data-source';
import { apiClient, toErrorMessage } from '../lib/api-client';
import { useLocalCategories, type Category } from './local/useLocalCategories';

interface UseCategoriesResult {
  categories: Category[];
  addCategory: (category: Omit<Category, 'id'>) => Promise<void>;
  deleteCategory: (id: string) => Promise<void>;
  addSubCategory: (categoryId: string, subCategoryName: string) => Promise<void>;
  deleteSubCategory: (categoryId: string, subCategoryName: string) => Promise<void>;
  isLoading: boolean;
  error: string | null;
}

const REMOTE_DATA_ENABLED = isRemoteDataEnabled();
const CATEGORIES_QUERY_KEY = ['categories'] as const;

function mapCategoryRows(rows: CategoryRow[]): Category[] {
  const childrenByParent = new Map<string, CategoryRow[]>();

  rows.forEach(row => {
    if (!row.parent_id) {
      return;
    }

    const existing = childrenByParent.get(row.parent_id) ?? [];
    existing.push(row);
    childrenByParent.set(row.parent_id, existing);
  });

  return rows
    .filter(row => row.parent_id === null)
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
      };
    });
}

function useLocalCategoryFallback(): UseCategoriesResult {
  const local = useLocalCategories();

  return {
    categories: local.categories,
    addCategory: async category => {
      local.addCategory(category);
    },
    deleteCategory: async id => {
      local.deleteCategory(id);
    },
    addSubCategory: async (categoryId, subCategoryName) => {
      local.addSubCategory(categoryId, subCategoryName);
    },
    deleteSubCategory: async (categoryId, subCategoryName) => {
      local.deleteSubCategory(categoryId, subCategoryName);
    },
    isLoading: false,
    error: null,
  };
}

function useRemoteCategories(): UseCategoriesResult {
  const queryClient = useQueryClient();

  const categoriesQuery = useQuery({
    queryKey: CATEGORIES_QUERY_KEY,
    queryFn: () => apiClient.listCategories(),
  });

  const addCategoryMutation = useMutation({
    mutationFn: async (category: Omit<Category, 'id'>) => {
      const parent = await apiClient.createCategory({
        name: category.name,
        type: category.type,
        icon: category.icon,
        parent_id: null,
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
    mutationFn: async (categoryId: string) => {
      const rows = categoriesQuery.data ?? [];
      const children = rows.filter(row => row.parent_id === categoryId && !row.is_system);

      for (const child of children) {
        await apiClient.deleteCategory(child.id);
      }

      await apiClient.deleteCategory(categoryId);
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

  const categories = useMemo(() => mapCategoryRows(categoriesQuery.data ?? []), [categoriesQuery.data]);

  const firstError =
    categoriesQuery.error ??
    addCategoryMutation.error ??
    deleteCategoryMutation.error ??
    addSubCategoryMutation.error ??
    deleteSubCategoryMutation.error;

  return {
    categories,
    addCategory: async category => {
      await addCategoryMutation.mutateAsync(category);
    },
    deleteCategory: async id => {
      await deleteCategoryMutation.mutateAsync(id);
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
      deleteCategoryMutation.isPending ||
      addSubCategoryMutation.isPending ||
      deleteSubCategoryMutation.isPending,
    error: firstError ? toErrorMessage(firstError) : null,
  };
}

export { type Category };

const useCategoriesImpl: () => UseCategoriesResult = REMOTE_DATA_ENABLED
  ? useRemoteCategories
  : useLocalCategoryFallback;

export function useCategories(): UseCategoriesResult {
  return useCategoriesImpl();
}
