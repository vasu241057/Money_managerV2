import { useLocalStorage } from './useLocalStorage';

export interface Category {
  id: string;
  name: string;
  icon: string;
  type: 'income' | 'expense';
  subCategories: string[];
}

const DEFAULT_CATEGORIES: Category[] = [
  { id: '1', name: 'Food', icon: 'Utensils', type: 'expense', subCategories: ['Groceries', 'Restaurants', 'Snacks'] },
  { id: '2', name: 'Transport', icon: 'Car', type: 'expense', subCategories: ['Fuel', 'Public Transport', 'Taxi'] },
  { id: '3', name: 'Shopping', icon: 'ShoppingBag', type: 'expense', subCategories: ['Clothes', 'Electronics', 'Home'] },
  { id: '4', name: 'Entertainment', icon: 'Film', type: 'expense', subCategories: ['Movies', 'Games', 'Events'] },
  { id: '5', name: 'Bills', icon: 'Receipt', type: 'expense', subCategories: ['Electricity', 'Water', 'Internet'] },
  { id: '6', name: 'Health', icon: 'HeartPulse', type: 'expense', subCategories: ['Doctor', 'Medicine', 'Fitness'] },
  { id: '7', name: 'Salary', icon: 'Briefcase', type: 'income', subCategories: ['Full-time', 'Freelance'] },
  { id: '8', name: 'Investment', icon: 'TrendingUp', type: 'income', subCategories: ['Stocks', 'Crypto', 'Real Estate'] },
];

export function useCategories() {
  const [categories, setCategories] = useLocalStorage<Category[]>('categories', DEFAULT_CATEGORIES);

  const addCategory = (category: Omit<Category, 'id'>) => {
    const newCategory = { ...category, id: crypto.randomUUID() };
    setCategories([...categories, newCategory]);
  };

  const deleteCategory = (id: string) => {
    setCategories(categories.filter(c => c.id !== id));
  };

  const addSubCategory = (categoryId: string, subCategoryName: string) => {
    setCategories(categories.map(c => {
      if (c.id === categoryId) {
        return { ...c, subCategories: [...(c.subCategories || []), subCategoryName] };
      }
      return c;
    }));
  };

  const deleteSubCategory = (categoryId: string, subCategoryName: string) => {
    setCategories(categories.map(c => {
      if (c.id === categoryId) {
        return { ...c, subCategories: (c.subCategories || []).filter(s => s !== subCategoryName) };
      }
      return c;
    }));
  };

  return {
    categories,
    addCategory,
    deleteCategory,
    addSubCategory,
    deleteSubCategory,
    DEFAULT_CATEGORIES
  };
}
