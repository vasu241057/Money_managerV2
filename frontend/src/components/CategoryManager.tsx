import React, { useState } from 'react';
import { useCategories } from '../hooks/useCategories';
import { Button } from './ui/Button';
import { Input } from './ui/Input';
import { X, Trash2, Plus } from 'lucide-react';
import '../styles/category-manager.css';

interface CategoryManagerProps {
  onClose: () => void;
}

export function CategoryManager({ onClose }: CategoryManagerProps) {
  const {
    categories,
    addCategory,
    deleteCategory,
    addSubCategory,
    deleteSubCategory,
    isLoading,
    error,
  } = useCategories();
  const [newCategoryName, setNewCategoryName] = useState('');
  const [type, setType] = useState<'income' | 'expense'>('expense');
  const [expandedCategory, setExpandedCategory] = useState<string | null>(null);
  const [newSubCategory, setNewSubCategory] = useState('');

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newCategoryName.trim()) return;

    try {
      await addCategory({
        name: newCategoryName.trim(),
        type,
        icon: 'Circle',
        subCategories: [],
      });
      setNewCategoryName('');
    } catch (addError) {
      const message =
        addError instanceof Error ? addError.message : 'Failed to create category';
      alert(message);
    }
  };

  const handleAddSub = async (e: React.FormEvent, categoryId: string) => {
    e.preventDefault();
    if (!newSubCategory.trim()) return;

    try {
      await addSubCategory(categoryId, newSubCategory.trim());
      setNewSubCategory('');
    } catch (addSubError) {
      const message =
        addSubError instanceof Error ? addSubError.message : 'Failed to create sub-category';
      alert(message);
    }
  };

  const filteredCategories = categories.filter(
    c => c.type === type && !c.isSystem,
  );

  return (
    <div className="modal-overlay">
      <div className="modal-content category-manager-modal">
        <div className="modal-header">
          <h2>Manage Categories</h2>
          <button onClick={onClose} className="close-btn"><X size={24} /></button>
        </div>

        <div className="type-toggle">
          <button 
            className={`type-btn ${type === 'expense' ? 'active expense' : ''}`}
            onClick={() => setType('expense')}
          >
            Expense
          </button>
          <button 
            className={`type-btn ${type === 'income' ? 'active income' : ''}`}
            onClick={() => setType('income')}
          >
            Income
          </button>
        </div>

        {error && <p style={{ color: '#dc2626', marginBottom: 8 }}>{error}</p>}

        <div className="category-list">
          {filteredCategories.map(cat => (
            <div key={cat.id} className="category-wrapper">
              <div className="category-item" onClick={() => setExpandedCategory(expandedCategory === cat.id ? null : cat.id)}>
                <span>{cat.name}</span>
                <div className="cat-actions">
                  <span className="sub-count">{(cat.subCategories || []).length} sub</span>
                  <button 
                    className="delete-cat-btn"
                    onClick={async (e) => {
                      e.stopPropagation();
                      try {
                        await deleteCategory(cat.id);
                      } catch (deleteError) {
                        const message =
                          deleteError instanceof Error ? deleteError.message : 'Failed to delete category';
                        alert(message);
                      }
                    }}
                  >
                    <Trash2 size={18} />
                  </button>
                </div>
              </div>
              
              {expandedCategory === cat.id && (
                <div className="sub-category-list">
                  {(cat.subCategories || []).length > 0 ? (
                    (cat.subCategories || []).map((sub, idx) => (
                      <div key={idx} className="sub-item">
                        <span>• {sub}</span>
                        <button 
                          className="delete-sub-btn"
                          onClick={async () => {
                            try {
                              await deleteSubCategory(cat.id, sub);
                            } catch (deleteSubError) {
                              const message =
                                deleteSubError instanceof Error
                                  ? deleteSubError.message
                                  : 'Failed to delete sub-category';
                              alert(message);
                            }
                          }}
                        >
                          <X size={14} />
                        </button>
                      </div>
                    ))
                  ) : (
                    <div className="no-sub-msg">No sub-categories</div>
                  )}
                  <form onSubmit={(e) => handleAddSub(e, cat.id)} className="add-sub-form">
                    <input 
                      className="sub-input"
                      placeholder="Add sub-category"
                      value={newSubCategory}
                      onChange={e => setNewSubCategory(e.target.value)}
                      onClick={e => e.stopPropagation()}
                    />
                    <button type="submit" className="add-sub-btn"><Plus size={14} /></button>
                  </form>
                </div>
              )}
            </div>
          ))}
        </div>

        <form onSubmit={handleAdd} className="add-category-form">
          <Input 
            placeholder="New Category Name"
            value={newCategoryName}
            onChange={e => setNewCategoryName(e.target.value)}
          />
          <Button type="submit" variant="secondary" disabled={isLoading}>
            <Plus size={18} style={{ marginRight: 8 }} /> Add Category
          </Button>
        </form>
      </div>
    </div>
  );
}
