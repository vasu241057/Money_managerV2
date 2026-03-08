import React, { useState } from 'react';
import { useAccounts, type Account } from '../hooks/useAccounts';
import { Button } from './ui/Button';
import { Input } from './ui/Input';
import { X, Trash2, Plus, Wallet, CreditCard, Banknote, Building } from 'lucide-react';
import '../styles/category-manager.css';

interface AccountManagerProps {
  onClose: () => void;
}

export function AccountManager({ onClose }: AccountManagerProps) {
  const { accounts, addAccount, deleteAccount } = useAccounts();
  const [newAccountName, setNewAccountName] = useState('');
  const [type, setType] = useState<Account['type']>('cash');

  const handleAdd = (e: React.FormEvent) => {
    e.preventDefault();
    if (!newAccountName.trim()) return;

    addAccount({
      name: newAccountName.trim(),
      type,
    });
    setNewAccountName('');
  };

  const getIcon = (type: Account['type']) => {
    switch (type) {
      case 'cash': return <Banknote size={18} />;
      case 'bank': return <Building size={18} />;
      case 'card': return <CreditCard size={18} />;
      default: return <Wallet size={18} />;
    }
  };

  return (
    <div className="modal-overlay">
      <div className="modal-content category-manager-modal">
        <div className="modal-header">
          <h2>Manage Accounts</h2>
          <button onClick={onClose} className="close-btn"><X size={24} /></button>
        </div>

        <div className="type-toggle">
          {(['cash', 'bank', 'card', 'other'] as const).map((t) => (
            <button 
              key={t}
              className={`type-btn ${type === t ? 'active' : ''}`}
              onClick={() => setType(t)}
              style={{ textTransform: 'capitalize' }}
            >
              {t}
            </button>
          ))}
        </div>

        <div className="category-list">
          {accounts.map(acc => (
            <div key={acc.id} className="category-wrapper">
              <div className="category-item">
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  {getIcon(acc.type)}
                  <span>{acc.name}</span>
                </div>
                <div className="cat-actions">
                  <button 
                    className="delete-cat-btn"
                    onClick={(e) => {
                      e.stopPropagation();
                      deleteAccount(acc.id);
                    }}
                  >
                    <Trash2 size={18} />
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>

        <form onSubmit={handleAdd} className="add-category-form">
          <Input 
            placeholder="New Account Name"
            value={newAccountName}
            onChange={e => setNewAccountName(e.target.value)}
          />
          <Button type="submit" variant="secondary">
            <Plus size={18} style={{ marginRight: 8 }} /> Add Account
          </Button>
        </form>
      </div>
    </div>
  );
}
