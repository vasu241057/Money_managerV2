import React, { useState } from 'react';
import { useAccounts, type Account } from '../hooks/useAccounts';
import { Button } from './ui/Button';
import { Input } from './ui/Input';
import { X, Trash2, Plus, Pencil, Wallet, CreditCard, Banknote, Building } from 'lucide-react';
import '../styles/category-manager.css';

interface AccountManagerProps {
  onClose: () => void;
}

export function AccountManager({ onClose }: AccountManagerProps) {
  const { accounts, addAccount, updateAccount, deleteAccount, isLoading, error } = useAccounts();
  const [editingAccount, setEditingAccount] = useState<Account | null>(null);
  const [newAccountName, setNewAccountName] = useState('');
  const [type, setType] = useState<Account['type']>('cash');
  const [last4, setLast4] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newAccountName.trim()) return;

    try {
      if (editingAccount) {
        await updateAccount(editingAccount.id, {
          name: newAccountName.trim(),
          type,
          instrument_last4: type === 'card' ? last4.replace(/\D/g, '').slice(-4) || null : null,
        });
      } else {
        await addAccount({
          name: newAccountName.trim(),
          type,
          instrument_last4: type === 'card' ? last4.replace(/\D/g, '').slice(-4) || null : null,
        });
      }
      setNewAccountName('');
      setLast4('');
      setEditingAccount(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to save account';
      alert(message);
    }
  };

  const startEdit = (acc: Account) => {
    setEditingAccount(acc);
    setNewAccountName(acc.name);
    setType(acc.type);
    setLast4(acc.instrument_last4 || '');
  };

  const cancelEdit = () => {
    setEditingAccount(null);
    setNewAccountName('');
    setLast4('');
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

        {error && <p style={{ color: '#dc2626', marginBottom: 8 }}>{error}</p>}

        <div className="category-list">
          {accounts.map(acc => (
            <div
              key={acc.id}
              className={`category-wrapper ${editingAccount?.id === acc.id ? 'category-wrapper--editing' : ''}`}
              style={{ cursor: 'pointer' }}
              onClick={() => startEdit(acc)}
            >
              <div className="category-item">
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  {getIcon(acc.type)}
                  <span>{acc.name}</span>
                </div>
                <div className="cat-actions">
                  {acc.type === 'card' && (
                    <span style={{ fontSize: 10, color: 'var(--text-secondary)', marginRight: 4 }}>
                      {acc.instrument_last4 ? `····${acc.instrument_last4}` : 'no last4'}
                    </span>
                  )}
                  <button
                    className="edit-cat-btn"
                    onClick={(e) => {
                      e.stopPropagation();
                      startEdit(acc);
                    }}
                  >
                    <Pencil size={16} />
                  </button>
                  <button 
                    className="delete-cat-btn"
                    onClick={async (e) => {
                      e.stopPropagation();
                      try {
                        await deleteAccount(acc.id);
                        if (editingAccount?.id === acc.id) cancelEdit();
                      } catch (deleteError) {
                        const message =
                          deleteError instanceof Error ? deleteError.message : 'Failed to delete account';
                        alert(message);
                      }
                    }}
                  >
                    <Trash2 size={18} />
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>

        <form onSubmit={handleSubmit} className="add-category-form">
          {editingAccount && (
            <p style={{ margin: 0, fontSize: 12, color: 'var(--accent-color)' }}>
              Editing: {editingAccount.name}
            </p>
          )}
          {type === 'card' && (
            <Input 
              placeholder="Last 4 digits (optional)"
              value={last4}
              maxLength={4}
              onChange={e => setLast4(e.target.value.replace(/\D/g, '').slice(0, 4))}
            />
          )}
          <Input 
            placeholder={editingAccount ? 'Account Name' : 'New Account Name'}
            value={newAccountName}
            onChange={e => setNewAccountName(e.target.value)}
          />
          <div style={{ display: 'flex', gap: 8 }}>
            <Button type="submit" variant="secondary" disabled={isLoading} style={{ flex: 1 }}>
              {editingAccount ? (
                <><Pencil size={16} style={{ marginRight: 6 }} /> Update</>
              ) : (
                <><Plus size={18} style={{ marginRight: 8 }} /> Add Account</>
              )}
            </Button>
            {editingAccount && (
              <Button type="button" variant="secondary" onClick={cancelEdit}>
                Cancel
              </Button>
            )}
          </div>
        </form>
      </div>
    </div>
  );
}
