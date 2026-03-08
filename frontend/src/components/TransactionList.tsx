import { useState } from 'react';
import { Trash2, ChevronDown, ChevronRight } from 'lucide-react';
import '../styles/transaction-list.css';
import type { Transaction } from '../hooks/useTransactions';

interface TransactionListProps {
  transactions: Transaction[];
  onDelete: (id: string) => Promise<void>;
  onEdit: (transaction: Transaction) => void;
  viewMode?: 'daily' | 'monthly';
}

interface SwipeState {
  id: string;
  startX: number;
  currentX: number;
  isSwiping: boolean;
}

const pad2 = (value: number) => value.toString().padStart(2, '0');

const toLocalDateKey = (value: Date) => {
  return `${value.getFullYear()}-${pad2(value.getMonth() + 1)}-${pad2(value.getDate())}`;
};

const fromLocalDateKey = (key: string) => {
  const [y, m, d] = key.split('-').map(Number);
  return new Date(y, m - 1, d);
};

export function TransactionList({ transactions, onDelete, onEdit, viewMode = 'daily' }: TransactionListProps) {
  const [expandedMonths, setExpandedMonths] = useState<Set<string>>(new Set());
  const [swipeState, setSwipeState] = useState<SwipeState | null>(null);
  const [openId, setOpenId] = useState<string | null>(null);
  
  // Constants for swipe behavior
  const REVEAL_WIDTH = 80;
  const AUTO_DELETE_THRESHOLD = 200;

  const toggleMonth = (key: string) => {
    const newExpanded = new Set(expandedMonths);
    if (newExpanded.has(key)) {
      newExpanded.delete(key);
    } else {
      newExpanded.add(key);
    }
    setExpandedMonths(newExpanded);
  };

  // Swipe handlers
  const handleTouchStart = (e: React.TouchEvent, id: string) => {
    // If another item is open, close it and don't start swipe on this one immediately
    if (openId && openId !== id) {
      setOpenId(null);
      return;
    }

    setSwipeState({
      id,
      startX: e.touches[0].clientX,
      currentX: openId === id ? REVEAL_WIDTH : 0, // Start from revealed position if already open
      isSwiping: true,
    });
  };

  const handleTouchMove = (e: React.TouchEvent) => {
    if (!swipeState || !swipeState.isSwiping) return;
    
    const currentX = e.touches[0].clientX;
    const diff = swipeState.startX - currentX;
    
    // Calculate new position based on initial state (open or closed)
    let newX = diff;
    if (openId === swipeState.id) {
      newX += REVEAL_WIDTH;
    }

    // Only allow right-to-left swipe (positive values)
    if (newX > 0) {
      setSwipeState({ ...swipeState, currentX: newX });
    }
  };

  const handleTouchEnd = () => {
    if (!swipeState) return;
    
    const { currentX, id } = swipeState;
    
    if (currentX > AUTO_DELETE_THRESHOLD) {
      // Long swipe - Auto delete
      void onDelete(id);
      setOpenId(null);
    } else if (currentX > REVEAL_WIDTH / 2) {
      // Partial swipe - Reveal delete button
      setOpenId(id);
    } else {
      // Swipe cancelled - Close
      setOpenId(null);
    }
    
    setSwipeState(null);
  };

  // Group transactions
  const grouped = transactions.reduce((acc, transaction) => {
    const date = new Date(transaction.date);
    let key: string;
    
    if (viewMode === 'monthly') {
      key = date.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
    } else {
      key = toLocalDateKey(date);
    }

    if (!acc[key]) acc[key] = [];
    acc[key].push(transaction);
    return acc;
  }, {} as Record<string, Transaction[]>);

  const sortedKeys = Object.keys(grouped).sort((a, b) =>
    viewMode === 'monthly'
      ? new Date(b).getTime() - new Date(a).getTime()
      : fromLocalDateKey(b).getTime() - fromLocalDateKey(a).getTime(),
  );

  const formatHeader = (key: string, isMonthHeader = false) => {
    if (isMonthHeader) return key;
    
    const localDate = fromLocalDateKey(key);

    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    if (localDate.toDateString() === today.toDateString()) return 'Today';
    if (localDate.toDateString() === yesterday.toDateString()) return 'Yesterday';
    
    return localDate.toLocaleDateString('en-US', { weekday: 'short', day: 'numeric', month: 'short' });
  };

  const getGroupTotal = (groupTransactions: Transaction[]) => {
    return groupTransactions.reduce((acc, t) => {
      if (t.type === 'expense') {
        return acc - t.amount;
      }

      if (t.type === 'income') {
        return acc + t.amount;
      }

      return acc;
    }, 0);
  };

  // Group by day within a month
  const groupByDay = (monthTransactions: Transaction[]) => {
    const dayGroups = monthTransactions.reduce((acc, transaction) => {
      const date = new Date(transaction.date);
      const key = toLocalDateKey(date);
      
      if (!acc[key]) acc[key] = [];
      acc[key].push(transaction);
      return acc;
    }, {} as Record<string, Transaction[]>);

    const sortedDayKeys = Object.keys(dayGroups).sort(
      (a, b) => fromLocalDateKey(b).getTime() - fromLocalDateKey(a).getTime(),
    );

    return { dayGroups, sortedDayKeys };
  };

  const renderTransaction = (t: Transaction) => {
    // Determine the current offset
    let offset = 0;
    if (swipeState?.id === t.id) {
      offset = swipeState.currentX;
    } else if (openId === t.id) {
      offset = REVEAL_WIDTH;
    }
    
    const amountPrefix = t.type === 'expense' ? '-' : t.type === 'income' ? '+' : '↔ ';

    return (
      <div 
        key={t.id} 
        className="transaction-item-wrapper"
        onTouchStart={(e) => handleTouchStart(e, t.id)}
        onTouchMove={handleTouchMove}
        onTouchEnd={handleTouchEnd}
      >
        <div 
          className="swipe-delete-action"
          onClick={(e) => {
            e.stopPropagation();
            void onDelete(t.id);
            setOpenId(null);
          }}
          style={{ width: Math.max(offset, REVEAL_WIDTH) }} // Expand background with swipe
        >
          <Trash2 size={20} />
          <span>Delete</span>
        </div>
        <div 
          className="transaction-item"
          style={{ transform: `translateX(-${offset}px)` }}
          onClick={() => {
            if (openId === t.id) {
              setOpenId(null); // Close if open
            } else if (!swipeState) {
              onEdit(t); // Edit if closed and not swiping
            }
          }}
        >
          <div className="t-icon">
            {t.category[0]}
          </div>
          <div className="t-details">
            <div className="t-main">
              <div className="t-cat-group">
                <span className="t-category">{t.category}</span>
                {t.subCategory && <span className="t-subcategory"> / {t.subCategory}</span>}
              </div>
              <span className={`t-amount ${t.type}`}>
                {amountPrefix}
                ₹{t.amount.toFixed(2)}
              </span>
            </div>
            <div className="t-sub">
              <span className="t-account">{t.accountName || t.accountId || 'Cash'}</span>
              {t.description && <span className="t-note"> • {t.description}</span>}
            </div>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="transaction-list">
      {sortedKeys.map(key => {
        const isExpanded = expandedMonths.has(key);
        const isMonthly = viewMode === 'monthly';
        const monthInitial = isMonthly ? formatHeader(key, true).charAt(0) : '';
        
        return (
          <div key={key} className="date-group">
            {isMonthly ? (
              <div 
                className="month-card"
                onClick={() => toggleMonth(key)}
              >
                <div className="chevron-icon">
                  {isExpanded ? <ChevronDown size={20} /> : <ChevronRight size={20} />}
                </div>
                <div className="month-icon">
                  {monthInitial}
                </div>
                <div className="month-details">
                  <div className="month-name">{formatHeader(key, true)}</div>
                  <div className="month-info">
                    {grouped[key].length} transaction{grouped[key].length !== 1 ? 's' : ''}
                  </div>
                </div>
                <span className={`month-total ${getGroupTotal(grouped[key]) < 0 ? 'expense' : 'income'}`}>
                  {getGroupTotal(grouped[key]) < 0 ? '-' : '+'}₹{Math.abs(getGroupTotal(grouped[key])).toFixed(2)}
                </span>
              </div>
            ) : (
              <div className="group-header">
                <h3 className="date-header">{formatHeader(key)}</h3>
                <span className={`day-total ${getGroupTotal(grouped[key]) < 0 ? 'expense' : 'income'}`}>
                  {getGroupTotal(grouped[key]) < 0 ? '-' : '+'}₹{Math.abs(getGroupTotal(grouped[key])).toFixed(2)}
                </span>
              </div>
            )}
            {!isMonthly && (
              <div className="transactions">
                {grouped[key].map(t => renderTransaction(t))}
              </div>
            )}
            {isMonthly && isExpanded && (() => {
              const { dayGroups, sortedDayKeys } = groupByDay(grouped[key]);
              return (
                <div className="month-expanded-content">
                  {sortedDayKeys.map(dayKey => (
                    <div key={dayKey} className="day-group">
                      <div className="group-header">
                        <h3 className="date-header">{formatHeader(dayKey)}</h3>
                        <span className={`day-total ${getGroupTotal(dayGroups[dayKey]) < 0 ? 'expense' : 'income'}`}>
                          {getGroupTotal(dayGroups[dayKey]) < 0 ? '-' : '+'}₹{Math.abs(getGroupTotal(dayGroups[dayKey])).toFixed(2)}
                        </span>
                      </div>
                      <div className="transactions">
                        {dayGroups[dayKey].map(t => renderTransaction(t))}
                      </div>
                    </div>
                  ))}
                </div>
              );
            })()}
          </div>
        );
      })}
      
      {transactions.length === 0 && (
        <div className="empty-state">
          <p>No transactions yet.</p>
        </div>
      )}
    </div>
  );
}
