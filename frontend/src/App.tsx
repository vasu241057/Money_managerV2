import { useState, useEffect } from 'react';
import { Layout } from './components/Layout';
import { Dashboard } from './components/Dashboard';
import { TransactionList } from './components/TransactionList';
import { TransactionForm } from './components/TransactionForm';
import { CategoryManager } from './components/CategoryManager';
import { AccountManager } from './components/AccountManager';
import { AnalyticsModal } from './components/AnalyticsModal';
import { SplashScreen } from './components/SplashScreen';
import { useTransactions, type Transaction } from './hooks/useTransactions';
import { Plus } from 'lucide-react';
import './styles/app.css';

function MoneyManagerApp() {
  const {
    transactions,
    addTransaction,
    deleteTransaction,
    isLoading: transactionsLoading,
    error: transactionsError,
  } = useTransactions();
  const [isFormOpen, setIsFormOpen] = useState(false);
  const [isCategoryManagerOpen, setIsCategoryManagerOpen] = useState(false);
  const [isAccountManagerOpen, setIsAccountManagerOpen] = useState(false);
  const [isAnalyticsOpen, setIsAnalyticsOpen] = useState(false);
  const [editingTransaction, setEditingTransaction] = useState<Transaction | null>(null);
  const [viewMode, setViewMode] = useState<'daily' | 'monthly'>('daily');

  return (
    <Layout>
      <div className="header">
        <h2>My Wallet</h2>
        <div className="header-actions">
          <button className="settings-btn" onClick={() => setIsAccountManagerOpen(true)}>
            Accounts
          </button>
          <button className="settings-btn" onClick={() => setIsCategoryManagerOpen(true)}>
            Categories
          </button>
        </div>
      </div>
      
      <Dashboard 
        transactions={transactions} 
        onBalanceClick={() => setIsAnalyticsOpen(true)}
      />

      {transactionsError && <p style={{ color: '#dc2626', margin: '8px 0' }}>{transactionsError}</p>}
      {transactionsLoading && transactions.length === 0 && (
        <p style={{ margin: '8px 0' }}>Loading transactions...</p>
      )}
      
      <div className="section-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h3>Transactions</h3>
        <div className="view-toggle">
          <button 
            className={`toggle-btn ${viewMode === 'daily' ? 'active' : ''}`}
            onClick={() => setViewMode('daily')}
          >
            Daily
          </button>
          <button 
            className={`toggle-btn ${viewMode === 'monthly' ? 'active' : ''}`}
            onClick={() => setViewMode('monthly')}
          >
            Monthly
          </button>
        </div>
      </div>
      
      <TransactionList 
        transactions={transactions} 
        onDelete={async (id) => {
          await deleteTransaction(id);
        }} 
        onEdit={(t) => {
          setEditingTransaction(t);
          setIsFormOpen(true);
        }}
        viewMode={viewMode}
      />

      <button className="fab" onClick={() => {
        setEditingTransaction(null);
        setIsFormOpen(true);
      }}>
        <Plus size={24} />
      </button>

      {isFormOpen && (
        <TransactionForm 
          key={editingTransaction?.id ?? 'new'}
          initialData={editingTransaction}
          onSubmit={addTransaction} 
          onClose={() => {
            setIsFormOpen(false);
            setEditingTransaction(null);
          }} 
        />
      )}

      {isCategoryManagerOpen && (
        <CategoryManager onClose={() => setIsCategoryManagerOpen(false)} />
      )}

      {isAccountManagerOpen && (
        <AccountManager onClose={() => setIsAccountManagerOpen(false)} />
      )}

      {isAnalyticsOpen && (
        <AnalyticsModal 
          transactions={transactions} 
          onClose={() => setIsAnalyticsOpen(false)} 
        />
      )}
    </Layout>
  );
}

export default function App() {
  const [showSplash, setShowSplash] = useState(true);
  const [isAppReady, setIsAppReady] = useState(false);

  useEffect(() => {
    const initApp = async () => {
      setIsAppReady(true);
    };

    initApp();
  }, []);

  return (
    <>
      {showSplash && (
        <SplashScreen 
          isAppReady={isAppReady}
          minDuration={1000} // Configurable duration
          onFinish={() => setShowSplash(false)} 
        />
      )}
      <MoneyManagerApp />
    </>
  );
}
