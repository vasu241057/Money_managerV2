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
  const { transactions, addTransaction, deleteTransaction } = useTransactions();
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
        onDelete={deleteTransaction} 
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
    // Simulate app initialization/loading
    // In a real app, this might wait for data fetching, auth check, etc.
    // Since our hooks are synchronous (localStorage), we're technically ready immediately.
    // But we'll use a small timeout to ensure the render cycle is complete or simulate a check.
    const initApp = async () => {
      // Simulate some async work if needed, or just set ready
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
