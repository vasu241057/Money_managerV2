import { useState, useEffect, useRef } from 'react';
import { Layout } from './components/Layout';
import { Dashboard } from './components/Dashboard';
import { ReviewInbox } from './components/ReviewInbox';
import { TransactionList } from './components/TransactionList';
import { TransactionForm } from './components/TransactionForm';
import { CategoryManager } from './components/CategoryManager';
import { AccountManager } from './components/AccountManager';
import { AnalyticsModal } from './components/AnalyticsModal';
import { SplashScreen } from './components/SplashScreen';
import { useTransactions, type Transaction } from './hooks/useTransactions';
import { useGmailConnection, useGmailConnectionForOAuthCallback } from './hooks/useGmailConnection';
import {
  parseGoogleOAuthCallbackParams,
  resolveGoogleOAuthCallbackMessage,
} from './lib/gmail-oauth';
import { Plus } from 'lucide-react';
import './styles/app.css';

function MoneyManagerApp() {
  const {
    transactions,
    reviewInbox,
    addTransaction,
    reviewTransaction,
    deleteTransaction,
    isLoading: transactionsLoading,
    error: transactionsError,
  } = useTransactions();
  const {
    connection: gmailConnection,
    connectGmail,
    disconnectGmail,
    isLoading: gmailLoading,
    error: gmailError,
  } = useGmailConnection();
  const [isFormOpen, setIsFormOpen] = useState(false);
  const [isCategoryManagerOpen, setIsCategoryManagerOpen] = useState(false);
  const [isAccountManagerOpen, setIsAccountManagerOpen] = useState(false);
  const [isAnalyticsOpen, setIsAnalyticsOpen] = useState(false);
  const [editingTransaction, setEditingTransaction] = useState<Transaction | null>(null);
  const [viewMode, setViewMode] = useState<'daily' | 'monthly'>('daily');
  const gmailStatusLabel = gmailConnection?.sync_status ?? 'DISCONNECTED';
  const gmailStatusClass = gmailConnection?.sync_status.toLowerCase() ?? 'disconnected';

  return (
    <Layout>
      <div className="header">
        <h2>My Wallet</h2>
        <div className="header-actions">
          <span
            className={`gmail-status-badge ${gmailStatusClass}`}
          >
            Gmail: {gmailStatusLabel}
          </span>
          {gmailConnection ? (
            <button
              className="settings-btn"
              onClick={() => {
                void disconnectGmail();
              }}
              disabled={gmailLoading}
            >
              Disconnect Gmail
            </button>
          ) : (
            <button
              className="settings-btn"
              onClick={() => {
                void connectGmail();
              }}
              disabled={gmailLoading}
            >
              Connect Gmail
            </button>
          )}
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

      <ReviewInbox
        items={reviewInbox}
        onReview={reviewTransaction}
      />

      {transactionsError && <p style={{ color: '#dc2626', margin: '8px 0' }}>{transactionsError}</p>}
      {gmailError && <p style={{ color: '#dc2626', margin: '8px 0' }}>{gmailError}</p>}
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

function GoogleOAuthCallbackScreen() {
  const {
    isAvailable: gmailAvailable,
    completeOAuthCallback,
    error: callbackError,
    isLoading,
  } = useGmailConnectionForOAuthCallback();
  const hasAttemptedRef = useRef(false);
  const [callbackCompleted, setCallbackCompleted] = useState(false);

  const callbackParams = parseGoogleOAuthCallbackParams(window.location.search);
  const { code, state, oauthError, hasCallbackParams } = callbackParams;

  useEffect(() => {
    if (hasAttemptedRef.current) {
      return;
    }
    if (!hasCallbackParams) {
      return;
    }
    hasAttemptedRef.current = true;

    const oauthCode = code as string;
    const oauthState = state as string;

    void completeOAuthCallback(oauthCode, oauthState)
      .then(() => {
        setCallbackCompleted(true);
      })
      .catch(() => undefined);
  }, [completeOAuthCallback, hasCallbackParams, code, state]);

  const message = resolveGoogleOAuthCallbackMessage({
    gmailAvailable,
    oauthError,
    code,
    state,
    callbackError,
    isLoading,
    callbackCompleted,
  });

  const buttonLabel = isLoading ? 'Back to App' : 'Continue';

  return (
    <Layout>
      <div style={{ paddingTop: 24 }}>
        <h2>Gmail Connection</h2>
        <p style={{ marginTop: 8 }}>{message}</p>
        <button
          className="settings-btn"
          style={{ marginTop: 12 }}
          onClick={() => {
            window.location.assign('/');
          }}
        >
          {buttonLabel}
        </button>
      </div>
    </Layout>
  );
}

export default function App() {
  const isOAuthCallbackRoute =
    typeof window !== 'undefined' && window.location.pathname === '/oauth/google/callback';
  const [showSplash, setShowSplash] = useState(true);
  const [isAppReady, setIsAppReady] = useState(false);

  useEffect(() => {
    const initApp = async () => {
      setIsAppReady(true);
    };

    initApp();
  }, []);

  if (isOAuthCallbackRoute) {
    return <GoogleOAuthCallbackScreen />;
  }

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
