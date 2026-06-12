import { ClerkLoaded, ClerkLoading, Show, SignInButton, SignUpButton } from '@clerk/react';
import { Plus } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { AppUserButton } from './components/AppUserButton';
import { ClerkTokenBridge } from './components/ClerkTokenBridge';
import { AccountManager } from './components/AccountManager';
import { AnalyticsModal } from './components/AnalyticsModal';
import { CategoryManager } from './components/CategoryManager';
import { Dashboard } from './components/Dashboard';
import { Layout } from './components/Layout';
import { ReviewInbox } from './components/ReviewInbox';
import { SplashScreen } from './components/SplashScreen';
import { TransactionForm } from './components/TransactionForm';
import { TransactionList } from './components/TransactionList';
import { useGmailConnection, useGmailConnectionForOAuthCallback } from './hooks/useGmailConnection';
import { useTransactions, type Transaction } from './hooks/useTransactions';
import {
  parseGoogleOAuthCallbackParams,
} from './lib/gmail-oauth';
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
    processingStats: gmailProcessingStats,
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
  const requiresReconnect =
    gmailConnection?.sync_status === 'AUTH_REVOKED' ||
    gmailConnection?.sync_status === 'ERROR_PAUSED';
  const showDisconnectButton = Boolean(gmailConnection && !requiresReconnect);
  const connectButtonLabel =
    gmailConnection && requiresReconnect ? 'Reconnect Gmail' : 'Connect Gmail';
  const connectedInboxLabel = gmailConnection?.email_address ?? 'Not connected';
  const hasGmailActivity = gmailProcessingStats.raw_email_total_count > 0;
  const latestProcessedAt =
    gmailProcessingStats.latest_email_internal_date ?? gmailProcessingStats.latest_email_created_at;

  return (
    <Layout>
      <div className="header">
        <h2>My Wallet</h2>
        <div className="header-actions">
          <div className="user-button-slot">
            <AppUserButton
              gmailConnected={showDisconnectButton}
              requiresReconnect={requiresReconnect}
              statusLabel={gmailStatusLabel}
              connectLabel={connectButtonLabel}
              onConnectGmail={() => { void connectGmail(); }}
              onDisconnectGmail={() => { void disconnectGmail(); }}
              onOpenAccounts={() => setIsAccountManagerOpen(true)}
              onOpenCategories={() => setIsCategoryManagerOpen(true)}
            />
          </div>
        </div>
      </div>
      <div className={`gmail-processing-card ${hasGmailActivity ? 'gmail-processing-card--active' : ''}`}>
        <p className="gmail-processing-title">Gmail Sync Monitor</p>
        <p className="gmail-processing-line">
          Connected inbox: <strong>{connectedInboxLabel}</strong>
        </p>
        <p className="gmail-processing-line">
          Emails seen: <strong>{gmailProcessingStats.raw_email_total_count}</strong> | Processed:{' '}
          <strong>{gmailProcessingStats.raw_email_processed_count}</strong> | Pending:{' '}
          <strong>{gmailProcessingStats.raw_email_pending_count}</strong> | Failed:{' '}
          <strong>{gmailProcessingStats.raw_email_failed_count}</strong>
        </p>
        {latestProcessedAt && (
          <p className="gmail-processing-line">
            Latest email time: {new Date(latestProcessedAt).toLocaleString()}
          </p>
        )}
        {!hasGmailActivity && <p className="gmail-processing-line">No Gmail emails ingested yet.</p>}
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

function MascotEnvelope() {
  return (
    <svg viewBox="0 0 100 100" fill="none" width="80" height="80">
      <circle cx="50" cy="50" r="48" fill="#1e293b" stroke="#334155" strokeWidth="1.5" />
      <rect x="26" y="32" width="48" height="34" rx="3" fill="#0f172a" stroke="#475569" strokeWidth="1.5" />
      <path d="M26 32l24 18 24-18" stroke="#475569" strokeWidth="1.5" fill="#1e293b" />
      <path d="M26 36l24 14 24-14" stroke="#64748b" strokeWidth="1.5" strokeDasharray="2 2" fill="none" />
      <circle cx="58" cy="42" r="3" fill="var(--accent-color)" opacity="0.8" />
      <circle cx="64" cy="42" r="3" fill="var(--accent-color)" opacity="0.5" />
      <circle cx="70" cy="42" r="3" fill="var(--accent-color)" opacity="0.3" />
      <circle cx="74" cy="32" r="8" fill="#0f172a" stroke="#475569" strokeWidth="1" />
      <path d="M72 30l2 2 3-3" stroke="#22c55e" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      <rect x="30" y="48" width="30" height="3" rx="1" fill="#334155" />
      <rect x="30" y="54" width="22" height="3" rx="1" fill="#334155" />
    </svg>
  );
}

function GoogleOAuthCallbackScreen({ onDone }: { onDone: () => void }) {
  const {
    completeOAuthCallback,
  } = useGmailConnectionForOAuthCallback();
  const hasAttemptedRef = useRef(false);
  const mountedRef = useRef(true);

  const callbackParams = useRef(parseGoogleOAuthCallbackParams(window.location.search)).current;
  const { code, state, oauthError, hasCallbackParams } = callbackParams;

  const [status, setStatus] = useState<'loading' | 'success' | 'error'>('loading');
  const [errorMsg, setErrorMsg] = useState('');
  const doneTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; };
  }, []);

  useEffect(() => {
    if (hasAttemptedRef.current) return;
    hasAttemptedRef.current = true;

    window.history.replaceState(null, '', '/');

    if (!hasCallbackParams) {
      if (mountedRef.current) {
        setErrorMsg('Missing connection parameters. Please try again.');
        setStatus('error');
      }
      return;
    }

    if (oauthError) {
      if (mountedRef.current) {
        setErrorMsg(`Google returned an error: ${oauthError}`);
        setStatus('error');
      }
      return;
    }

    void completeOAuthCallback(code as string, state as string)
      .then(() => {
        if (!mountedRef.current) return;
        setStatus('success');
        doneTimerRef.current = setTimeout(onDone, 1800);
      })
      .catch((err: unknown) => {
        if (!mountedRef.current) return;
        const msg = err instanceof Error ? err.message : 'Failed to connect Gmail. Please try again.';
        setErrorMsg(msg);
        setStatus('error');
      });
  }, []);

  useEffect(() => {
    return () => {
      if (doneTimerRef.current) clearTimeout(doneTimerRef.current);
    };
  }, []);

  const handleContinue = () => {
    window.history.replaceState(null, '', '/');
    onDone();
  };

  return (
    <div className="oauth-callback-wrapper">
      <div className="oauth-callback-card">
        {status === 'loading' && (
          <>
            <div className="oauth-callback-loader-mascot">
              <MascotEnvelope />
            </div>
            <div className="oauth-callback-loader">
              <span className="oauth-callback-loader-text">connecting</span>
              <span className="oauth-callback-loader-bar" />
            </div>
            <p className="oauth-callback-subtext">Linking your Gmail inbox...</p>
          </>
        )}
        {status === 'success' && (
          <>
            <div className="oauth-callback-checkmark">
              <svg viewBox="0 0 24 24" fill="none" width="48" height="48">
                <circle cx="12" cy="12" r="11" stroke="#22c55e" strokeWidth="1.5" fill="none" />
                <path d="M7 12l3 3 7-7" stroke="#22c55e" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </div>
            <h2 className="oauth-callback-title">Gmail Connected</h2>
            <p className="oauth-callback-subtext">Your inbox is linked. Taking you home...</p>
          </>
        )}
        {status === 'error' && (
          <>
            <div className="oauth-callback-error-icon">
              <svg viewBox="0 0 24 24" fill="none" width="48" height="48">
                <circle cx="12" cy="12" r="11" stroke="#ef4444" strokeWidth="1.5" fill="none" />
                <path d="M12 8v4M12 16h.01" stroke="#ef4444" strokeWidth="2" strokeLinecap="round" />
              </svg>
            </div>
            <h2 className="oauth-callback-title">Connection Failed</h2>
            <p className="oauth-callback-subtext">{errorMsg}</p>
            <button className="oauth-callback-btn" onClick={handleContinue}>
              Back to App
            </button>
          </>
        )}
      </div>
    </div>
  );
}

function AuthScreen() {
  return (
    <div className="auth-wrapper">
      <div className="auth-card">
        <h2>Sign in to Money Manager</h2>
        <p className="auth-subtitle">Use mobile number or Google account to continue.</p>
        <div className="auth-actions">
          <SignInButton mode="modal">
            <button className="settings-btn auth-action-btn">Sign in</button>
          </SignInButton>
          <SignUpButton mode="modal">
            <button className="settings-btn auth-action-btn">Sign up</button>
          </SignUpButton>
        </div>
      </div>
    </div>
  );
}

function AuthenticatedApp() {
  const [isOAuthCallbackScreen, setIsOAuthCallbackScreen] = useState(
    typeof window !== 'undefined' && window.location.pathname === '/oauth/google/callback'
  );
  const [showSplash, setShowSplash] = useState(!isOAuthCallbackScreen);
  const [isAppReady, setIsAppReady] = useState(false);

  useEffect(() => {
    const initApp = async () => {
      setIsAppReady(true);
    };

    void initApp();
  }, []);

  if (isOAuthCallbackScreen) {
    return <GoogleOAuthCallbackScreen onDone={() => setIsOAuthCallbackScreen(false)} />;
  }

  return (
    <>
      {showSplash && (
        <SplashScreen
          isAppReady={isAppReady}
          minDuration={1000}
          onFinish={() => setShowSplash(false)}
        />
      )}
      <MoneyManagerApp />
    </>
  );
}

export default function App() {
  const isOAuthCallback =
    typeof window !== 'undefined' && window.location.pathname === '/oauth/google/callback';

  return (
    <>
      <ClerkLoading>
        {isOAuthCallback ? (
          <div className="oauth-callback-wrapper">
            <div className="oauth-callback-card">
              <div className="oauth-callback-loader" />
            </div>
          </div>
        ) : (
          <Layout>
            <p style={{ marginTop: 24 }}>Loading authentication...</p>
          </Layout>
        )}
      </ClerkLoading>
      <ClerkLoaded>
        <Show when="signed-out">
          <AuthScreen />
        </Show>
        <Show when="signed-in">
          <ClerkTokenBridge />
          <AuthenticatedApp />
        </Show>
      </ClerkLoaded>
    </>
  );
}
