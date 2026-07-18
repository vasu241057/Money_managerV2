import { ClerkLoaded, ClerkLoading, Show, SignInButton, SignUpButton } from '@clerk/react';
import { useEffect, useRef, useState } from 'react';
import { ClerkTokenBridge } from './components/ClerkTokenBridge';
import { MobileShell } from './components/layout/MobileShell';
import { BottomNav, type ScreenId } from './components/layout/BottomNav';
import { HomeDashboard } from './components/screens/HomeDashboard';
import { TransactionsLedger } from './components/screens/TransactionsLedger';
import { TransactionEditor } from './components/screens/TransactionEditor';
import { ReviewQueue, EmptyReviewQueue } from './components/screens/ReviewQueue';
import { AnalyticsScreen } from './components/screens/AnalyticsScreen';
import { SettingsHub } from './components/screens/SettingsHub';
import { AccountsCardsManager } from './components/screens/AccountsCardsManager';
import { parseGoogleOAuthCallbackParams } from './lib/gmail-oauth';
import { ensureLegacyLocalDataMigrated } from './lib/legacy-local-migration';
import { queryClient } from './lib/query-client';
import { useGmailConnectionForOAuthCallback } from './hooks/useGmailConnection';
import { useTransactions } from './hooks/useTransactions';
import type { Transaction } from './hooks/useTransactions';
import './styles/app.css';

type Overlay = 'editor' | 'review' | null;
type SettingsSubscreen = 'main' | 'accounts';

function LedgerApp() {
	const [activeScreen, setActiveScreen] = useState<ScreenId>('home');
	const [overlay, setOverlay] = useState<Overlay>(null);
	const [editorMode, setEditorMode] = useState<'add' | 'edit'>('add');
	const [editingTransaction, setEditingTransaction] = useState<Transaction | null>(null);
	const [reviewedCount, setReviewedCount] = useState(0);
	const [settingsSubscreen, setSettingsSubscreen] = useState<SettingsSubscreen>('main');

	const { reviewInbox } = useTransactions();

	useEffect(() => {
		// One-time legacy localStorage migration. Runs detached: it never
		// blocks or fails queries (errors are swallowed inside and retried
		// next launch); a refetch afterwards picks up anything it migrated.
		void ensureLegacyLocalDataMigrated().then(() => queryClient.invalidateQueries());
	}, []);

	const openEditor = (mode: 'add' | 'edit') => {
		setEditorMode(mode);
		if (mode === 'add') setEditingTransaction(null);
		setOverlay('editor');
	};
	const closeOverlay = () => {
		setOverlay(null);
		setEditingTransaction(null);
		setReviewedCount(0);
	};

	const openReview = () => {
		setReviewedCount(0);
		setOverlay('review');
	};

	const handleHomeNavigate = (screen: ScreenId) => {
		if (screen === 'add') {
			openEditor('add');
		} else if (screen === 'review') {
			openReview();
		} else {
			setActiveScreen(screen);
		}
	};

	return (
		<MobileShell>
			{activeScreen === 'home' && (
				<HomeDashboard
					onNavigate={handleHomeNavigate}
					onEditTransaction={(t) => {
						setEditingTransaction(t);
						openEditor('edit');
					}}
				/>
			)}

			{activeScreen === 'ledger' && (
				<TransactionsLedger
					onNavigate={(screen) => {
						if (screen === 'add') {
							openEditor('add');
						} else {
							setActiveScreen(screen);
						}
					}}
					onEditTransaction={(t) => {
						setEditingTransaction(t);
						openEditor('edit');
					}}
				/>
			)}

			{activeScreen === 'analytics' && (
				<AnalyticsScreen
					onEditTransaction={(t) => {
						setEditingTransaction(t);
						openEditor('edit');
					}}
				/>
			)}

			{activeScreen === 'settings' && settingsSubscreen === 'main' && (
				<SettingsHub
					onOpenAccounts={() => setSettingsSubscreen('accounts')}
					onSignOut={() => setActiveScreen('home')}
				/>
			)}
			{activeScreen === 'settings' && settingsSubscreen === 'accounts' && (
				<AccountsCardsManager onBack={() => setSettingsSubscreen('main')} />
			)}

			<BottomNav
				activeTab={activeScreen}
				onTabChange={(tab) => {
					setActiveScreen(tab);
					if (tab !== 'settings') setSettingsSubscreen('main');
				}}
			/>

			{overlay === 'editor' && (
				<TransactionEditor
					mode={editorMode}
					editingTransaction={editingTransaction}
					isManual={editorMode === 'add' || (editingTransaction?.isManual ?? true)}
					onClose={closeOverlay}
				/>
			)}

			{overlay === 'review' && reviewInbox.length > 0 && reviewInbox[0] && (
				<ReviewQueue
					key={reviewInbox[0].id}
					transaction={reviewInbox[0]}
					position={reviewedCount + 1}
					totalCount={reviewedCount + reviewInbox.length}
					onReviewed={() => setReviewedCount((c) => c + 1)}
					onClose={closeOverlay}
				/>
			)}

			{overlay === 'review' && reviewInbox.length === 0 && (
				<EmptyReviewQueue
					onClose={closeOverlay}
					onNavigate={(screen) => {
						closeOverlay();
						setActiveScreen(screen);
					}}
				/>
			)}
		</MobileShell>
	);
}

function MascotEnvelope() {
	return (
		<svg viewBox="0 0 100 100" fill="none" width="80" height="80">
			<circle cx="50" cy="50" r="48" fill="#1e293b" stroke="#334155" strokeWidth="1.5" />
			<rect x="26" y="32" width="48" height="34" rx="3" fill="#0f172a" stroke="#475569" strokeWidth="1.5" />
			<path d="M26 32l24 18 24-18" stroke="#475569" strokeWidth="1.5" fill="#1e293b" />
			<path d="M26 36l24 14 24-14" stroke="#64748b" strokeWidth="1.5" strokeDasharray="2 2" fill="none" />
			<circle cx="58" cy="42" r="3" fill="#0D5C4B" opacity="0.8" />
			<circle cx="64" cy="42" r="3" fill="#0D5C4B" opacity="0.5" />
			<circle cx="70" cy="42" r="3" fill="#0D5C4B" opacity="0.3" />
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

		// oauthError first: hasCallbackParams is false whenever Google returns
		// an error, so the generic branch would otherwise mask the real reason.
		if (oauthError) {
			if (mountedRef.current) {
				setErrorMsg(`Google returned an error: ${oauthError}`);
				setStatus('error');
			}
			return;
		}

		if (!hasCallbackParams) {
			if (mountedRef.current) {
				setErrorMsg('Missing connection parameters. Please try again.');
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
		<div
			className="flex items-center justify-center min-h-[100dvh] px-6"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			<div className="w-full max-w-[340px] flex flex-col">
				<div className="flex flex-col items-center mb-10">
					<div
						className="w-16 h-16 rounded-[14px] flex items-center justify-center mb-5"
						style={{
							background: 'rgba(13, 92, 75, 0.08)',
							border: '1px solid rgba(13, 92, 75, 0.15)',
						}}
					>
						<svg viewBox="0 0 24 24" fill="none" width="30" height="30">
							<rect x="3" y="7" width="18" height="12" rx="2" stroke="var(--ledger-accent)" strokeWidth="1.5" fill="none" />
							<path d="M3 11h18" stroke="var(--ledger-accent)" strokeWidth="1.5" />
							<circle cx="17" cy="15" r="1.2" fill="var(--ledger-accent)" />
						</svg>
					</div>
					<h1
						className="text-[26px] font-bold tracking-tight text-center mb-2"
						style={{ color: 'var(--ledger-fg)' }}
					>
						Money Manager
					</h1>
					<p
						className="text-sm text-center leading-relaxed"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Your inbox, automatically ledgered.
					</p>
				</div>

				<div
					className="rounded-[14px] p-5 mb-6"
					style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
				>
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3"
						style={{ color: 'var(--ledger-muted)' }}
					>
						What you get
					</p>
					<ul className="space-y-2.5">
						{[
							'Gmail transactions auto-imported',
							'Ledger-grade categorization',
							'AI-classified, user-verified',
						].map((line) => (
							<li key={line} className="flex items-start gap-2.5">
								<span
									className="mt-1.5 w-1.5 h-1.5 rounded-full shrink-0"
									style={{ background: 'var(--ledger-accent)' }}
								/>
								<span
									className="text-xs leading-relaxed"
									style={{ color: 'var(--ledger-fg)' }}
								>
									{line}
								</span>
							</li>
						))}
					</ul>
				</div>

				<div className="flex flex-col gap-3">
					<SignInButton mode="modal">
						<button
							className="w-full h-14 rounded-[10px] text-sm font-bold active:scale-[0.98] transition-all flex items-center justify-center"
							style={{
								background: 'var(--ledger-accent)',
								color: 'var(--ledger-bg)',
								boxShadow: '0 8px 24px -8px var(--ledger-accent)',
							}}
						>
							Sign in
						</button>
					</SignInButton>
					<SignUpButton mode="modal">
						<button
							className="w-full h-14 rounded-[10px] text-sm font-bold active:scale-[0.98] transition-all flex items-center justify-center"
							style={{
								background: 'var(--ledger-card)',
								color: 'var(--ledger-fg)',
								border: '1px solid var(--ledger-rule)',
							}}
						>
							Create account
						</button>
					</SignUpButton>
				</div>

				<p
					className="text-[10px] text-center mt-6 leading-relaxed"
					style={{ color: 'var(--ledger-muted)' }}
				>
					By continuing you agree to connect Gmail for transaction import.
				</p>
			</div>
		</div>
	);
}

function AuthenticatedApp() {
	const [isOAuthCallbackScreen, setIsOAuthCallbackScreen] = useState(
		typeof window !== 'undefined' && window.location.pathname === '/oauth/google/callback'
	);

	if (isOAuthCallbackScreen) {
		return <GoogleOAuthCallbackScreen onDone={() => setIsOAuthCallbackScreen(false)} />;
	}

	return <LedgerApp />;
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
					<div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '100vh', color: 'var(--ledger-muted)' }}>
						Loading...
					</div>
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
