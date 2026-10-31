import { ClerkLoaded, ClerkLoading, Show, SignInButton, SignUpButton } from '@clerk/react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { ClerkTokenBridge } from './components/ClerkTokenBridge';
import { MobileShell } from './components/layout/MobileShell';
import { BottomNav, type ScreenId } from './components/layout/BottomNav';
import { HomeDashboard } from './components/screens/HomeDashboard';
import { TransactionsLedger } from './components/screens/TransactionsLedger';
import { TransactionEditor } from './components/screens/TransactionEditor';
import { ReviewQueue, EmptyReviewQueue } from './components/screens/ReviewQueue';
import { AnalyticsScreen } from './components/screens/AnalyticsScreen';
import { BudgetScreen } from './components/screens/BudgetScreen';
import { SettingsHub } from './components/screens/SettingsHub';
import { AccountsCardsManager } from './components/screens/AccountsCardsManager';
import { AiUsageDashboard } from './components/screens/AiUsageDashboard';
import { CategorySettings } from './components/screens/CategorySettings';
import { MerchantSettings } from './components/screens/MerchantSettings';
import { GmailConnectionSettings } from './components/screens/GmailConnectionSettings';
import { DeleteAccountSettings } from './components/screens/DeleteAccountSettings';
import { buildReturnToAppUrl, parseGoogleOAuthCallbackParams } from './lib/gmail-oauth';
import { ensureLegacyLocalDataMigrated } from './lib/legacy-local-migration';
import { isAndroidBrowser } from './lib/native-app';
import { queryClient } from './lib/query-client';
import { isInLastThreeCalendarDaysIST } from './lib/review-scope';
import { useGmailConnectionForOAuthCallback } from './hooks/useGmailConnection';
import { SkeletonBar, SkeletonRules } from './components/ui/Skeleton';
import { useTransactions } from './hooks/useTransactions';
import type { Transaction } from './hooks/useTransactions';
import './styles/app.css';

type Overlay = 'editor' | 'review' | null;
type SettingsSubscreen =
	| 'main'
	| 'accounts'
	| 'categories'
	| 'merchants'
	| 'connection'
	| 'ai-usage'
	| 'delete-account';
// 'single' is the ledger's swipe-to-review: it answers exactly the row that was
// swiped and closes, instead of walking the rest of the inbox.
type ReviewScope = 'recent' | 'all' | 'single';

// Every screen and overlay owns a URL so a refresh (or back/forward) lands
// exactly where the user was. History API only — no router dependency.
interface RouteState {
	screen: ScreenId;
	settingsSubscreen: SettingsSubscreen;
	overlay: Overlay;
	editorTxnId: string | 'new' | null;
	reviewScope: ReviewScope;
}

function parseRoute(pathname: string): RouteState {
	const base: RouteState = {
		screen: 'home',
		settingsSubscreen: 'main',
		overlay: null,
		editorTxnId: null,
		reviewScope: 'all',
	};
	if (pathname === '/ledger') return { ...base, screen: 'ledger' };
	if (pathname === '/budget') return { ...base, screen: 'budget' };
	if (pathname === '/analytics') return { ...base, screen: 'analytics' };
	if (pathname === '/settings') return { ...base, screen: 'settings' };
	if (pathname === '/settings/accounts') {
		return { ...base, screen: 'settings', settingsSubscreen: 'accounts' };
	}
	if (pathname === '/settings/categories') {
		return { ...base, screen: 'settings', settingsSubscreen: 'categories' };
	}
	if (pathname === '/settings/merchants') {
		return { ...base, screen: 'settings', settingsSubscreen: 'merchants' };
	}
	if (pathname === '/settings/connection') {
		return { ...base, screen: 'settings', settingsSubscreen: 'connection' };
	}
	if (pathname === '/settings/ai-usage') {
		return { ...base, screen: 'settings', settingsSubscreen: 'ai-usage' };
	}
	if (pathname === '/settings/delete-account') {
		return { ...base, screen: 'settings', settingsSubscreen: 'delete-account' };
	}
	if (pathname === '/review/recent') {
		return { ...base, overlay: 'review', reviewScope: 'recent' };
	}
	if (pathname === '/review') return { ...base, overlay: 'review', reviewScope: 'all' };
	if (pathname === '/transaction/new') return { ...base, overlay: 'editor', editorTxnId: 'new' };
	const txnMatch = /^\/transaction\/([0-9a-fA-F-]{8,})$/.exec(pathname);
	if (txnMatch?.[1]) return { ...base, overlay: 'editor', editorTxnId: txnMatch[1] };
	return base;
}

function pathForScreen(screen: ScreenId, settingsSubscreen: SettingsSubscreen): string {
	if (screen === 'ledger') return '/ledger';
	if (screen === 'budget') return '/budget';
	if (screen === 'analytics') return '/analytics';
	if (screen === 'settings') {
		if (settingsSubscreen === 'accounts') return '/settings/accounts';
		if (settingsSubscreen === 'categories') return '/settings/categories';
		if (settingsSubscreen === 'merchants') return '/settings/merchants';
		if (settingsSubscreen === 'connection') return '/settings/connection';
		if (settingsSubscreen === 'ai-usage') return '/settings/ai-usage';
		if (settingsSubscreen === 'delete-account') return '/settings/delete-account';
		return '/settings';
	}
	return '/';
}

function syncPath(path: string, replace = false) {
	if (`${window.location.pathname}${window.location.search}` !== path) {
		window.history[replace ? 'replaceState' : 'pushState'](null, '', path);
	}
}

function LedgerApp() {
	const [initialRoute] = useState(() => parseRoute(window.location.pathname));
	const [activeScreen, setActiveScreen] = useState<ScreenId>(initialRoute.screen);
	const [overlay, setOverlay] = useState<Overlay>(
		initialRoute.overlay === 'review' || initialRoute.editorTxnId === 'new'
			? initialRoute.overlay
			: null,
	);
	const [editorMode, setEditorMode] = useState<'add' | 'edit'>(
		initialRoute.editorTxnId === 'new' ? 'add' : 'edit',
	);
	const [editingTransaction, setEditingTransaction] = useState<Transaction | null>(null);
	const [reviewedCount, setReviewedCount] = useState(0);
	const [reviewScope, setReviewScope] = useState<ReviewScope>(initialRoute.reviewScope);
	const [reviewTargetId, setReviewTargetId] = useState<string | null>(null);
	const [settingsSubscreen, setSettingsSubscreen] = useState<SettingsSubscreen>(
		initialRoute.settingsSubscreen,
	);
	// A deep-linked /transaction/:id must wait for the feed to deliver the row.
	const [pendingTxnId, setPendingTxnId] = useState<string | null>(
		initialRoute.editorTxnId && initialRoute.editorTxnId !== 'new'
			? initialRoute.editorTxnId
			: null,
	);

	const { reviewInbox, transactions, isFeedComplete } = useTransactions();
	const scopedReviewInbox = useMemo(() => {
		if (reviewScope === 'single') {
			return reviewInbox.filter((transaction) => transaction.id === reviewTargetId);
		}
		const scoped = reviewScope === 'all'
			? reviewInbox
			: reviewInbox.filter((transaction) => isInLastThreeCalendarDaysIST(transaction.date));
		if (!reviewTargetId) return scoped;
		const targetIndex = scoped.findIndex(transaction => transaction.id === reviewTargetId);
		if (targetIndex <= 0) return scoped;
		return [scoped[targetIndex]!, ...scoped.slice(0, targetIndex), ...scoped.slice(targetIndex + 1)];
	}, [reviewInbox, reviewScope, reviewTargetId]);

	useEffect(() => {
		// One-time legacy localStorage migration. Runs detached: it never
		// blocks or fails queries (errors are swallowed inside and retried
		// next launch); a refetch afterwards picks up anything it migrated.
		void ensureLegacyLocalDataMigrated().then(() => queryClient.invalidateQueries());
	}, []);

	// Resolve a deep-linked transaction once the (incrementally loading)
	// feed contains it; a confirmed miss falls back home.
	useEffect(() => {
		if (!pendingTxnId) return;
		const found = transactions.find((t) => t.id === pendingTxnId);
		if (found) {
			setEditingTransaction(found);
			setEditorMode('edit');
			setOverlay('editor');
			setPendingTxnId(null);
		} else if (isFeedComplete) {
			setPendingTxnId(null);
			setOverlay(null);
			setActiveScreen('home');
			syncPath('/', true);
		}
	}, [pendingTxnId, transactions, isFeedComplete]);

	// Back/forward buttons re-derive all state from the URL.
	useEffect(() => {
		const onPopState = () => {
			const route = parseRoute(window.location.pathname);
			setActiveScreen(route.screen);
			setSettingsSubscreen(route.settingsSubscreen);
			setReviewedCount(0);
			setReviewTargetId(null);
			setReviewScope(route.reviewScope);
			if (route.overlay === 'editor') {
				if (route.editorTxnId === 'new') {
					setEditorMode('add');
					setEditingTransaction(null);
					setOverlay('editor');
				} else if (route.editorTxnId) {
					setOverlay(null);
					setPendingTxnId(route.editorTxnId);
				}
			} else {
				setOverlay(route.overlay);
				setEditingTransaction(null);
				setPendingTxnId(null);
			}
		};
		window.addEventListener('popstate', onPopState);
		return () => window.removeEventListener('popstate', onPopState);
	}, []);

	const navigateToScreen = (screen: ScreenId, subscreen: SettingsSubscreen = 'main') => {
		setActiveScreen(screen);
		setSettingsSubscreen(subscreen);
		syncPath(pathForScreen(screen, subscreen));
	};

	const openEditor = (mode: 'add' | 'edit', transaction: Transaction | null = null) => {
		setEditorMode(mode);
		setEditingTransaction(mode === 'add' ? null : transaction);
		setOverlay('editor');
		syncPath(mode === 'add' ? '/transaction/new' : `/transaction/${transaction?.id ?? ''}`);
	};
	const closeOverlay = () => {
		setOverlay(null);
		setEditingTransaction(null);
		setPendingTxnId(null);
		setReviewedCount(0);
		setReviewTargetId(null);
		syncPath(pathForScreen(activeScreen, settingsSubscreen));
	};

	const openReview = (scope: ReviewScope = 'all', targetId: string | null = null) => {
		setReviewedCount(0);
		setReviewScope(scope);
		setReviewTargetId(targetId);
		setOverlay('review');
		syncPath(scope === 'recent' ? '/review/recent' : '/review');
	};

	// The ledger opens on the same month the Budget screen was showing, so the
	// rows it lists are the ones behind that month's number.
	const openBudgetLedger = (month: string) => {
		setActiveScreen('ledger');
		setSettingsSubscreen('main');
		syncPath(`/ledger?budget=in&month=${month}`);
	};

	const handleHomeNavigate = (screen: ScreenId) => {
		if (screen === 'add') {
			openEditor('add');
		} else if (screen === 'review') {
			openReview();
		} else {
			navigateToScreen(screen);
		}
	};

	return (
		<MobileShell>
			{activeScreen === 'home' && (
				<HomeDashboard
					onNavigate={handleHomeNavigate}
					onOpenReview={openReview}
					onEditTransaction={(t) => openEditor('edit', t)}
				/>
			)}

			{activeScreen === 'ledger' && (
				<TransactionsLedger
					onNavigate={(screen) => {
						if (screen === 'add') {
							openEditor('add');
						} else {
							navigateToScreen(screen);
						}
					}}
					onEditTransaction={(t) => openEditor('edit', t)}
					onReviewTransaction={(t) => openReview('single', t.id)}
				/>
			)}

			{activeScreen === 'analytics' && (
				<AnalyticsScreen onEditTransaction={(t) => openEditor('edit', t)} />
			)}

			{activeScreen === 'budget' && (
				<BudgetScreen onOpenBudgetLedger={openBudgetLedger} />
			)}

			{activeScreen === 'settings' && settingsSubscreen === 'main' && (
				<SettingsHub
					onOpenAccounts={() => navigateToScreen('settings', 'accounts')}
					onOpenBudget={() => navigateToScreen('budget')}
					onOpenCategories={() => navigateToScreen('settings', 'categories')}
					onOpenMerchants={() => navigateToScreen('settings', 'merchants')}
					onOpenConnection={() => navigateToScreen('settings', 'connection')}
					onOpenAiUsage={() => navigateToScreen('settings', 'ai-usage')}
					onOpenDeleteAccount={() => navigateToScreen('settings', 'delete-account')}
					onSignOut={() => navigateToScreen('home')}
				/>
			)}
			{activeScreen === 'settings' && settingsSubscreen === 'accounts' && (
				<AccountsCardsManager onBack={() => navigateToScreen('settings', 'main')} />
			)}
			{activeScreen === 'settings' && settingsSubscreen === 'categories' && (
				<CategorySettings onBack={() => navigateToScreen('settings', 'main')} />
			)}
			{activeScreen === 'settings' && settingsSubscreen === 'merchants' && (
				<MerchantSettings onBack={() => navigateToScreen('settings', 'main')} />
			)}
			{activeScreen === 'settings' && settingsSubscreen === 'connection' && (
				<GmailConnectionSettings onBack={() => navigateToScreen('settings', 'main')} />
			)}
			{activeScreen === 'settings' && settingsSubscreen === 'ai-usage' && (
				<AiUsageDashboard onBack={() => navigateToScreen('settings', 'main')} />
			)}
			{activeScreen === 'settings' && settingsSubscreen === 'delete-account' && (
				<DeleteAccountSettings
					onBack={() => navigateToScreen('settings', 'main')}
					onDeleted={() => navigateToScreen('home')}
				/>
			)}

			<BottomNav
				activeTab={activeScreen}
				onTabChange={(tab) => navigateToScreen(tab)}
			/>

			{/* Deep link resolving: cover the screen immediately so home never
			    flashes while the feed delivers the linked transaction. */}
			{pendingTxnId !== null && overlay === null && (
				<div
					className="absolute inset-0 z-50 flex flex-col"
					style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
				>
					<header
						className="h-[56px] shrink-0 flex items-center px-5"
						style={{ borderBottom: '1px solid var(--ledger-rule)' }}
					>
						<SkeletonBar className="w-8 h-8 rounded-full" />
					</header>
					<div className="flex-1 flex items-center justify-center">
						<SkeletonRules caption="Opening transaction" />
					</div>
				</div>
			)}

			{overlay === 'editor' && (
				<TransactionEditor
					mode={editorMode}
					editingTransaction={editingTransaction}
					isManual={editorMode === 'add' || (editingTransaction?.isManual ?? true)}
					onClose={closeOverlay}
				/>
			)}

			{overlay === 'review' && scopedReviewInbox.length > 0 && scopedReviewInbox[0] && (
				<ReviewQueue
					key={scopedReviewInbox[0].id}
					transaction={scopedReviewInbox[0]}
					position={reviewedCount + 1}
					totalCount={reviewedCount + scopedReviewInbox.length}
					// Swipe-to-review answers one row and leaves; the queue modes
					// stay open and advance to the next transaction.
					onReviewed={() => {
						if (reviewScope === 'single') {
							closeOverlay();
							return;
						}
						setReviewedCount((c) => c + 1);
					}}
					onClose={closeOverlay}
				/>
			)}

			{/*
			  * Single scope never shows "all caught up": the optimistic update
			  * empties its one-row inbox a render before onReviewed closes the
			  * overlay, which would flash the empty state on every swipe review.
			  */}
			{overlay === 'review' && reviewScope !== 'single' && scopedReviewInbox.length === 0 && (
				<EmptyReviewQueue
					onClose={closeOverlay}
					onNavigate={(screen) => {
						closeOverlay();
						navigateToScreen(screen);
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

// The Android app opens Google's consent screen in a Chrome tab, where the
// user is signed out. If Android does not hand the redirect back to the app
// on its own, this is the page the tab lands on.
function ReturnToAppScreen() {
	return (
		<div className="oauth-callback-wrapper">
			<div className="oauth-callback-card">
				<h2 className="oauth-callback-title">One more step</h2>
				<p className="oauth-callback-subtext">
					Go back to the Money Manager app to finish linking Gmail.
				</p>
				<a className="oauth-callback-btn" href={buildReturnToAppUrl(window.location.search)}>
					Return to the app
				</a>
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
					{isOAuthCallback && isAndroidBrowser() ? <ReturnToAppScreen /> : <AuthScreen />}
				</Show>
				<Show when="signed-in">
					<ClerkTokenBridge />
					<AuthenticatedApp />
				</Show>
			</ClerkLoaded>
		</>
	);
}
