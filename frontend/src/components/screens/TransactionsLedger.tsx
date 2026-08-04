import { useEffect, useMemo, useRef, useState } from 'react';
import { CheckCircle2, ChevronDown, ChevronRight, Loader2, Plus, Search, SlidersHorizontal, Trash2, X } from 'lucide-react';
import type { BudgetTreatment } from '../../../../shared/types';
import type { ScreenId } from '../layout/BottomNav';
import { useTransactions, type Transaction } from '../../hooks/useTransactions';
import { LedgerListSkeleton } from '../ui/Skeleton';
import { TransactionArtwork } from '../ui/TransactionArtwork';
import {
	useMerchantArtworkPhase,
	type ArtworkMode,
	type MerchantArtworkPhase,
} from '../../hooks/useMerchantArtworkPhase';
import {
	transactionMetaLabel,
	transactionPrimaryLabel,
} from '../../lib/transaction-display';
import {
	matchesLedgerFilter,
	resolveSwipeOutcome,
	type LedgerFilter,
} from '../../lib/ledger-interactions';
import '../../styles/ledger.css';

interface TransactionsLedgerProps {
	onNavigate: (screen: ScreenId) => void;
	onEditTransaction?: (t: Transaction) => void;
	onReviewTransaction?: (t: Transaction) => void;
}

function formatSignedRupees(amount: number, type: string): string {
	const sign = type === 'income' ? '+' : type === 'expense' ? '−' : '↔';
	const abs = Math.abs(amount);
	const formatted = abs.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
	return `${sign}₹${formatted}`;
}

function dayKey(date: Date): string {
	return `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}`;
}

function dayLabel(date: Date): string {
	const now = new Date();
	const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
	const yesterday = new Date(today.getTime() - 86400000);
	const d = new Date(date.getFullYear(), date.getMonth(), date.getDate());
	if (d.getTime() === today.getTime()) return 'Today';
	if (d.getTime() === yesterday.getTime()) return 'Yesterday';
	return date.toLocaleDateString('en-IN', { weekday: 'short', month: 'short', day: 'numeric' });
}

function monthKey(date: Date): string {
	return `${date.getFullYear()}-${date.getMonth()}`;
}

function monthLabel(date: Date): string {
	return date.toLocaleDateString('en-IN', { month: 'long', year: 'numeric' });
}

function useDebouncedValue(value: string, delayMs: number): string {
	const [debounced, setDebounced] = useState(value);
	useEffect(() => {
		const timeout = window.setTimeout(() => setDebounced(value), delayMs);
		return () => window.clearTimeout(timeout);
	}, [delayMs, value]);
	return debounced;
}

interface DayGroup {
	key: string;
	label: string;
	date: Date;
	transactions: Transaction[];
	net: number;
}

interface LedgerTransactionRowProps {
	transaction: Transaction;
	isLast: boolean;
	viewMode: 'daily' | 'monthly';
	artworkPhase: MerchantArtworkPhase;
	onEditTransaction?: (transaction: Transaction) => void;
	onReviewTransaction?: (transaction: Transaction) => void;
	onDelete: (transaction: Transaction) => Promise<void>;
	onSetBudgetTreatment: (transaction: Transaction, treatment: BudgetTreatment) => Promise<void>;
	openRowId: string | null;
	setOpenRowId: (id: string | null) => void;
}

const SWIPE_ACTION_WIDTH = 88;
const SWIPE_AXIS_SLOP = 5;

type SwipeDirection = 'left' | 'right';

function directionForOffset(offset: number): SwipeDirection | null {
	if (offset > 0) return 'right';
	if (offset < 0) return 'left';
	return null;
}

function LedgerTransactionRow({
	transaction,
	isLast,
	viewMode,
	artworkPhase,
	onEditTransaction,
	onReviewTransaction,
	onDelete,
	onSetBudgetTreatment,
	openRowId,
	setOpenRowId,
}: LedgerTransactionRowProps) {
	const [settledOffset, setSettledOffset] = useState(0);
	const [committing, setCommitting] = useState<SwipeDirection | null>(null);
	const rowRef = useRef<HTMLButtonElement | null>(null);
	const swipeRootRef = useRef<HTMLDivElement | null>(null);
	const offsetRef = useRef(0);
	const gesture = useRef<{
		x: number;
		y: number;
		lastX: number;
		lastTime: number;
		velocityX: number;
		startOffset: number;
		rowWidth: number;
		opened: boolean;
		axis: 'x' | 'y' | null;
	} | null>(null);
	const didSwipe = useRef(false);
	const isIncome = transaction.type === 'income';
	const isTransfer = transaction.type === 'transfer';
	const isReview = transaction.status === 'NEEDS_REVIEW';
	const primaryLabel = transactionPrimaryLabel(transaction);
	const budgetIncluded = transaction.effectiveBudgetTreatment === 'INCLUDE';
	const isAdjustment = transaction.budgetEffect === 'ADJUSTMENT';
	const nextTreatment: BudgetTreatment = budgetIncluded ? 'EXCLUDE' : 'INCLUDE';
	const budgetMeta = isAdjustment
		? 'Budget adjustment'
		: budgetIncluded ? null : 'Outside budget';
	const relationshipMeta = transaction.budgetRelationship
		? transaction.budgetRelationship.role === 'SOURCE'
			? `${transaction.budgetRelationship.kind === 'REFUND' ? 'Refund' : 'Reimbursement'} for ${transaction.budgetRelationship.related_label}`
			: transaction.budgetRelationship.related_count > 1
				? `${transaction.budgetRelationship.related_count} refunds linked`
				: `${transaction.budgetRelationship.kind === 'REFUND' ? 'Refund' : 'Reimbursement'} linked`
		: null;
	const metaLabel = [transactionMetaLabel(transaction, viewMode), relationshipMeta]
		.filter(Boolean)
		.join(' · ');

	const displayedOffset = openRowId === transaction.id ? settledOffset : 0;
	const rightActionLabel = isReview
		? 'Review'
		: nextTreatment === 'INCLUDE' ? 'Count' : 'Outside';
	const runRightAction = () => isReview
		? Promise.resolve(onReviewTransaction?.(transaction))
		: onSetBudgetTreatment(transaction, nextTreatment);

	const applyOffset = (nextOffset: number, animate: boolean) => {
		offsetRef.current = nextOffset;
		const row = rowRef.current;
		const root = swipeRootRef.current;
		if (row) {
			row.style.transition = animate
				? 'transform 240ms cubic-bezier(0.2, 0.82, 0.2, 1)'
				: 'none';
			row.style.transform = nextOffset === 0 ? 'none' : `translate3d(${nextOffset}px, 0, 0)`;
		}
		if (root) {
			root.dataset.swipeDirection = directionForOffset(nextOffset) ?? 'none';
			root.style.setProperty('--swipe-progress', String(Math.min(1, Math.abs(nextOffset) / SWIPE_ACTION_WIDTH)));
		}
	};

	const settleAt = (nextOffset: number) => {
		setSettledOffset(nextOffset);
		applyOffset(nextOffset, true);
		setOpenRowId(nextOffset === 0 ? null : transaction.id);
	};

	const closeRow = () => {
		settleAt(0);
	};

	const commitSwipe = (direction: SwipeDirection, rowWidth: number) => {
		setCommitting(direction);
		setSettledOffset(direction === 'right' ? rowWidth : -rowWidth);
		applyOffset(direction === 'right' ? rowWidth : -rowWidth, true);
		setOpenRowId(transaction.id);
		if ('vibrate' in navigator) navigator.vibrate(8);

		window.setTimeout(() => {
			const action = direction === 'right'
				? runRightAction()
				: onDelete(transaction);
			void action.finally(() => {
				setCommitting(null);
				setSettledOffset(0);
				applyOffset(0, false);
				setOpenRowId(null);
			});
		}, 150);
	};

	const finishGesture = () => {
		const current = gesture.current;
		if (!current) return;
		const wasHorizontal = current.axis === 'x';
		gesture.current = null;
		if (!wasHorizontal) return;

		const finalOffset = offsetRef.current;
		const distance = Math.abs(finalOffset);
		didSwipe.current = distance > 6;
		const outcome = resolveSwipeOutcome(finalOffset, current.velocityX, current.rowWidth);
		if (outcome === 'COMMIT_RIGHT') commitSwipe('right', current.rowWidth);
		else if (outcome === 'COMMIT_LEFT') commitSwipe('left', current.rowWidth);
		else if (outcome === 'OPEN_RIGHT') settleAt(SWIPE_ACTION_WIDTH);
		else if (outcome === 'OPEN_LEFT') settleAt(-SWIPE_ACTION_WIDTH);
		else closeRow();
	};

	const cancelGesture = () => {
		const current = gesture.current;
		if (!current) return;
		gesture.current = null;
		didSwipe.current = current.axis === 'x';
		settleAt(current.startOffset);
	};

	return (
		<div
			ref={swipeRootRef}
			className="ledger-swipe-row relative overflow-hidden"
			data-swipe-direction={directionForOffset(displayedOffset) ?? 'none'}
			data-committing={committing ?? 'none'}
			style={!isLast ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
		>
			<div className="absolute inset-0">
				<button
					type="button"
					onFocus={() => settleAt(SWIPE_ACTION_WIDTH)}
					onClick={() => { closeRow(); void runRightAction(); }}
					className="ledger-swipe-action ledger-swipe-action--budget flex items-center text-[10px] font-bold uppercase tracking-wider"
					style={isReview
						? { background: 'var(--ledger-amber-bg)', color: 'var(--ledger-amber)' }
						: { background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}
					aria-label={isReview
						? 'Review transaction'
						: `${nextTreatment === 'INCLUDE' ? 'Include' : 'Exclude'} transaction in monthly budget`}
				>
					<span className="ledger-swipe-action__content">
						{isReview
							? <CheckCircle2 size={17} />
							: <span className="text-lg leading-none">{nextTreatment === 'INCLUDE' ? '+' : '−'}</span>}
						<span>{rightActionLabel}</span>
					</span>
				</button>
				<button
					type="button"
					onFocus={() => settleAt(-SWIPE_ACTION_WIDTH)}
					onClick={() => { closeRow(); void onDelete(transaction); }}
					className="ledger-swipe-action ledger-swipe-action--delete flex items-center justify-end text-[10px] font-bold uppercase tracking-wider"
					style={{ background: 'var(--ledger-negative)', color: '#fff' }}
					aria-label="Delete transaction"
				>
					<span className="ledger-swipe-action__content"><Trash2 size={16} /><span>Delete</span></span>
				</button>
			</div>
			<button
				ref={rowRef}
				type="button"
				disabled={committing !== null}
				onPointerDown={(event) => {
					if (event.button !== 0 || committing !== null) return;
					const startOffset = openRowId === transaction.id ? settledOffset : 0;
					applyOffset(startOffset, false);
					gesture.current = {
						x: event.clientX,
						y: event.clientY,
						lastX: event.clientX,
						lastTime: event.timeStamp,
						velocityX: 0,
						startOffset,
						rowWidth: event.currentTarget.getBoundingClientRect().width,
						opened: openRowId === transaction.id,
						axis: null,
					};
					event.currentTarget.setPointerCapture(event.pointerId);
				}}
				onPointerMove={(event) => {
					const current = gesture.current;
					if (!current) return;
					const dx = event.clientX - current.x;
					const dy = event.clientY - current.y;
					if (current.axis === null && Math.max(Math.abs(dx), Math.abs(dy)) > SWIPE_AXIS_SLOP) {
						current.axis = Math.abs(dx) > Math.abs(dy) * 1.08 ? 'x' : 'y';
					}
					if (current.axis !== 'x') return;
					event.preventDefault();
					if (!current.opened) {
						current.opened = true;
						setOpenRowId(transaction.id);
					}
					const elapsed = Math.max(1, event.timeStamp - current.lastTime);
					const instantaneousVelocity = (event.clientX - current.lastX) / elapsed;
					current.velocityX = current.velocityX * 0.65 + instantaneousVelocity * 0.35;
					current.lastX = event.clientX;
					current.lastTime = event.timeStamp;
					const maxTravel = current.rowWidth * 0.68;
					applyOffset(Math.max(-maxTravel, Math.min(maxTravel, current.startOffset + dx)), false);
				}}
				onPointerUp={finishGesture}
				onPointerCancel={cancelGesture}
				onClick={(event) => {
					if (didSwipe.current) {
						didSwipe.current = false;
						event.preventDefault();
						return;
					}
					if (displayedOffset !== 0) {
						closeRow();
						return;
					}
					onEditTransaction?.(transaction);
				}}
				className="ledger-swipe-foreground relative w-full flex items-center justify-between px-4 py-3.5 text-left hover:bg-black/[0.02] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset"
				style={{
					background: !isReview && !budgetIncluded && !isAdjustment
						? 'color-mix(in srgb, var(--ledger-card) 72%, var(--ledger-bg))'
						: 'var(--ledger-card)',
					transform: displayedOffset === 0 ? undefined : `translate3d(${displayedOffset}px, 0, 0)`,
					transition: 'transform 240ms cubic-bezier(0.2, 0.82, 0.2, 1)',
					touchAction: 'pan-y',
					boxShadow: isAdjustment
						? 'inset 3px 0 var(--ledger-positive)'
						: !isReview && !budgetIncluded ? 'inset 3px 0 var(--ledger-rule)' : undefined,
				}}
			>
			<div className="flex items-center gap-3 min-w-0 flex-1">
				<TransactionArtwork transaction={transaction} phase={artworkPhase} />
				<div className="min-w-0">
					<p className="text-sm font-semibold truncate" style={{ color: 'var(--ledger-fg)' }}>
						{primaryLabel}
					</p>
					<p className="text-xs mt-0.5 truncate" style={{ color: 'var(--ledger-muted)' }}>
						{metaLabel}
					</p>
				</div>
			</div>
			<div className="shrink-0 ml-3 flex flex-col items-end self-stretch py-0.5 justify-start">
				<p
					className="text-sm font-bold tnum"
					style={{
						color: isIncome
							? 'var(--ledger-positive)'
							: isTransfer
								? 'var(--ledger-muted)'
								: 'var(--ledger-fg)',
					}}
				>
					{formatSignedRupees(transaction.amount, transaction.type)}
				</p>
				{isReview && (
					<span
						className="mt-2 rounded-full px-2 py-0.5 text-[9px] leading-none font-bold uppercase tracking-[0.1em]"
						style={{
							background: 'var(--ledger-amber-bg)',
							color: 'var(--ledger-amber)',
							border: '1px solid color-mix(in srgb, var(--ledger-amber) 22%, transparent)',
						}}
					>
						Review
					</span>
				)}
				{!isReview && budgetMeta && (
					<span className="mt-2 text-[9px] leading-none font-bold uppercase tracking-[0.08em]" style={{ color: isAdjustment ? 'var(--ledger-positive)' : 'var(--ledger-muted)' }}>{budgetMeta}</span>
				)}
			</div>
			</button>
		</div>
	);
}

export function TransactionsLedger({
	onNavigate,
	onEditTransaction,
	onReviewTransaction,
}: TransactionsLedgerProps) {
	const [searchInput, setSearchInput] = useState('');
	const searchQuery = useDebouncedValue(searchInput.trim(), 220);
	const {
		transactions,
		deleteTransaction,
		restoreTransaction,
		setBudgetTreatment,
		loadMore,
		hasMore,
		isFetchingMore,
		isInitialLoading,
		error,
	} = useTransactions({ search: searchQuery, autoLoadAll: false });

	const [viewMode, setViewMode] = useState<'daily' | 'monthly'>('daily');
	const [filterChip, setFilterChip] = useState<LedgerFilter>(() => {
		const requested = new URLSearchParams(window.location.search).get('budget');
		return requested === 'in' ? 'in-budget' : requested === 'out' ? 'outside-budget' : 'all';
	});
	const [artworkMode, setArtworkMode] = useState<ArtworkMode>('auto');
	const [filtersOpen, setFiltersOpen] = useState(false);
	const [expandedMonthKey, setExpandedMonthKey] = useState('');
	const [openRowId, setOpenRowId] = useState<string | null>(null);
	const [deletedTransaction, setDeletedTransaction] = useState<Transaction | null>(null);
	const [actionError, setActionError] = useState<string | null>(null);
	const scrollContainerRef = useRef<HTMLElement | null>(null);
	const loadMoreSentinelRef = useRef<HTMLDivElement | null>(null);
	const artworkPhase = useMerchantArtworkPhase(artworkMode);
	const hasActiveControls = filterChip !== 'all' || artworkMode !== 'auto' || searchInput.trim() !== '';

	const resetScrollPosition = () => {
		scrollContainerRef.current?.scrollTo({ top: 0, behavior: 'auto' });
		setOpenRowId(null);
	};

	const selectViewMode = (nextMode: 'daily' | 'monthly') => {
		if (nextMode === viewMode) return;
		resetScrollPosition();
		setExpandedMonthKey('');
		setViewMode(nextMode);
	};

	const clearAllFilters = () => {
		setFilterChip('all');
		setArtworkMode('auto');
		setSearchInput('');
		setExpandedMonthKey('');
		setFiltersOpen(false);
		resetScrollPosition();
		if (window.location.pathname === '/ledger' && window.location.search) {
			window.history.replaceState(null, '', '/ledger');
		}
	};

	useEffect(() => {
		if (!deletedTransaction) return;
		const timeout = window.setTimeout(() => setDeletedTransaction(null), 6_000);
		return () => window.clearTimeout(timeout);
	}, [deletedTransaction]);

	useEffect(() => {
		if (!filtersOpen) return;

		const closeOnEscape = (event: KeyboardEvent) => {
			if (event.key === 'Escape') setFiltersOpen(false);
		};
		window.addEventListener('keydown', closeOnEscape);
		return () => window.removeEventListener('keydown', closeOnEscape);
	}, [filtersOpen]);

	useEffect(() => {
		const root = scrollContainerRef.current;
		const sentinel = loadMoreSentinelRef.current;
		if (!root || !sentinel || !hasMore) return;

		const observer = new IntersectionObserver(entries => {
			if (entries.some(entry => entry.isIntersecting) && !isFetchingMore) {
				void loadMore();
			}
		}, { root, rootMargin: '900px 0px' });
		observer.observe(sentinel);
		return () => observer.disconnect();
	}, [hasMore, isFetchingMore, loadMore]);

	useEffect(() => {
		scrollContainerRef.current?.scrollTo({ top: 0, behavior: 'auto' });
	}, [searchQuery]);

	const filtered = useMemo(() => {
		return transactions.filter(transaction => matchesLedgerFilter(transaction, filterChip));
	}, [transactions, filterChip]);

	const groups = useMemo(() => {
		const sorted = [...filtered].sort((a, b) => b.date.getTime() - a.date.getTime());
		const map = new Map<string, DayGroup>();

		const transactionNet = (transaction: Transaction) => {
			if (filterChip === 'in-budget' || filterChip === 'adjustments') {
				if (transaction.budgetEffect === 'SPEND') return -(transaction.budgetEffectAmount ?? transaction.amount);
				if (transaction.budgetEffect === 'ADJUSTMENT') return transaction.budgetEffectAmount ?? transaction.amount;
				return 0;
			}
			return transaction.type === 'income' ? transaction.amount : transaction.type === 'expense' ? -transaction.amount : 0;
		};

		for (const t of sorted) {
			const key = viewMode === 'daily' ? dayKey(t.date) : monthKey(t.date);
			const label = viewMode === 'daily' ? dayLabel(t.date) : monthLabel(t.date);
			const existing = map.get(key);
			if (existing) {
				existing.transactions.push(t);
				existing.net += transactionNet(t);
			} else {
				const net = transactionNet(t);
				map.set(key, { key, label, date: t.date, transactions: [t], net });
			}
		}

		return Array.from(map.values());
	}, [filtered, viewMode, filterChip]);

	const handleDelete = async (transaction: Transaction) => {
		setActionError(null);
		setOpenRowId(null);
		try {
			await deleteTransaction(transaction.id);
			setDeletedTransaction(transaction);
		} catch (cause) {
			setActionError(cause instanceof Error ? cause.message : 'Could not delete transaction.');
		}
	};

	const handleBudgetTreatment = async (transaction: Transaction, treatment: BudgetTreatment) => {
		setActionError(null);
		setOpenRowId(null);
		try {
			await setBudgetTreatment(transaction.id, treatment);
		} catch (cause) {
			setActionError(cause instanceof Error ? cause.message : 'Could not update the monthly plan.');
		}
	};

	const undoDelete = async () => {
		if (!deletedTransaction) return;
		const transaction = deletedTransaction;
		setActionError(null);
		try {
			await restoreTransaction(transaction.id);
			setDeletedTransaction(null);
		} catch (cause) {
			setActionError(cause instanceof Error ? cause.message : 'Could not restore transaction.');
		}
	};

	return (
		<div
			className="flex-1 flex flex-col min-h-0 relative"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			<header
				className="h-[56px] shrink-0 flex items-center justify-between px-5 z-10"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<h1 className="font-bold text-xl tracking-tight" style={{ color: 'var(--ledger-fg)' }}>
					Ledger
				</h1>
				<button
					onClick={() => setFiltersOpen((open) => !open)}
					className="relative w-8 h-8 rounded-full flex items-center justify-center transition-colors"
					style={filtersOpen || hasActiveControls
						? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
						: { background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
					aria-expanded={filtersOpen}
					aria-controls="ledger-filter-dialog"
					aria-haspopup="dialog"
					aria-label="Ledger filters and artwork"
				>
					<SlidersHorizontal size={15} />
					{hasActiveControls && (
						<span
							className="absolute -right-0.5 -top-0.5 w-2 h-2 rounded-full"
							style={{ background: 'var(--ledger-amber)', border: '2px solid var(--ledger-bg)' }}
						/>
					)}
				</button>
			</header>

			<div
				className="px-5 py-3 shrink-0 z-10"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<div
					className="grid grid-cols-2 gap-1 rounded-[10px] p-1"
					style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
				>
					<button
						onClick={() => selectViewMode('daily')}
						className="rounded-[7px] px-3 py-1.5 text-xs font-semibold transition-colors"
						style={
							viewMode === 'daily'
								? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
								: { color: 'var(--ledger-muted)' }
						}
					>
						By day
					</button>
					<button
						onClick={() => selectViewMode('monthly')}
						className="rounded-[7px] px-3 py-1.5 text-xs font-semibold transition-colors"
						style={
							viewMode === 'monthly'
								? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
								: { color: 'var(--ledger-muted)' }
						}
					>
						By month
					</button>
				</div>
				<label className="ledger-search mt-2.5 flex h-10 items-center gap-2 rounded-[10px] px-3" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
					<Search size={15} className="shrink-0" style={{ color: 'var(--ledger-muted)' }} />
					<input
						type="search"
						value={searchInput}
						onChange={event => setSearchInput(event.target.value)}
						placeholder="Merchant, amount, note or date"
						aria-label="Search ledger by merchant, amount, note, or date"
						maxLength={120}
						className="min-w-0 flex-1 bg-transparent text-xs outline-none"
						style={{ color: 'var(--ledger-fg)' }}
					/>
					{searchInput && (
						<button type="button" onClick={() => setSearchInput('')} className="w-6 h-6 rounded-full flex items-center justify-center" style={{ color: 'var(--ledger-muted)' }} aria-label="Clear search">
							<X size={13} />
						</button>
					)}
				</label>
			</div>

			{filtersOpen && (
				<>
					<button
						type="button"
						className="absolute inset-0 z-20 cursor-default"
						style={{ background: 'color-mix(in srgb, var(--ledger-fg) 16%, transparent)' }}
						onClick={() => setFiltersOpen(false)}
						aria-label="Close Ledger filters"
					/>
					<div
						id="ledger-filter-dialog"
						role="dialog"
						aria-modal="true"
						aria-label="Ledger filters and artwork"
						className="absolute top-[48px] right-5 z-30 w-[calc(100%-2.5rem)] max-w-[360px] rounded-[14px] px-4 py-3.5"
						style={{
							background: 'var(--ledger-card)',
							border: '1px solid var(--ledger-rule)',
							boxShadow: '0 18px 44px rgba(16, 24, 20, 0.2)',
						}}
					>
						<div className="mb-3 flex items-center justify-between gap-3">
							<p className="text-sm font-bold" style={{ color: 'var(--ledger-fg)' }}>
								Filter ledger
							</p>
							<div className="flex items-center gap-2">
								<button type="button" onClick={clearAllFilters} disabled={!hasActiveControls} className="text-[10px] font-bold disabled:opacity-35" style={{ color: 'var(--ledger-accent)' }}>Clear all</button>
								<button
									type="button"
									onClick={() => setFiltersOpen(false)}
									className="w-7 h-7 rounded-full flex items-center justify-center transition-colors"
									style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
									aria-label="Close"
								>
									<X size={14} />
								</button>
							</div>
						</div>

						<div className="flex flex-wrap items-center gap-2">
							<span
								className="w-full text-[9px] font-bold uppercase tracking-[0.18em]"
								style={{ color: 'var(--ledger-muted)' }}
							>
								Transactions
							</span>
							{(['all', 'income', 'expense', 'review', 'in-budget', 'outside-budget', 'adjustments'] as const).map((chip) => (
								<button
									key={chip}
									onClick={() => {
										resetScrollPosition();
										setFilterChip(chip);
										setFiltersOpen(false);
										setExpandedMonthKey('');
									}}
									className="shrink-0 rounded-full text-xs font-medium px-3 py-1 transition-colors"
									style={
										filterChip === chip
											? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
											: { background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }
									}
								>
									{chip === 'review'
										? 'Needs review'
										: chip === 'in-budget'
											? 'In budget'
											: chip === 'outside-budget'
												? 'Outside budget'
												: chip.charAt(0).toUpperCase() + chip.slice(1)}
								</button>
							))}
							<p className="w-full text-[10px] leading-relaxed mt-1" style={{ color: 'var(--ledger-muted)' }}>
								Swipe right to review or change budget · swipe left to delete
							</p>
						</div>

						<div
							className="mt-3 pt-3 flex items-end justify-between gap-3"
							style={{ borderTop: '1px solid var(--ledger-rule)' }}
						>
							<div className="min-w-0">
								<p
									className="text-[9px] font-bold uppercase tracking-[0.18em]"
									style={{ color: 'var(--ledger-muted)' }}
								>
									Artwork
								</p>
								<p className="mt-1 text-[10px] leading-tight" style={{ color: 'var(--ledger-muted)' }}>
									{artworkMode === 'auto'
										? 'Auto · changes every 10s'
										: artworkMode === 'category'
											? 'Category icons pinned'
											: 'Merchant logos pinned'}
								</p>
							</div>
							<div
								className="grid grid-cols-2 gap-1 rounded-[10px] p-1 shrink-0"
								style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)' }}
								role="group"
								aria-label="Ledger artwork"
							>
								<button
									type="button"
									aria-pressed={artworkMode === 'category'}
									onClick={() => setArtworkMode((current) => current === 'category' ? 'auto' : 'category')}
									className="rounded-[7px] px-2.5 py-1.5 text-[11px] font-semibold transition-colors"
									style={artworkMode === 'category'
										? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
										: { color: 'var(--ledger-muted)' }}
								>
									Icons
								</button>
								<button
									type="button"
									aria-pressed={artworkMode === 'merchant'}
									onClick={() => setArtworkMode((current) => current === 'merchant' ? 'auto' : 'merchant')}
									className="rounded-[7px] px-2.5 py-1.5 text-[11px] font-semibold transition-colors"
									style={artworkMode === 'merchant'
										? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
										: { color: 'var(--ledger-muted)' }}
								>
									Merchants
								</button>
							</div>
						</div>
					</div>
				</>
			)}

			<main ref={scrollContainerRef} className="ledger-scroll flex-1 overflow-y-auto pb-24 px-5 pt-2 min-h-0">
				{(error || actionError) && (
					<p className="text-xs mb-4" style={{ color: 'var(--ledger-negative)' }}>{actionError ?? error}</p>
				)}

				{isInitialLoading && transactions.length === 0 ? (
					<LedgerListSkeleton />
				) : filtered.length === 0 ? (
					<div className="flex flex-col items-center justify-center py-16">
						<p className="text-sm font-medium" style={{ color: 'var(--ledger-muted)' }}>No transactions found</p>
						<p className="text-xs mt-1" style={{ color: 'var(--ledger-muted)', opacity: 0.7 }}>
							{searchQuery
								? 'Try another merchant, amount, note, or date'
								: filterChip !== 'all' ? 'Try a different filter' : 'Add one with the + button'}
						</p>
					</div>
				) : (
					groups.map((group) => {
						const netColor = group.net > 0
							? 'var(--ledger-positive)'
							: group.net < 0
								? 'var(--ledger-negative)'
								: 'var(--ledger-muted)';
						const netLabel = `${group.net > 0 ? '+' : group.net < 0 ? '−' : ''}₹${Math.abs(group.net).toLocaleString('en-IN', {
							minimumFractionDigits: 2,
							maximumFractionDigits: 2,
						})}`;

						if (viewMode === 'monthly') {
							const isExpanded = expandedMonthKey === group.key;
							return (
								<div
									key={group.key}
									className="ledger-render-section ledger-render-section--month rounded-[14px] overflow-hidden mb-3"
									style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
								>
									<button
										onClick={() => setExpandedMonthKey(isExpanded ? '' : group.key)}
										className="w-full flex items-center justify-between gap-4 px-4 py-3.5 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset"
										aria-expanded={isExpanded}
									>
										<div className="flex items-center gap-3 min-w-0">
											<span
												className="w-8 h-8 rounded-[8px] flex items-center justify-center shrink-0"
												style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
											>
												{isExpanded ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
											</span>
											<div className="min-w-0">
												<p className="text-sm font-bold truncate" style={{ color: 'var(--ledger-fg)' }}>{group.label}</p>
												<p className="text-[11px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
													{group.transactions.length} transaction{group.transactions.length !== 1 ? 's' : ''}
												</p>
											</div>
										</div>
										<span className="text-sm font-bold tnum shrink-0" style={{ color: netColor }}>
											{netLabel}
										</span>
									</button>
									{isExpanded && (
										<div style={{ borderTop: '1px solid var(--ledger-rule)' }}>
											{group.transactions.map((transaction, index) => (
												<LedgerTransactionRow
													key={transaction.id}
													transaction={transaction}
													isLast={index === group.transactions.length - 1}
													viewMode="monthly"
													artworkPhase={artworkPhase}
													onEditTransaction={onEditTransaction}
													onReviewTransaction={onReviewTransaction}
													onDelete={handleDelete}
													onSetBudgetTreatment={handleBudgetTreatment}
													openRowId={openRowId}
													setOpenRowId={setOpenRowId}
												/>
											))}
										</div>
									)}
								</div>
							);
						}

						return (
							<div key={group.key} className="ledger-render-section ledger-render-section--day mb-2">
								<div
									className="flex items-center justify-between px-1 py-2"
									style={{ background: 'var(--ledger-bg)' }}
								>
									<h3 className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
										{group.label}
									</h3>
									<span className="text-xs font-semibold tnum" style={{ color: netColor }}>
										{netLabel}
									</span>
								</div>
								<div
									className="rounded-[14px] overflow-hidden"
									style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
								>
									{group.transactions.map((transaction, index) => (
										<LedgerTransactionRow
											key={transaction.id}
											transaction={transaction}
											isLast={index === group.transactions.length - 1}
											viewMode="daily"
											artworkPhase={artworkPhase}
											onEditTransaction={onEditTransaction}
											onReviewTransaction={onReviewTransaction}
											onDelete={handleDelete}
											onSetBudgetTreatment={handleBudgetTreatment}
											openRowId={openRowId}
											setOpenRowId={setOpenRowId}
										/>
									))}
								</div>
							</div>
						);
					})
				)}
				{hasMore && (
					<div ref={loadMoreSentinelRef} className="h-14 flex items-center justify-center" aria-live="polite">
						{isFetchingMore && <Loader2 size={16} className="animate-spin" style={{ color: 'var(--ledger-muted)' }} aria-label="Loading older transactions" />}
					</div>
				)}
			</main>

			<button
				onClick={() => onNavigate('add')}
				className="absolute bottom-5 right-5 w-14 h-14 rounded-full flex items-center justify-center shadow-lg active:scale-95 transition-all z-20"
				style={{
					background: 'var(--ledger-accent)',
					color: 'var(--ledger-bg)',
					boxShadow: '0 8px 24px -8px var(--ledger-accent)',
				}}
			>
				<Plus size={24} />
			</button>

			{deletedTransaction && (
				<div className="absolute left-5 right-20 bottom-5 z-30 rounded-[12px] px-4 py-3 flex items-center gap-3" style={{ background: 'var(--ledger-fg)', color: 'var(--ledger-bg)', boxShadow: '0 12px 32px rgba(0,0,0,0.22)' }} role="status">
					<p className="text-xs font-semibold flex-1 truncate">Transaction deleted</p>
					<button type="button" onClick={() => { void undoDelete(); }} className="text-xs font-bold uppercase tracking-wider" style={{ color: 'var(--ledger-accent)' }}>Undo</button>
				</div>
			)}
		</div>
	);
}

export function EmptyTransactionsLedger({ onNavigate }: TransactionsLedgerProps) {
	return (
		<div
			className="flex-1 flex flex-col min-h-0 relative"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			<header
				className="h-[56px] shrink-0 flex items-center justify-between px-5 z-10"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<h1 className="font-bold text-xl tracking-tight" style={{ color: 'var(--ledger-fg)' }}>
					Ledger
				</h1>
				<div className="w-8 h-8" />
			</header>

			<main className="flex-1 flex flex-col items-center justify-center px-5">
				<p className="text-sm font-medium" style={{ color: 'var(--ledger-muted)' }}>No transactions yet</p>
				<p className="text-xs mt-1" style={{ color: 'var(--ledger-muted)', opacity: 0.7 }}>
					Add one with the + button
				</p>
			</main>

			<button
				onClick={() => onNavigate('add')}
				className="absolute bottom-5 right-5 w-14 h-14 rounded-full flex items-center justify-center shadow-lg active:scale-95 transition-all z-20"
				style={{
					background: 'var(--ledger-accent)',
					color: 'var(--ledger-bg)',
					boxShadow: '0 8px 24px -8px var(--ledger-accent)',
				}}
			>
				<Plus size={24} />
			</button>
		</div>
	);
}
