import { useEffect, useMemo, useState } from 'react';
import { ChevronDown, ChevronRight, Plus, SlidersHorizontal, X } from 'lucide-react';
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

interface TransactionsLedgerProps {
	onNavigate: (screen: ScreenId) => void;
	onEditTransaction?: (t: Transaction) => void;
}

type FilterChip = 'all' | 'income' | 'expense' | 'review';

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
}

function LedgerTransactionRow({
	transaction,
	isLast,
	viewMode,
	artworkPhase,
	onEditTransaction,
}: LedgerTransactionRowProps) {
	const isIncome = transaction.type === 'income';
	const isTransfer = transaction.type === 'transfer';
	const isReview = transaction.status === 'NEEDS_REVIEW';
	const primaryLabel = transactionPrimaryLabel(transaction);
	const metaLabel = transactionMetaLabel(transaction, viewMode);

	return (
		<button
			onClick={() => onEditTransaction?.(transaction)}
			className="w-full flex items-center justify-between px-4 py-3.5 text-left transition-colors hover:bg-black/[0.02] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset"
			style={!isLast ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
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
			</div>
		</button>
	);
}

export function TransactionsLedger({ onNavigate, onEditTransaction }: TransactionsLedgerProps) {
	const {
		transactions,
		isLoading,
		error,
	} = useTransactions();

	const [viewMode, setViewMode] = useState<'daily' | 'monthly'>('daily');
	const [filterChip, setFilterChip] = useState<FilterChip>('all');
	const [artworkMode, setArtworkMode] = useState<ArtworkMode>('auto');
	const [filtersOpen, setFiltersOpen] = useState(false);
	const [expandedMonthKey, setExpandedMonthKey] = useState('');
	const artworkPhase = useMerchantArtworkPhase(artworkMode);
	const hasActiveControls = filterChip !== 'all' || artworkMode !== 'auto';

	useEffect(() => {
		if (!filtersOpen) return;

		const closeOnEscape = (event: KeyboardEvent) => {
			if (event.key === 'Escape') setFiltersOpen(false);
		};
		window.addEventListener('keydown', closeOnEscape);
		return () => window.removeEventListener('keydown', closeOnEscape);
	}, [filtersOpen]);

	const filtered = useMemo(() => {
		return transactions.filter((t) => {
			if (filterChip === 'income') return t.type === 'income';
			if (filterChip === 'expense') return t.type === 'expense';
			if (filterChip === 'review') return t.status === 'NEEDS_REVIEW';
			return true;
		});
	}, [transactions, filterChip]);

	const groups = useMemo(() => {
		const sorted = [...filtered].sort((a, b) => b.date.getTime() - a.date.getTime());
		const map = new Map<string, DayGroup>();

		for (const t of sorted) {
			const key = viewMode === 'daily' ? dayKey(t.date) : monthKey(t.date);
			const label = viewMode === 'daily' ? dayLabel(t.date) : monthLabel(t.date);
			const existing = map.get(key);
			if (existing) {
				existing.transactions.push(t);
				if (t.type === 'income') existing.net += t.amount;
				else if (t.type === 'expense') existing.net -= t.amount;
			} else {
				const net = t.type === 'income' ? t.amount : t.type === 'expense' ? -t.amount : 0;
				map.set(key, { key, label, date: t.date, transactions: [t], net });
			}
		}

		return Array.from(map.values());
	}, [filtered, viewMode]);

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
						onClick={() => setViewMode('daily')}
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
						onClick={() => {
							setViewMode('monthly');
							setExpandedMonthKey('');
						}}
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

						<div className="flex flex-wrap items-center gap-2">
							<span
								className="w-full text-[9px] font-bold uppercase tracking-[0.18em]"
								style={{ color: 'var(--ledger-muted)' }}
							>
								Transactions
							</span>
							{(['all', 'income', 'expense', 'review'] as const).map((chip) => (
								<button
									key={chip}
									onClick={() => {
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
									{chip === 'review' ? 'Needs review' : chip.charAt(0).toUpperCase() + chip.slice(1)}
								</button>
							))}
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

			<main className="flex-1 overflow-y-auto pb-24 px-5 pt-2 min-h-0">
				{error && (
					<p className="text-xs mb-4" style={{ color: 'var(--ledger-negative)' }}>{error}</p>
				)}

				{isLoading && transactions.length === 0 ? (
					<LedgerListSkeleton />
				) : filtered.length === 0 ? (
					<div className="flex flex-col items-center justify-center py-16">
						<p className="text-sm font-medium" style={{ color: 'var(--ledger-muted)' }}>No transactions found</p>
						<p className="text-xs mt-1" style={{ color: 'var(--ledger-muted)', opacity: 0.7 }}>
							{filterChip !== 'all' ? 'Try a different filter' : 'Add one with the + button'}
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
									className="rounded-[14px] overflow-hidden mb-3"
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
												/>
											))}
										</div>
									)}
								</div>
							);
						}

						return (
							<div key={group.key} className="mb-2">
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
										/>
									))}
								</div>
							</div>
						);
					})
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
