import { useMemo, useState } from 'react';
import {
	Plus, SlidersHorizontal, Circle, ArrowLeftRight,
} from 'lucide-react';
import type { ScreenId } from '../layout/BottomNav';
import { useTransactions, type Transaction } from '../../hooks/useTransactions';
import { LedgerListSkeleton } from '../ui/Skeleton';

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

export function TransactionsLedger({ onNavigate, onEditTransaction }: TransactionsLedgerProps) {
	const {
		transactions,
		isLoading,
		error,
	} = useTransactions();

	const [viewMode, setViewMode] = useState<'daily' | 'monthly'>('daily');
	const [filterChip, setFilterChip] = useState<FilterChip>('all');

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
					Transactions
				</h1>
				<div className="flex items-center gap-2">
					<button
						className="w-8 h-8 rounded-full flex items-center justify-center transition-colors"
						style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
					>
						<SlidersHorizontal size={15} />
					</button>
				</div>
			</header>

			<div
				className="px-5 py-3 flex items-center justify-between shrink-0 z-10 gap-4"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<div className="flex items-center gap-1 shrink-0">
					<button
						onClick={() => setViewMode('daily')}
						className="rounded-full px-3 py-1 text-xs font-medium transition-colors"
						style={
							viewMode === 'daily'
								? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
								: { background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }
						}
					>
						Daily
					</button>
					<button
						onClick={() => setViewMode('monthly')}
						className="rounded-full px-3 py-1 text-xs font-medium transition-colors"
						style={
							viewMode === 'monthly'
								? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
								: { background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }
						}
					>
						Monthly
					</button>
				</div>

				<div className="flex-1 overflow-x-auto flex items-center gap-2 pb-1 -mb-1 no-scrollbar">
					{(['all', 'income', 'expense', 'review'] as const).map((chip) => (
						<button
							key={chip}
							onClick={() => setFilterChip(chip)}
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
			</div>

			<main className="flex-1 overflow-y-auto pb-24 px-5 pt-5 min-h-0">
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
					groups.map((group) => (
						<div key={group.key} className="mb-6">
							<div className="flex items-center justify-between mb-2 px-1">
								<h3 className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
									{group.label}
								</h3>
								<span
									className="text-xs font-semibold tnum"
									style={{
										color: group.net > 0
											? 'var(--ledger-positive)'
											: group.net < 0
												? 'var(--ledger-negative)'
												: 'var(--ledger-muted)',
									}}
								>
									{group.net > 0 ? '+' : group.net < 0 ? '−' : ''}
									₹{Math.abs(group.net).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
								</span>
							</div>

							<div
								className="rounded-[10px] overflow-hidden"
								style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
							>
								{group.transactions.map((t, i) => {
									const isLast = i === group.transactions.length - 1;
									const isIncome = t.type === 'income';
									const isExpense = t.type === 'expense';
									const isTransfer = t.type === 'transfer';
									const isReview = t.status === 'NEEDS_REVIEW';
									const iconBg = isIncome
										? 'rgba(45, 122, 79, 0.08)'
										: isExpense
											? 'rgba(196, 83, 54, 0.08)'
											: 'var(--ledger-rule)';
									const iconColor = isIncome
										? 'var(--ledger-positive)'
										: isExpense
											? 'var(--ledger-negative)'
											: 'var(--ledger-muted)';
									const label = t.merchantName ?? t.description ?? t.category;
									const subLabel = [t.accountName, t.subCategory].filter(Boolean).join(' · ');

									return (
										<button
											key={t.id}
											onClick={() => onEditTransaction?.(t)}
											className="w-full flex items-center justify-between p-3.5 text-left transition-colors hover:bg-black/[0.02]"
											style={!isLast ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
										>
											<div className="flex items-center gap-3 min-w-0">
												<div
													className="w-9 h-9 rounded-[6px] flex items-center justify-center shrink-0"
													style={{ background: iconBg, color: iconColor }}
												>
													{isTransfer ? <ArrowLeftRight size={15} /> : <Circle size={15} />}
												</div>
												<div className="min-w-0">
													<div className="flex items-center gap-2">
														<p className="text-sm font-semibold truncate" style={{ color: 'var(--ledger-fg)' }}>
															{label}
														</p>
														{isReview && (
															<span
																className="text-[10px] font-semibold px-2 py-0.5 rounded-full uppercase tracking-wider shrink-0"
																style={{
																	background: 'var(--ledger-amber-bg)',
																	color: 'var(--ledger-amber)',
																	border: '1px solid rgba(184, 115, 42, 0.2)',
																}}
															>
																Review
															</span>
														)}
													</div>
													<p className="text-xs mt-0.5 truncate" style={{ color: 'var(--ledger-muted)' }}>
														{subLabel || t.category}
													</p>
												</div>
											</div>
											<p
												className="text-sm font-semibold tnum shrink-0 ml-3"
												style={{
													color: isIncome
														? 'var(--ledger-positive)'
														: isExpense
															? 'var(--ledger-fg)'
															: 'var(--ledger-muted)',
												}}
											>
												{formatSignedRupees(t.amount, t.type)}
											</p>
										</button>
									);
								})}
							</div>
						</div>
					))
				)}
			</main>

			<button
				onClick={() => onNavigate('add')}
				className="absolute bottom-[80px] right-5 w-14 h-14 rounded-full flex items-center justify-center shadow-lg active:scale-95 transition-all z-20"
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
					Transactions
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
				className="absolute bottom-[80px] right-5 w-14 h-14 rounded-full flex items-center justify-center shadow-lg active:scale-95 transition-all z-20"
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
