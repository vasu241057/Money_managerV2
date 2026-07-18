import { useMemo, useState } from 'react';
import { Plus, ArrowRight, ChevronRight, Loader2, Circle } from 'lucide-react';
import type { ScreenId } from '../layout/BottomNav';
import { useTransactions, type Transaction } from '../../hooks/useTransactions';
import { useGmailConnection } from '../../hooks/useGmailConnection';

interface HomeDashboardProps {
	onNavigate: (screen: ScreenId) => void;
	onEditTransaction?: (t: Transaction) => void;
}

function formatSignedRupees(amount: number, type: string): string {
	const sign = type === 'income' ? '+' : type === 'expense' ? '−' : '↔';
	const abs = Math.abs(amount);
	const formatted = abs.toLocaleString('en-IN', {
		minimumFractionDigits: 2,
		maximumFractionDigits: 2,
	});
	return `${sign}₹${formatted}`;
}

function relativeDate(date: Date): string {
	const now = new Date();
	const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
	const yesterday = new Date(today.getTime() - 86400000);
	const d = new Date(date.getFullYear(), date.getMonth(), date.getDate());
	if (d.getTime() === today.getTime()) return 'Today';
	if (d.getTime() === yesterday.getTime()) return 'Yesterday';
	const days = Math.floor((today.getTime() - d.getTime()) / 86400000);
	if (days < 7) return `${days} days ago`;
	return date.toLocaleDateString('en-IN', { month: 'short', day: 'numeric' });
}

export function HomeDashboard({ onNavigate, onEditTransaction }: HomeDashboardProps) {
	const {
		transactions,
		reviewInbox,
		isLoading,
		error,
	} = useTransactions();
	const {
		connection,
	} = useGmailConnection();

	const [heroRange, setHeroRange] = useState<'month' | 'all'>('month');

	const stats = useMemo(() => {
		const now = new Date();
		const thisYear = now.getFullYear();
		const thisMonth = now.getMonth();
		let income = 0;
		let expense = 0;
		for (const t of transactions) {
			if (
				heroRange === 'month' &&
				(t.date.getFullYear() !== thisYear || t.date.getMonth() !== thisMonth)
			) {
				continue;
			}
			if (t.type === 'income') income += t.amount;
			else if (t.type === 'expense') expense += t.amount;
		}
		return {
			income,
			expense,
			balance: income - expense,
		};
	}, [transactions, heroRange]);

	const recent = useMemo(() => {
		return [...transactions]
			.sort((a, b) => b.date.getTime() - a.date.getTime())
			.slice(0, 5);
	}, [transactions]);

	// The review backlog is huge (months of drained email); the callout leads
	// with just the last 3 days, a swipe reveals the total.
	const [reviewPane, setReviewPane] = useState(0);
	const reviewLastThreeDaysCount = useMemo(() => {
		const now = new Date();
		const cutoff = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 2).getTime();
		return reviewInbox.filter((t) => t.date.getTime() >= cutoff).length;
	}, [reviewInbox]);

	const reviewCount = reviewInbox.length;
	const gmailConnected = connection?.sync_status === 'ACTIVE' || connection?.sync_status === 'DORMANT';

	return (
		<div
			className="flex-1 flex flex-col min-h-0 relative"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			<header
				className="h-[56px] shrink-0 flex items-center justify-between px-5 z-10"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<h1
					className="font-bold text-xl tracking-tight"
					style={{ color: 'var(--ledger-fg)' }}
				>
					My Wallet
				</h1>
				<button
					onClick={() => onNavigate('settings')}
					className="w-8 h-8 rounded-full flex items-center justify-center transition-colors"
					style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
				>
					<svg viewBox="0 0 24 24" fill="none" width="16" height="16">
						<circle cx="12" cy="8" r="4" stroke="currentColor" strokeWidth="1.5" />
						<path d="M4 20c0-4 4-6 8-6s8 2 8 6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
					</svg>
				</button>
			</header>

			<main
				className="flex-1 overflow-y-auto pb-24 px-5 pt-6 min-h-0"
			>
				{error && (
					<p className="text-xs mb-4" style={{ color: 'var(--ledger-negative)' }}>{error}</p>
				)}

				{/* BALANCE HERO */}
				<section className="mb-6">
					<div className="flex items-center justify-between mb-2">
						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em]"
							style={{ color: 'var(--ledger-muted)' }}
						>
							{heroRange === 'month'
								? `${new Date().toLocaleDateString('en-IN', { month: 'long' })} Balance`
								: 'Total Balance'}
						</p>
						<div
							className="flex items-center rounded-full p-0.5"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
							{(['month', 'all'] as const).map((r) => (
								<button
									key={r}
									onClick={() => setHeroRange(r)}
									className="px-2.5 py-1 rounded-full text-[9px] font-bold uppercase tracking-wider transition-all"
									style={
										heroRange === r
											? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
											: { color: 'var(--ledger-muted)' }
									}
								>
									{r === 'month' ? 'This Month' : 'All Time'}
								</button>
							))}
						</div>
					</div>
					{isLoading && transactions.length === 0 ? (
						<div className="flex items-center gap-2 py-2">
							<Loader2 size={20} className="animate-spin" style={{ color: 'var(--ledger-muted)' }} />
							<span className="text-sm" style={{ color: 'var(--ledger-muted)' }}>Loading…</span>
						</div>
					) : (() => {
						const intPart = Math.floor(Math.round(Math.abs(stats.balance) * 100) / 100).toLocaleString('en-IN');
						// Lakh/crore figures shrink instead of clipping off-screen.
						const sizeClass = intPart.length > 9 ? 'text-3xl' : intPart.length > 7 ? 'text-4xl' : 'text-5xl';
						return (
							<div className="flex items-baseline gap-1 mb-4 whitespace-nowrap">
								<span className="text-2xl font-medium" style={{ color: 'var(--ledger-muted)' }}>₹</span>
								<span
									className={`${sizeClass} font-bold tnum tracking-tight`}
									style={{ color: stats.balance < 0 ? 'var(--ledger-negative)' : 'var(--ledger-fg)' }}
								>
									{intPart}
								</span>
								<span className="text-2xl font-medium tnum" style={{ color: 'var(--ledger-muted)' }}>
									.{(Math.round(Math.abs(stats.balance) * 100) % 100).toString().padStart(2, '0')}
								</span>
							</div>
						);
					})()}

					<div
						className="flex items-center justify-between pt-4"
						style={{ borderTop: '1px solid var(--ledger-rule)' }}
					>
						<div className="flex-1">
							<p
								className="text-[10px] font-bold uppercase tracking-[0.15em] mb-1"
								style={{ color: 'var(--ledger-muted)' }}
							>
								Income
							</p>
							<p className="text-base font-semibold tnum" style={{ color: 'var(--ledger-positive)' }}>
								+₹{stats.income.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
							</p>
						</div>
						<div className="w-px h-10 mx-4" style={{ background: 'var(--ledger-rule)' }} />
						<div className="flex-1 text-right">
							<p
								className="text-[10px] font-bold uppercase tracking-[0.15em] mb-1"
								style={{ color: 'var(--ledger-muted)' }}
							>
								Expense
							</p>
							<p className="text-base font-semibold tnum" style={{ color: 'var(--ledger-negative)' }}>
								−₹{stats.expense.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
							</p>
						</div>
					</div>
				</section>

				{/* GMAIL SYNC */}
				<div className="flex items-center gap-2 mb-5">
					<span
						className="w-1.5 h-1.5 rounded-full shrink-0"
						style={{ background: gmailConnected ? 'var(--ledger-positive)' : 'var(--ledger-muted)' }}
					/>
					<p className="text-xs font-medium" style={{ color: 'var(--ledger-muted)' }}>
						{gmailConnected
							? `Gmail synced · ${connection?.email_address ?? ''}`
							: 'Gmail not connected'}
					</p>
				</div>

				{/* NEEDS REVIEW — swipe between last-3-days and total */}
				{reviewCount > 0 && (
					<div className="mb-8">
						<div
							className="flex overflow-x-auto snap-x snap-mandatory no-scrollbar gap-3"
							onScroll={(e) => {
								const el = e.currentTarget;
								const next = Math.round(el.scrollLeft / el.clientWidth);
								if (next !== reviewPane) setReviewPane(next);
							}}
						>
							{[
								{
									count: reviewLastThreeDaysCount,
									title: `${reviewLastThreeDaysCount} from the last 3 days`,
									sub: reviewLastThreeDaysCount > 0
										? 'Categorize recent imports'
										: 'Nothing recent — swipe for the full backlog',
								},
								{
									count: reviewCount,
									title: `${reviewCount} in total`,
									sub: 'The whole backlog, oldest included',
								},
							].map((paneData, paneIndex) => (
								<button
									key={paneIndex}
									onClick={() => onNavigate('review')}
									className="w-full shrink-0 snap-center flex items-center justify-between p-4 rounded-[10px] text-left transition-transform active:scale-[0.98]"
									style={{
										background: 'var(--ledger-amber-bg)',
										border: '1px solid var(--ledger-amber)',
									}}
								>
									<div className="flex-1 min-w-0">
										<p className="text-sm font-bold mb-0.5" style={{ color: 'var(--ledger-amber)' }}>
											Needs review · {paneData.title}
										</p>
										<p className="text-xs truncate" style={{ color: 'var(--ledger-amber)', opacity: 0.7 }}>
											{paneData.sub}
										</p>
									</div>
									<span className="flex items-center gap-1 text-xs font-bold shrink-0 ml-3" style={{ color: 'var(--ledger-amber)' }}>
										Review
										<ArrowRight size={14} strokeWidth={2.5} />
									</span>
								</button>
							))}
						</div>
						<div className="flex justify-center gap-1.5 mt-2">
							{[0, 1].map((i) => (
								<span
									key={i}
									className="w-1 h-1 rounded-full transition-colors"
									style={{ background: i === reviewPane ? 'var(--ledger-amber)' : 'var(--ledger-rule)' }}
								/>
							))}
						</div>
					</div>
				)}

				{/* RECENT ACTIVITY */}
				<section>
					<div className="flex items-center justify-between mb-3">
						<h3 className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
							Recent Activity
						</h3>
						<button
							onClick={() => onNavigate('ledger')}
							className="flex items-center gap-0.5 text-xs font-medium"
							style={{ color: 'var(--ledger-accent)' }}
						>
							All
							<ChevronRight size={14} />
						</button>
					</div>

					{recent.length === 0 && !isLoading ? (
						<p className="text-sm py-4" style={{ color: 'var(--ledger-muted)' }}>
							No transactions yet
						</p>
					) : (
						<div
							className="rounded-[10px] overflow-hidden"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
							{recent.map((t, i) => {
								const isLast = i === recent.length - 1;
								const isIncome = t.type === 'income';
								const isExpense = t.type === 'expense';
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
								const label = t.merchantName ?? t.category;
								const subLabel = [t.accountName, relativeDate(t.date)].filter(Boolean).join(' · ');

								return (
									<button
										key={t.id}
										onClick={() => (onEditTransaction ? onEditTransaction(t) : onNavigate('ledger'))}
										className="w-full flex items-center justify-between p-3.5 text-left transition-colors"
										style={!isLast ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
									>
										<div className="flex items-center gap-3">
											<div
												className="w-9 h-9 rounded-full flex items-center justify-center shrink-0"
												style={{ background: iconBg, color: iconColor }}
											>
												<Circle size={16} />
											</div>
											<div>
												<p className="text-sm font-semibold" style={{ color: 'var(--ledger-fg)' }}>{label}</p>
												<p className="text-xs mt-0.5" style={{ color: 'var(--ledger-muted)' }}>{subLabel}</p>
											</div>
										</div>
										<p
											className="text-sm font-semibold tnum"
											style={{ color: isIncome ? 'var(--ledger-positive)' : isExpense ? 'var(--ledger-fg)' : 'var(--ledger-muted)' }}
										>
											{formatSignedRupees(t.amount, t.type)}
										</p>
									</button>
								);
							})}
						</div>
					)}
				</section>
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
