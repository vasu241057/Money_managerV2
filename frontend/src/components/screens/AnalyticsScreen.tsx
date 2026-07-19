import { useMemo, useState } from 'react';
import {
	PieChart as RechartsPieChart, Pie, Cell, ResponsiveContainer, Tooltip,
} from 'recharts';
import { ChevronLeft, ChevronRight, Loader2, X } from 'lucide-react';
import { useTransactions, type Transaction } from '../../hooks/useTransactions';
import { useAnalyticsSummary, monthKeyIST } from '../../hooks/useAnalyticsSummary';
import { useAccounts } from '../../hooks/useAccounts';
import { paiseToRupees } from '../../lib/money';
import { SkeletonBar, SkeletonDonut } from '../ui/Skeleton';

// Cards are covered by the account breakdown: card transactions ride their
// backing folio, so a separate by-card view would only repeat it.
type Breakdown = 'category' | 'account';

interface BreakdownItem {
	id: string | null;
	name: string;
	valuePaise: number;
	value: number; // rupees, for the chart
	txnCount: number;
}

const LEDGER_COLORS = [
	'var(--ledger-accent)',
	'var(--ledger-amber)',
	'var(--ledger-positive)',
	'var(--ledger-negative)',
	'var(--ledger-muted)',
	'#5B7C99',
	'#A0826D',
];

function formatRupees(amount: number): string {
	return amount.toLocaleString('en-IN', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function formatRupeesPaise(amount: number): string {
	return amount.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function shiftMonthKey(monthKey: string, delta: number): string {
	const [yearStr, monthStr] = monthKey.split('-');
	const shifted = new Date(Number(yearStr), Number(monthStr) - 1 + delta, 1);
	return `${shifted.getFullYear()}-${String(shifted.getMonth() + 1).padStart(2, '0')}`;
}

function monthKeyLabel(monthKey: string): string {
	const [yearStr, monthStr] = monthKey.split('-');
	return new Date(Number(yearStr), Number(monthStr) - 1, 1).toLocaleDateString('en-IN', {
		month: 'long',
		year: 'numeric',
	});
}

interface AnalyticsScreenProps {
	onEditTransaction: (transaction: Transaction) => void;
}

export function AnalyticsScreen({ onEditTransaction }: AnalyticsScreenProps) {
	const { summary, isLoading } = useAnalyticsSummary();
	// Feed is only needed for the drill-down rows; it loads in the background
	// and shares its cache with Home and the Ledger.
	const { transactions, isLoading: feedLoading } = useTransactions();
	const { accounts } = useAccounts();
	const [breakdown, setBreakdown] = useState<Breakdown>('category');
	const [selectedMonth, setSelectedMonth] = useState<string | null>(null);
	const [selectedSlice, setSelectedSlice] = useState<{ id: string | null; name: string } | null>(null);

	const months = summary?.months ?? [];
	const currentMonthKey = monthKeyIST(new Date());
	const latestDataMonth = months.length > 0 ? months[months.length - 1]?.month ?? null : null;
	// Default to the latest month that has data (falls back to the current month)
	const activeMonthKey = selectedMonth ?? latestDataMonth ?? currentMonthKey;
	const activeMonth = months.find((m) => m.month === activeMonthKey) ?? null;

	const oldestMonth = months[0]?.month ?? currentMonthKey;
	const forwardLimit =
		latestDataMonth && latestDataMonth > currentMonthKey ? latestDataMonth : currentMonthKey;
	const canGoBack = activeMonthKey > oldestMonth;
	const canGoForward = activeMonthKey < forwardLimit;

	const stepMonth = (delta: number) => {
		setSelectedMonth(shiftMonthKey(activeMonthKey, delta));
		setSelectedSlice(null);
	};

	const accountsById = useMemo(() => new Map(accounts.map((a) => [a.id, a])), [accounts]);

	const totals = useMemo(() => {
		const income = paiseToRupees(activeMonth?.income_in_paise ?? 0);
		const expense = paiseToRupees(activeMonth?.expense_in_paise ?? 0);
		return { income, expense, net: income - expense };
	}, [activeMonth]);

	const data = useMemo<BreakdownItem[]>(() => {
		if (!activeMonth) return [];
		if (breakdown === 'category') {
			return activeMonth.expense_by_category.map((slice) => ({
				id: slice.category_id,
				name: slice.category_name ?? 'Uncategorized',
				valuePaise: slice.amount_in_paise,
				value: Math.round(paiseToRupees(slice.amount_in_paise)),
				txnCount: slice.txn_count,
			}));
		}
		return activeMonth.expense_by_account.map((slice) => ({
			id: slice.account_id,
			name: slice.account_id
				? accountsById.get(slice.account_id)?.name ?? 'Unknown account'
				: 'No account',
			valuePaise: slice.amount_in_paise,
			value: Math.round(paiseToRupees(slice.amount_in_paise)),
			txnCount: slice.txn_count,
		}));
	}, [activeMonth, breakdown, accountsById]);

	const selectedBucket = selectedSlice
		? data.find((item) => item.id === selectedSlice.id) ?? null
		: null;

	const sliceTransactions = useMemo(() => {
		if (!selectedSlice) return [];
		return transactions
			.filter((t) => {
				if (t.type !== 'expense') return false;
				if (monthKeyIST(t.date) !== activeMonthKey) return false;
				if (breakdown === 'category') {
					return (t.persistedCategoryId ?? null) === selectedSlice.id;
				}
				return (t.accountId ?? null) === selectedSlice.id;
			})
			.sort((a, b) => b.date.getTime() - a.date.getTime());
	}, [transactions, selectedSlice, breakdown, activeMonthKey]);

	const periodLabel = monthKeyLabel(activeMonthKey);
	const totalExpense = totals.expense;

	// Lakh/crore figures shrink to fit their third of the card instead of
	// clipping off-screen; all three columns share one size.
	const longestSummary = Math.max(
		formatRupees(totals.income).length,
		formatRupees(totals.expense).length,
		formatRupees(Math.abs(totals.net)).length,
	);
	const summaryAmountClass =
		longestSummary > 8 ? 'text-xs' : longestSummary > 6 ? 'text-sm' : 'text-lg';

	return (
		<div
			className="flex-1 flex flex-col min-h-0"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			<header
				className="h-[56px] shrink-0 flex items-center justify-between px-5 z-10"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<h1 className="font-bold text-xl tracking-tight" style={{ color: 'var(--ledger-fg)' }}>
					Analytics
				</h1>
				<div className="w-8 h-8" />
			</header>

			<main className="flex-1 overflow-y-auto pb-24 px-5 pt-5">
				{isLoading && !summary ? (
					<div aria-label="Loading analytics">
						<SkeletonBar className="h-11 w-full rounded-[10px] mb-4" />
						<div
							className="rounded-[10px] p-5 mb-4"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
							<SkeletonBar className="h-2.5 w-20 rounded-full mb-5" />
							<div className="flex items-baseline gap-2.5">
								{[0, 1, 2].map((i) => (
									<div key={i} className="flex-1 flex flex-col gap-2">
										<SkeletonBar className="h-2 w-12 rounded-full" delayMs={i * 120} />
										<SkeletonBar className="h-5 w-full rounded-[6px]" delayMs={i * 120 + 60} />
									</div>
								))}
							</div>
						</div>
						<div className="flex gap-2 mb-4">
							<SkeletonBar className="h-9 flex-1 rounded-[10px]" />
							<SkeletonBar className="h-9 flex-1 rounded-[10px]" delayMs={150} />
						</div>
						<div
							className="rounded-[10px] p-5"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
							<SkeletonBar className="h-2.5 w-32 rounded-full mb-6" />
							<div className="py-4">
								<SkeletonDonut />
							</div>
							<div className="flex flex-col gap-3 mt-6">
								{[0, 1, 2].map((i) => (
									<div key={i} className="flex items-center justify-between">
										<SkeletonBar className="h-2.5 w-28 rounded-full" delayMs={i * 120} />
										<SkeletonBar className="h-2.5 w-16 rounded-full" delayMs={i * 120 + 60} />
									</div>
								))}
							</div>
						</div>
					</div>
				) : (
					<>
						<div
							className="flex items-center justify-between mb-4 rounded-[10px] px-2 py-1.5"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
							<button
								onClick={() => stepMonth(-1)}
								disabled={!canGoBack}
								className="p-1.5 rounded-full disabled:opacity-30"
								style={{ color: 'var(--ledger-fg)' }}
								aria-label="Previous month"
							>
								<ChevronLeft size={18} />
							</button>
							<span
								className="text-xs font-bold uppercase tracking-[0.15em]"
								style={{ color: 'var(--ledger-fg)' }}
							>
								{periodLabel}
							</span>
							<button
								onClick={() => stepMonth(1)}
								disabled={!canGoForward}
								className="p-1.5 rounded-full disabled:opacity-30"
								style={{ color: 'var(--ledger-fg)' }}
								aria-label="Next month"
							>
								<ChevronRight size={18} />
							</button>
						</div>

						<div
							className="rounded-[10px] p-5 mb-4"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
							<p
								className="text-[10px] font-bold uppercase tracking-[0.2em] mb-4"
								style={{ color: 'var(--ledger-muted)' }}
							>
								Summary
							</p>
							<div className="flex items-baseline gap-2.5">
								<div className="flex-1">
									<p
										className="text-[10px] font-bold uppercase tracking-wider mb-1"
										style={{ color: 'var(--ledger-muted)' }}
									>
										Income
									</p>
									<p className={`${summaryAmountClass} font-bold tnum whitespace-nowrap`} style={{ color: 'var(--ledger-positive)' }}>
										+₹{formatRupees(totals.income)}
									</p>
								</div>
								<div className="w-px self-stretch" style={{ background: 'var(--ledger-rule)' }} />
								<div className="flex-1">
									<p
										className="text-[10px] font-bold uppercase tracking-wider mb-1"
										style={{ color: 'var(--ledger-muted)' }}
									>
										Expense
									</p>
									<p className={`${summaryAmountClass} font-bold tnum whitespace-nowrap`} style={{ color: 'var(--ledger-negative)' }}>
										−₹{formatRupees(totals.expense)}
									</p>
								</div>
								<div className="w-px self-stretch" style={{ background: 'var(--ledger-rule)' }} />
								<div className="flex-1">
									<p
										className="text-[10px] font-bold uppercase tracking-wider mb-1"
										style={{ color: 'var(--ledger-muted)' }}
									>
										Net
									</p>
									<p
										className={`${summaryAmountClass} font-bold tnum whitespace-nowrap`}
										style={{ color: totals.net >= 0 ? 'var(--ledger-positive)' : 'var(--ledger-negative)' }}
									>
										{totals.net >= 0 ? '+' : '−'}₹{formatRupees(Math.abs(totals.net))}
									</p>
								</div>
							</div>
						</div>

						<div className="flex gap-2 mb-4">
							{(['category', 'account'] as const).map((b) => (
								<button
									key={b}
									onClick={() => { setBreakdown(b); setSelectedSlice(null); }}
									className="flex-1 rounded-[10px] px-3 py-2 text-xs font-medium transition-colors capitalize"
									style={
										breakdown === b
											? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
											: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
									}
								>
									{b}
								</button>
							))}
						</div>

						{totalExpense === 0 ? (
							<div
								className="rounded-[10px] p-8 text-center"
								style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
							>
								<p className="text-sm font-medium" style={{ color: 'var(--ledger-muted)' }}>
									No expenses in {periodLabel}
								</p>
								<p className="text-xs mt-1" style={{ color: 'var(--ledger-muted)', opacity: 0.7 }}>
									Step to another month or add a transaction
								</p>
							</div>
						) : (
							<div
								className="rounded-[10px] p-5 mb-4"
								style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
							>
								<h3
									className="text-[10px] font-bold uppercase tracking-[0.2em] mb-4"
									style={{ color: 'var(--ledger-muted)' }}
								>
									Spending by {breakdown}
								</h3>

								<div className="w-full h-[240px]">
									<ResponsiveContainer width="100%" height="100%">
										<RechartsPieChart>
											<Pie
												data={data}
												cx="50%"
												cy="50%"
												innerRadius={50}
												outerRadius={90}
												paddingAngle={2}
												dataKey="value"
												stroke="none"
												onClick={(_entry: unknown, index: number) => {
													const item = data[index];
													if (item) setSelectedSlice({ id: item.id, name: item.name });
												}}
											>
												{data.map((item, index) => (
													<Cell key={item.id ?? 'none'} fill={LEDGER_COLORS[index % LEDGER_COLORS.length]} />
												))}
											</Pie>
											<Tooltip
												formatter={(value: number | string | undefined, name: string | undefined) => {
													const amount = typeof value === 'number' ? value : Number(value ?? 0);
													return [`₹${formatRupeesPaise(amount)}`, name ?? ''];
												}}
												contentStyle={{
													background: 'var(--ledger-card)',
													border: '1px solid var(--ledger-rule)',
													borderRadius: '8px',
													fontSize: '12px',
													color: 'var(--ledger-fg)',
												}}
											/>
										</RechartsPieChart>
									</ResponsiveContainer>
								</div>

								<div className="mt-4 space-y-0">
									{data.map((item, index) => {
										const percentage = totalExpense > 0
											? ((paiseToRupees(item.valuePaise) / totalExpense) * 100).toFixed(1)
											: '0.0';
										return (
											<button
												key={item.id ?? 'none'}
												onClick={() => setSelectedSlice({ id: item.id, name: item.name })}
												className="w-full flex items-center justify-between py-2.5 text-left"
												style={
													index < data.length - 1
														? { borderBottom: '1px solid var(--ledger-rule)' }
														: undefined
												}
											>
												<div className="flex items-center gap-2.5 min-w-0">
													<span
														className="w-2.5 h-2.5 rounded-full shrink-0"
														style={{ backgroundColor: LEDGER_COLORS[index % LEDGER_COLORS.length] }}
													/>
													<span className="text-sm truncate" style={{ color: 'var(--ledger-fg)' }}>
														{item.name}
													</span>
												</div>
												<div className="flex items-center gap-3 shrink-0">
													<span className="text-sm font-semibold tnum" style={{ color: 'var(--ledger-fg)' }}>
														₹{item.value.toLocaleString('en-IN')}
													</span>
													<span className="text-xs tnum w-10 text-right" style={{ color: 'var(--ledger-muted)' }}>
														{percentage}%
													</span>
												</div>
											</button>
										);
									})}
								</div>
							</div>
						)}
					</>
				)}
			</main>

			{selectedSlice !== null && (
				<div
					className="absolute inset-0 z-40 flex items-center justify-center px-6"
					style={{ background: 'rgba(0, 0, 0, 0.45)', backdropFilter: 'blur(3px)' }}
					onClick={() => setSelectedSlice(null)}
				>
					<div
						className="w-full max-w-[380px] rounded-[14px] flex flex-col overflow-hidden"
						style={{
							background: 'var(--ledger-card)',
							border: '1px solid var(--ledger-rule)',
							maxHeight: '65vh',
							boxShadow: '0 24px 48px -16px rgba(0, 0, 0, 0.35)',
						}}
						onClick={(e) => e.stopPropagation()}
					>
						<div
							className="px-5 pt-4 pb-3 flex items-start justify-between shrink-0"
							style={{ borderBottom: '1px solid var(--ledger-rule)' }}
						>
							<div className="min-w-0">
								<p
									className="text-[10px] font-bold uppercase tracking-[0.2em] truncate"
									style={{ color: 'var(--ledger-muted)' }}
								>
									{selectedSlice.name} · {periodLabel}
								</p>
								<p className="text-lg font-bold tnum mt-1" style={{ color: 'var(--ledger-fg)' }}>
									−₹{formatRupeesPaise(paiseToRupees(selectedBucket?.valuePaise ?? 0))}
									<span className="text-xs font-medium ml-2" style={{ color: 'var(--ledger-muted)' }}>
										{selectedBucket?.txnCount ?? sliceTransactions.length} txn{(selectedBucket?.txnCount ?? sliceTransactions.length) !== 1 ? 's' : ''}
									</span>
								</p>
							</div>
							<button
								onClick={() => setSelectedSlice(null)}
								className="w-7 h-7 rounded-full flex items-center justify-center shrink-0 ml-3"
								style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
								aria-label="Close"
							>
								<X size={15} />
							</button>
						</div>

						<div className="overflow-y-auto px-5 pb-2">
							{sliceTransactions.map((t, i) => (
								<button
									key={t.id}
									onClick={() => onEditTransaction(t)}
									className="w-full flex items-center justify-between gap-3 py-3 text-left"
									style={
										i < sliceTransactions.length - 1
											? { borderBottom: '1px solid var(--ledger-rule)' }
											: undefined
									}
								>
									<div className="min-w-0">
										<p className="text-sm font-medium truncate" style={{ color: 'var(--ledger-fg)' }}>
											{t.merchantName ?? t.description ?? t.category}
										</p>
										<p className="text-[11px] mt-0.5 tnum" style={{ color: 'var(--ledger-muted)' }}>
											{t.date.toLocaleDateString('en-IN', { day: 'numeric', month: 'short' })}
											{t.status === 'NEEDS_REVIEW' ? ' · needs review' : ''}
										</p>
									</div>
									<p
										className="text-sm font-semibold tnum shrink-0"
										style={{ color: 'var(--ledger-negative)' }}
									>
										−₹{formatRupeesPaise(t.amount)}
									</p>
								</button>
							))}
							{sliceTransactions.length === 0 && feedLoading && (
								<div className="flex items-center justify-center gap-2 py-6">
									<Loader2 size={16} className="animate-spin" style={{ color: 'var(--ledger-muted)' }} />
									<span className="text-sm" style={{ color: 'var(--ledger-muted)' }}>Loading transactions…</span>
								</div>
							)}
							{sliceTransactions.length === 0 && !feedLoading && (
								<p className="text-sm py-6 text-center" style={{ color: 'var(--ledger-muted)' }}>
									No transactions
								</p>
							)}
						</div>
					</div>
				</div>
			)}
		</div>
	);
}
