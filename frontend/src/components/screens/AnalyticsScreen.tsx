import { useMemo, useState } from 'react';
import {
	PieChart as RechartsPieChart, Pie, Cell, ResponsiveContainer, Tooltip,
} from 'recharts';
import { ChevronLeft, ChevronRight, Loader2, X } from 'lucide-react';
import { useTransactions, type Transaction } from '../../hooks/useTransactions';

// Cards are covered by the account breakdown: card transactions ride their
// backing folio, so a separate by-card view would only repeat it.
type Breakdown = 'category' | 'account';

interface BreakdownItem {
	name: string;
	value: number;
}

function breakdownKey(t: Transaction, breakdown: Breakdown): string {
	if (breakdown === 'category') return t.subCategory ?? t.category ?? 'Uncategorized';
	return t.accountName ?? 'No account';
}

interface MonthKey {
	year: number;
	month: number;
}

function compareMonths(a: MonthKey, b: MonthKey): number {
	return a.year !== b.year ? a.year - b.year : a.month - b.month;
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

function isSameMonth(date: Date, year: number, month: number): boolean {
	return date.getFullYear() === year && date.getMonth() === month;
}

function formatRupees(amount: number): string {
	return amount.toLocaleString('en-IN', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function formatRupeesPaise(amount: number): string {
	return amount.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

interface AnalyticsScreenProps {
	onEditTransaction: (transaction: Transaction) => void;
}

export function AnalyticsScreen({ onEditTransaction }: AnalyticsScreenProps) {
	const { transactions, isLoading } = useTransactions();
	const [breakdown, setBreakdown] = useState<Breakdown>('category');
	const [selectedSlice, setSelectedSlice] = useState<string | null>(null);
	// null = auto: latest month that has data (falls back to the current month)
	const [selectedMonth, setSelectedMonth] = useState<MonthKey | null>(null);

	const now = new Date();
	const thisYear = now.getFullYear();
	const thisMonth = now.getMonth();

	const monthBounds = useMemo(() => {
		const current: MonthKey = { year: thisYear, month: thisMonth };
		let oldest = current;
		let newestWithData: MonthKey | null = null;
		for (const t of transactions) {
			const key: MonthKey = { year: t.date.getFullYear(), month: t.date.getMonth() };
			if (compareMonths(key, oldest) < 0) oldest = key;
			if (!newestWithData || compareMonths(key, newestWithData) > 0) newestWithData = key;
		}
		const forwardLimit =
			newestWithData && compareMonths(newestWithData, current) > 0 ? newestWithData : current;
		return { oldest, newestWithData, forwardLimit };
	}, [transactions, thisYear, thisMonth]);

	const activeMonth =
		selectedMonth ?? monthBounds.newestWithData ?? { year: thisYear, month: thisMonth };
	const canGoBack = compareMonths(activeMonth, monthBounds.oldest) > 0;
	const canGoForward = compareMonths(activeMonth, monthBounds.forwardLimit) < 0;

	const stepMonth = (delta: number) => {
		const next = new Date(activeMonth.year, activeMonth.month + delta, 1);
		setSelectedMonth({ year: next.getFullYear(), month: next.getMonth() });
	};

	const periodTransactions = useMemo(() => {
		return transactions.filter((t) => isSameMonth(t.date, activeMonth.year, activeMonth.month));
	}, [transactions, activeMonth.year, activeMonth.month]);

	const totals = useMemo(() => {
		let income = 0;
		let expense = 0;
		for (const t of periodTransactions) {
			if (t.type === 'income') income += t.amount;
			else if (t.type === 'expense') expense += t.amount;
		}
		return { income, expense, net: income - expense };
	}, [periodTransactions]);

	const data = useMemo<BreakdownItem[]>(() => {
		const map = new Map<string, number>();
		for (const t of periodTransactions) {
			if (t.type !== 'expense') continue;
			const key = breakdownKey(t, breakdown);
			map.set(key, (map.get(key) ?? 0) + t.amount);
		}
		return Array.from(map.entries())
			.map(([name, value]) => ({ name, value: Math.round(value) }))
			.sort((a, b) => b.value - a.value);
	}, [periodTransactions, breakdown]);

	const sliceTransactions = useMemo(() => {
		if (!selectedSlice) return [];
		return periodTransactions
			.filter((t) => t.type === 'expense' && breakdownKey(t, breakdown) === selectedSlice)
			.sort((a, b) => b.date.getTime() - a.date.getTime());
	}, [periodTransactions, breakdown, selectedSlice]);
	const sliceTotal = sliceTransactions.reduce((acc, t) => acc + t.amount, 0);

	const periodLabel = new Date(activeMonth.year, activeMonth.month, 1).toLocaleDateString(
		'en-IN',
		{ month: 'long', year: 'numeric' },
	);
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
				{isLoading && periodTransactions.length === 0 ? (
					<div className="flex items-center gap-2 py-8">
						<Loader2 size={20} className="animate-spin" style={{ color: 'var(--ledger-muted)' }} />
						<span className="text-sm" style={{ color: 'var(--ledger-muted)' }}>Loading analytics…</span>
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
												onClick={(entry: unknown) => {
													const name = (entry as { name?: unknown })?.name;
													if (typeof name === 'string') setSelectedSlice(name);
												}}
											>
												{data.map((_, index) => (
													<Cell key={`cell-${index}`} fill={LEDGER_COLORS[index % LEDGER_COLORS.length]} />
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
											? ((item.value / totalExpense) * 100).toFixed(1)
											: '0.0';
										return (
											<button
												key={item.name}
												onClick={() => setSelectedSlice(item.name)}
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
									{selectedSlice} · {periodLabel}
								</p>
								<p className="text-lg font-bold tnum mt-1" style={{ color: 'var(--ledger-fg)' }}>
									−₹{formatRupeesPaise(sliceTotal)}
									<span className="text-xs font-medium ml-2" style={{ color: 'var(--ledger-muted)' }}>
										{sliceTransactions.length} txn{sliceTransactions.length !== 1 ? 's' : ''}
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
							{sliceTransactions.length === 0 && (
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