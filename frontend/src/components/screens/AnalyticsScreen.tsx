import { useMemo, useState } from 'react';
import {
	PieChart as RechartsPieChart, Pie, Cell, ResponsiveContainer, Tooltip,
} from 'recharts';
import { Loader2 } from 'lucide-react';
import { useTransactions } from '../../hooks/useTransactions';

type Period = 'thisMonth' | 'lastMonth';
type Breakdown = 'category' | 'account' | 'card';

interface BreakdownItem {
	name: string;
	value: number;
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

export function AnalyticsScreen() {
	const { transactions, isLoading } = useTransactions();
	const [period, setPeriod] = useState<Period>('thisMonth');
	const [breakdown, setBreakdown] = useState<Breakdown>('category');

	const now = new Date();
	const thisYear = now.getFullYear();
	const thisMonth = now.getMonth();
	const lastMonthDate = new Date(thisYear, thisMonth - 1, 1);
	const lastYear = lastMonthDate.getFullYear();
	const lastMonth = lastMonthDate.getMonth();

	const periodTransactions = useMemo(() => {
		return transactions.filter((t) => {
			if (period === 'thisMonth') return isSameMonth(t.date, thisYear, thisMonth);
			return isSameMonth(t.date, lastYear, lastMonth);
		});
	}, [transactions, period, thisYear, thisMonth, lastYear, lastMonth]);

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
			let key: string;
			if (breakdown === 'category') {
				key = t.subCategory ?? t.category ?? 'Uncategorized';
			} else if (breakdown === 'account') {
				key = t.accountName ?? 'No account';
			} else {
				key = t.creditCardLabel ?? 'No card';
			}
			map.set(key, (map.get(key) ?? 0) + t.amount);
		}
		return Array.from(map.entries())
			.map(([name, value]) => ({ name, value: Math.round(value) }))
			.sort((a, b) => b.value - a.value);
	}, [periodTransactions, breakdown]);

	const periodLabel = period === 'thisMonth' ? 'This Month' : 'Last Month';
	const totalExpense = totals.expense;

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
							className="rounded-[10px] p-5 mb-4"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
							<p
								className="text-[10px] font-bold uppercase tracking-[0.2em] mb-4"
								style={{ color: 'var(--ledger-muted)' }}
							>
								{periodLabel}
							</p>
							<div className="flex items-baseline gap-4">
								<div className="flex-1">
									<p
										className="text-[10px] font-bold uppercase tracking-wider mb-1"
										style={{ color: 'var(--ledger-muted)' }}
									>
										Income
									</p>
									<p className="text-lg font-bold tnum" style={{ color: 'var(--ledger-positive)' }}>
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
									<p className="text-lg font-bold tnum" style={{ color: 'var(--ledger-negative)' }}>
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
										className="text-lg font-bold tnum"
										style={{ color: totals.net >= 0 ? 'var(--ledger-positive)' : 'var(--ledger-negative)' }}
									>
										{totals.net >= 0 ? '+' : '−'}₹{formatRupees(Math.abs(totals.net))}
									</p>
								</div>
							</div>
						</div>

						<div className="flex gap-2 mb-3">
							{(['thisMonth', 'lastMonth'] as const).map((p) => (
								<button
									key={p}
									onClick={() => setPeriod(p)}
									className="rounded-full px-4 py-1.5 text-xs font-medium transition-colors"
									style={
										period === p
											? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
											: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
									}
								>
									{p === 'thisMonth' ? 'This Month' : 'Last Month'}
								</button>
							))}
						</div>

						<div className="flex gap-2 mb-4">
							{(['category', 'account', 'card'] as const).map((b) => (
								<button
									key={b}
									onClick={() => setBreakdown(b)}
									className="flex-1 rounded-[10px] px-3 py-2 text-xs font-medium transition-colors capitalize"
									style={
										breakdown === b
											? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
											: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
									}
								>
									{b === 'card' ? 'By Card' : b}
								</button>
							))}
						</div>

						{totalExpense === 0 ? (
							<div
								className="rounded-[10px] p-8 text-center"
								style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
							>
								<p className="text-sm font-medium" style={{ color: 'var(--ledger-muted)' }}>
									No expenses in {periodLabel.toLowerCase()}
								</p>
								<p className="text-xs mt-1" style={{ color: 'var(--ledger-muted)', opacity: 0.7 }}>
									Try the other period or add a transaction
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
											>
												{data.map((_, index) => (
													<Cell key={`cell-${index}`} fill={LEDGER_COLORS[index % LEDGER_COLORS.length]} />
												))}
											</Pie>
											<Tooltip
												formatter={(value: number | string | undefined) => {
													const amount = typeof value === 'number' ? value : Number(value ?? 0);
													return [`₹${formatRupeesPaise(amount)}`, ''];
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
											<div
												key={item.name}
												className="flex items-center justify-between py-2.5"
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
											</div>
										);
									})}
								</div>
							</div>
						)}
					</>
				)}
			</main>
		</div>
	);
}