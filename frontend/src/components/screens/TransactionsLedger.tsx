import { createElement, useMemo, useState } from 'react';
import {
	AppWindow, ArrowDownCircle, ArrowLeftRight, ArrowUpCircle, Armchair, Award,
	Baby, BadgePercent, BedDouble, Bike, Bitcoin, Blocks, Bone, Book, BookOpen,
	Briefcase, Building, Bus, Car,
	CarTaxiFront, ChevronDown, ChevronRight, Circle, CircleDashed, CircleParking,
	CirclePlus, Clapperboard, Clock, Coffee, CreditCard, Droplets, Dumbbell, Film,
	Flame, Flower2, Fuel, Gamepad2,
	Gift, GraduationCap, Hammer, HandHelping, HeartHandshake, HeartPulse, Home,
	IdCard, Landmark, Laptop, LineChart, MapPin, MonitorPlay, Palette, PawPrint,
	PackageOpen, Percent, Pill, Plane, Plus, Receipt, ReceiptText, Repeat, Scissors, School,
	Shield, Shirt,
	ShoppingBag, ShoppingCart, Signal, SlidersHorizontal, Smartphone, Sparkles,
	Stethoscope, Store, TestTube, Ticket, TrainFront, TrendingUp, Tv, Undo2,
	Utensils, UtensilsCrossed, Wallet, Wifi, Wrench, Zap, type LucideIcon,
} from 'lucide-react';
import type { ScreenId } from '../layout/BottomNav';
import { useTransactions, type Transaction } from '../../hooks/useTransactions';
import { LedgerListSkeleton } from '../ui/Skeleton';

interface TransactionsLedgerProps {
	onNavigate: (screen: ScreenId) => void;
	onEditTransaction?: (t: Transaction) => void;
}

type FilterChip = 'all' | 'income' | 'expense' | 'review';

const CATEGORY_ICONS: Record<string, LucideIcon> = {
	AppWindow,
	ArrowDownCircle,
	ArrowLeftRight,
	ArrowUpCircle,
	Armchair,
	Award,
	Baby,
	BadgePercent,
	BedDouble,
	Bike,
	Bitcoin,
	Blocks,
	Bone,
	Book,
	BookOpen,
	Briefcase,
	Building,
	Bus,
	Car,
	CarTaxiFront,
	Circle,
	CircleDashed,
	CirclePlus,
	Clapperboard,
	Clock,
	Coffee,
	CreditCard,
	Droplets,
	Dumbbell,
	Film,
	Flame,
	Flower2,
	Fuel,
	Gamepad2,
	Gift,
	GraduationCap,
	Hammer,
	HandHelping,
	HeartHandshake,
	HeartPulse,
	Home,
	IdCard,
	Landmark,
	Laptop,
	LineChart,
	MapPin,
	MonitorPlay,
	Palette,
	ParkingCircle: CircleParking,
	PawPrint,
	Percent,
	Pill,
	Plane,
	Receipt,
	ReceiptText,
	Repeat,
	Scissors,
	School,
	Shield,
	Shirt,
	ShoppingBag,
	ShoppingCart,
	Signal,
	Smartphone,
	Sparkles,
	Stethoscope,
	Store,
	TestTube,
	Ticket,
	TrainFront,
	TrendingUp,
	Tv,
	Undo2,
	Utensils,
	UtensilsCrossed,
	Wallet,
	Wifi,
	Wrench,
	Zap,
};

// Category names are the semantic source of truth for the ledger. The stored
// icon token remains a fallback for user-created and future categories, but
// the complete system taxonomy is mapped here so a transport-looking glyph
// never stands in for food delivery (and a painting palette never means beauty).
const CATEGORY_NAME_ICONS: Record<string, LucideIcon> = {
	// Structural fallbacks (normally never assigned directly)
	Expense: Circle,
	Income: ArrowDownCircle,
	Transfer: ArrowLeftRight,

	// Expense parents
	Food: Utensils,
	Transport: Car,
	Shopping: ShoppingBag,
	Bills: ReceiptText,
	Health: HeartPulse,
	Entertainment: Film,
	'Credit Card Spend': CreditCard,
	Housing: Home,
	Education: GraduationCap,
	Travel: Plane,
	'Personal Care': Sparkles,
	Financial: Landmark,
	'Gifts & Donations': Gift,
	Pets: PawPrint,
	Subscriptions: MonitorPlay,
	'Family & Kids': Baby,
	Miscellaneous: Circle,

	// Income and transfer parents
	Salary: Briefcase,
	Refund: Undo2,
	Cashback: Gift,
	'Freelance & Business': Briefcase,
	Investments: TrendingUp,
	'Gifts Received': Gift,
	'Other Income': CirclePlus,
	'Card Bill Payment': CreditCard,
	'Wallet Transfer': Wallet,
	'Broker Transfer': LineChart,
	'Self Transfer': ArrowLeftRight,
	'Other Transfer': CircleDashed,

	// Food
	Groceries: ShoppingCart,
	Restaurants: UtensilsCrossed,
	'Food Delivery': PackageOpen,
	'Snacks & Beverages': Coffee,

	// Transport
	Fuel: Fuel,
	'Public Transport': Bus,
	'Taxi & Rideshare': CarTaxiFront,
	'Parking & Tolls': CircleParking,
	'Vehicle Maintenance': Wrench,

	// Shopping
	'Clothing & Fashion': Shirt,
	Electronics: Smartphone,
	'Home & Furniture': Armchair,
	'Beauty & Cosmetics': Sparkles,

	// Bills
	Electricity: Zap,
	Water: Droplets,
	Gas: Flame,
	'Internet & Broadband': Wifi,
	'Mobile Recharge': Signal,
	'DTH & Cable': Tv,

	// Health
	'Doctor & Consultation': Stethoscope,
	'Medicines & Pharmacy': Pill,
	'Lab Tests': TestTube,
	'Fitness & Gym': Dumbbell,

	// Entertainment
	'Movies & Shows': Clapperboard,
	Games: Gamepad2,
	'Events & Concerts': Ticket,
	'OTT & Streaming': MonitorPlay,

	// Housing, education, and travel
	Rent: Home,
	'Home Maintenance': Hammer,
	'House Help': HandHelping,
	'School & College Fees': School,
	'Courses & Coaching': BookOpen,
	'Books & Supplies': Book,
	Flights: Plane,
	'Trains & Buses': TrainFront,
	'Hotels & Stays': BedDouble,
	'Trip Activities': MapPin,

	// Personal care and financial
	'Salon & Grooming': Scissors,
	'Spa & Wellness': Flower2,
	'Loan EMI': Landmark,
	'Bank Fees & Charges': BadgePercent,
	'Insurance Premiums': Shield,
	Taxes: Receipt,

	// Gifts, pets, subscriptions, and family
	'Gifts Given': Gift,
	'Charity & Donations': HeartHandshake,
	'Pet Food & Supplies': Bone,
	'Vet & Grooming': PawPrint,
	'Software & Apps': AppWindow,
	Memberships: IdCard,
	Childcare: Baby,
	'Kids Activities': Blocks,

	// Income subcategories
	'Full-time Salary': Briefcase,
	'Bonus & Incentives': Award,
	'Part-time & Stipend': Clock,
	'Freelance Projects': Laptop,
	'Business Income': Store,
	'Stocks & Mutual Funds': LineChart,
	'Dividends & Interest': Percent,
	Crypto: Bitcoin,
	'Rental Income': Building,
	'Purchase Refund': Undo2,
	Reimbursements: ReceiptText,
};

function categoryIconFor(transaction: Transaction): LucideIcon {
	return CATEGORY_NAME_ICONS[transaction.subCategory ?? '']
		?? CATEGORY_NAME_ICONS[transaction.category]
		?? CATEGORY_ICONS[transaction.categoryIcon ?? '']
		?? Circle;
}

function categoryColorFor(transaction: Transaction): string {
	if (transaction.type === 'income') return 'var(--ledger-positive)';
	if (transaction.type === 'transfer') return 'var(--ledger-muted)';

	if (['Transport', 'Travel'].includes(transaction.category)) {
		return 'var(--ledger-category-blue)';
	}
	if (['Shopping', 'Entertainment', 'Personal Care', 'Subscriptions'].includes(transaction.category)) {
		return 'var(--ledger-category-plum)';
	}
	if (['Health', 'Family & Kids', 'Pets', 'Gifts & Donations'].includes(transaction.category)) {
		return 'var(--ledger-category-rose)';
	}
	if (['Food', 'Bills', 'Housing', 'Education'].includes(transaction.category)) {
		return 'var(--ledger-amber)';
	}
	return 'var(--ledger-accent)';
}

function transactionPrimaryLabel(transaction: Transaction): string {
	return transaction.merchantName
		?? transaction.description
		?? transaction.counterpartyRaw
		?? transaction.subCategory
		?? transaction.category
		?? 'Transaction';
}

function paymentSourceLabel(transaction: Transaction): string | null {
	if (transaction.creditCardLabel) {
		return transaction.instrumentLast4
			? `${transaction.creditCardLabel} ·· ${transaction.instrumentLast4}`
			: transaction.creditCardLabel;
	}
	return transaction.accountName ?? null;
}

function transactionMetaLabel(transaction: Transaction, viewMode: 'daily' | 'monthly'): string {
	const primaryLabel = transactionPrimaryLabel(transaction);
	const noteLabel = transaction.description !== primaryLabel ? transaction.description : null;
	const categoryLabel = transaction.subCategory ?? transaction.category;
	const paymentSource = paymentSourceLabel(transaction);
	const transactionDay = viewMode === 'monthly'
		? transaction.date.toLocaleDateString('en-IN', { day: 'numeric', month: 'short' })
		: null;

	return [
		noteLabel,
		paymentSource,
		categoryLabel !== primaryLabel ? categoryLabel : null,
		transactionDay,
	].filter(Boolean).join(' · ');
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
	onEditTransaction?: (transaction: Transaction) => void;
}

function LedgerTransactionRow({
	transaction,
	isLast,
	viewMode,
	onEditTransaction,
}: LedgerTransactionRowProps) {
	const isIncome = transaction.type === 'income';
	const isTransfer = transaction.type === 'transfer';
	const isReview = transaction.status === 'NEEDS_REVIEW';
	const iconColor = categoryColorFor(transaction);
	const primaryLabel = transactionPrimaryLabel(transaction);
	const metaLabel = transactionMetaLabel(transaction, viewMode);

	return (
		<button
			onClick={() => onEditTransaction?.(transaction)}
			className="w-full flex items-center justify-between px-4 py-3.5 text-left transition-colors hover:bg-black/[0.02] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset"
			style={!isLast ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
		>
			<div className="flex items-center gap-3 min-w-0 flex-1">
				<div
					className="w-10 h-10 rounded-[10px] flex items-center justify-center shrink-0"
					style={{
						background: `color-mix(in srgb, ${iconColor} 11%, transparent)`,
						color: iconColor,
					}}
					role="img"
					aria-label={`${transaction.subCategory ?? transaction.category} category`}
				>
					{createElement(categoryIconFor(transaction), { size: 17, strokeWidth: 1.9 })}
				</div>
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
	const [filtersOpen, setFiltersOpen] = useState(false);
	const [expandedMonthKey, setExpandedMonthKey] = useState('');

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
					style={filtersOpen || filterChip !== 'all'
						? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
						: { background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
					aria-expanded={filtersOpen}
					aria-label="Transaction filters"
				>
					<SlidersHorizontal size={15} />
					{filterChip !== 'all' && (
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
				<div
					className="px-5 py-3 flex flex-wrap items-center gap-2 shrink-0"
					style={{ borderBottom: '1px solid var(--ledger-rule)', background: 'var(--ledger-card)' }}
				>
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
