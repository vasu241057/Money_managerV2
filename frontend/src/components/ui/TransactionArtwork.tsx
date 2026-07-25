import { createElement, useState } from 'react';
import {
	AppWindow, ArrowDownCircle, ArrowLeftRight, ArrowUpCircle, Armchair, Award,
	Baby, BadgePercent, BedDouble, Bike, Bitcoin, Blocks, Bone, Book, BookOpen,
	Briefcase, Building, Bus, Car, CarTaxiFront, Circle, CircleDashed, CircleParking,
	CirclePlus, Clapperboard, Clock, Coffee, CreditCard, Droplets, Dumbbell, Film,
	Flame, Flower2, Fuel, Gamepad2, Gift, GraduationCap, Hammer, HandHelping,
	HeartHandshake, HeartPulse, Home, IdCard, Landmark, Laptop, LineChart, MapPin,
	MonitorPlay, Palette, PawPrint, Percent, Pill, Plane, Receipt, ReceiptText,
	Repeat, Scissors, School, Shield, Shirt, ShoppingBag, ShoppingCart, Signal,
	Smartphone, Sparkles, Stethoscope, Store, TestTube, Ticket, TrainFront,
	TrendingUp, Tv, Undo2, Utensils, UtensilsCrossed, Wallet, Wifi, Wrench,
	Zap, PackageOpen, type LucideIcon,
} from 'lucide-react';
import type { Transaction } from '../../types/domain';
import type { MerchantArtworkPhase } from '../../hooks/useMerchantArtworkPhase';
import '../../styles/merchant-logo-flip.css';

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

// Category names are the semantic source of truth. The stored icon token is a
// fallback for user-created and future categories.
const CATEGORY_NAME_ICONS: Record<string, LucideIcon> = {
	Expense: Circle,
	Income: ArrowDownCircle,
	Transfer: ArrowLeftRight,
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
	Groceries: ShoppingCart,
	Restaurants: UtensilsCrossed,
	'Food Delivery': PackageOpen,
	'Snacks & Beverages': Coffee,
	Fuel,
	'Public Transport': Bus,
	'Taxi & Rideshare': CarTaxiFront,
	'Parking & Tolls': CircleParking,
	'Vehicle Maintenance': Wrench,
	'Clothing & Fashion': Shirt,
	Electronics: Smartphone,
	'Home & Furniture': Armchair,
	'Beauty & Cosmetics': Sparkles,
	Electricity: Zap,
	Water: Droplets,
	Gas: Flame,
	'Internet & Broadband': Wifi,
	'Mobile Recharge': Signal,
	'DTH & Cable': Tv,
	'Doctor & Consultation': Stethoscope,
	'Medicines & Pharmacy': Pill,
	'Lab Tests': TestTube,
	'Fitness & Gym': Dumbbell,
	'Movies & Shows': Clapperboard,
	Games: Gamepad2,
	'Events & Concerts': Ticket,
	'OTT & Streaming': MonitorPlay,
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
	'Salon & Grooming': Scissors,
	'Spa & Wellness': Flower2,
	'Loan EMI': Landmark,
	'Bank Fees & Charges': BadgePercent,
	'Insurance Premiums': Shield,
	Taxes: Receipt,
	'Gifts Given': Gift,
	'Charity & Donations': HeartHandshake,
	'Pet Food & Supplies': Bone,
	'Vet & Grooming': PawPrint,
	'Software & Apps': AppWindow,
	Memberships: IdCard,
	Childcare: Baby,
	'Kids Activities': Blocks,
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
	return CATEGORY_NAME_ICONS[transaction.category]
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

interface TransactionArtworkProps {
	transaction: Transaction;
	phase: MerchantArtworkPhase;
	size?: 'default' | 'compact';
}

export function TransactionArtwork({
	transaction,
	phase,
	size = 'default',
}: TransactionArtworkProps) {
	const [failedMerchantLogoUrl, setFailedMerchantLogoUrl] = useState<string | null>(null);
	const merchantLogoUrl = transaction.merchantLogoUrl ?? null;
	const hasUsableMerchantLogo = merchantLogoUrl !== null
		&& merchantLogoUrl !== failedMerchantLogoUrl;
	const showMerchantFace = phase.showMerchantLogos && hasUsableMerchantLogo;
	const animationClass = hasUsableMerchantLogo && phase.animationCycle > 0
		? showMerchantFace
			? ' ledger-artwork__rotor--flip-to-merchant'
			: ' ledger-artwork__rotor--flip-to-category'
		: '';
	const iconColor = categoryColorFor(transaction);

	return (
		<div
			className={`ledger-artwork${size === 'compact' ? ' ledger-artwork--compact' : ''}`}
			data-artwork-face={showMerchantFace ? 'merchant' : 'category'}
			role="img"
			aria-label={`${transaction.category} category${transaction.merchantName ? `, ${transaction.merchantName} merchant` : ''}`}
		>
			<div className={`ledger-artwork__rotor${animationClass}`}>
				<span
					className="ledger-artwork__face ledger-artwork__face--category"
					style={{
						background: `color-mix(in srgb, ${iconColor} 11%, transparent)`,
						color: iconColor,
					}}
				>
					{createElement(categoryIconFor(transaction), {
						size: size === 'compact' ? 16 : 17,
						strokeWidth: 1.9,
					})}
				</span>
				<span className="ledger-artwork__face ledger-artwork__face--merchant">
					{merchantLogoUrl && (
						<img
							src={merchantLogoUrl}
							alt=""
							loading="lazy"
							decoding="async"
							onError={() => setFailedMerchantLogoUrl(merchantLogoUrl)}
						/>
					)}
				</span>
			</div>
		</div>
	);
}
