import { useMemo, useState } from 'react';
import {
	AlertCircle,
	ArrowDownLeft,
	ArrowRight,
	CalendarDays,
	Check,
	ChevronDown,
	ChevronLeft,
	ChevronRight,
	CircleDollarSign,
	Gauge,
	Link2,
	Loader2,
	Pencil,
	Repeat2,
	RotateCcw,
	ShieldCheck,
	X,
} from 'lucide-react';
import { useBudget } from '../../hooks/useBudget';
import { useCategories } from '../../hooks/useCategories';
import { paiseToRupees, rupeesToPaise } from '../../lib/money';

interface BudgetScreenProps {
	onOpenBudgetLedger: () => void;
}

interface FixedCostCandidate {
	id: string;
	label: string;
	trackedCategoryIds: string[];
}

const FIXED_COST_PATTERN = /(emi|loan|rent|insurance|subscription|software|membership)/i;

function formatMoney(paise: number, showSign = false): string {
	const value = paiseToRupees(Math.abs(paise)).toLocaleString('en-IN', {
		minimumFractionDigits: 0,
		maximumFractionDigits: 0,
	});
	const sign = showSign ? (paise < 0 ? '−' : '+') : paise < 0 ? '−' : '';
	return `${sign}₹${value}`;
}

function shiftMonth(month: string, offset: number): string {
	const [year, monthNumber] = month.split('-').map(Number);
	const next = new Date(Date.UTC(year, monthNumber - 1 + offset, 1));
	return `${next.getUTCFullYear()}-${String(next.getUTCMonth() + 1).padStart(2, '0')}`;
}

function monthLabel(month: string, includeYear = true): string {
	const [year, monthNumber] = month.split('-').map(Number);
	return new Date(Date.UTC(year, monthNumber - 1, 1)).toLocaleDateString('en-IN', {
		month: 'long',
		year: includeYear ? 'numeric' : undefined,
		timeZone: 'UTC',
	});
}

function monthFromTimestamp(value: string): string {
	const parts = new Intl.DateTimeFormat('en-CA', {
		timeZone: 'Asia/Kolkata',
		year: 'numeric',
		month: '2-digit',
	}).formatToParts(new Date(value));
	return `${parts.find(part => part.type === 'year')?.value}-${parts.find(part => part.type === 'month')?.value}`;
}

function getMonthTiming(month: string, currentMonth: string) {
	const [year, monthNumber] = month.split('-').map(Number);
	const daysInMonth = new Date(Date.UTC(year, monthNumber, 0)).getUTCDate();
	if (month < currentMonth) {
		return { phase: 'PAST' as const, day: daysInMonth, daysInMonth, daysLeft: 0 };
	}
	if (month > currentMonth) {
		return { phase: 'FUTURE' as const, day: 0, daysInMonth, daysLeft: daysInMonth };
	}
	const today = Number(new Intl.DateTimeFormat('en-CA', {
		timeZone: 'Asia/Kolkata',
		day: 'numeric',
	}).format(new Date()));
	return {
		phase: 'CURRENT' as const,
		day: today,
		daysInMonth,
		daysLeft: daysInMonth - today + 1,
	};
}

function SectionHeading({
	title,
	caption,
	action,
}: {
	title: string;
	caption?: string;
	action?: React.ReactNode;
}) {
	return (
		<div className="mb-3 flex items-end justify-between gap-4">
			<div>
				<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>{title}</p>
				{caption && <p className="mt-1 text-xs leading-relaxed" style={{ color: 'var(--ledger-muted)' }}>{caption}</p>}
			</div>
			{action}
		</div>
	);
}

function BudgetSkeleton() {
	return (
		<div className="budget-skeleton" aria-label="Loading monthly budget">
			<div className="budget-skeleton__hero" />
			<div className="budget-skeleton__line w-28" />
			<div className="budget-skeleton__card" />
			<div className="budget-skeleton__line w-36" />
			<div className="budget-skeleton__card h-36" />
		</div>
	);
}

export function BudgetScreen({ onOpenBudgetLedger }: BudgetScreenProps) {
	const currentMonth = new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' }).slice(0, 7);
	const [month, setMonth] = useState(currentMonth);
	const {
		overview,
		isLoading,
		isSaving,
		error,
		retry,
		savePlan,
		linkRefund,
		dismissSuggestion,
		confirmRecurring,
		updateRecurring,
	} = useBudget(month);
	const { categories } = useCategories();
	const [editingPlan, setEditingPlan] = useState(false);
	const [limitInputOverride, setLimitInputOverride] = useState<string | null>(null);
	const [outsideCategoryIdsOverride, setOutsideCategoryIdsOverride] = useState<Set<string> | null>(null);
	const [saveError, setSaveError] = useState<string | null>(null);
	const [showAllCategories, setShowAllCategories] = useState(false);
	const [rulesOpen, setRulesOpen] = useState(false);

	const fixedCostCandidates = useMemo(() => {
		const result: FixedCostCandidate[] = [];
		for (const category of categories.filter(item => item.type === 'expense')) {
			const childIds = Object.values(category.subCategoryIds ?? {});
			const parentMatches = FIXED_COST_PATTERN.test(category.name);
			if (parentMatches) {
				result.push({
					id: category.id,
					label: category.name,
					trackedCategoryIds: [category.id, ...childIds],
				});
				continue;
			}
			for (const subCategory of category.subCategories) {
				if (!FIXED_COST_PATTERN.test(subCategory)) continue;
				const id = category.subCategoryIds?.[subCategory];
				if (id) result.push({ id, label: `${category.name} · ${subCategory}`, trackedCategoryIds: [id] });
			}
		}
		return result.filter((item, index) => result.findIndex(other => other.id === item.id) === index);
	}, [categories]);

	const summary = overview?.summary ?? null;
	const limitInput = limitInputOverride ?? (
		summary?.limit_in_paise == null ? '' : String(paiseToRupees(summary.limit_in_paise))
	);
	const outsideCategoryIds = outsideCategoryIdsOverride ?? new Set(
		overview?.category_rules
			.filter(rule => rule.treatment === 'EXCLUDE')
			.map(rule => rule.category_id) ?? [],
	);
	const fixedSpendByCandidate = useMemo(() => {
		const amounts = new Map(summary?.categories.map(category => [category.category_id, category.net_in_paise]) ?? []);
		return new Map(fixedCostCandidates.map(candidate => [
			candidate.id,
			candidate.trackedCategoryIds.reduce((total, id) => total + (amounts.get(id) ?? 0), 0),
		]));
	}, [fixedCostCandidates, summary?.categories]);

	const refundSuggestions = overview?.suggestions.filter(suggestion => suggestion.type === 'REFUND_MATCH') ?? [];
	const recurringSuggestions = overview?.suggestions.filter(suggestion => suggestion.type === 'RECURRING_PATTERN') ?? [];
	const decisionCount = refundSuggestions.length + recurringSuggestions.length;
	const hasPlan = summary?.limit_in_paise !== null && summary?.limit_in_paise !== undefined;
	const timing = getMonthTiming(month, currentMonth);
	const rawUsedPercent = hasPlan && summary
		? (summary.used_in_paise / (summary.limit_in_paise ?? 1)) * 100
		: 0;
	const usedPercent = Math.min(100, Math.max(0, rawUsedPercent));
	const todayPercent = timing.phase === 'CURRENT' ? (timing.day / timing.daysInMonth) * 100 : null;
	const remaining = summary?.remaining_in_paise ?? 0;
	const dailyAllowance = timing.phase === 'CURRENT' && remaining > 0
		? Math.floor(remaining / timing.daysLeft)
		: 0;
	const expectedSpend = hasPlan && summary && timing.phase === 'CURRENT'
		? Math.floor((summary.limit_in_paise ?? 0) * timing.day / timing.daysInMonth)
		: null;
	const paceDifference = expectedSpend === null || !summary ? null : expectedSpend - summary.used_in_paise;
	const categoryMax = Math.max(1, ...(summary?.categories.map(category => category.net_in_paise) ?? [1]));
	const shownCategories = showAllCategories ? summary?.categories ?? [] : summary?.categories.slice(0, 5) ?? [];
	const activeRecurringCount = overview?.recurring_series.filter(series => series.status === 'ACTIVE').length ?? 0;
	const allFixedOutside = fixedCostCandidates.length > 0
		&& fixedCostCandidates.every(candidate =>
			candidate.trackedCategoryIds.every(categoryId => outsideCategoryIds.has(categoryId)),
		);

	const moveToMonth = (offset: number) => {
		setLimitInputOverride(null);
		setOutsideCategoryIdsOverride(null);
		setEditingPlan(false);
		setShowAllCategories(false);
		setMonth(current => shiftMonth(current, offset));
	};

	const returnToCurrentMonth = () => {
		setLimitInputOverride(null);
		setOutsideCategoryIdsOverride(null);
		setEditingPlan(false);
		setShowAllCategories(false);
		setMonth(currentMonth);
	};

	const saveCurrentPlan = async () => {
		const parsedLimit = Number(limitInput.replace(/[₹,\s]/g, ''));
		if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
			setSaveError('Enter a monthly limit greater than zero.');
			return;
		}
		setSaveError(null);
		try {
			const categoryRules = fixedCostCandidates.flatMap(candidate =>
				candidate.trackedCategoryIds.map(categoryId => ({
					category_id: categoryId,
					treatment: outsideCategoryIds.has(categoryId) ? 'EXCLUDE' as const : null,
				})),
			);
			await savePlan(
				rupeesToPaise(parsedLimit),
				categoryRules,
			);
			setEditingPlan(false);
			setLimitInputOverride(null);
			setOutsideCategoryIdsOverride(null);
		} catch (cause) {
			setSaveError(cause instanceof Error ? cause.message : 'Could not save the monthly plan.');
		}
	};

	const runBudgetAction = (action: Promise<unknown>, fallbackMessage: string) => {
		setSaveError(null);
		void action.catch(cause => {
			setSaveError(cause instanceof Error ? cause.message : fallbackMessage);
		});
	};

	return (
		<div className="flex-1 flex flex-col min-h-0 relative" style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}>
			<header className="h-[56px] shrink-0 flex items-center justify-between px-5 z-10" style={{ borderBottom: '1px solid var(--ledger-rule)' }}>
				<div className="flex items-center gap-2.5">
					<Gauge size={19} strokeWidth={2.2} style={{ color: 'var(--ledger-accent)' }} />
					<h1 className="font-bold text-xl tracking-tight">Budget</h1>
				</div>
				{hasPlan && !editingPlan ? (
					<button type="button" onClick={() => setEditingPlan(true)} className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }} aria-label="Edit monthly plan">
						<Pencil size={14} />
					</button>
				) : <div className="w-8 h-8" />}
			</header>

			{hasPlan && !editingPlan && (
				<div className="px-4 py-2.5 flex items-center justify-between shrink-0" style={{ borderBottom: '1px solid var(--ledger-rule)' }}>
					<button type="button" onClick={() => moveToMonth(-1)} className="w-9 h-9 rounded-full flex items-center justify-center" style={{ color: 'var(--ledger-muted)' }} aria-label="Previous month"><ChevronLeft size={18} /></button>
					<div className="text-center">
						<p className="text-sm font-bold tnum">{monthLabel(month)}</p>
						{month !== currentMonth && <button type="button" onClick={returnToCurrentMonth} className="mt-0.5 text-[10px] font-bold" style={{ color: 'var(--ledger-accent)' }}>Back to this month</button>}
					</div>
					<button type="button" onClick={() => moveToMonth(1)} className="w-9 h-9 rounded-full flex items-center justify-center" style={{ color: 'var(--ledger-muted)' }} aria-label="Next month"><ChevronRight size={18} /></button>
				</div>
			)}

			<main className="flex-1 overflow-y-auto px-5 pt-5 pb-24 min-h-0">
				{(error || saveError) && (
					<div className="mb-4 rounded-[12px] p-3.5 flex items-start gap-3" style={{ background: 'var(--ledger-amber-bg)', border: '1px solid color-mix(in srgb, var(--ledger-negative) 25%, var(--ledger-rule))' }}>
						<AlertCircle size={17} className="shrink-0 mt-0.5" style={{ color: 'var(--ledger-negative)' }} />
						<div className="min-w-0 flex-1"><p className="text-xs font-semibold">{saveError ?? error}</p><button type="button" onClick={() => { setSaveError(null); void retry(); }} className="mt-1.5 text-[10px] font-bold" style={{ color: 'var(--ledger-accent)' }}>Try again</button></div>
					</div>
				)}

				{isLoading && !overview ? (
					<BudgetSkeleton />
				) : editingPlan || !hasPlan ? (
					<section>
						<div className="mb-6">
							<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-accent)' }}>{hasPlan ? 'Edit monthly plan' : 'Start a monthly plan'}</p>
							<h2 className="text-2xl font-bold tracking-tight mt-2">Set your flexible spending limit</h2>
							<p className="text-sm leading-relaxed mt-2" style={{ color: 'var(--ledger-muted)' }}>This is the amount you want to watch day to day. Cash flow still includes salary, transfers, and fixed commitments.</p>
						</div>

						<label className="block mb-7">
							<span className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>Monthly limit</span>
							<div className="budget-limit-input mt-2 flex items-baseline rounded-[14px] px-4 py-4" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
								<span className="text-xl" style={{ color: 'var(--ledger-muted)' }}>₹</span>
								<input value={limitInput} onChange={event => setLimitInputOverride(event.target.value)} inputMode="decimal" placeholder="20,000" aria-label="Monthly spending limit in rupees" className="min-w-0 flex-1 bg-transparent outline-none text-3xl font-bold tnum ml-2" style={{ fontSize: '28px' }} />
							</div>
						</label>

						{fixedCostCandidates.length > 0 && (
							<section className="mb-7">
								<SectionHeading
									title="Fixed commitments"
									caption="These choices apply across months. You can still override any transaction later."
									action={<button type="button" onClick={() => setOutsideCategoryIdsOverride(new Set(allFixedOutside ? [] : fixedCostCandidates.flatMap(candidate => candidate.trackedCategoryIds)))} className="shrink-0 text-[10px] font-bold" style={{ color: 'var(--ledger-accent)' }}>{allFixedOutside ? 'Count all' : 'Move all outside'}</button>}
								/>
								<div className="rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
									{fixedCostCandidates.map((candidate, index) => {
										const outside = candidate.trackedCategoryIds.every(categoryId => outsideCategoryIds.has(categoryId));
										const spent = fixedSpendByCandidate.get(candidate.id) ?? 0;
										return (
											<button key={candidate.id} type="button" onClick={() => setOutsideCategoryIdsOverride(current => {
											const next = new Set(current ?? outsideCategoryIds);
											for (const categoryId of candidate.trackedCategoryIds) {
												if (outside) next.delete(categoryId); else next.add(categoryId);
											}
												return next;
											})} className="w-full flex items-center gap-3 px-4 py-3 text-left" style={index < fixedCostCandidates.length - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}>
												<span className="w-5 h-5 rounded-[6px] flex items-center justify-center shrink-0" style={outside ? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' } : { border: '1px solid var(--ledger-rule)', color: 'transparent' }}><Check size={13} /></span>
												<span className="min-w-0 flex-1"><span className="block text-sm font-semibold truncate">{candidate.label}</span>{spent > 0 && <span className="block text-[10px] mt-0.5 tnum" style={{ color: 'var(--ledger-muted)' }}>{formatMoney(spent)} this month</span>}</span>
												<span className="text-[10px] font-bold" style={{ color: outside ? 'var(--ledger-muted)' : 'var(--ledger-accent)' }}>{outside ? 'Outside' : 'Counted'}</span>
											</button>
										);
									})}
								</div>
							</section>
						)}

						<div className="flex gap-2">
							{hasPlan && <button type="button" onClick={() => { setLimitInputOverride(null); setOutsideCategoryIdsOverride(null); setEditingPlan(false); }} className="h-12 px-5 rounded-[10px] text-sm font-bold" style={{ border: '1px solid var(--ledger-rule)', color: 'var(--ledger-muted)' }}>Cancel</button>}
							<button type="button" onClick={() => { void saveCurrentPlan(); }} disabled={isSaving} className="h-12 flex-1 rounded-[10px] text-sm font-bold flex items-center justify-center gap-2" style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)', opacity: isSaving ? 0.6 : 1 }}>
								{isSaving && <Loader2 size={15} className="animate-spin" />} Save monthly plan
							</button>
						</div>
					</section>
				) : summary ? (
					<>
						{summary.limit_source_month && summary.limit_source_month !== month && (
							<div className="mb-3 flex items-center gap-2 rounded-[10px] px-3 py-2" style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}>
								<RotateCcw size={13} className="shrink-0" /><p className="text-[10px] font-semibold">Limit carried forward from {monthLabel(summary.limit_source_month, false)}. Editing creates a plan for {monthLabel(month, false)}.</p>
							</div>
						)}

						<section className="budget-runway rounded-[18px] p-5 mb-6" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
							<div className="flex items-start justify-between gap-4">
								<div className="min-w-0">
									<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: remaining < 0 ? 'var(--ledger-negative)' : 'var(--ledger-muted)' }}>{remaining < 0 ? 'Over plan by' : timing.phase === 'PAST' ? 'Left unspent' : timing.phase === 'FUTURE' ? 'Planned room' : 'Safe to spend'}</p>
									<p className="text-4xl font-bold tnum tracking-tight mt-1" style={{ color: remaining < 0 ? 'var(--ledger-negative)' : 'var(--ledger-fg)' }}>{formatMoney(Math.abs(remaining))}</p>
								</div>
								<div className="shrink-0 text-right">
									<span className="w-10 h-10 ml-auto rounded-full flex items-center justify-center" style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}><CalendarDays size={18} /></span>
									<p className="mt-1.5 text-[10px] font-bold" style={{ color: 'var(--ledger-muted)' }}>{timing.phase === 'CURRENT' ? `${timing.daysLeft} day${timing.daysLeft === 1 ? '' : 's'} left` : timing.phase === 'PAST' ? 'Month closed' : 'Upcoming month'}</p>
								</div>
							</div>

							{timing.phase === 'CURRENT' && (
								<div className="mt-5 flex items-end justify-between gap-3">
									<div><p className="text-lg font-bold tnum" style={{ color: remaining < 0 ? 'var(--ledger-negative)' : 'var(--ledger-accent)' }}>{remaining < 0 ? '₹0/day' : `${formatMoney(dailyAllowance)}/day`}</p><p className="text-[10px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>{remaining < 0 ? 'Pause flexible spending to recover' : 'Available through month end'}</p></div>
									{paceDifference !== null && <p className="max-w-[145px] text-right text-[10px] font-semibold leading-snug" style={{ color: paceDifference >= 0 ? 'var(--ledger-positive)' : 'var(--ledger-negative)' }}>{paceDifference >= 0 ? `${formatMoney(paceDifference)} below today’s pace` : `${formatMoney(Math.abs(paceDifference))} more spent than today’s pace`}</p>}
								</div>
							)}

							<div className="mt-5">
								<div className="budget-runway__rail" role="progressbar" aria-label="Monthly plan used" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(usedPercent)}>
									<div className="budget-runway__fill" style={{ width: `${usedPercent}%`, background: rawUsedPercent >= 100 ? 'var(--ledger-negative)' : 'var(--ledger-accent)' }} />
									{todayPercent !== null && <span className="budget-runway__today" style={{ left: `${todayPercent}%` }}><span>Today</span></span>}
								</div>
								<div className="flex justify-between mt-4 text-[10px] tnum" style={{ color: 'var(--ledger-muted)' }}><span>{formatMoney(summary.used_in_paise)} used</span><span>{formatMoney(summary.limit_in_paise ?? 0)} limit</span></div>
							</div>
						</section>

						{decisionCount > 0 && (
							<section className="mb-7">
								<SectionHeading title="Decisions" caption="Confirm only what should change your available budget." action={<span className="rounded-full px-2 py-1 text-[10px] font-bold" style={{ background: 'var(--ledger-amber-bg)', color: 'var(--ledger-amber)' }}>{decisionCount}</span>} />
								<div className="flex flex-col gap-2.5">
									{refundSuggestions.map(suggestion => {
										const purchaseMonth = monthFromTimestamp(suggestion.target_txn_date);
										return (
											<article key={suggestion.fingerprint} className="rounded-[14px] p-4" style={{ background: 'var(--ledger-card)', border: '1px solid color-mix(in srgb, var(--ledger-amber) 36%, var(--ledger-rule))' }}>
												<div className="flex items-start gap-3"><span className="w-9 h-9 rounded-[9px] flex items-center justify-center shrink-0" style={{ background: 'var(--ledger-amber-bg)', color: 'var(--ledger-amber)' }}><Link2 size={16} /></span><div className="min-w-0 flex-1"><p className="text-sm font-bold truncate">Money back from {suggestion.label}?</p><p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>{formatMoney(suggestion.source_amount_in_paise)} received · {suggestion.can_auto_link ? 'High-confidence card match' : 'Possible purchase match'}</p></div></div>
												<p className="mt-3 rounded-[9px] px-3 py-2 text-[11px] leading-relaxed" style={{ background: 'var(--ledger-bg)', color: suggestion.budget_impact_in_paise > 0 ? 'var(--ledger-positive)' : 'var(--ledger-muted)' }}>{suggestion.budget_impact_in_paise > 0 ? `Linking returns ${formatMoney(suggestion.budget_impact_in_paise)} to ${monthLabel(purchaseMonth, false)}’s budget.` : 'This purchase is outside the budget. Linking still keeps its history accurate.'}</p>
											<div className="mt-3 flex items-center justify-end gap-2"><button type="button" disabled={isSaving} onClick={() => runBudgetAction(dismissSuggestion('REFUND_MATCH', suggestion.fingerprint), 'Could not dismiss this match.')} className="rounded-full px-3 py-2 text-[11px] font-bold" style={{ color: 'var(--ledger-muted)' }}>Not related</button><button type="button" disabled={isSaving} onClick={() => runBudgetAction(linkRefund(suggestion.source_transaction_id, suggestion.target_transaction_id), 'Could not link this refund.')} className="rounded-full px-3.5 py-2 text-[11px] font-bold" style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }}>Link money back</button></div>
											</article>
										);
									})}

									{recurringSuggestions.map(suggestion => (
										<article key={suggestion.fingerprint} className="rounded-[14px] p-4" style={{ background: 'var(--ledger-card)', border: '1px solid color-mix(in srgb, var(--ledger-amber) 36%, var(--ledger-rule))' }}>
											<div className="flex items-start gap-3"><span className="w-9 h-9 rounded-[9px] flex items-center justify-center shrink-0" style={{ background: 'var(--ledger-amber-bg)', color: 'var(--ledger-amber)' }}><Repeat2 size={16} /></span><div className="min-w-0 flex-1"><p className="text-sm font-bold truncate">Recurring {suggestion.label} charge?</p><p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>{suggestion.occurrence_count} consistent {suggestion.cadence.toLowerCase()} charges · {formatMoney(suggestion.amount_min_in_paise)}–{formatMoney(suggestion.amount_max_in_paise)}</p></div><button type="button" disabled={isSaving} onClick={() => runBudgetAction(dismissSuggestion('RECURRING_PATTERN', suggestion.fingerprint), 'Could not dismiss this recurring pattern.')} className="w-8 h-8 rounded-full flex items-center justify-center shrink-0" style={{ color: 'var(--ledger-muted)' }} aria-label="Dismiss recurring suggestion"><X size={14} /></button></div>
											<p className="mt-3 text-[11px] leading-relaxed" style={{ color: 'var(--ledger-muted)' }}>Choose once and future matching charges will follow the same rule.</p>
											<div className="grid grid-cols-2 gap-2 mt-3"><button type="button" disabled={isSaving} onClick={() => runBudgetAction(confirmRecurring(suggestion.fingerprint, suggestion.transaction_ids, 'INCLUDE'), 'Could not save this recurring rule.')} className="rounded-[9px] py-2.5 text-[11px] font-bold" style={suggestion.suggested_treatment === 'INCLUDE' ? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' } : { border: '1px solid var(--ledger-rule)', color: 'var(--ledger-muted)' }}>Count charges</button><button type="button" disabled={isSaving} onClick={() => runBudgetAction(confirmRecurring(suggestion.fingerprint, suggestion.transaction_ids, 'EXCLUDE'), 'Could not save this recurring rule.')} className="rounded-[9px] py-2.5 text-[11px] font-bold" style={suggestion.suggested_treatment === 'EXCLUDE' ? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' } : { border: '1px solid var(--ledger-rule)', color: 'var(--ledger-muted)' }}>Keep outside</button></div>
										</article>
									))}
								</div>
							</section>
						)}

						<section className="mb-7">
							<SectionHeading title="This month" />
							<div className="grid grid-cols-3 rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
								{[
									{ label: 'Counted', value: summary.counted_spending_in_paise, icon: CircleDollarSign, color: 'var(--ledger-fg)' },
									{ label: 'Returned', value: summary.adjustments_in_paise, icon: ArrowDownLeft, color: 'var(--ledger-positive)' },
									{ label: 'Outside', value: summary.outside_plan_in_paise, icon: ShieldCheck, color: 'var(--ledger-muted)' },
								].map(({ label, value, icon: Icon, color }, index) => (
									<div key={label} className="min-w-0 px-3 py-3.5" style={index < 2 ? { borderRight: '1px solid var(--ledger-rule)' } : undefined}><Icon size={14} style={{ color }} /><p className="mt-2 text-sm font-bold tnum truncate" style={{ color }}>{formatMoney(value)}</p><p className="mt-0.5 text-[9px] font-bold uppercase tracking-wider" style={{ color: 'var(--ledger-muted)' }}>{label}</p></div>
								))}
							</div>
						</section>

						{summary.categories.length > 0 && (
							<section className="mb-7">
								<SectionHeading title="Where it went" action={<button type="button" onClick={onOpenBudgetLedger} className="text-xs font-bold flex items-center gap-1" style={{ color: 'var(--ledger-accent)' }}>Open ledger <ArrowRight size={13} /></button>} />
								<div className="rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
									{shownCategories.map((category, index) => (
										<div key={category.category_id ?? 'uncategorized'} className="px-4 py-3" style={index < shownCategories.length - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}>
											<div className="flex items-center justify-between gap-3"><span className="text-sm font-semibold truncate">{category.category_name}</span><span className="text-sm font-bold tnum shrink-0">{formatMoney(category.net_in_paise)}</span></div>
											<div className="mt-2 h-1 rounded-full overflow-hidden" style={{ background: 'var(--ledger-rule)' }}><div className="h-full rounded-full" style={{ width: `${Math.max(2, category.net_in_paise / categoryMax * 100)}%`, background: 'var(--ledger-accent)' }} /></div>
										</div>
									))}
									{summary.categories.length > 5 && <button type="button" onClick={() => setShowAllCategories(current => !current)} className="w-full py-3 text-[11px] font-bold flex items-center justify-center gap-1" style={{ color: 'var(--ledger-accent)', borderTop: '1px solid var(--ledger-rule)' }}>{showAllCategories ? 'Show less' : `Show ${summary.categories.length - 5} more`}<ChevronDown size={13} style={{ transform: showAllCategories ? 'rotate(180deg)' : undefined }} /></button>}
								</div>
							</section>
						)}

						{(overview?.recurring_series.length ?? 0) > 0 && (
							<section className="mb-7">
								<button type="button" onClick={() => setRulesOpen(current => !current)} className="w-full rounded-[14px] px-4 py-3.5 flex items-center gap-3 text-left" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }} aria-expanded={rulesOpen}>
									<span className="w-9 h-9 rounded-[9px] flex items-center justify-center" style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}><Repeat2 size={16} /></span><span className="min-w-0 flex-1"><span className="block text-sm font-bold">Recurring rules</span><span className="block text-[10px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>{activeRecurringCount} active · {(overview?.recurring_series.length ?? 0) - activeRecurringCount} paused</span></span><ChevronDown size={16} style={{ color: 'var(--ledger-muted)', transform: rulesOpen ? 'rotate(180deg)' : undefined }} />
								</button>
								{rulesOpen && <div className="mt-2 rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>{overview?.recurring_series.map((series, index) => (
									<div key={series.id} className="px-4 py-3" style={index < (overview?.recurring_series.length ?? 0) - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}>
										<div className="flex items-center justify-between gap-3"><div className="min-w-0"><p className="text-sm font-semibold truncate">{series.label}</p><p className="text-[10px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>{series.cadence === 'MONTHLY' ? 'Monthly' : 'Weekly'} · {series.status === 'PAUSED' ? 'Paused' : series.budget_treatment === 'EXCLUDE' ? 'Outside budget' : 'Counted'}</p></div><button type="button" disabled={isSaving} onClick={() => runBudgetAction(updateRecurring(series.id, { status: series.status === 'ACTIVE' ? 'PAUSED' : 'ACTIVE' }), 'Could not update this recurring rule.')} className="text-[10px] font-bold" style={{ color: series.status === 'PAUSED' ? 'var(--ledger-accent)' : 'var(--ledger-muted)' }}>{series.status === 'PAUSED' ? 'Resume' : 'Pause'}</button></div>
										{series.status === 'ACTIVE' && <div className="grid grid-cols-2 gap-1 mt-2 rounded-[8px] p-1" style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)' }}><button type="button" disabled={isSaving} onClick={() => runBudgetAction(updateRecurring(series.id, { treatment: 'INCLUDE' }), 'Could not update this recurring rule.')} className="rounded-[6px] py-1.5 text-[10px] font-bold" style={series.budget_treatment === 'INCLUDE' ? { background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' } : { color: 'var(--ledger-muted)' }}>Counted</button><button type="button" disabled={isSaving} onClick={() => runBudgetAction(updateRecurring(series.id, { treatment: 'EXCLUDE' }), 'Could not update this recurring rule.')} className="rounded-[6px] py-1.5 text-[10px] font-bold" style={series.budget_treatment === 'EXCLUDE' ? { background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' } : { color: 'var(--ledger-muted)' }}>Outside</button></div>}
									</div>
								))}</div>}
							</section>
						)}

						<div className="rounded-[12px] px-4 py-3 flex gap-3" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}><Gauge size={16} className="shrink-0 mt-0.5" style={{ color: 'var(--ledger-accent)' }} /><p className="text-[11px] leading-relaxed" style={{ color: 'var(--ledger-muted)' }}><strong style={{ color: 'var(--ledger-fg)' }}>Budget is your flexible runway.</strong> Cash flow and Analytics still show every real movement, including commitments and transfers.</p></div>
					</>
				) : null}
			</main>
		</div>
	);
}
