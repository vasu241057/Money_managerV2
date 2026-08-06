import { useEffect, useMemo, useState } from 'react';
import {
	ChevronLeft, KeyRound, Search, Repeat, ArrowRight, Check, CheckCircle2, Loader2, AlertTriangle,
} from 'lucide-react';
import type { ScreenId } from '../layout/BottomNav';
import { useCategories } from '../../hooks/useCategories';
import { useTransactions, type Transaction } from '../../hooks/useTransactions';
import { apiClient } from '../../lib/api-client';
import type { GlobalMerchantListItem, MerchantType, TransactionType } from '../../../../shared/types';
import { allowedTransactionTypes, transactionDirection } from '../../lib/transaction-adapter';
import { TransactionTypeSelector } from '../ui/TransactionTypeSelector';

interface ReviewQueueProps {
	transaction: Transaction;
	position: number;
	totalCount: number;
	/** Receives how many other transactions the merchant rule carried along. */
	onReviewed: (backfilledCount: number) => void;
	onClose: () => void;
}

const inputStyle: React.CSSProperties = {
	background: 'var(--ledger-card)',
	border: '1px solid var(--ledger-rule)',
	color: 'var(--ledger-fg)',
	fontSize: '16px',
};

const PAYMENT_METHOD_LABELS: Record<string, string> = {
	upi: 'UPI',
	credit_card: 'Credit Card',
	debit_card: 'Debit Card',
	netbanking: 'Net Banking',
	cash: 'Cash',
	unknown: 'Unknown',
};

function formatSignedRupees(amount: number, type: string): string {
	const sign = type === 'income' ? '+' : type === 'expense' ? '−' : '↔';
	const abs = Math.abs(amount);
	const formatted = abs.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
	return `${sign}₹${formatted}`;
}

function formatDateDisplay(date: Date): string {
	return date.toLocaleDateString('en-IN', { month: 'short', day: 'numeric', year: 'numeric' });
}

function merchantTypeBadge(type: MerchantType): string {
	switch (type) {
		case 'MERCHANT': return 'MERCHANT';
		case 'TRANSFER_INSTITUTION': return 'TRANSFER_INST.';
		case 'AGGREGATOR': return 'AGGREGATOR';
		case 'P2P': return 'P2P';
	}
}

interface SelectedMerchant {
	id: string | null;
	name: string | null;
	type: MerchantType | null;
}

export function ReviewQueue({ transaction, position, totalCount, onReviewed, onClose }: ReviewQueueProps) {
	const { categories: allCategories, isLoading: categoriesLoading } = useCategories();
	const { reviewTransaction, isLoading: reviewLoading } = useTransactions();
	const [selectedType, setSelectedType] = useState<TransactionType>(transaction.type);
	const direction = transactionDirection(transaction);

	const categoriesForType = useMemo(
		() => allCategories.filter((c) => c.type === selectedType),
		[allCategories, selectedType],
	);

	const [hasUserTouched, setHasUserTouched] = useState(false);
	const [selectedParentId, setSelectedParentId] = useState<string | null>(null);
	const [selectedSubId, setSelectedSubId] = useState<string | null>(null);

	const [merchantQuery, setMerchantQuery] = useState<string>(
		transaction.merchantName ?? transaction.counterpartyRaw ?? '',
	);
	const [selectedMerchant, setSelectedMerchant] = useState<SelectedMerchant>(() => ({
		id: transaction.merchantId ?? null,
		name: transaction.merchantName ?? null,
		type: transaction.merchantType ?? null,
	}));
	const [merchantResults, setMerchantResults] = useState<GlobalMerchantListItem[]>([]);
	const [merchantQueryLoading, setMerchantQueryLoading] = useState(false);

	const [note, setNote] = useState('');
	const [ruleToggleOn, setRuleToggleOn] = useState(true);
	const [ruleSearchKey, setRuleSearchKey] = useState<string>(transaction.searchKey ?? '');
	const [submitError, setSubmitError] = useState<string | null>(null);

	useEffect(() => {
		if (hasUserTouched) return;
		let parentId: string | null = null;
		let subId: string | null = null;
		const persistedId = transaction.persistedCategoryId ?? null;
		if (transaction.subCategoryId) {
			subId = transaction.subCategoryId;
			const parent = categoriesForType.find((c) =>
				Object.values(c.subCategoryIds ?? {}).includes(transaction.subCategoryId as string),
			);
			parentId = parent?.id ?? transaction.categoryId ?? null;
		} else if (transaction.categoryId) {
			parentId = transaction.categoryId;
		} else if (persistedId) {
			for (const c of categoriesForType) {
				if (c.id === persistedId) { parentId = c.id; break; }
				const subIds = Object.values(c.subCategoryIds ?? {});
				if (subIds.includes(persistedId)) { parentId = c.id; subId = persistedId; break; }
			}
		}
		setSelectedParentId(parentId);
		setSelectedSubId(subId);
	}, [
		hasUserTouched,
		categoriesForType,
		transaction.id,
		transaction.persistedCategoryId,
		transaction.categoryId,
		transaction.subCategoryId,
	]);

	useEffect(() => {
		const q = merchantQuery.trim();
		if (!q) {
			setMerchantResults([]);
			setMerchantQueryLoading(false);
			return;
		}
		let cancelled = false;
		setMerchantQueryLoading(true);
		const timer = setTimeout(() => {
			apiClient
				.listGlobalMerchants({ q, limit: 10 })
				.then((res) => {
					if (!cancelled) setMerchantResults(res);
				})
				.catch(() => {
					if (!cancelled) setMerchantResults([]);
				})
				.finally(() => {
					if (!cancelled) setMerchantQueryLoading(false);
				});
		}, 300);
		return () => {
			cancelled = true;
			clearTimeout(timer);
		};
	}, [merchantQuery]);

	const selectedParent = categoriesForType.find((c) => c.id === selectedParentId);
	const subCategories = selectedParent?.subCategories ?? [];
	const subCategoryIds = selectedParent?.subCategoryIds ?? {};

	// The rule's search key always belongs to the transaction's DETECTED
	// counterparty — picking a real merchant behind a gateway must not
	// re-enable the prompt, or the rule would tag every payment through
	// that gateway with this merchant and auto-verify them.
	const isAggregator =
		transaction.merchantType === 'AGGREGATOR' || selectedMerchant?.type === 'AGGREGATOR';
	const hasCategorySelected = Boolean(selectedSubId ?? selectedParentId);

	const handleSelectParent = (parentId: string) => {
		setHasUserTouched(true);
		if (selectedParentId === parentId) {
			setSelectedParentId(null);
			setSelectedSubId(null);
		} else {
			setSelectedParentId(parentId);
			setSelectedSubId(null);
		}
	};

	const handleSelectSub = (subId: string) => {
		setHasUserTouched(true);
		setSelectedSubId(selectedSubId === subId ? null : subId);
	};

	const handleSelectMerchant = (m: GlobalMerchantListItem) => {
		setHasUserTouched(true);
		setSelectedMerchant({ id: m.id, name: m.canonical_name, type: m.type });
		setMerchantQuery(m.canonical_name);
	};

	const handleTypeChange = (nextType: TransactionType) => {
		if (nextType === selectedType) return;
		setSelectedType(nextType);
		setSelectedParentId(null);
		setSelectedSubId(null);
		setHasUserTouched(true);
	};

	const handleSave = async () => {
		// Saving before categories load would send category_id: null — an
		// explicit clear that verifies the transaction uncategorized.
		if (reviewLoading || categoriesLoading) return;
		setSubmitError(null);
		const categoryIdToPersist = selectedSubId ?? selectedParentId ?? null;
		const payload: {
			type?: TransactionType;
			category_id?: string | null;
			merchant_id?: string | null;
			user_note?: string | null;
			apply_rule?: boolean;
			rule_search_key?: string | null;
		} = {
			type: selectedType,
			category_id: categoryIdToPersist,
			merchant_id: selectedMerchant?.id ?? null,
			user_note: note.trim() ? note : null,
		};
		if (ruleToggleOn && hasCategorySelected && !isAggregator) {
			payload.apply_rule = true;
			payload.rule_search_key = ruleSearchKey.trim() ? ruleSearchKey : null;
		}
		try {
			const { backfilledCount } = await reviewTransaction({ transactionId: transaction.id, payload });
			onReviewed(backfilledCount);
		} catch (err) {
			const msg = err instanceof Error ? err.message : 'Failed to review. Try again.';
			setSubmitError(msg);
		}
	};

	const isLast = position >= totalCount && totalCount > 0;
	const progressPercent = totalCount > 0 ? ((position - 1) / totalCount) * 100 : 0;

	const counterparty = transaction.merchantName ?? transaction.counterpartyRaw ?? 'Unknown';
	const paymentLabel = PAYMENT_METHOD_LABELS[transaction.paymentMethod ?? 'unknown'] ?? 'Unknown';
	const instrumentLast4 = transaction.instrumentLast4 ?? '';
	const confidencePct = transaction.aiConfidenceScore != null ? Math.round(transaction.aiConfidenceScore * 100) : null;
	const confidenceLabel = confidencePct != null ? `AI ${confidencePct}% confident` : 'Needs review';
	const amountColor = selectedType === 'income' ? 'var(--ledger-positive)' : selectedType === 'transfer' ? 'var(--ledger-muted)' : 'var(--ledger-fg)';
	const typePillBg = selectedType === 'income' ? 'rgba(45, 122, 79, 0.12)' : selectedType === 'transfer' ? 'rgba(13, 92, 75, 0.12)' : 'rgba(196, 83, 54, 0.12)';
	const typePillFg = selectedType === 'income' ? 'var(--ledger-positive)' : selectedType === 'transfer' ? 'var(--ledger-accent)' : 'var(--ledger-negative)';
	const typeLabel = selectedType.charAt(0).toUpperCase() + selectedType.slice(1);
	const showDefaultPrompt = hasCategorySelected && !isAggregator;
	const showAggregatorWarning = hasCategorySelected && isAggregator;

	return (
		<div
			className="absolute inset-0 z-50 flex flex-col"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			<header
				className="h-[56px] shrink-0 flex items-center justify-between px-5 z-10 relative"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<button
					onClick={onClose}
					className="w-8 h-8 rounded-full flex items-center justify-center transition-colors"
					style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
				>
					<ChevronLeft size={20} />
				</button>

				<div className="flex flex-col items-center justify-center">
					<h1 className="font-bold text-lg tracking-tight leading-tight" style={{ color: 'var(--ledger-fg)' }}>
						Review
					</h1>
					<span className="text-[11px] font-medium tnum" style={{ color: 'var(--ledger-muted)' }}>
						{position} of {totalCount}
					</span>
				</div>

				<div className="w-8 h-8" />
			</header>

			<div className="h-[2px] w-full shrink-0" style={{ background: 'var(--ledger-rule)' }}>
				<div
					className="h-full transition-all duration-300"
					style={{ width: `${progressPercent}%`, background: 'var(--ledger-amber)' }}
				/>
			</div>

			<main className="flex-1 overflow-y-auto px-5 pt-5 pb-28">
				<div
					className="rounded-[10px] p-5 mb-6"
					style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
				>
					<div className="flex justify-between items-start mb-4">
						<span
							className="text-[10px] font-bold tracking-[0.15em] px-2.5 py-1 rounded-full uppercase"
							style={{ background: typePillBg, color: typePillFg }}
						>
							{typeLabel}
						</span>
						<span
							className="text-[10px] font-bold px-2.5 py-1 rounded-full"
							style={{
								background: 'var(--ledger-amber-bg)',
								color: 'var(--ledger-amber)',
							}}
						>
							{confidenceLabel}
						</span>
					</div>

					<h2 className="text-4xl tracking-tight font-bold tnum mb-4" style={{ color: amountColor }}>
						{formatSignedRupees(transaction.amount, selectedType)}
					</h2>

					<div className="space-y-1.5 text-xs leading-relaxed mb-4">
						<p style={{ color: 'var(--ledger-muted)' }}>
							<span className="inline-block w-14 uppercase tracking-wider text-[10px] font-bold">Date</span>
							<span className="tnum" style={{ color: 'var(--ledger-fg)' }}>{formatDateDisplay(transaction.date)}</span>
						</p>
						<p style={{ color: 'var(--ledger-muted)' }}>
							<span className="inline-block w-14 uppercase tracking-wider text-[10px] font-bold">To</span>
							<span className="font-medium" style={{ color: 'var(--ledger-fg)' }}>{counterparty}</span>
						</p>
						<p style={{ color: 'var(--ledger-muted)' }}>
							<span className="inline-block w-14 uppercase tracking-wider text-[10px] font-bold">Via</span>
							<span style={{ color: 'var(--ledger-fg)' }}>{paymentLabel}</span>
						</p>
						{instrumentLast4 && (
							<p style={{ color: 'var(--ledger-muted)' }}>
								<span className="inline-block w-14 uppercase tracking-wider text-[10px] font-bold">Instr</span>
								<span className="tnum" style={{ color: 'var(--ledger-fg)' }}>····{instrumentLast4}</span>
							</p>
						)}
					</div>

					{transaction.searchKey && (
						<div
							className="pt-3 flex items-center gap-1.5"
							style={{ borderTop: '1px solid var(--ledger-rule)' }}
						>
							<KeyRound size={12} className="shrink-0" style={{ color: 'var(--ledger-muted)' }} />
							<p className="text-[10px] truncate" style={{ color: 'var(--ledger-muted)' }}>
								Search key: {transaction.searchKey}
							</p>
						</div>
					)}
				</div>

				<div className="mb-6">
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3 px-1"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Type
					</p>
					<TransactionTypeSelector
						value={selectedType}
						allowedTypes={allowedTransactionTypes(direction)}
						onChange={handleTypeChange}
					/>
				</div>

				<div className="mb-6">
					<div className="flex justify-between items-baseline px-1 mb-3">
						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em]"
							style={{ color: 'var(--ledger-muted)' }}
						>
							Category
						</p>
						{transaction.classificationSource === 'AI' && hasCategorySelected && !hasUserTouched && (
							<span
								className="text-[10px] font-medium px-2 py-0.5 rounded-full"
								style={{ background: 'var(--ledger-amber-bg)', color: 'var(--ledger-amber)' }}
							>
								AI suggested
							</span>
						)}
					</div>
					{categoriesForType.length === 0 ? (
						<p className="text-xs px-1" style={{ color: 'var(--ledger-muted)' }}>
							No {selectedType} categories available
						</p>
					) : (
						<div className="flex gap-2 overflow-x-auto pb-2 -mb-2 no-scrollbar px-1">
							{categoriesForType.map((cat) => (
								<button
									key={cat.id}
									onClick={() => handleSelectParent(cat.id)}
									className="shrink-0 rounded-full px-4 py-2 text-xs font-medium transition-colors"
									style={
										selectedParentId === cat.id
											? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
											: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
									}
								>
									{cat.name}
								</button>
							))}
						</div>
					)}
				</div>

				{subCategories.length > 0 && (
					<div className="mb-6">
						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3 px-1"
							style={{ color: 'var(--ledger-muted)' }}
						>
							Sub-category
						</p>
						<div className="flex gap-2 overflow-x-auto pb-2 -mb-2 no-scrollbar px-1">
							{subCategories.map((sub) => {
								const subId = subCategoryIds[sub];
								return (
									<button
										key={sub}
										onClick={() => subId && handleSelectSub(subId)}
										className="shrink-0 rounded-full px-4 py-2 text-xs font-medium transition-colors"
										style={
											selectedSubId === subId
												? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
												: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
										}
									>
										{sub}
									</button>
								);
							})}
						</div>
					</div>
				)}

				<div className="mb-6">
					<div className="flex justify-between items-baseline px-1 mb-3">
						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em]"
							style={{ color: 'var(--ledger-muted)' }}
						>
							Merchant
						</p>
						<span className="text-xs" style={{ color: 'var(--ledger-muted)' }}>
							(optional)
						</span>
					</div>

					<div className="relative mb-2">
						<Search size={16} className="absolute left-3.5 top-1/2 -translate-y-1/2" style={{ color: 'var(--ledger-muted)' }} />
						<input
							type="text"
							value={merchantQuery}
							onChange={(e) => setMerchantQuery(e.target.value)}
							className="w-full rounded-[10px] pl-10 pr-10 py-3 text-sm focus:outline-none transition-all"
							style={inputStyle}
							placeholder="Search merchant"
						/>
						{merchantQueryLoading && (
							<Loader2
								size={14}
								className="absolute right-3.5 top-1/2 -translate-y-1/2 animate-spin"
								style={{ color: 'var(--ledger-muted)' }}
							/>
						)}
					</div>

					{merchantResults.length > 0 && (
						<div
							className="rounded-[10px] overflow-hidden"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
{merchantResults.map((m, i) => {
							const isSelected = selectedMerchant?.id === m.id;
							const isMatch =
								m.canonical_name.toLowerCase() === merchantQuery.trim().toLowerCase();
							const highlighted = isSelected || isMatch;
							return (
								<button
									key={m.id}
									onClick={() => handleSelectMerchant(m)}
									className="w-full flex items-center justify-between px-4 py-3 text-left transition-colors"
									style={
										i < merchantResults.length - 1
											? { borderBottom: '1px solid var(--ledger-rule)' }
											: undefined
									}
								>
									<span
										className="text-sm font-semibold flex items-center gap-2"
										style={{
											color: highlighted ? 'var(--ledger-accent)' : 'var(--ledger-fg)',
										}}
									>
										{isSelected && <Check size={13} strokeWidth={2.5} style={{ color: 'var(--ledger-accent)' }} />}
										{m.canonical_name}
										{isMatch && !isSelected && (
											<span
												className="text-[9px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded"
												style={{
													color: 'var(--ledger-accent)',
													background: 'rgba(13, 92, 75, 0.08)',
												}}
											>
												Match
											</span>
										)}
									</span>
									<span
										className="text-[10px] font-medium px-2 py-0.5 rounded-md"
										style={
											highlighted
												? { color: 'var(--ledger-accent)', background: 'rgba(13, 92, 75, 0.1)' }
												: { color: 'var(--ledger-muted)', background: 'var(--ledger-rule)' }
										}
									>
										{merchantTypeBadge(m.type)}
									</span>
								</button>
							);
						})}
						</div>
					)}

					{merchantQuery.trim() && merchantResults.length === 0 && !merchantQueryLoading && (
						<p className="text-[11px] px-1 mt-2" style={{ color: 'var(--ledger-muted)' }}>
							No merchants match "{merchantQuery.trim()}"
						</p>
					)}
				</div>

				<div className="mb-6">
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3 px-1"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Note
					</p>
					<textarea
						value={note}
						onChange={(e) => setNote(e.target.value)}
						className="w-full rounded-[10px] px-4 py-3.5 text-sm focus:outline-none transition-all resize-none"
						style={inputStyle}
						rows={2}
						placeholder="Add a note for this transaction"
					/>
				</div>

				{showDefaultPrompt && (
					<div className="mb-6">
						<div
							className="rounded-[10px] overflow-hidden"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
							<div className="px-4 py-3.5 flex justify-between items-center">
								<div className="flex gap-3 items-start">
									<div
										className="mt-0.5 w-8 h-8 rounded-[6px] flex items-center justify-center shrink-0"
										style={{ background: 'rgba(13, 92, 75, 0.1)' }}
									>
										<Repeat size={16} style={{ color: 'var(--ledger-accent)' }} />
									</div>
									<div className="pr-4">
										<p className="text-sm font-semibold mb-0.5" style={{ color: 'var(--ledger-fg)' }}>
											Use this category for future {selectedType} transactions with {selectedMerchant?.name ?? 'this merchant'}?
										</p>
										<p className="text-[11px] leading-tight" style={{ color: 'var(--ledger-muted)' }}>
											The merchant is remembered for every direction; this category is remembered only for {selectedType}.
											Existing {selectedType} transactions for this merchant are updated too, except any you categorized yourself.
										</p>
									</div>
								</div>

								<button
									onClick={() => setRuleToggleOn(!ruleToggleOn)}
									className="relative shrink-0 w-11 h-6 rounded-full transition-colors duration-200 ease-in-out"
									style={{
										background: ruleToggleOn ? 'var(--ledger-accent)' : 'var(--ledger-rule)',
									}}
									aria-label="Toggle default category rule"
								>
									<div
										className={`absolute left-1 top-1 w-4 h-4 rounded-full transition-transform duration-200 ease-in-out ${ruleToggleOn ? 'translate-x-5' : 'translate-x-0'}`}
										style={{ background: 'var(--ledger-bg)' }}
									/>
								</button>
							</div>

							{ruleToggleOn && (
								<div
									className="px-4 pb-4 pt-1"
									style={{ borderTop: '1px solid var(--ledger-rule)' }}
								>
									<label
										className="block text-[11px] font-medium mb-1.5 mt-2"
										style={{ color: 'var(--ledger-muted)' }}
									>
										Rule search key
									</label>
									<input
										type="text"
										value={ruleSearchKey}
										onChange={(e) => setRuleSearchKey(e.target.value)}
										className="w-full rounded-[8px] px-3 py-2 text-xs focus:outline-none"
										style={{
											background: 'var(--ledger-bg)',
											border: '1px solid var(--ledger-rule)',
											color: 'var(--ledger-fg)',
											fontSize: '16px',
										}}
										placeholder={transaction.searchKey ?? 'Search key'}
									/>
									<p className="text-[10px] mt-1.5 ml-1" style={{ color: 'var(--ledger-muted)' }}>
										Default uses detected search key
									</p>
								</div>
							)}
						</div>
					</div>
				)}

				{showAggregatorWarning && (
					<div className="mb-6">
						<div
							className="rounded-[10px] px-4 py-3.5 flex items-start gap-3"
							style={{
								background: 'var(--ledger-amber-bg)',
								border: '1px solid rgba(184, 115, 42, 0.2)',
							}}
						>
							<AlertTriangle size={15} className="shrink-0 mt-0.5" style={{ color: 'var(--ledger-amber)' }} />
							<p className="text-xs leading-relaxed" style={{ color: 'var(--ledger-amber)' }}>
								Default category rules are disabled for aggregators (e.g. Razorpay, PayU) — they mask the real merchant, so a default would miscategorize other purchases through the same gateway.
							</p>
						</div>
					</div>
				)}

				{submitError && (
					<div className="mb-6">
						<p className="text-xs px-1" style={{ color: 'var(--ledger-negative)' }}>{submitError}</p>
					</div>
				)}
			</main>

			<div
				className="absolute bottom-0 left-0 right-0 p-4 pt-6 pointer-events-none z-20"
				style={{
					background: 'linear-gradient(to top, var(--ledger-bg) 60%, transparent)',
				}}
			>
				<button
					onClick={() => { void handleSave(); }}
					disabled={reviewLoading || categoriesLoading}
					className="pointer-events-auto w-full h-14 rounded-[10px] text-[15px] font-bold active:scale-[0.98] transition-all flex items-center justify-center gap-2"
					style={{
						background: 'var(--ledger-amber)',
						color: '#fff',
						boxShadow: '0 8px 24px -8px var(--ledger-amber)',
						opacity: (reviewLoading || categoriesLoading) ? 0.6 : 1,
					}}
				>
					{reviewLoading && <Loader2 size={18} className="animate-spin" />}
					{isLast ? 'Save & Finish' : 'Save & Next'}
					{isLast ? <Check size={18} strokeWidth={2.5} /> : <ArrowRight size={18} strokeWidth={2.5} />}
				</button>
			</div>
		</div>
	);
}

export function EmptyReviewQueue({
	onClose,
	onNavigate,
}: {
	onClose: () => void;
	onNavigate: (screen: ScreenId) => void;
}) {
	return (
		<div
			className="absolute inset-0 z-50 flex flex-col"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			<header
				className="h-[56px] shrink-0 flex items-center justify-between px-5 z-10"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<button
					onClick={onClose}
					className="w-8 h-8 rounded-full flex items-center justify-center transition-colors"
					style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
				>
					<ChevronLeft size={20} />
				</button>
				<div className="flex flex-col items-center justify-center">
					<h1 className="font-bold text-lg tracking-tight leading-tight" style={{ color: 'var(--ledger-fg)' }}>
						Review
					</h1>
				</div>
				<div className="w-8 h-8" />
			</header>

			<div className="h-[2px] w-full shrink-0" style={{ background: 'var(--ledger-rule)' }} />

			<main className="flex-1 flex flex-col items-center justify-center px-6 text-center">
				<div className="mb-6 rounded-full p-4" style={{ background: 'rgba(45, 122, 79, 0.1)' }}>
					<CheckCircle2 size={64} strokeWidth={1.5} style={{ color: 'var(--ledger-positive)' }} />
				</div>

				<h2 className="text-2xl font-bold mb-2" style={{ color: 'var(--ledger-fg)' }}>
					All caught up!
				</h2>
				<p
					className="text-sm mb-8 max-w-[260px] mx-auto leading-relaxed"
					style={{ color: 'var(--ledger-muted)' }}
				>
					No transactions need review right now.
				</p>

				<button
					onClick={() => onNavigate('home')}
					className="w-full h-12 rounded-[10px] text-sm font-semibold active:scale-[0.98] transition-all"
					style={{
						background: 'var(--ledger-accent)',
						color: 'var(--ledger-bg)',
						boxShadow: '0 8px 24px -8px var(--ledger-accent)',
					}}
				>
					Back to Home
				</button>
			</main>
		</div>
	);
}
