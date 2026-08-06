import { useMemo, useState } from 'react';
import {
	AlertCircle,
	AlertTriangle,
	Check,
	ChevronDown,
	Loader2,
	Receipt,
	Search,
	Store,
	X,
} from 'lucide-react';
import type { TransactionType, UserMerchantListItem } from '../../../../shared/types';
import { useCategories } from '../../hooks/useCategories';
import { useUserMerchants, useUserMerchantTransactions } from '../../hooks/useUserMerchants';
import { paiseToRupees } from '../../lib/money';
import { SettingsPageHeader } from '../ui/SettingsPageHeader';

interface MerchantSettingsProps {
	onBack: () => void;
}

const TYPE_LABEL: Record<TransactionType, string> = {
	expense: 'Spending',
	income: 'Money in',
	transfer: 'Transfers',
};

/** Which existing transactions a save should rewrite. */
type ApplyMode = 'FUTURE_ONLY' | 'BACKFILL';

/**
 * A save targets either the normalized merchant (every descriptor under it) or
 * one raw descriptor. The merchant scope is the blunt one, so only it warns.
 */
interface SaveTarget {
	merchant: UserMerchantListItem;
	transactionType: TransactionType;
	transactionCount: number;
	/** Undefined for the normalized merchant, set for a raw descriptor. */
	searchKey?: string;
	label: string;
}

/** A pending merchant backfill, holding its category so confirm cannot drift. */
interface ConfirmState {
	target: SaveTarget;
	categoryId: string;
}

function typeKeyOf(merchantId: string, transactionType: TransactionType): string {
	return `${merchantId}:${transactionType}`;
}

function descriptorKeyOf(
	merchantId: string,
	searchKey: string,
	transactionType: TransactionType,
): string {
	return `${merchantId}:${searchKey}:${transactionType}`;
}

function formatRupees(paise: number): string {
	return `₹${paiseToRupees(Math.abs(paise)).toLocaleString('en-IN', {
		minimumFractionDigits: 0,
		maximumFractionDigits: 0,
	})}`;
}

function formatDate(value: string): string {
	return new Date(value).toLocaleDateString('en-IN', {
		day: 'numeric',
		month: 'short',
		year: 'numeric',
	});
}

function MerchantSkeleton() {
	return (
		<div className="rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
			{[0, 1, 2, 3, 4].map(row => (
				<div
					key={row}
					className="h-[64px] flex items-center gap-3 px-4"
					style={row < 4 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
				>
					<span className="w-9 h-9 rounded-[9px] shrink-0" style={{ background: 'var(--ledger-rule)' }} />
					<span className="flex-1 h-3 rounded-full" style={{ background: 'var(--ledger-rule)' }} />
				</div>
			))}
		</div>
	);
}

/**
 * Category chips plus the two apply buttons. "New ones only" is the primary
 * action: it cannot rewrite history, so it is the safe default.
 */
function CategoryEditor({
	categoriesForType,
	pendingCategoryId,
	onSelectCategory,
	transactionType,
	transactionCount,
	scopeNote,
	pendingMode,
	onApply,
}: {
	categoriesForType: Array<{ id: string; name: string }>;
	pendingCategoryId: string | null;
	onSelectCategory: (categoryId: string) => void;
	transactionType: TransactionType;
	transactionCount: number;
	scopeNote: string;
	/** Which button is mid-save, so only that one shows a spinner. */
	pendingMode: ApplyMode | null;
	onApply: (mode: ApplyMode) => void;
}) {
	const isSaving = pendingMode !== null;
	const disabled = !pendingCategoryId || isSaving;
	return (
		<div className="px-4 pb-4" style={{ background: 'var(--ledger-bg)' }}>
			<p className="pt-3 pb-2 text-[10px] font-bold uppercase tracking-[0.16em]" style={{ color: 'var(--ledger-muted)' }}>
				Category
			</p>
			{categoriesForType.length === 0 ? (
				<p className="text-[11px] pb-2" style={{ color: 'var(--ledger-muted)' }}>
					No {transactionType} categories available.
				</p>
			) : (
				<div className="flex flex-wrap gap-1.5">
					{categoriesForType.map(category => {
						const selected = pendingCategoryId === category.id;
						return (
							<button
								key={category.id}
								type="button"
								onClick={() => onSelectCategory(category.id)}
								className="rounded-full px-3 py-1.5 text-[11px] font-semibold"
								style={selected
									? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
									: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }}
							>
								{category.name}
							</button>
						);
					})}
				</div>
			)}

			<div className="grid grid-cols-2 gap-2 mt-4">
				<button
					type="button"
					disabled={disabled}
					onClick={() => onApply('FUTURE_ONLY')}
					className="rounded-[9px] py-2.5 text-[11px] font-bold flex items-center justify-center gap-1.5"
					style={{
						background: 'var(--ledger-accent)',
						color: 'var(--ledger-bg)',
						opacity: disabled ? 0.5 : 1,
					}}
				>
					{pendingMode === 'FUTURE_ONLY' && <Loader2 size={13} className="animate-spin" />}
					New ones only
				</button>
				<button
					type="button"
					disabled={disabled}
					onClick={() => onApply('BACKFILL')}
					className="rounded-[9px] py-2.5 text-[11px] font-bold flex items-center justify-center gap-1.5"
					style={{
						border: '1px solid var(--ledger-rule)',
						color: 'var(--ledger-fg)',
						opacity: disabled ? 0.5 : 1,
					}}
				>
					{pendingMode === 'BACKFILL' && <Loader2 size={13} className="animate-spin" />}
					Apply to all {transactionCount}
				</button>
			</div>
			<p className="mt-2 text-[10px] leading-relaxed" style={{ color: 'var(--ledger-muted)' }}>
				{scopeNote}
			</p>
		</div>
	);
}

export function MerchantSettings({ onBack }: MerchantSettingsProps) {
	const { merchants, isLoading, error, retry, setCategory } = useUserMerchants();
	const { categories } = useCategories();
	const [search, setSearch] = useState('');
	const [openTypeKey, setOpenTypeKey] = useState<string | null>(null);
	const [openDescriptorKey, setOpenDescriptorKey] = useState<string | null>(null);
	const [merchantPendingCategoryId, setMerchantPendingCategoryId] = useState<string | null>(null);
	const [descriptorPendingCategoryId, setDescriptorPendingCategoryId] = useState<string | null>(null);
	// Encoded as "<scope key>:<mode>" so only the clicked button spins.
	const [pendingAction, setPendingAction] = useState<string | null>(null);
	const [confirmState, setConfirmState] = useState<ConfirmState | null>(null);
	const [transactionsMerchant, setTransactionsMerchant] = useState<UserMerchantListItem | null>(null);
	const [saveError, setSaveError] = useState<string | null>(null);
	const [savedNotice, setSavedNotice] = useState<string | null>(null);

	const visibleMerchants = useMemo(() => {
		const query = search.trim().toLowerCase();
		if (!query) return merchants;
		return merchants.filter(merchant =>
			merchant.canonical_name.toLowerCase().includes(query)
			|| merchant.descriptors.some(descriptor => descriptor.label.toLowerCase().includes(query)),
		);
	}, [merchants, search]);

	const openMerchantType = (
		merchantId: string,
		transactionType: TransactionType,
		currentCategoryId: string | null,
	) => {
		const key = typeKeyOf(merchantId, transactionType);
		setSaveError(null);
		setSavedNotice(null);
		setOpenDescriptorKey(null);
		setDescriptorPendingCategoryId(null);
		if (openTypeKey === key) {
			setOpenTypeKey(null);
			setMerchantPendingCategoryId(null);
			return;
		}
		setOpenTypeKey(key);
		setMerchantPendingCategoryId(currentCategoryId);
	};

	const openDescriptor = (key: string, currentCategoryId: string | null) => {
		setSaveError(null);
		setSavedNotice(null);
		if (openDescriptorKey === key) {
			setOpenDescriptorKey(null);
			setDescriptorPendingCategoryId(null);
			return;
		}
		setOpenDescriptorKey(key);
		setDescriptorPendingCategoryId(currentCategoryId);
	};

	const runSave = async (target: SaveTarget, mode: ApplyMode, categoryId: string) => {
		const scopeKey = target.searchKey === undefined
			? typeKeyOf(target.merchant.merchant_id, target.transactionType)
			: descriptorKeyOf(target.merchant.merchant_id, target.searchKey, target.transactionType);
		setSaveError(null);
		setPendingAction(`${scopeKey}:${mode}`);
		try {
			const backfilledCount = await setCategory({
				merchantId: target.merchant.merchant_id,
				transactionType: target.transactionType,
				categoryId,
				backfill: mode === 'BACKFILL',
				...(target.searchKey === undefined ? {} : { searchKey: target.searchKey }),
			});
			if (target.searchKey === undefined) {
				setOpenTypeKey(null);
				setMerchantPendingCategoryId(null);
			} else {
				setOpenDescriptorKey(null);
				setDescriptorPendingCategoryId(null);
			}
			setSavedNotice(
				mode === 'BACKFILL'
					? `${target.label}: ${backfilledCount} transaction${backfilledCount === 1 ? '' : 's'} updated.`
					: `${target.label} saved for future ${TYPE_LABEL[target.transactionType].toLowerCase()}.`,
			);
		} catch (cause) {
			setSaveError(cause instanceof Error ? cause.message : 'Could not save this category.');
		} finally {
			setPendingAction(null);
		}
	};

	/** Merchant-level backfill is the blunt one, so it asks first. */
	const requestSave = (target: SaveTarget, mode: ApplyMode, categoryId: string | null) => {
		if (!categoryId || pendingAction !== null) return;
		if (mode === 'BACKFILL' && target.searchKey === undefined) {
			setSaveError(null);
			setConfirmState({ target, categoryId });
			return;
		}
		void runSave(target, mode, categoryId);
	};

	return (
		<div className="flex-1 flex flex-col min-h-0 relative" style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}>
			<SettingsPageHeader
				title="Merchants"
				subtitle={`${merchants.length} with transactions`}
				onBack={onBack}
			/>

			<main className="flex-1 overflow-y-auto px-5 pt-5 pb-24 min-h-0">
				<div className="w-full max-w-[720px] mx-auto">
					<p className="mb-4 text-[11px] leading-relaxed" style={{ color: 'var(--ledger-muted)' }}>
						A merchant category covers every descriptor underneath it. Open a descriptor to give one
						raw counterparty its own category — that one wins. Busiest merchants first.
					</p>

					<div
						className="ledger-search mb-4 flex items-center gap-2 rounded-[10px] px-3 h-11"
						style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
					>
						<Search size={15} className="shrink-0" style={{ color: 'var(--ledger-muted)' }} />
						<input
							type="search"
							value={search}
							onChange={event => setSearch(event.target.value)}
							placeholder="Search merchants or descriptors"
							aria-label="Search merchants or descriptors"
							className="min-w-0 flex-1 bg-transparent outline-none text-sm"
						/>
					</div>

					{(error || saveError) && (
						<div
							className="mb-4 rounded-[12px] p-3.5 flex items-start gap-3"
							style={{ background: 'var(--ledger-amber-bg)', border: '1px solid color-mix(in srgb, var(--ledger-negative) 25%, var(--ledger-rule))' }}
						>
							<AlertCircle size={17} className="shrink-0 mt-0.5" style={{ color: 'var(--ledger-negative)' }} />
							<div className="min-w-0 flex-1">
								<p className="text-xs font-semibold">{saveError ?? error}</p>
								<button
									type="button"
									onClick={() => { setSaveError(null); void retry(); }}
									className="mt-1.5 text-[10px] font-bold"
									style={{ color: 'var(--ledger-accent)' }}
								>
									Try again
								</button>
							</div>
						</div>
					)}

					{savedNotice && (
						<div
							className="mb-4 rounded-[12px] px-3.5 py-3 flex items-center gap-2.5"
							style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}
						>
							<Check size={15} className="shrink-0" />
							<p className="text-xs font-semibold">{savedNotice}</p>
						</div>
					)}

					{isLoading && merchants.length === 0 ? (
						<MerchantSkeleton />
					) : visibleMerchants.length === 0 ? (
						<p className="py-10 text-center text-xs" style={{ color: 'var(--ledger-muted)' }}>
							{merchants.length === 0 ? 'No merchants yet.' : 'No merchants match that search.'}
						</p>
					) : (
						<div className="flex flex-col gap-2.5">
							{visibleMerchants.map(merchant => (
								<article
									key={merchant.merchant_id}
									className="rounded-[14px] overflow-hidden"
									style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
								>
									<div className="flex items-center gap-3 px-4 py-3">
										<span
											className="w-9 h-9 rounded-[9px] flex items-center justify-center shrink-0 overflow-hidden"
											style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}
										>
											{merchant.logo_url
												? <img src={merchant.logo_url} alt="" className="w-full h-full object-cover" />
												: <Store size={16} />}
										</span>
										<div className="min-w-0 flex-1">
											<p className="text-sm font-bold truncate">{merchant.canonical_name}</p>
											<p className="text-[10px] mt-0.5 tnum" style={{ color: 'var(--ledger-muted)' }}>
												{merchant.transaction_count} transaction{merchant.transaction_count === 1 ? '' : 's'}
												{merchant.descriptors.length > 1 && ` · ${merchant.descriptors.length} descriptors`}
											</p>
										</div>
										<button
											type="button"
											onClick={() => setTransactionsMerchant(merchant)}
											className="shrink-0 w-8 h-8 rounded-full flex items-center justify-center"
											style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
											aria-label={`View ${merchant.canonical_name} transactions`}
										>
											<Receipt size={14} />
										</button>
									</div>

									{merchant.breakdown.map(breakdown => {
										const key = typeKeyOf(merchant.merchant_id, breakdown.transaction_type);
										const isOpen = openTypeKey === key;
										const categoriesForType = categories.filter(
											category => category.type === breakdown.transaction_type,
										);
										const currentLabel = breakdown.rule_category_name
											?? breakdown.common_category_name
											?? 'Uncategorized';
										const descriptorsForType = merchant.descriptors.filter(
											descriptor => descriptor.transaction_type === breakdown.transaction_type,
										);
										const merchantTarget: SaveTarget = {
											merchant,
											transactionType: breakdown.transaction_type,
											transactionCount: breakdown.transaction_count,
											label: merchant.canonical_name,
										};
										return (
											<div key={key} style={{ borderTop: '1px solid var(--ledger-rule)' }}>
												<button
													type="button"
													onClick={() => openMerchantType(
														merchant.merchant_id,
														breakdown.transaction_type,
														breakdown.rule_category_id ?? breakdown.common_category_id,
													)}
													className="w-full flex items-center gap-3 px-4 py-3 text-left"
													aria-expanded={isOpen}
												>
													<span className="min-w-0 flex-1">
														<span className="block text-[9px] font-bold uppercase tracking-[0.16em]" style={{ color: 'var(--ledger-muted)' }}>
															{TYPE_LABEL[breakdown.transaction_type]} · {breakdown.transaction_count}
														</span>
														<span className="block text-xs font-semibold mt-1 truncate">{currentLabel}</span>
													</span>
													{breakdown.rule_category_id && (
														<span
															className="shrink-0 rounded-full px-2 py-0.5 text-[8px] font-bold uppercase tracking-wider"
															style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}
														>
															Saved
														</span>
													)}
													<ChevronDown
														size={15}
														className="shrink-0"
														style={{ color: 'var(--ledger-muted)', transform: isOpen ? 'rotate(180deg)' : undefined }}
													/>
												</button>

												{isOpen && (
													<>
														<CategoryEditor
															categoriesForType={categoriesForType}
															pendingCategoryId={merchantPendingCategoryId}
															onSelectCategory={setMerchantPendingCategoryId}
															transactionType={breakdown.transaction_type}
															transactionCount={breakdown.transaction_count}
															scopeNote={`Covers every descriptor under ${merchant.canonical_name}. A descriptor with its own category keeps that one.`}
															pendingMode={
																pendingAction === `${key}:FUTURE_ONLY` ? 'FUTURE_ONLY'
																	: pendingAction === `${key}:BACKFILL` ? 'BACKFILL'
																		: null
															}
															onApply={mode => requestSave(merchantTarget, mode, merchantPendingCategoryId)}
														/>

														{descriptorsForType.length > 0 && (
															<div className="px-4 pb-4" style={{ background: 'var(--ledger-bg)' }}>
																<p className="pt-1 pb-2 text-[10px] font-bold uppercase tracking-[0.16em]" style={{ color: 'var(--ledger-muted)' }}>
																	Raw descriptors
																</p>
																<div className="rounded-[10px] overflow-hidden" style={{ border: '1px solid var(--ledger-rule)' }}>
																	{descriptorsForType.map((descriptor, index) => {
																		const rawKey = descriptorKeyOf(
																			merchant.merchant_id,
																			descriptor.search_key,
																			descriptor.transaction_type,
																		);
																		const rawOpen = openDescriptorKey === rawKey;
																		const rawLabel = descriptor.rule_category_name
																			?? descriptor.common_category_name
																			?? 'Uncategorized';
																		const descriptorTarget: SaveTarget = {
																			merchant,
																			transactionType: descriptor.transaction_type,
																			transactionCount: descriptor.transaction_count,
																			searchKey: descriptor.search_key,
																			label: descriptor.label,
																		};
																		return (
																			<div
																				key={rawKey}
																				style={index < descriptorsForType.length - 1
																					? { borderBottom: '1px solid var(--ledger-rule)' }
																					: undefined}
																			>
																				<button
																					type="button"
																					onClick={() => openDescriptor(
																						rawKey,
																						descriptor.rule_category_id ?? descriptor.common_category_id,
																					)}
																					className="w-full flex items-center gap-2.5 px-3 py-2.5 text-left"
																					style={{ background: 'var(--ledger-card)' }}
																					aria-expanded={rawOpen}
																				>
																					<span className="min-w-0 flex-1">
																						<span className="block text-[11px] font-semibold truncate">{descriptor.label}</span>
																						<span className="block text-[9px] mt-0.5 tnum" style={{ color: 'var(--ledger-muted)' }}>
																							{descriptor.transaction_count} · {rawLabel}
																						</span>
																					</span>
																					{descriptor.rule_category_id && (
																						<span
																							className="shrink-0 rounded-full px-2 py-0.5 text-[8px] font-bold uppercase tracking-wider"
																							style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}
																						>
																							Saved
																						</span>
																					)}
																					<ChevronDown
																						size={14}
																						className="shrink-0"
																						style={{ color: 'var(--ledger-muted)', transform: rawOpen ? 'rotate(180deg)' : undefined }}
																					/>
																				</button>
																				{rawOpen && (
																					<CategoryEditor
																						categoriesForType={categoriesForType}
																						pendingCategoryId={descriptorPendingCategoryId}
																						onSelectCategory={setDescriptorPendingCategoryId}
																						transactionType={descriptor.transaction_type}
																						transactionCount={descriptor.transaction_count}
																						scopeNote={`Applies only to “${descriptor.label}”, and overrides the ${merchant.canonical_name} category for it.`}
																						pendingMode={
																							pendingAction === `${rawKey}:FUTURE_ONLY` ? 'FUTURE_ONLY'
																								: pendingAction === `${rawKey}:BACKFILL` ? 'BACKFILL'
																									: null
																						}
																						onApply={mode => requestSave(descriptorTarget, mode, descriptorPendingCategoryId)}
																					/>
																				)}
																			</div>
																		);
																	})}
																</div>
															</div>
														)}
													</>
												)}
											</div>
										);
									})}
								</article>
							))}
						</div>
					)}
				</div>
			</main>

			{confirmState && (
				<BackfillConfirmDialog
					target={confirmState.target}
					onCancel={() => setConfirmState(null)}
					onConfirm={() => {
						const { target, categoryId } = confirmState;
						setConfirmState(null);
						void runSave(target, 'BACKFILL', categoryId);
					}}
				/>
			)}

			{transactionsMerchant && (
				<MerchantTransactionsDialog
					merchant={transactionsMerchant}
					onClose={() => setTransactionsMerchant(null)}
				/>
			)}
		</div>
	);
}

/**
 * Merchant-level backfill rewrites history across every descriptor at once, so
 * it states the risk with the two cases from this user's own data.
 */
function BackfillConfirmDialog({
	target,
	onCancel,
	onConfirm,
}: {
	target: SaveTarget;
	onCancel: () => void;
	onConfirm: () => void;
}) {
	return (
		<div className="absolute inset-0 z-50 flex items-end justify-center" role="presentation">
			<button
				type="button"
				className="absolute inset-0 cursor-default"
				style={{ background: 'color-mix(in srgb, var(--ledger-fg) 24%, transparent)' }}
				onClick={onCancel}
				aria-label="Cancel rewrite"
			/>
			<section
				role="dialog"
				aria-modal="true"
				aria-labelledby="backfill-confirm-title"
				className="relative w-full max-w-[520px] rounded-t-[18px] p-5"
				style={{ background: 'var(--ledger-card)', borderTop: '1px solid var(--ledger-rule)' }}
			>
				<div className="flex items-start gap-3">
					<span
						className="w-9 h-9 rounded-[9px] flex items-center justify-center shrink-0"
						style={{ background: 'var(--ledger-amber-bg)', color: 'var(--ledger-amber)' }}
					>
						<AlertTriangle size={17} />
					</span>
					<div className="min-w-0">
						<p id="backfill-confirm-title" className="text-sm font-bold">
							Rewrite all {target.transactionCount} {target.label} transactions?
						</p>
						<p className="text-[11px] leading-relaxed mt-1.5" style={{ color: 'var(--ledger-muted)' }}>
							This overwrites every descriptor under this merchant, including categories you set by
							hand. One brand often covers several kinds of spending:
						</p>
					</div>
				</div>

				<ul className="mt-3 flex flex-col gap-2">
					<li className="rounded-[9px] px-3 py-2.5 text-[11px] leading-relaxed" style={{ background: 'var(--ledger-bg)' }}>
						<strong>Amazon Pay</strong> arrives as <em>IN GROCERY</em>, <em>IN E COMMERCE</em>,{' '}
						<em>WALLET LOAD</em> and more. One category flattens all of them into one.
					</li>
					<li className="rounded-[9px] px-3 py-2.5 text-[11px] leading-relaxed" style={{ background: 'var(--ledger-bg)' }}>
						<strong>Swiggy</strong> covers Instamart, food delivery and Dineout, and most of its
						descriptors do not say which. A single category will be wrong for some of them.
					</li>
				</ul>

				<p className="mt-3 text-[11px] leading-relaxed" style={{ color: 'var(--ledger-muted)' }}>
					If this merchant means several things, set the category per descriptor instead, or use
					“New ones only”.
				</p>

				<div className="grid grid-cols-2 gap-2 mt-4">
					<button
						type="button"
						onClick={onCancel}
						className="rounded-[9px] py-3 text-[11px] font-bold"
						style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }}
					>
						Keep history
					</button>
					<button
						type="button"
						onClick={onConfirm}
						className="rounded-[9px] py-3 text-[11px] font-bold"
						style={{ border: '1px solid var(--ledger-rule)', color: 'var(--ledger-negative)' }}
					>
						Rewrite all {target.transactionCount}
					</button>
				</div>
			</section>
		</div>
	);
}

function MerchantTransactionsDialog({
	merchant,
	onClose,
}: {
	merchant: UserMerchantListItem;
	onClose: () => void;
}) {
	const { transactions, isLoading, error } = useUserMerchantTransactions(merchant.merchant_id);

	return (
		<div className="absolute inset-0 z-50 flex items-end justify-center" role="presentation">
			<button
				type="button"
				className="absolute inset-0 cursor-default"
				style={{ background: 'color-mix(in srgb, var(--ledger-fg) 24%, transparent)' }}
				onClick={onClose}
				aria-label="Close transactions"
			/>
			<section
				role="dialog"
				aria-modal="true"
				aria-labelledby="merchant-transactions-title"
				className="relative w-full max-w-[520px] max-h-[82%] flex flex-col rounded-t-[18px]"
				style={{ background: 'var(--ledger-card)', borderTop: '1px solid var(--ledger-rule)' }}
			>
				<header
					className="shrink-0 flex items-center gap-3 px-5 py-4"
					style={{ borderBottom: '1px solid var(--ledger-rule)' }}
				>
					<div className="min-w-0 flex-1">
						<p id="merchant-transactions-title" className="text-sm font-bold truncate">
							{merchant.canonical_name}
						</p>
						<p className="text-[10px] mt-0.5 tnum" style={{ color: 'var(--ledger-muted)' }}>
							{merchant.transaction_count} transaction{merchant.transaction_count === 1 ? '' : 's'}
						</p>
					</div>
					<button
						type="button"
						onClick={onClose}
						className="shrink-0 w-8 h-8 rounded-full flex items-center justify-center"
						style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
						aria-label="Close transactions"
					>
						<X size={15} />
					</button>
				</header>

				<div className="flex-1 overflow-y-auto min-h-0">
					{error ? (
						<p className="px-5 py-8 text-center text-xs" style={{ color: 'var(--ledger-negative)' }}>{error}</p>
					) : isLoading ? (
						<p className="px-5 py-8 text-center text-xs" style={{ color: 'var(--ledger-muted)' }}>Loading transactions…</p>
					) : transactions.length === 0 ? (
						<p className="px-5 py-8 text-center text-xs" style={{ color: 'var(--ledger-muted)' }}>No transactions.</p>
					) : (
						transactions.map((transaction, index) => (
							<div
								key={transaction.transaction_id}
								className="px-5 py-3 flex items-start gap-3"
								style={index < transactions.length - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
							>
								<div className="min-w-0 flex-1">
									<p className="text-xs font-semibold truncate">{transaction.descriptor ?? merchant.canonical_name}</p>
									<p className="text-[10px] mt-0.5 tnum" style={{ color: 'var(--ledger-muted)' }}>
										{formatDate(transaction.txn_date)} · {transaction.category_name ?? 'Uncategorized'}
										{transaction.status === 'NEEDS_REVIEW' && ' · Needs review'}
									</p>
								</div>
								<p
									className="text-xs font-bold tnum shrink-0"
									style={{ color: transaction.type === 'income' ? 'var(--ledger-positive)' : 'var(--ledger-fg)' }}
								>
									{transaction.type === 'income' ? '+' : transaction.type === 'expense' ? '−' : '↔'}
									{formatRupees(transaction.amount_in_paise)}
								</p>
							</div>
						))
					)}
				</div>
			</section>
		</div>
	);
}
