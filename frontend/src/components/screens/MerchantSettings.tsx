import { useMemo, useState } from 'react';
import {
	AlertCircle,
	Check,
	ChevronDown,
	Loader2,
	Search,
	Store,
} from 'lucide-react';
import type {
	TransactionType,
	UserMerchantCategoryBreakdown,
	UserMerchantListItem,
} from '../../../../shared/types';
import { useCategories } from '../../hooks/useCategories';
import { useUserMerchants } from '../../hooks/useUserMerchants';
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

function breakdownKey(merchantId: string, transactionType: TransactionType): string {
	return `${merchantId}:${transactionType}`;
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

export function MerchantSettings({ onBack }: MerchantSettingsProps) {
	const { merchants, isLoading, isSaving, error, retry, setCategory } = useUserMerchants();
	const { categories } = useCategories();
	const [search, setSearch] = useState('');
	const [openKey, setOpenKey] = useState<string | null>(null);
	const [pendingCategoryId, setPendingCategoryId] = useState<string | null>(null);
	const [saveError, setSaveError] = useState<string | null>(null);
	const [savedNotice, setSavedNotice] = useState<string | null>(null);

	const visibleMerchants = useMemo(() => {
		const query = search.trim().toLowerCase();
		if (!query) return merchants;
		return merchants.filter(merchant => merchant.canonical_name.toLowerCase().includes(query));
	}, [merchants, search]);

	const openBreakdown = (merchantId: string, transactionType: TransactionType, currentCategoryId: string | null) => {
		const key = breakdownKey(merchantId, transactionType);
		setSaveError(null);
		setSavedNotice(null);
		if (openKey === key) {
			setOpenKey(null);
			setPendingCategoryId(null);
			return;
		}
		setOpenKey(key);
		setPendingCategoryId(currentCategoryId);
	};

	const save = async (
		merchant: UserMerchantListItem,
		breakdown: UserMerchantCategoryBreakdown,
		mode: ApplyMode,
	) => {
		if (!pendingCategoryId || isSaving) return;
		setSaveError(null);
		try {
			const backfilledCount = await setCategory({
				merchantId: merchant.merchant_id,
				transactionType: breakdown.transaction_type,
				categoryId: pendingCategoryId,
				backfill: mode === 'BACKFILL',
			});
			setOpenKey(null);
			setPendingCategoryId(null);
			setSavedNotice(
				mode === 'BACKFILL'
					? `${merchant.canonical_name}: ${backfilledCount} transaction${backfilledCount === 1 ? '' : 's'} updated.`
					: `${merchant.canonical_name} saved for future ${TYPE_LABEL[breakdown.transaction_type].toLowerCase()}.`,
			);
		} catch (cause) {
			setSaveError(cause instanceof Error ? cause.message : 'Could not save this merchant category.');
		}
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
						Pick a category once per merchant and every future transaction follows it. Busiest merchants first.
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
							placeholder="Search merchants"
							aria-label="Search merchants"
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
											</p>
										</div>
									</div>

									{merchant.breakdown.map(breakdown => {
										const key = breakdownKey(merchant.merchant_id, breakdown.transaction_type);
										const isOpen = openKey === key;
										const categoriesForType = categories.filter(
											category => category.type === breakdown.transaction_type,
										);
										const currentLabel = breakdown.rule_category_name
											?? breakdown.common_category_name
											?? 'Uncategorized';
										return (
											<div key={key} style={{ borderTop: '1px solid var(--ledger-rule)' }}>
												<button
													type="button"
													onClick={() => openBreakdown(
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
													<div className="px-4 pb-4" style={{ background: 'var(--ledger-bg)' }}>
														<p className="pt-3 pb-2 text-[10px] font-bold uppercase tracking-[0.16em]" style={{ color: 'var(--ledger-muted)' }}>
															Category
														</p>
														{categoriesForType.length === 0 ? (
															<p className="text-[11px] pb-2" style={{ color: 'var(--ledger-muted)' }}>
																No {breakdown.transaction_type} categories available.
															</p>
														) : (
															<div className="flex flex-wrap gap-1.5">
																{categoriesForType.map(category => {
																	const selected = pendingCategoryId === category.id;
																	return (
																		<button
																			key={category.id}
																			type="button"
																			onClick={() => setPendingCategoryId(category.id)}
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
																disabled={!pendingCategoryId || isSaving}
																onClick={() => { void save(merchant, breakdown, 'FUTURE_ONLY'); }}
																className="rounded-[9px] py-2.5 text-[11px] font-bold flex items-center justify-center gap-1.5"
																style={{
																	border: '1px solid var(--ledger-rule)',
																	color: 'var(--ledger-fg)',
																	opacity: !pendingCategoryId || isSaving ? 0.5 : 1,
																}}
															>
																{isSaving && <Loader2 size={13} className="animate-spin" />}
																New ones only
															</button>
															<button
																type="button"
																disabled={!pendingCategoryId || isSaving}
																onClick={() => { void save(merchant, breakdown, 'BACKFILL'); }}
																className="rounded-[9px] py-2.5 text-[11px] font-bold flex items-center justify-center gap-1.5"
																style={{
																	background: 'var(--ledger-accent)',
																	color: 'var(--ledger-bg)',
																	opacity: !pendingCategoryId || isSaving ? 0.5 : 1,
																}}
															>
																{isSaving && <Loader2 size={13} className="animate-spin" />}
																Apply to all {breakdown.transaction_count}
															</button>
														</div>
														<p className="mt-2 text-[10px] leading-relaxed" style={{ color: 'var(--ledger-muted)' }}>
															“New ones only” leaves your existing transactions untouched. “Apply to all” also rewrites every
															{' '}{TYPE_LABEL[breakdown.transaction_type].toLowerCase()} transaction for this merchant, including ones you set by hand.
														</p>
													</div>
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
		</div>
	);
}
