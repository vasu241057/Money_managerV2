import { useState } from 'react';
import { useAuth } from '@clerk/react';
import {
	ChevronRight, ChevronDown, Trash2, X, Plus, LogOut, Loader2,
	EyeOff, RotateCcw, Pencil, Check, ReceiptText,
	Gauge,
} from 'lucide-react';
import { useGmailConnection } from '../../hooks/useGmailConnection';
import { SettingsSkeleton } from '../ui/Skeleton';
import { InstallAppCard } from '../ui/InstallAppCard';
import { useAccounts } from '../../hooks/useAccounts';
import { useCreditCards } from '../../hooks/useCreditCards';
import { useCategories } from '../../hooks/useCategories';
import { useAiUsageSummary } from '../../hooks/useAiUsage';
import type { OAuthSyncStatus } from '../../../../shared/types';

interface SettingsHubProps {
	onOpenAccounts: () => void;
	onOpenBudget: () => void;
	onOpenAiUsage: () => void;
	onSignOut: () => void;
}

const inputStyle: React.CSSProperties = {
	background: 'var(--ledger-card)',
	border: '1px solid var(--ledger-rule)',
	color: 'var(--ledger-fg)',
	fontSize: '16px',
};

const cardStyle: React.CSSProperties = {
	background: 'var(--ledger-card)',
	border: '1px solid var(--ledger-rule)',
};

function DotLeader() {
	return (
		<span
			className="flex-1 mx-2 self-end"
			style={{
				borderBottom: '1px dotted rgba(139, 134, 128, 0.35)',
				transform: 'translateY(-4px)',
				minWidth: '20px',
			}}
		/>
	);
}

const STATUS_DOT_COLOR: Record<OAuthSyncStatus, string> = {
	ACTIVE: 'var(--ledger-positive)',
	DORMANT: 'var(--ledger-amber)',
	AUTH_REVOKED: 'var(--ledger-negative)',
	ERROR_PAUSED: 'var(--ledger-amber)',
};

const STATUS_LABEL: Record<OAuthSyncStatus, string> = {
	ACTIVE: 'Connected',
	DORMANT: 'Dormant',
	AUTH_REVOKED: 'Reconnect needed',
	ERROR_PAUSED: 'Paused — reconnect',
};

function formatSyncAgo(timestampMs: number): string {
	if (!timestampMs || timestampMs <= 0) return 'never synced';
	const diff = Date.now() - timestampMs;
	if (diff < 60_000) return 'synced just now';
	const mins = Math.floor(diff / 60_000);
	if (mins < 60) return `synced ${mins}m ago`;
	const hours = Math.floor(mins / 60);
	if (hours < 24) return `synced ${hours}h ago`;
	const days = Math.floor(hours / 24);
	return `synced ${days}d ago`;
}

function formatUsd(value: number): string {
	if (value > 0 && value < 0.01) return `$${value.toFixed(4)}`;
	return `$${value.toFixed(2)}`;
}

export function SettingsHub({
	onOpenAccounts,
	onOpenBudget,
	onOpenAiUsage,
	onSignOut,
}: SettingsHubProps) {
	const { signOut } = useAuth();
	const [categoryType, setCategoryType] = useState<'expense' | 'income'>('expense');
	const [expandedCategoryId, setExpandedCategoryId] = useState<string | null>(null);
	const [newCategoryName, setNewCategoryName] = useState('');
	const [newSubNames, setNewSubNames] = useState<Record<string, string>>({});
	const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null);
	const [renamingId, setRenamingId] = useState<string | null>(null);
	const [renameValue, setRenameValue] = useState('');
	const [hiddenOpen, setHiddenOpen] = useState(false);

	const {
		connection,
		processingStats,
		connectGmail,
		disconnectGmail,
		isLoading: gmailLoading,
		error: gmailError,
	} = useGmailConnection();

	const { accounts } = useAccounts();
	const { creditCards } = useCreditCards();
	const { summary: aiUsageSummary, isAdmin: isAiUsageAdmin } = useAiUsageSummary();
	const {
		categories: allCategories,
		hiddenCategories,
		addCategory,
		renameCategory,
		deleteCategory,
		restoreCategory,
		addSubCategory,
		deleteSubCategory,
		isLoading: categoriesLoading,
		error: categoriesError,
	} = useCategories();

	const categories = allCategories.filter((c) => c.type === categoryType);
	const hiddenForType = hiddenCategories.filter((c) => c.type === categoryType);

	const commitRename = () => {
		if (renamingId && renameValue.trim()) {
			void renameCategory(renamingId, renameValue.trim());
		}
		setRenamingId(null);
		setRenameValue('');
	};

	const syncStatus = connection?.sync_status ?? null;
	const statusDotColor = syncStatus ? STATUS_DOT_COLOR[syncStatus] : 'var(--ledger-muted)';
	const statusLabel = syncStatus ? STATUS_LABEL[syncStatus] : 'Disconnected';
	const requiresReconnect = syncStatus === 'AUTH_REVOKED' || syncStatus === 'ERROR_PAUSED';
	const isConnected = connection !== null;
	const emailAddress = connection?.email_address ?? 'Not connected';
	const syncAgo = connection ? formatSyncAgo(connection.last_sync_timestamp) : '';

	const emailStats = [
		{ label: 'Total', value: processingStats.raw_email_total_count, color: 'var(--ledger-fg)' },
		{ label: 'Pending', value: processingStats.raw_email_pending_count, color: 'var(--ledger-amber)' },
		{ label: 'Processed', value: processingStats.raw_email_processed_count, color: 'var(--ledger-positive)' },
		{ label: 'Failed', value: processingStats.raw_email_failed_count, color: 'var(--ledger-negative)' },
		{ label: 'Unrecognized', value: processingStats.raw_email_unrecognized_count, color: 'var(--ledger-muted)' },
		{ label: 'Ignored', value: processingStats.raw_email_ignored_count, color: 'var(--ledger-muted)' },
	];
	const aiMonthCost =
		aiUsageSummary?.account?.current_month_cost_usd ??
		aiUsageSummary?.tracked_current_month_cost_usd ??
		0;
	const aiAllTimeCost =
		aiUsageSummary?.account?.all_time_cost_usd ??
		aiUsageSummary?.tracked_all_time_cost_usd ??
		0;
	const aiCreditTotal = aiUsageSummary?.account?.purchased_credits_usd ?? null;
	const aiCreditRemaining = aiCreditTotal !== null
		? aiUsageSummary?.account?.credits_remaining_usd ?? null
		: aiUsageSummary?.account?.remaining_usd ?? null;
	const hasAiCreditRunway = aiCreditTotal !== null && aiCreditTotal > 0 && aiCreditRemaining !== null;
	const aiCreditUsedPercent = hasAiCreditRunway
		? Math.min(100, Math.max(0, ((aiCreditTotal - aiCreditRemaining) / aiCreditTotal) * 100))
		: 0;

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
					Settings
				</h1>
				<span
					className="text-[10px] font-bold uppercase tracking-[0.2em] tnum"
					style={{ color: 'var(--ledger-muted)' }}
				>
					v0.1.0
				</span>
			</header>

			<main className="flex-1 overflow-y-auto px-5 pt-6 pb-24 min-h-0">
				{categoriesLoading && allCategories.length === 0 ? (
					<SettingsSkeleton />
				) : (
				<>
				<section className="mb-8">
					<p className="text-[10px] font-bold uppercase tracking-[0.2em] mb-4" style={{ color: 'var(--ledger-muted)' }}>Planning</p>
					<button type="button" onClick={onOpenBudget} className="w-full flex items-center gap-3 rounded-[10px] px-4 py-3.5 text-left" style={cardStyle}>
						<span className="w-9 h-9 rounded-[9px] flex items-center justify-center shrink-0" style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}><Gauge size={18} /></span>
						<span className="flex-1"><span className="block text-sm font-semibold">Monthly plan</span><span className="block text-[11px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>Flexible limit, fixed costs, and recurring rules</span></span>
						<ChevronRight size={16} style={{ color: 'var(--ledger-muted)' }} />
					</button>
				</section>

				<section className="mb-8">
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-4"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Connection
					</p>

					<div
						className="rounded-[10px] overflow-hidden"
						style={cardStyle}
					>
						{/* Status row */}
						<div className="px-4 py-3 flex items-center justify-between" style={{ borderBottom: '1px solid var(--ledger-rule)' }}>
							<div className="flex items-center gap-2.5 min-w-0">
								<span
									className="w-2 h-2 rounded-full shrink-0"
									style={{ background: statusDotColor }}
								/>
								<p className="text-sm font-semibold truncate" style={{ color: 'var(--ledger-fg)' }}>
									{emailAddress}
								</p>
							</div>
							<div className="text-right shrink-0 ml-2">
								<span
									className="text-[10px] font-bold uppercase tracking-wider block"
									style={{ color: statusDotColor }}
								>
									{statusLabel}
								</span>
								{isConnected && (
									<span className="text-[10px] tnum" style={{ color: 'var(--ledger-muted)' }}>
										{syncAgo}
									</span>
								)}
							</div>
						</div>

						{/* Stats grid */}
						<div className="grid grid-cols-3">
							{emailStats.map((stat, i) => {
								const isRightCol = (i + 1) % 3 !== 0;
								const isTopRow = i < 3;
								return (
									<div
										key={stat.label}
										className="px-3 py-2.5 text-center"
										style={{
											borderRight: isRightCol ? '1px solid var(--ledger-rule)' : undefined,
											borderBottom: isTopRow ? '1px solid var(--ledger-rule)' : undefined,
										}}
									>
										<p
											className="text-lg font-bold tnum leading-tight"
											style={{ color: stat.color }}
										>
											{stat.value}
										</p>
										<p
											className="text-[9px] font-bold uppercase tracking-wider mt-0.5"
											style={{ color: 'var(--ledger-muted)' }}
										>
											{stat.label}
										</p>
									</div>
								);
							})}
						</div>

						{/* Error */}
						{gmailError && (
							<div
								className="px-4 py-2"
								style={{ borderTop: '1px solid var(--ledger-rule)' }}
							>
								<p className="text-xs" style={{ color: 'var(--ledger-negative)' }}>
									{gmailError}
								</p>
							</div>
						)}

						{/* Action buttons */}
						<div
							className="px-4 py-3 flex gap-2"
							style={{ borderTop: '1px solid var(--ledger-rule)' }}
						>
							{!isConnected && (
								<button
									onClick={() => { void connectGmail(); }}
									disabled={gmailLoading}
									className="flex-1 h-11 rounded-[8px] text-sm font-semibold transition-colors flex items-center justify-center gap-2"
									style={{
										background: 'var(--ledger-accent)',
										color: 'var(--ledger-bg)',
										opacity: gmailLoading ? 0.6 : 1,
									}}
								>
									{gmailLoading && <Loader2 size={14} className="animate-spin" />}
									Connect Gmail
								</button>
							)}
							{isConnected && requiresReconnect && (
								<>
									<button
										onClick={() => { void connectGmail(); }}
										disabled={gmailLoading}
										className="flex-1 h-11 rounded-[8px] text-sm font-semibold transition-colors flex items-center justify-center gap-2"
										style={{
											background: 'var(--ledger-accent)',
											color: 'var(--ledger-bg)',
											opacity: gmailLoading ? 0.6 : 1,
										}}
									>
										{gmailLoading && <Loader2 size={14} className="animate-spin" />}
										Reconnect
									</button>
									<button
										onClick={() => { void disconnectGmail(); }}
										disabled={gmailLoading}
										className="flex-1 h-11 rounded-[8px] text-sm font-semibold transition-colors"
										style={{
											background: 'transparent',
											color: 'var(--ledger-negative)',
											border: '1px solid var(--ledger-negative)',
											opacity: gmailLoading ? 0.6 : 1,
										}}
									>
										Disconnect
									</button>
								</>
							)}
							{isConnected && !requiresReconnect && (
								<button
									onClick={() => { void disconnectGmail(); }}
									disabled={gmailLoading}
									className="flex-1 h-11 rounded-[8px] text-sm font-semibold transition-colors flex items-center justify-center gap-2"
									style={{
										background: 'transparent',
										color: 'var(--ledger-negative)',
										border: '1px solid var(--ledger-negative)',
										opacity: gmailLoading ? 0.6 : 1,
									}}
								>
									{gmailLoading && <Loader2 size={14} className="animate-spin" />}
									Disconnect
								</button>
							)}
						</div>
					</div>
				</section>

				{isAiUsageAdmin && aiUsageSummary && (
					<section className="mb-8">
						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em] mb-4"
							style={{ color: 'var(--ledger-muted)' }}
						>
							AI operations
						</p>
						<button
							type="button"
							onClick={onOpenAiUsage}
							className="w-full rounded-[10px] overflow-hidden text-left active:scale-[0.99] transition-transform"
							style={cardStyle}
						>
							<div className="px-4 py-3.5 flex items-center gap-3">
								<div
									className="w-9 h-9 rounded-[9px] flex items-center justify-center shrink-0"
									style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}
								>
									<ReceiptText size={18} />
								</div>
								<div className="min-w-0 flex-1">
									<p className="text-sm font-bold" style={{ color: 'var(--ledger-fg)' }}>
										AI operations
									</p>
									<p className="text-[11px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
										{aiUsageSummary.tracked_current_month_request_count} calls
										{aiCreditRemaining !== null ? ` · ${formatUsd(aiCreditRemaining)} remaining` : ' tracked this month'}
									</p>
								</div>
								<ChevronRight size={16} style={{ color: 'var(--ledger-muted)' }} />
							</div>
							{hasAiCreditRunway && (
								<div className="px-4 pb-3.5">
									<div className="ai-runway-rail h-1.5 rounded-full overflow-hidden">
										<div className="ai-runway-fill h-full rounded-full" style={{ width: `${aiCreditUsedPercent}%` }} />
									</div>
									<div className="flex justify-between mt-1.5 text-[9px] tnum" style={{ color: 'var(--ledger-muted)' }}>
										<span>{formatUsd(aiCreditTotal - aiCreditRemaining)} used</span>
										<span>{formatUsd(aiCreditTotal)} purchased</span>
									</div>
								</div>
							)}
							<div
								className="grid grid-cols-2"
								style={{ borderTop: '1px solid var(--ledger-rule)' }}
							>
								<div className="px-4 py-3" style={{ borderRight: '1px solid var(--ledger-rule)' }}>
									<p className="text-lg font-bold tnum" style={{ color: 'var(--ledger-accent)' }}>
										{formatUsd(aiMonthCost)}
									</p>
									<p className="text-[9px] font-bold uppercase tracking-wider" style={{ color: 'var(--ledger-muted)' }}>
										This month
									</p>
								</div>
								<div className="px-4 py-3">
									<p className="text-lg font-bold tnum" style={{ color: 'var(--ledger-fg)' }}>
										{formatUsd(aiAllTimeCost)}
									</p>
									<p className="text-[9px] font-bold uppercase tracking-wider" style={{ color: 'var(--ledger-muted)' }}>
										All time
									</p>
								</div>
							</div>
						</button>
					</section>
				)}

				<section className="mb-8">
					<div className="flex items-center justify-between mb-4">
						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em]"
							style={{ color: 'var(--ledger-muted)' }}
						>
							Categories
						</p>
						<div className="flex gap-1.5">
							{(['expense', 'income'] as const).map((t) => {
								const isActive = categoryType === t;
								return (
									<button
										key={t}
										onClick={() => {
											setCategoryType(t);
											setExpandedCategoryId(null);
										}}
										className="rounded-full px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wider transition-colors"
										style={
											isActive
												? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
												: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
										}
									>
										{t}
									</button>
								);
							})}
						</div>
					</div>

					<div className="space-y-4">
						{categoriesLoading && categories.length === 0 ? (
							<div className="flex items-center gap-2 py-2">
								<Loader2 size={16} className="animate-spin" style={{ color: 'var(--ledger-muted)' }} />
								<span className="text-sm" style={{ color: 'var(--ledger-muted)' }}>Loading…</span>
							</div>
						) : categories.length === 0 ? (
							<p className="text-sm" style={{ color: 'var(--ledger-muted)' }}>
								No {categoryType} categories yet
							</p>
						) : (
						categories.map((category) => {
							const isExpanded = expandedCategoryId === category.id;
							const subInputValue = newSubNames[category.id] ?? '';

							return (
								<div key={category.id}>
									{renamingId === category.id ? (
										<div className="w-full flex items-center gap-2">
											<input
												type="text"
												value={renameValue}
												autoFocus
												onChange={(e) => setRenameValue(e.target.value)}
												onKeyDown={(e) => {
													if (e.key === 'Enter') commitRename();
													if (e.key === 'Escape') { setRenamingId(null); setRenameValue(''); }
												}}
												className="flex-1 rounded-[8px] px-3 py-2 text-base font-bold focus:outline-none"
												style={inputStyle}
											/>
											<button
												onClick={commitRename}
												disabled={!renameValue.trim()}
												style={{ color: 'var(--ledger-accent)', opacity: renameValue.trim() ? 1 : 0.4 }}
											>
												<Check size={18} />
											</button>
											<button
												onClick={() => { setRenamingId(null); setRenameValue(''); }}
												style={{ color: 'var(--ledger-muted)' }}
											>
												<X size={18} />
											</button>
										</div>
									) : (
									<button
										onClick={() => setExpandedCategoryId(isExpanded ? null : category.id)}
										className="w-full flex items-center justify-between text-left transition-colors"
									>
										<div className="flex items-center gap-2">
											<ChevronDown
												size={18}
												className={`transition-transform ${isExpanded ? '' : '-rotate-90'}`}
												style={{ color: 'var(--ledger-muted)' }}
											/>
											<span
												className="text-lg font-bold tracking-tight"
												style={{ color: 'var(--ledger-fg)' }}
											>
												{category.name}
											</span>
											{category.isSystem && (
												<span
													className="text-[9px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
													style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
												>
													Sys
												</span>
											)}
										</div>
										{category.isSystem ? (
											<span
												role="button"
												title="Hide category"
												onClick={(e) => {
													e.stopPropagation();
													void deleteCategory(category.id);
												}}
												style={{ color: 'var(--ledger-muted)', opacity: 0.6 }}
											>
												<EyeOff size={15} />
											</span>
										) : (
											<span className="flex items-center gap-3">
												<span
													role="button"
													onClick={(e) => {
														e.stopPropagation();
														setRenamingId(category.id);
														setRenameValue(category.name);
														setConfirmDeleteId(null);
													}}
													style={{ color: 'var(--ledger-muted)', opacity: 0.6 }}
												>
													<Pencil size={14} />
												</span>
												<span
													role="button"
													onClick={(e) => {
														e.stopPropagation();
														setConfirmDeleteId(category.id);
														setRenamingId(null);
													}}
													style={{ color: 'var(--ledger-negative)', opacity: 0.4 }}
												>
													<Trash2 size={15} />
												</span>
											</span>
										)}
									</button>
									)}

									{confirmDeleteId === category.id && (
										<div
											className="mt-1.5 ml-[26px] rounded-[10px] px-3 py-2.5 flex items-center justify-between gap-3"
											style={{ background: 'var(--ledger-amber-bg)', border: '1px solid rgba(184, 115, 42, 0.2)' }}
										>
											<p className="text-xs leading-snug" style={{ color: 'var(--ledger-amber)' }}>
												Delete "{category.name}" and its subcategories? Categories used by transactions can't be deleted.
											</p>
											<span className="flex items-center gap-2 shrink-0">
												<button
													onClick={() => {
														void deleteCategory(category.id);
														setConfirmDeleteId(null);
													}}
													className="text-xs font-bold px-2.5 py-1 rounded-full"
													style={{ color: '#fff', background: 'var(--ledger-negative)' }}
												>
													Delete
												</button>
												<button
													onClick={() => setConfirmDeleteId(null)}
													className="text-xs font-medium"
													style={{ color: 'var(--ledger-muted)' }}
												>
													Cancel
												</button>
											</span>
										</div>
									)}

									{isExpanded && (
										<div className="mt-1.5 ml-[26px]">
											<div
												className="rounded-[10px] overflow-hidden"
												style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
											>
												{category.subCategories.length > 0 ? (
													category.subCategories.map((sub, i) => {
														const isSubSystem = category.subCategorySystem?.[sub] ?? false;
														return (
															<div
																key={sub}
																className="px-3 py-2.5 flex items-center justify-between"
																style={i < category.subCategories.length - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
															>
																<span className="text-sm" style={{ color: 'var(--ledger-fg)' }}>
																	{sub}
																</span>
																{isSubSystem ? (
																	<button
																		title="Hide subcategory"
																		onClick={() => {
																			const subId = category.subCategoryIds?.[sub];
																			if (subId) void deleteCategory(subId);
																		}}
																		style={{ color: 'var(--ledger-muted)', opacity: 0.6 }}
																	>
																		<EyeOff size={13} />
																	</button>
																) : (
																	<button
																		onClick={() => { void deleteSubCategory(category.id, sub); }}
																		style={{ color: 'var(--ledger-muted)' }}
																	>
																		<X size={13} />
																	</button>
																)}
															</div>
														);
													})
												) : (
													<p className="px-3 py-2.5 text-xs" style={{ color: 'var(--ledger-muted)' }}>
														No subcategories yet
													</p>
												)}

												<div
													className="px-3 py-2.5 flex items-center gap-2"
													style={{ borderTop: '1px solid var(--ledger-rule)', background: 'var(--ledger-bg)' }}
												>
													<input
														type="text"
														placeholder="Add subcategory"
														value={subInputValue}
														onChange={(e) =>
															setNewSubNames({ ...newSubNames, [category.id]: e.target.value })
														}
														onKeyDown={(e) => {
															if (e.key === 'Enter' && subInputValue.trim()) {
																void addSubCategory(category.id, subInputValue.trim());
																setNewSubNames({ ...newSubNames, [category.id]: '' });
															}
														}}
														className="flex-1 rounded-[6px] px-2.5 py-1.5 focus:outline-none transition-all"
														style={inputStyle}
													/>
													<button
														onClick={() => {
															if (!subInputValue.trim()) return;
															void addSubCategory(category.id, subInputValue.trim());
															setNewSubNames({ ...newSubNames, [category.id]: '' });
														}}
														disabled={!subInputValue.trim() || categoriesLoading}
														className="w-8 h-8 rounded-[6px] flex items-center justify-center shrink-0 active:scale-95 transition-all"
														style={{
															background: 'var(--ledger-accent)',
															color: 'var(--ledger-bg)',
															opacity: (!subInputValue.trim() || categoriesLoading) ? 0.5 : 1,
														}}
													>
														<Plus size={14} />
													</button>
												</div>
											</div>
										</div>
									)}
								</div>
							);
						})
						)}
					</div>

					{categoriesError && (
						<p className="text-xs mt-3" style={{ color: 'var(--ledger-negative)' }}>{categoriesError}</p>
					)}

					<div
						className="mt-4 pt-4 flex items-center gap-2"
						style={{ borderTop: '1px solid var(--ledger-rule)' }}
					>
						<input
							type="text"
							placeholder="Add new category"
							value={newCategoryName}
							onChange={(e) => setNewCategoryName(e.target.value)}
							onKeyDown={(e) => {
								if (e.key === 'Enter' && newCategoryName.trim()) {
									void addCategory({
										name: newCategoryName.trim(),
										type: categoryType,
										icon: 'Circle',
										subCategories: [],
									});
									setNewCategoryName('');
								}
							}}
							className="flex-1 rounded-[8px] px-3 py-2.5 focus:outline-none transition-all"
							style={inputStyle}
						/>
						<button
							onClick={() => {
								if (!newCategoryName.trim()) return;
								void addCategory({
									name: newCategoryName.trim(),
									type: categoryType,
									icon: 'Circle',
									subCategories: [],
								});
								setNewCategoryName('');
							}}
							disabled={!newCategoryName.trim() || categoriesLoading}
							className="w-10 h-10 rounded-[8px] flex items-center justify-center shrink-0 active:scale-95 transition-all"
							style={{
								background: 'var(--ledger-accent)',
								color: 'var(--ledger-bg)',
								opacity: (!newCategoryName.trim() || categoriesLoading) ? 0.5 : 1,
							}}
						>
							{categoriesLoading ? <Loader2 size={16} className="animate-spin" /> : <Plus size={18} />}
						</button>
					</div>

					{hiddenForType.length > 0 && (
						<div
							className="mt-4 pt-4"
							style={{ borderTop: '1px dashed var(--ledger-rule)' }}
						>
							<button
								onClick={() => setHiddenOpen(!hiddenOpen)}
								className="w-full flex items-center gap-2 text-left"
							>
								<ChevronDown
									size={14}
									className={`transition-transform ${hiddenOpen ? '' : '-rotate-90'}`}
									style={{ color: 'var(--ledger-muted)' }}
								/>
								<span
									className="text-[10px] font-bold uppercase tracking-[0.2em]"
									style={{ color: 'var(--ledger-muted)' }}
								>
									Hidden categories ({hiddenForType.length})
								</span>
							</button>

							{hiddenOpen && (
								<div
									className="rounded-[10px] overflow-hidden mt-2"
									style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
								>
									{hiddenForType.map((h, i) => (
										<div
											key={h.id}
											className="px-3 py-2.5 flex items-center justify-between"
											style={i < hiddenForType.length - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
										>
											<span className="text-sm" style={{ color: 'var(--ledger-muted)' }}>
												{h.parentName ? `${h.parentName} · ${h.name}` : h.name}
											</span>
											<button
												onClick={() => { void restoreCategory(h.id); }}
												className="flex items-center gap-1.5 text-xs font-medium shrink-0"
												style={{ color: 'var(--ledger-accent)' }}
											>
												<RotateCcw size={12} />
												Restore
											</button>
										</div>
									))}
								</div>
							)}
						</div>
					)}
				</section>

				<section className="mb-8">
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-4"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Instruments
					</p>

					<button
						onClick={onOpenAccounts}
						className="w-full flex items-baseline py-2 text-left transition-colors"
					>
						<span
							className="text-base font-semibold"
							style={{ color: 'var(--ledger-fg)' }}
						>
							Accounts &amp; Cards
						</span>
						<DotLeader />
						<span
							className="text-[11px] font-medium tnum shrink-0"
							style={{ color: 'var(--ledger-muted)' }}
						>
							{(() => {
								// Card-type accounts are card backing folios — already
								// counted as cards, so only non-card accounts count here.
								const acctCount = accounts.filter((a) => a.type !== 'card').length;
								return `${acctCount} acct${acctCount !== 1 ? 's' : ''} · ${creditCards.length} card${creditCards.length !== 1 ? 's' : ''}`;
							})()}
						</span>
						<ChevronRight
							size={16}
							className="ml-1.5 shrink-0"
							style={{ color: 'var(--ledger-muted)', transform: 'translateY(-1px)' }}
						/>
					</button>
				</section>

				<section className="mb-8">
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-4"
						style={{ color: 'var(--ledger-muted)' }}
					>
						App
					</p>
					<InstallAppCard />
				</section>

				<div
					className="my-6"
					style={{ borderTop: '1px dashed var(--ledger-rule)' }}
				/>

				<button
					onClick={() => {
						void signOut().then(onSignOut);
					}}
					className="w-full h-12 rounded-[10px] text-base font-bold flex items-center justify-center gap-2 active:scale-[0.98] transition-all"
					style={{
						background: 'transparent',
						color: 'var(--ledger-negative)',
						border: '1px solid var(--ledger-negative)',
					}}
				>
					<LogOut size={18} />
					Sign out
				</button>
				</>
				)}
			</main>
		</div>
	);
}
