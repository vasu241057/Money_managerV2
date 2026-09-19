import { useAuth } from '@clerk/react';
import {
	ChevronRight,
	Gauge,
	LogOut,
	Mail,
	ReceiptText,
	Store,
	Tags,
	Trash2,
	WalletCards,
	type LucideIcon,
} from 'lucide-react';
import type { OAuthSyncStatus } from '../../../../shared/types';
import { useAccounts } from '../../hooks/useAccounts';
import { useAiUsageSummary } from '../../hooks/useAiUsage';
import { useCategories } from '../../hooks/useCategories';
import { useCreditCards } from '../../hooks/useCreditCards';
import { useGmailConnection } from '../../hooks/useGmailConnection';
import { InstallAppCard } from '../ui/InstallAppCard';

interface SettingsHubProps {
	onOpenAccounts: () => void;
	onOpenBudget: () => void;
	onOpenCategories: () => void;
	onOpenMerchants: () => void;
	onOpenConnection: () => void;
	onOpenAiUsage: () => void;
	onOpenDeleteAccount: () => void;
	onSignOut: () => void;
}

interface SettingsRowProps {
	icon: LucideIcon;
	title: string;
	description: string;
	onClick: () => void;
	isLast?: boolean;
	badge?: string;
	destructive?: boolean;
}

const STATUS_LABEL: Record<OAuthSyncStatus, string> = {
	ACTIVE: 'Connected',
	DORMANT: 'Dormant',
	AUTH_REVOKED: 'Reconnect needed',
	ERROR_PAUSED: 'Paused — reconnect',
};

function formatUsd(value: number): string {
	if (value > 0 && value < 0.01) return `$${value.toFixed(4)}`;
	return `$${value.toFixed(2)}`;
}

function SettingsRow({
	icon: Icon,
	title,
	description,
	onClick,
	isLast = false,
	badge,
	destructive = false,
}: SettingsRowProps) {
	return (
		<button
			type="button"
			onClick={onClick}
			className="w-full min-h-[68px] flex items-center gap-3 px-4 py-3 text-left active:bg-black/[0.02] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset"
			style={{ borderBottom: isLast ? undefined : '1px solid var(--ledger-rule)' }}
		>
			<span
				className="w-10 h-10 rounded-[10px] flex items-center justify-center shrink-0"
				style={{
					background: destructive
						? 'color-mix(in srgb, var(--ledger-negative) 10%, transparent)'
						: 'var(--ledger-nav-active)',
					color: destructive ? 'var(--ledger-negative)' : 'var(--ledger-accent)',
				}}
			>
				<Icon size={18} />
			</span>
			<span className="min-w-0 flex-1">
				<span className="flex items-center gap-2">
					<span className="text-sm font-bold truncate" style={{ color: destructive ? 'var(--ledger-negative)' : 'var(--ledger-fg)' }}>
						{title}
					</span>
					{badge && (
						<span className="rounded px-1.5 py-0.5 text-[8px] font-bold uppercase tracking-wider" style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}>
							{badge}
						</span>
					)}
				</span>
				<span className="block text-[11px] mt-0.5 truncate" style={{ color: 'var(--ledger-muted)' }}>
					{description}
				</span>
			</span>
			{!destructive && <ChevronRight size={16} className="shrink-0" style={{ color: 'var(--ledger-muted)' }} />}
		</button>
	);
}

function SettingsGroup({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<section className="mb-6">
			<p className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3" style={{ color: 'var(--ledger-muted)' }}>
				{label}
			</p>
			<div className="rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
				{children}
			</div>
		</section>
	);
}

export function SettingsHub({
	onOpenAccounts,
	onOpenBudget,
	onOpenCategories,
	onOpenMerchants,
	onOpenConnection,
	onOpenAiUsage,
	onOpenDeleteAccount,
	onSignOut,
}: SettingsHubProps) {
	const { signOut } = useAuth();
	const { accounts, isLoading: accountsLoading } = useAccounts();
	const { creditCards, isLoading: cardsLoading } = useCreditCards();
	const {
		categories,
		hiddenCategories,
		isLoading: categoriesLoading,
	} = useCategories();
	const {
		connection,
		isLoading: connectionLoading,
	} = useGmailConnection();
	const { summary: aiUsageSummary, isAdmin: isAiUsageAdmin } = useAiUsageSummary();

	const accountCount = accounts.filter(account => account.type !== 'card').length;
	const accountDescription = accountsLoading || cardsLoading
		? 'Loading accounts and cards…'
		: `${accountCount} account${accountCount === 1 ? '' : 's'} · ${creditCards.length} card${creditCards.length === 1 ? '' : 's'}`;
	const expenseCount = categories.filter(category => category.type === 'expense').length;
	const incomeCount = categories.filter(category => category.type === 'income').length;
	const categoryDescription = categoriesLoading && categories.length === 0
		? 'Loading categories…'
		: `${expenseCount} expense · ${incomeCount} income${hiddenCategories.length > 0 ? ` · ${hiddenCategories.length} hidden` : ''}`;
	const connectionDescription = connectionLoading && !connection
		? 'Checking connection…'
		: connection
			? `${connection.email_address} · ${STATUS_LABEL[connection.sync_status]}`
			: 'Not connected';
	const aiMonthCost = aiUsageSummary?.account?.current_month_cost_usd
		?? aiUsageSummary?.tracked_current_month_cost_usd
		?? 0;

	return (
		<div className="flex-1 flex flex-col min-h-0 relative" style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}>
			<header className="h-[56px] shrink-0 flex items-center px-5 z-10" style={{ borderBottom: '1px solid var(--ledger-rule)' }}>
				<h1 className="font-bold text-xl tracking-tight">Settings</h1>
			</header>

			<main className="flex-1 overflow-y-auto px-5 pt-6 pb-24 min-h-0">
				<div className="w-full max-w-[720px] mx-auto">
					<SettingsGroup label="Money">
						<SettingsRow
							icon={Gauge}
							title="Monthly plan"
							description="Flexible limit, fixed costs, and recurring rules"
							onClick={onOpenBudget}
						/>
						<SettingsRow
							icon={WalletCards}
							title="Accounts & cards"
							description={accountDescription}
							onClick={onOpenAccounts}
						/>
						<SettingsRow
							icon={Tags}
							title="Categories"
							description={categoryDescription}
							onClick={onOpenCategories}
						/>
						<SettingsRow
							icon={Store}
							title="Merchants"
							description="Set a category per merchant"
							onClick={onOpenMerchants}
							isLast
						/>
					</SettingsGroup>

					<SettingsGroup label="Automation">
						<SettingsRow
							icon={Mail}
							title="Gmail connection"
							description={connectionDescription}
							onClick={onOpenConnection}
							isLast
						/>
					</SettingsGroup>

					{isAiUsageAdmin && aiUsageSummary && (
						<SettingsGroup label="Administration">
							<SettingsRow
								icon={ReceiptText}
								title="AI operations"
								description={`${aiUsageSummary.tracked_current_month_request_count} calls · ${formatUsd(aiMonthCost)} this month`}
								onClick={onOpenAiUsage}
								badge="Admin"
								isLast
							/>
						</SettingsGroup>
					)}

					<section className="mb-6">
						<p className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3" style={{ color: 'var(--ledger-muted)' }}>
							App & account
						</p>
						<InstallAppCard />
						<div className="mt-3 rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
							<SettingsRow
								icon={LogOut}
								title="Sign out"
								description="End this session on this device"
								onClick={() => { void signOut().then(onSignOut); }}
								destructive
							/>
							<SettingsRow
								icon={Trash2}
								title="Delete account"
								description="Permanently erase your data and sign-in"
								onClick={onOpenDeleteAccount}
								destructive
								isLast
							/>
						</div>
					</section>

					<p className="pb-2 text-center text-[9px] font-bold uppercase tracking-[0.18em] tnum" style={{ color: 'var(--ledger-muted)' }}>
						Money Manager · v0.1.0
					</p>
				</div>
			</main>
		</div>
	);
}
