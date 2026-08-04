import { Loader2, Mail } from 'lucide-react';
import type { OAuthSyncStatus } from '../../../../shared/types';
import { useGmailConnection } from '../../hooks/useGmailConnection';
import { SettingsPageHeader } from '../ui/SettingsPageHeader';

interface GmailConnectionSettingsProps {
	onBack: () => void;
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
	if (!timestampMs || timestampMs <= 0) return 'Never synced';
	const diff = Date.now() - timestampMs;
	if (diff < 60_000) return 'Synced just now';
	const mins = Math.floor(diff / 60_000);
	if (mins < 60) return `Synced ${mins}m ago`;
	const hours = Math.floor(mins / 60);
	if (hours < 24) return `Synced ${hours}h ago`;
	return `Synced ${Math.floor(hours / 24)}d ago`;
}

export function GmailConnectionSettings({ onBack }: GmailConnectionSettingsProps) {
	const {
		connection,
		processingStats,
		connectGmail,
		disconnectGmail,
		isLoading,
		error,
	} = useGmailConnection();

	const syncStatus = connection?.sync_status ?? null;
	const statusColor = syncStatus ? STATUS_DOT_COLOR[syncStatus] : 'var(--ledger-muted)';
	const statusLabel = syncStatus ? STATUS_LABEL[syncStatus] : 'Disconnected';
	const requiresReconnect = syncStatus === 'AUTH_REVOKED' || syncStatus === 'ERROR_PAUSED';
	const isConnected = connection !== null;
	const stats = [
		{ label: 'Total', value: processingStats.raw_email_total_count, color: 'var(--ledger-fg)' },
		{ label: 'Pending', value: processingStats.raw_email_pending_count, color: 'var(--ledger-amber)' },
		{ label: 'Processed', value: processingStats.raw_email_processed_count, color: 'var(--ledger-positive)' },
		{ label: 'Failed', value: processingStats.raw_email_failed_count, color: 'var(--ledger-negative)' },
		{ label: 'Unrecognized', value: processingStats.raw_email_unrecognized_count, color: 'var(--ledger-muted)' },
		{ label: 'Ignored', value: processingStats.raw_email_ignored_count, color: 'var(--ledger-muted)' },
	];

	return (
		<div className="flex-1 flex flex-col min-h-0 relative" style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}>
			<SettingsPageHeader title="Gmail connection" subtitle="Email imports & processing" onBack={onBack} />

			<main className="flex-1 overflow-y-auto px-5 pt-6 pb-24 min-h-0">
				<div className="w-full max-w-[720px] mx-auto">
					<section className="mb-6">
						<p className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3" style={{ color: 'var(--ledger-muted)' }}>
							Connection
						</p>
						<div className="rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
							<div className="flex items-center gap-3 px-4 py-4">
								<span className="w-10 h-10 rounded-[10px] flex items-center justify-center shrink-0" style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}>
									<Mail size={18} />
								</span>
								<div className="min-w-0 flex-1">
									<p className="text-sm font-bold truncate">{connection?.email_address ?? 'No Gmail account connected'}</p>
									<p className="text-[11px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
										{connection ? formatSyncAgo(connection.last_sync_timestamp) : 'Connect an account to import transaction emails'}
									</p>
								</div>
								<span className="flex items-center gap-1.5 shrink-0 text-[10px] font-bold uppercase tracking-wider" style={{ color: statusColor }}>
									<span className="w-2 h-2 rounded-full" style={{ background: statusColor }} />
									{statusLabel}
								</span>
							</div>

							{error && (
								<p className="px-4 py-2.5 text-xs" style={{ color: 'var(--ledger-negative)', borderTop: '1px solid var(--ledger-rule)' }}>
									{error}
								</p>
							)}

							<div className="flex gap-2 px-4 py-3" style={{ background: 'var(--ledger-bg)', borderTop: '1px solid var(--ledger-rule)' }}>
								{(!isConnected || requiresReconnect) && (
									<button
										type="button"
										onClick={() => { void connectGmail(); }}
										disabled={isLoading}
										className="flex-1 h-11 rounded-[9px] text-sm font-semibold flex items-center justify-center gap-2 disabled:opacity-60"
										style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }}
									>
										{isLoading && <Loader2 size={14} className="animate-spin" />}
										{requiresReconnect ? 'Reconnect' : 'Connect Gmail'}
									</button>
								)}
								{isConnected && (
									<button
										type="button"
										onClick={() => { void disconnectGmail(); }}
										disabled={isLoading}
										className="flex-1 h-11 rounded-[9px] text-sm font-semibold flex items-center justify-center gap-2 disabled:opacity-60"
										style={{ color: 'var(--ledger-negative)', border: '1px solid var(--ledger-negative)' }}
									>
										{isLoading && <Loader2 size={14} className="animate-spin" />}
										Disconnect
									</button>
								)}
							</div>
						</div>
					</section>

					<section>
						<p className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3" style={{ color: 'var(--ledger-muted)' }}>
							Email processing
						</p>
						<div className="grid grid-cols-3 rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
							{stats.map((stat, index) => (
								<div
									key={stat.label}
									className="px-2 py-3 text-center"
									style={{
										borderRight: (index + 1) % 3 !== 0 ? '1px solid var(--ledger-rule)' : undefined,
										borderBottom: index < 3 ? '1px solid var(--ledger-rule)' : undefined,
									}}
								>
									<p className="text-lg font-bold tnum leading-tight" style={{ color: stat.color }}>{stat.value}</p>
									<p className="text-[9px] font-bold uppercase tracking-wider mt-1" style={{ color: 'var(--ledger-muted)' }}>{stat.label}</p>
								</div>
							))}
						</div>
					</section>
				</div>
			</main>
		</div>
	);
}
