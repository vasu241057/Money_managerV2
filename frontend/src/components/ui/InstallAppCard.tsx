import {
	Check,
	Download,
	Loader2,
	PlusSquare,
	Share,
	Smartphone,
} from 'lucide-react';
import { useInstallApp } from '../../hooks/useInstallApp';

export function InstallAppCard() {
	const {
		canPrompt,
		install,
		isAppleMobile,
		isInstalled,
		isPrompting,
		lastOutcome,
	} = useInstallApp();

	return (
		<div
			className="overflow-hidden rounded-[10px]"
			style={{
				background: 'var(--ledger-card)',
				border: '1px solid var(--ledger-rule)',
			}}
		>
			<div className="flex items-start gap-3 px-4 py-4">
				<div
					className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[9px]"
					style={{
						background: 'rgba(13, 92, 75, 0.08)',
						border: '1px solid rgba(13, 92, 75, 0.14)',
						color: 'var(--ledger-accent)',
					}}
				>
					{isInstalled ? <Check size={19} strokeWidth={2.2} /> : <Smartphone size={19} />}
				</div>

				<div className="min-w-0 flex-1">
					<div className="flex items-center justify-between gap-2">
						<p className="text-sm font-bold" style={{ color: 'var(--ledger-fg)' }}>
							Money Manager app
						</p>
						<span
							className="shrink-0 rounded-full px-2 py-0.5 text-[9px] font-bold uppercase tracking-[0.14em]"
							style={{
								background: isInstalled
									? 'rgba(45, 122, 79, 0.1)'
									: 'rgba(13, 92, 75, 0.08)',
								color: isInstalled
									? 'var(--ledger-positive)'
									: 'var(--ledger-accent)',
							}}
						>
							{isInstalled ? 'Installed' : 'Home screen'}
						</span>
					</div>

					<p
						className="mt-1 text-xs leading-relaxed"
						style={{ color: 'var(--ledger-muted)' }}
					>
						{isInstalled
							? 'Opens full-screen from your home screen.'
							: 'Open your ledger like an app, without browser controls.'}
					</p>
				</div>
			</div>

			{!isInstalled && (
				<div
					className="px-4 py-3"
					style={{
						background: 'var(--ledger-bg)',
						borderTop: '1px solid var(--ledger-rule)',
					}}
				>
					{canPrompt ? (
						<button
							type="button"
							onClick={() => { void install(); }}
							disabled={isPrompting}
							className="flex h-11 w-full items-center justify-center gap-2 rounded-[8px] text-sm font-semibold transition-all active:scale-[0.98] disabled:opacity-60"
							style={{
								background: 'var(--ledger-accent)',
								color: 'var(--ledger-bg)',
							}}
						>
							{isPrompting
								? <Loader2 size={15} className="animate-spin" />
								: <Download size={15} />}
							{isPrompting ? 'Opening install…' : 'Add to home screen'}
						</button>
					) : isAppleMobile ? (
						<div className="flex items-center gap-2.5">
							<span className="flex items-center gap-1.5 text-xs font-semibold" style={{ color: 'var(--ledger-fg)' }}>
								<Share size={14} style={{ color: 'var(--ledger-accent)' }} />
								Tap Share
							</span>
							<span aria-hidden="true" style={{ color: 'var(--ledger-muted)' }}>→</span>
							<span className="flex items-center gap-1.5 text-xs font-semibold" style={{ color: 'var(--ledger-fg)' }}>
								<PlusSquare size={14} style={{ color: 'var(--ledger-accent)' }} />
								Add to Home Screen
							</span>
						</div>
					) : (
						<p className="text-xs leading-relaxed" style={{ color: 'var(--ledger-muted)' }}>
							Open your browser menu and choose <strong style={{ color: 'var(--ledger-fg)' }}>Install app</strong>.
						</p>
					)}

					{lastOutcome === 'dismissed' && (
						<p
							className="mt-2 text-center text-[10px]"
							role="status"
							style={{ color: 'var(--ledger-muted)' }}
						>
							Installation cancelled. You can try again anytime.
						</p>
					)}
				</div>
			)}
		</div>
	);
}
