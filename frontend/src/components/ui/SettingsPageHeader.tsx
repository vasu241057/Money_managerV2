import { ChevronLeft } from 'lucide-react';

interface SettingsPageHeaderProps {
	title: string;
	subtitle: string;
	onBack: () => void;
}

export function SettingsPageHeader({ title, subtitle, onBack }: SettingsPageHeaderProps) {
	return (
		<header
			className="h-[56px] shrink-0 flex items-center gap-3 px-4 z-10"
			style={{ background: 'var(--ledger-bg)', borderBottom: '1px solid var(--ledger-rule)' }}
		>
			<button
				type="button"
				onClick={onBack}
				aria-label="Back to settings"
				className="w-9 h-9 rounded-full flex items-center justify-center active:scale-95 transition-transform"
				style={{ color: 'var(--ledger-fg)' }}
			>
				<ChevronLeft size={20} />
			</button>
			<div className="min-w-0">
				<h1 className="font-bold text-xl tracking-tight truncate">{title}</h1>
				<p className="text-[9px] uppercase font-bold tracking-[0.18em] truncate" style={{ color: 'var(--ledger-muted)' }}>
					{subtitle}
				</p>
			</div>
		</header>
	);
}
