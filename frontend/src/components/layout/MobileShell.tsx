import type { ReactNode } from 'react';

interface MobileShellProps {
	children: ReactNode;
}

export function MobileShell({ children }: MobileShellProps) {
	return (
		<div
			className="app-shell pt-safe w-full h-[100dvh] min-h-[100dvh] flex flex-col relative overflow-hidden"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			{children}
		</div>
	);
}
