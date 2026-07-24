import type { ReactNode } from 'react';

interface MobileShellProps {
	children: ReactNode;
}

export function MobileShell({ children }: MobileShellProps) {
	return (
		<div
			className="pt-safe w-full max-w-[375px] h-[100dvh] flex flex-col relative overflow-hidden mx-auto"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			{children}
		</div>
	);
}
