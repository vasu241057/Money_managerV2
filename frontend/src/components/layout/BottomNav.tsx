import { Home, List as ListIcon, PieChart, Settings } from 'lucide-react';

// 'review' and 'add' are overlay signals routed by App, never tabs.
export type ScreenId = 'home' | 'ledger' | 'analytics' | 'settings' | 'review' | 'add';

interface BottomNavProps {
	activeTab: ScreenId;
	onTabChange: (tab: ScreenId) => void;
}

const TABS: { id: ScreenId; label: string; icon: typeof Home }[] = [
	{ id: 'home', label: 'Home', icon: Home },
	{ id: 'ledger', label: 'Ledger', icon: ListIcon },
	{ id: 'analytics', label: 'Analytics', icon: PieChart },
	{ id: 'settings', label: 'Settings', icon: Settings },
];

export function BottomNav({ activeTab, onTabChange }: BottomNavProps) {
	return (
		<nav
			className="h-[64px] pb-safe shrink-0 flex items-center justify-around px-2 z-10"
			style={{
				background: 'var(--ledger-card)',
				borderTop: '1px solid var(--ledger-rule)',
			}}
		>
			{TABS.map(({ id, label, icon: Icon }) => {
				const isActive = activeTab === id;
				return (
					<button
						key={id}
						onClick={() => onTabChange(id)}
						className="flex flex-col items-center justify-center w-16 h-full gap-1 transition-colors"
						style={{
							color: isActive ? 'var(--ledger-accent)' : 'var(--ledger-muted)',
						}}
					>
						<Icon
							size={22}
							style={isActive ? { fill: 'rgba(13, 92, 75, 0.15)' } : undefined}
						/>
						<span className="text-[10px] font-medium">{label}</span>
					</button>
				);
			})}
		</nav>
	);
}
