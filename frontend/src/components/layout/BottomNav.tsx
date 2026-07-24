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
		<nav className="ledger-bottom-nav shrink-0" aria-label="Primary navigation">
			{TABS.map(({ id, label, icon: Icon }) => {
				const isActive = activeTab === id;
				return (
					<button
						key={id}
						type="button"
						onClick={() => onTabChange(id)}
						aria-current={isActive ? 'page' : undefined}
						className={`ledger-bottom-nav__item${isActive ? ' ledger-bottom-nav__item--active' : ''}`}
					>
						<Icon
							className="ledger-bottom-nav__icon"
							size={24}
							strokeWidth={isActive ? 2.4 : 2}
						/>
						<span className="ledger-bottom-nav__label">{label}</span>
					</button>
				);
			})}
		</nav>
	);
}
