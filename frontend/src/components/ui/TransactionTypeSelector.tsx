import { ArrowLeftRight, Info, X } from 'lucide-react';
import { useState } from 'react';
import type { TransactionType } from '../../../../shared/types';

interface TransactionTypeSelectorProps {
	value: TransactionType;
	allowedTypes?: readonly TransactionType[];
	onChange: (type: TransactionType) => void;
}

const OPTIONS: Array<{ type: TransactionType; label: string; color: string }> = [
	{ type: 'expense', label: 'Expense', color: 'var(--ledger-negative)' },
	{ type: 'income', label: 'Income', color: 'var(--ledger-positive)' },
	{ type: 'transfer', label: 'Transfer', color: 'var(--ledger-accent)' },
];

export function TransactionTypeSelector({
	value,
	allowedTypes = OPTIONS.map((option) => option.type),
	onChange,
}: TransactionTypeSelectorProps) {
	const [showTransferInfo, setShowTransferInfo] = useState(false);

	return (
		<>
			<div className="flex gap-2 w-full">
				{OPTIONS.map((option) => {
					const selected = value === option.type;
					const enabled = allowedTypes.includes(option.type);
					return (
						<div key={option.type} className="relative flex-1 min-w-0">
							<button
								type="button"
								onClick={() => enabled && onChange(option.type)}
								disabled={!enabled}
								className="w-full h-12 rounded-[10px] px-2 text-sm font-semibold flex items-center justify-center gap-1.5 transition-colors"
								style={
									selected
										? { background: option.color, color: '#fff', opacity: enabled ? 1 : 0.5 }
										: {
											background: 'var(--ledger-card)',
											color: 'var(--ledger-muted)',
											border: '1px solid var(--ledger-rule)',
											opacity: enabled ? 1 : 0.4,
										}
								}
							>
								{option.type === 'transfer' && <ArrowLeftRight size={14} />}
								{option.label}
							</button>
							{option.type === 'transfer' && (
								<button
									type="button"
									onClick={() => setShowTransferInfo(true)}
									className="absolute right-1 top-1 w-6 h-6 rounded-full flex items-center justify-center"
									style={{ color: selected ? '#fff' : 'var(--ledger-muted)' }}
									aria-label="What counts as a transfer?"
								>
									<Info size={13} />
								</button>
							)}
						</div>
					);
				})}
			</div>

			{value === 'transfer' && (
				<p className="text-[11px] mt-2 text-center" style={{ color: 'var(--ledger-muted)' }}>
					Money moved between accounts you own.
				</p>
			)}

			{showTransferInfo && (
				<div
					className="fixed inset-0 z-[80] flex items-end justify-center p-4"
					style={{ background: 'rgba(0, 0, 0, 0.45)' }}
					onClick={() => setShowTransferInfo(false)}
					role="presentation"
				>
					<div
						className="w-full max-w-md rounded-[14px] p-5"
						style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
						onClick={(event) => event.stopPropagation()}
						role="dialog"
						aria-modal="true"
						aria-labelledby="transfer-info-title"
					>
						<div className="flex items-center justify-between mb-3">
							<h2 id="transfer-info-title" className="text-base font-bold">What is a transfer?</h2>
							<button
								type="button"
								onClick={() => setShowTransferInfo(false)}
								className="w-8 h-8 rounded-full flex items-center justify-center"
								style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
								aria-label="Close transfer explanation"
							>
								<X size={16} />
							</button>
						</div>
						<p className="text-sm leading-relaxed" style={{ color: 'var(--ledger-muted)' }}>
							Use Transfer when money moves between accounts you own, such as paying your own credit card.
							Money sent to someone else is usually an Expense; money received from someone else is usually Income.
						</p>
					</div>
				</div>
			)}
		</>
	);
}
