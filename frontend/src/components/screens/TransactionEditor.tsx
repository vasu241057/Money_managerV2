import { useMemo, useState } from 'react';
import { ChevronLeft, Calendar, Lock, Loader2, Trash2 } from 'lucide-react';
import { useCategories } from '../../hooks/useCategories';
import { useAccounts } from '../../hooks/useAccounts';
import { useTransactions, type Transaction } from '../../hooks/useTransactions';
import type { TransactionType } from '../../../../shared/types';

interface TransactionEditorProps {
	mode: 'add' | 'edit';
	editingTransaction?: Transaction | null;
	isManual?: boolean;
	onClose: () => void;
}

const inputStyle: React.CSSProperties = {
	background: 'var(--ledger-card)',
	border: '1px solid var(--ledger-rule)',
	color: 'var(--ledger-fg)',
	fontSize: '16px',
};

function formatDateInput(date: Date): string {
	// Local date parts, not toISOString(): UTC would show the previous day
	// for early-morning IST timestamps.
	const y = date.getFullYear();
	const m = String(date.getMonth() + 1).padStart(2, '0');
	const d = String(date.getDate()).padStart(2, '0');
	return `${y}-${m}-${d}`;
}

function formatDateDisplay(date: Date): string {
	return date.toLocaleDateString('en-IN', { month: 'short', day: 'numeric', year: 'numeric' });
}

function parseAmountInput(value: string): number | null {
	const cleaned = value.replace(/[₹,\s]/g, '');
	const num = parseFloat(cleaned);
	return isNaN(num) ? null : num;
}

export function TransactionEditor({
	mode,
	editingTransaction = null,
	isManual = true,
	onClose,
}: TransactionEditorProps) {
	const { categories: allCategories } = useCategories();
	const { accounts } = useAccounts();
	const { addTransaction, deleteTransaction, isLoading: txLoading } = useTransactions();

	const isBankDetected = mode === 'edit' && !isManual;
	const immutableLocked = isBankDetected;

	const [type, setType] = useState<TransactionType>(editingTransaction?.type ?? 'expense');
	const [amountStr, setAmountStr] = useState(
		editingTransaction ? editingTransaction.amount.toFixed(2).replace(/\.00$/, '') : '',
	);
	const [date, setDate] = useState(
		editingTransaction ? formatDateInput(editingTransaction.date) : formatDateInput(new Date()),
	);
	const [selectedParentId, setSelectedParentId] = useState<string>(
		editingTransaction?.categoryId ?? '',
	);
	const [selectedSubId, setSelectedSubId] = useState<string>(
		editingTransaction?.subCategoryId ?? '',
	);
	const [selectedAccountId, setSelectedAccountId] = useState<string>(
		editingTransaction?.accountId ?? '',
	);
	const [note, setNote] = useState(editingTransaction?.description ?? '');
	const [submitError, setSubmitError] = useState<string | null>(null);

	const title = mode === 'edit' ? 'Edit Transaction' : 'Add Transaction';
	const saveLabel = mode === 'edit' ? 'Update Transaction' : 'Save Transaction';

	// Filter categories by selected type, build parent → sub mapping
	const categoriesForType = useMemo(
		() => allCategories.filter((c) => c.type === type),
		[allCategories, type],
	);

	const selectedParent = categoriesForType.find((c) => c.id === selectedParentId);
	const subCategories = selectedParent?.subCategories ?? [];
	const subCategoryIds = selectedParent?.subCategoryIds ?? {};

	const handleSave = async () => {
		if (txLoading) return;
		setSubmitError(null);
		const amount = parseAmountInput(amountStr);
		if (!amount && !immutableLocked) return;

		try {
			if (mode === 'edit' && editingTransaction) {
				// Reuse the original Date object when the input still shows the same
				// day — a reparsed local-noon Date would read as an immutable-field
				// change and force a needless recreate.
				const dateChanged = formatDateInput(editingTransaction.date) !== date;
				await addTransaction({
					...editingTransaction,
					amount: immutableLocked ? editingTransaction.amount : (amount ?? editingTransaction.amount),
					type: immutableLocked ? editingTransaction.type : type,
					date:
						immutableLocked || !dateChanged
							? editingTransaction.date
							: new Date(date + 'T12:00:00'),
					categoryId: selectedParentId || null,
					subCategoryId: selectedSubId || null,
					accountId: selectedAccountId || null,
					description: note || undefined,
				});
			} else {
				await addTransaction({
					id: '',
					amount: amount ?? 0,
					type,
					category: 'Uncategorized',
					categoryId: selectedParentId || null,
					subCategoryId: selectedSubId || null,
					date: new Date(date + 'T12:00:00'),
					description: note || undefined,
					accountId: selectedAccountId || null,
					isManual: true,
				});
			}
			onClose();
		} catch (err) {
			setSubmitError(err instanceof Error ? err.message : 'Failed to save. Try again.');
		}
	};

	const handleDelete = async () => {
		if (!editingTransaction || txLoading) return;
		setSubmitError(null);
		try {
			await deleteTransaction(editingTransaction.id);
			onClose();
		} catch (err) {
			setSubmitError(err instanceof Error ? err.message : 'Failed to delete. Try again.');
		}
	};

	const canSave = immutableLocked || parseAmountInput(amountStr) !== null;

	return (
		<div
			className="absolute inset-0 z-50 flex flex-col"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			<header
				className="h-[56px] shrink-0 flex items-center justify-between px-5 z-10"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<button
					onClick={onClose}
					className="w-8 h-8 rounded-full flex items-center justify-center transition-colors"
					style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
				>
					<ChevronLeft size={20} />
				</button>
				<h1 className="font-bold text-xl tracking-tight" style={{ color: 'var(--ledger-fg)' }}>
					{title}
				</h1>
				<div className="w-8 h-8" />
			</header>

			<main className="flex-1 overflow-y-auto px-5 pt-5 pb-8">
				{/* BANK-DETECTED BANNER */}
				{isBankDetected && (
					<div
						className="rounded-[10px] px-4 py-3 mb-6 flex items-start gap-3"
						style={{
							background: 'var(--ledger-amber-bg)',
							border: '1px solid rgba(184, 115, 42, 0.2)',
						}}
					>
						<Lock size={15} className="shrink-0 mt-0.5" style={{ color: 'var(--ledger-amber)' }} />
						<p className="text-xs leading-relaxed" style={{ color: 'var(--ledger-amber)' }}>
							Imported from Gmail. Amount, type, and date are locked. Category, account, and note remain editable.
						</p>
					</div>
				)}

				{/* TYPE TOGGLE */}
				<div className="mb-8">
					<div className={`flex gap-2 w-full ${immutableLocked ? 'opacity-50 cursor-not-allowed' : ''}`}>
						<button
							onClick={() => {
								if (immutableLocked || type === 'expense') return;
								setType('expense');
								setSelectedParentId('');
								setSelectedSubId('');
							}}
							disabled={immutableLocked}
							className="flex-1 h-12 rounded-[10px] text-sm font-semibold flex items-center justify-center transition-colors"
							style={
								type === 'expense'
									? { background: 'var(--ledger-negative)', color: '#fff' }
									: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
							}
						>
							Expense
						</button>
						<button
							onClick={() => {
								if (immutableLocked || type === 'income') return;
								setType('income');
								setSelectedParentId('');
								setSelectedSubId('');
							}}
							disabled={immutableLocked}
							className="flex-1 h-12 rounded-[10px] text-sm font-semibold flex items-center justify-center transition-colors"
							style={
								type === 'income'
									? { background: 'var(--ledger-positive)', color: '#fff' }
									: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
							}
						>
							Income
						</button>
					</div>
					{immutableLocked && (
						<p className="text-[11px] italic mt-2 text-center px-4" style={{ color: 'var(--ledger-muted)' }}>
							Amount, type, and date cannot be changed for bank-detected transactions.
						</p>
					)}
				</div>

				{/* AMOUNT */}
				<div className={`mb-8 flex flex-col items-center ${immutableLocked ? 'opacity-60 pointer-events-none' : ''}`}>
					<div className="flex items-baseline justify-center relative w-full px-8">
						<span
							className="text-2xl font-medium absolute left-4 top-1/2 -translate-y-1/2"
							style={{ color: 'var(--ledger-muted)' }}
						>
							₹
						</span>
						<input
							type="text"
							inputMode="decimal"
							value={amountStr}
							onChange={(e) => setAmountStr(e.target.value)}
							readOnly={immutableLocked}
							className="w-full text-center text-5xl tracking-tighter font-bold tnum bg-transparent outline-none py-2"
							style={{ color: 'var(--ledger-fg)', fontSize: '16px' }}
							placeholder="0"
						/>
					</div>
					<div
						className="w-full mt-2"
						style={{
							borderTop: '1px solid var(--ledger-rule)',
							borderBottom: '1px solid var(--ledger-rule)',
							height: '3px',
						}}
					/>
				</div>

				{/* DATE */}
				<div className={`mb-6 ${immutableLocked ? 'opacity-60 pointer-events-none' : ''}`}>
					<label className="block w-full">
						<div
							className="w-full rounded-[10px] px-4 py-3.5 flex justify-between items-center transition-colors cursor-pointer"
							style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
						>
							<span
								className="text-[10px] font-bold uppercase tracking-[0.2em]"
								style={{ color: 'var(--ledger-muted)' }}
							>
								Date
							</span>
							<div className="flex items-center gap-2">
								<span className="text-sm font-medium tnum" style={{ color: 'var(--ledger-fg)' }}>
									{formatDateDisplay(new Date(date + 'T12:00:00'))}
								</span>
								<Calendar size={16} style={{ color: 'var(--ledger-muted)' }} />
							</div>
						</div>
						<input
							type="date"
							value={date}
							onChange={(e) => setDate(e.target.value)}
							className="sr-only"
						/>
					</label>
				</div>

				{/* CATEGORY */}
				<div className="mb-6">
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3 px-1"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Category
					</p>
					{categoriesForType.length === 0 ? (
						<p className="text-xs px-1" style={{ color: 'var(--ledger-muted)' }}>
							No {type} categories available
						</p>
					) : (
						<div className="flex gap-2 overflow-x-auto pb-2 -mb-2 no-scrollbar px-1">
							{categoriesForType.map((cat) => (
								<button
									key={cat.id}
									onClick={() => {
										if (selectedParentId !== cat.id) {
											setSelectedParentId(cat.id);
											setSelectedSubId('');
										}
									}}
									className="shrink-0 rounded-full px-4 py-2 text-xs font-medium transition-colors"
									style={
										selectedParentId === cat.id
											? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
											: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
									}
								>
									{cat.name}
								</button>
							))}
						</div>
					)}
				</div>

				{/* SUB-CATEGORY */}
				{subCategories.length > 0 && (
					<div className="mb-6">
						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3 px-1"
							style={{ color: 'var(--ledger-muted)' }}
						>
							Sub-category
						</p>
						<div className="flex gap-2 overflow-x-auto pb-2 -mb-2 no-scrollbar px-1">
							{subCategories.map((sub) => {
								const subId = subCategoryIds[sub];
								return (
									<button
										key={sub}
										onClick={() => subId && setSelectedSubId(selectedSubId === subId ? '' : subId)}
										className="shrink-0 rounded-full px-4 py-2 text-xs font-medium transition-colors"
										style={
											selectedSubId === subId
												? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
												: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
										}
									>
										{sub}
									</button>
								);
							})}
						</div>
					</div>
				)}

				{/* ACCOUNT */}
				<div className="mb-6">
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3 px-1"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Account
					</p>
					{accounts.length === 0 ? (
						<p className="text-xs px-1" style={{ color: 'var(--ledger-muted)' }}>
							No accounts available
						</p>
					) : (
						<div className="flex gap-2 overflow-x-auto pb-2 -mb-2 no-scrollbar px-1">
							{accounts.map((acc) => (
								<button
									key={acc.id}
									onClick={() => setSelectedAccountId(selectedAccountId === acc.id ? '' : acc.id)}
									className="shrink-0 rounded-full px-4 py-2 text-xs font-medium transition-colors"
									style={
										selectedAccountId === acc.id
											? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
											: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
									}
								>
									{acc.name}
								</button>
							))}
						</div>
					)}
				</div>

				{/* NOTE */}
				<div className="mb-8">
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3 px-1"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Note
					</p>
					<textarea
						value={note}
						onChange={(e) => setNote(e.target.value)}
						className="w-full rounded-[10px] px-4 py-3.5 text-sm focus:outline-none transition-all resize-none"
						style={inputStyle}
						rows={3}
						placeholder="Add a note"
					/>
				</div>

				{submitError && (
					<p className="text-xs mb-4" style={{ color: 'var(--ledger-negative)' }}>{submitError}</p>
				)}

				{/* ACTION BUTTONS */}
				<div className="flex flex-col gap-3 mt-4">
					<button
						onClick={() => { void handleSave(); }}
						disabled={!canSave || txLoading}
						className="w-full h-14 rounded-[10px] text-sm font-bold active:scale-[0.98] transition-all flex items-center justify-center gap-2"
						style={{
							background: 'var(--ledger-accent)',
							color: 'var(--ledger-bg)',
							boxShadow: '0 8px 24px -8px var(--ledger-accent)',
							opacity: (!canSave || txLoading) ? 0.5 : 1,
						}}
					>
						{txLoading && <Loader2 size={16} className="animate-spin" />}
						{saveLabel}
					</button>

					{mode === 'edit' && editingTransaction && !isBankDetected && (
						<button
							onClick={() => { void handleDelete(); }}
							disabled={txLoading}
							className="w-full h-14 rounded-[10px] text-sm font-bold active:scale-[0.98] transition-all flex items-center justify-center gap-2"
							style={{
								background: 'var(--ledger-card)',
								color: 'var(--ledger-negative)',
								border: '1px solid rgba(196, 83, 54, 0.3)',
							}}
						>
							<Trash2 size={16} />
							Delete Transaction
						</button>
					)}
				</div>
			</main>
		</div>
	);
}
