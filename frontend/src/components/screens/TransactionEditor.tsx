import { useMemo, useState } from 'react';
import {
	Calendar,
	ChevronLeft,
	Lock,
	Loader2,
	Link2,
	Store,
	Trash2,
	UserRound,
} from 'lucide-react';
import { useCategories } from '../../hooks/useCategories';
import { useAccounts } from '../../hooks/useAccounts';
import { useTransactions, type Transaction } from '../../hooks/useTransactions';
import type { BudgetTreatment, EventDirection, TransactionType } from '../../../../shared/types';
import {
	allowedTransactionTypes,
	transactionDirection,
} from '../../lib/transaction-adapter';
import { TransactionTypeSelector } from '../ui/TransactionTypeSelector';
import { useBudgetRelationshipActions, useRefundTargets } from '../../hooks/useBudget';

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

function MerchantIdentityCard({ transaction }: { transaction: Transaction }) {
	const detectedName = transaction.counterpartyRaw?.trim() || null;
	const isP2P = transaction.merchantType === 'P2P';
	const normalizedName = transaction.merchantName?.trim()
		|| (isP2P ? detectedName : null);
	const displayName = normalizedName ?? detectedName ?? 'Not identified';
	const identityLabel = isP2P ? 'Payee' : 'Merchant';
	const displayCaption = normalizedName
		? isP2P ? 'Normalized payee' : 'Normalized merchant'
		: 'Detected merchant';
	const [logoFailed, setLogoFailed] = useState(false);
	const showLogo = Boolean(transaction.merchantLogoUrl) && !logoFailed;

	return (
		<section aria-labelledby="transaction-merchant-heading">
			<p
				id="transaction-merchant-heading"
				className="text-[10px] font-bold uppercase tracking-[0.2em] mb-2 px-1"
				style={{ color: 'var(--ledger-muted)' }}
			>
				{identityLabel}
			</p>
			<div
				className="rounded-[10px] overflow-hidden"
				style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
			>
				<div className="flex items-center gap-3 px-3.5 py-3">
					<div
						className="w-11 h-11 shrink-0 rounded-[10px] flex items-center justify-center overflow-hidden"
						style={{
							background: showLogo ? '#fff' : 'var(--ledger-rule)',
							color: 'var(--ledger-accent)',
							border: '1px solid var(--ledger-rule)',
						}}
					>
						{showLogo ? (
							<img
								src={transaction.merchantLogoUrl ?? undefined}
								alt=""
								className="w-full h-full object-contain p-1.5"
								onError={() => setLogoFailed(true)}
							/>
						) : isP2P ? (
							<UserRound size={20} strokeWidth={1.8} />
						) : (
							<Store size={20} strokeWidth={1.8} />
						)}
					</div>
					<div className="min-w-0 flex-1">
						<p
							className="text-[9px] font-bold uppercase tracking-[0.15em] mb-0.5"
							style={{ color: 'var(--ledger-muted)' }}
						>
							{displayCaption}
						</p>
						<p
							className="text-base font-bold truncate"
							title={displayName}
							style={{ color: 'var(--ledger-fg)' }}
						>
							{displayName}
						</p>
					</div>
					{transaction.merchantType && (
						<span
							className="shrink-0 rounded-full px-2 py-1 text-[9px] font-bold uppercase tracking-[0.1em]"
							style={{
								background: 'var(--ledger-nav-active)',
								color: 'var(--ledger-accent)',
							}}
						>
							{isP2P ? 'Person' : transaction.merchantType.toLowerCase().replace('_', ' ')}
						</span>
					)}
				</div>
				<div
					className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 px-3.5 py-2.5"
					style={{ borderTop: '1px solid var(--ledger-rule)' }}
				>
					<span className="text-[10px] font-semibold" style={{ color: 'var(--ledger-muted)' }}>
						Detected name
					</span>
					<span
						className="text-xs font-medium text-right truncate"
						title={detectedName ?? 'Not available'}
						style={{ color: 'var(--ledger-fg)' }}
					>
						{detectedName ?? 'Not available'}
					</span>
				</div>
			</div>
		</section>
	);
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
	const {
		unlinkRelationship,
		linkRelationship,
		isUnlinking,
		isLinking,
		error: unlinkError,
	} = useBudgetRelationshipActions();

	const isBankDetected = mode === 'edit' && !isManual;
	const sourceFieldsLocked = isBankDetected || Boolean(editingTransaction?.budgetRelationship);
	const hasMerchantIdentity = Boolean(
		editingTransaction?.merchantName?.trim()
		|| editingTransaction?.counterpartyRaw?.trim(),
	);

	const [type, setType] = useState<TransactionType>(editingTransaction?.type ?? 'expense');
	const existingDirection = editingTransaction
		? transactionDirection(editingTransaction)
		: null;
	const [transferDirection, setTransferDirection] = useState<EventDirection>(
		existingDirection ?? 'debit',
	);
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
	const [budgetTreatment, setBudgetTreatment] = useState<BudgetTreatment>(
		editingTransaction?.budgetTreatment ?? 'AUTO',
	);
	const [submitError, setSubmitError] = useState<string | null>(null);
	const [relationshipRemoved, setRelationshipRemoved] = useState(false);
	const [linkPickerOpen, setLinkPickerOpen] = useState(false);
	const [linkKind, setLinkKind] = useState<'REFUND' | 'REIMBURSEMENT'>('REFUND');
	const [relationshipAddedLabel, setRelationshipAddedLabel] = useState<string | null>(null);
	const { targets: refundTargets, isLoading: targetsLoading, error: targetsError } = useRefundTargets(
		mode === 'edit' && editingTransaction?.type === 'income' ? editingTransaction.id : null,
		linkPickerOpen,
	);

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
	const allowedTypes = editingTransaction?.budgetRelationship
		? [editingTransaction.type]
		: existingDirection ? allowedTransactionTypes(existingDirection) : undefined;
	const selectedDirection = type === 'transfer'
		? transferDirection
		: type === 'income' ? 'credit' : 'debit';

	const handleTypeChange = (nextType: TransactionType) => {
		if (nextType === type) return;
		if (nextType === 'transfer') {
			setTransferDirection(type === 'income' ? 'credit' : 'debit');
		} else {
			setTransferDirection(nextType === 'income' ? 'credit' : 'debit');
		}
		setType(nextType);
		setSelectedParentId('');
		setSelectedSubId('');
	};

	const handleSave = async () => {
		if (txLoading) return;
		setSubmitError(null);
		const amount = parseAmountInput(amountStr);
		if (!amount && !sourceFieldsLocked) return;

		try {
			if (mode === 'edit' && editingTransaction) {
				// Reuse the original Date object when the input still shows the same
				// day — a reparsed local-noon Date would read as an immutable-field
				// change and force a needless recreate.
				const dateChanged = formatDateInput(editingTransaction.date) !== date;
				await addTransaction({
					...editingTransaction,
					amount: sourceFieldsLocked ? editingTransaction.amount : (amount ?? editingTransaction.amount),
					type,
					direction: existingDirection ?? selectedDirection,
					date:
						sourceFieldsLocked || !dateChanged
							? editingTransaction.date
							: new Date(date + 'T12:00:00'),
					categoryId: selectedParentId || null,
					subCategoryId: selectedSubId || null,
					accountId: selectedAccountId || null,
					description: note || undefined,
					budgetTreatment,
				});
			} else {
				await addTransaction({
					id: '',
					amount: amount ?? 0,
					type,
					direction: selectedDirection,
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

	const canSave = sourceFieldsLocked || parseAmountInput(amountStr) !== null;

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

			<main className="flex-1 overflow-y-auto px-5 pt-4 pb-8 flex flex-col gap-5">
				{/* BANK-DETECTED BANNER */}
				{isBankDetected && (
					<div
						className="rounded-[10px] px-3.5 py-2.5 flex items-center gap-2.5"
						style={{
							background: 'var(--ledger-amber-bg)',
							border: '1px solid rgba(184, 115, 42, 0.2)',
						}}
					>
						<Lock size={14} className="shrink-0" style={{ color: 'var(--ledger-amber)' }} />
						<p className="text-[10px] leading-none whitespace-nowrap" style={{ color: 'var(--ledger-amber)' }}>
							Gmail import · Edit type, category, account &amp; note
						</p>
					</div>
				)}

				{/* TYPE */}
				<div>
					<TransactionTypeSelector
						value={type}
						allowedTypes={allowedTypes}
						onChange={handleTypeChange}
					/>
					{mode === 'add' && type === 'transfer' && (
						<div className="grid grid-cols-2 gap-2 mt-3">
							{(['debit', 'credit'] as const).map((direction) => (
								<button
									key={direction}
									type="button"
									onClick={() => setTransferDirection(direction)}
									className="h-9 rounded-[9px] text-xs font-semibold"
									style={transferDirection === direction
										? { background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }
										: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }}
								>
									{direction === 'debit' ? 'Money out' : 'Money in'}
								</button>
							))}
						</div>
					)}
					{sourceFieldsLocked && (
						<p
							className="text-[11px] italic mt-2 text-center px-4"
							style={{ color: 'var(--ledger-muted)' }}
						>
							{isBankDetected
								? 'Amount and date come from the bank and cannot be changed.'
								: 'Unlink the refund or reimbursement before changing amount or date.'}
						</p>
					)}
					{editingTransaction?.budgetRelationship && (
						<p className="text-[11px] italic mt-2 text-center px-4" style={{ color: 'var(--ledger-muted)' }}>
							Unlink the refund or reimbursement before changing its type.
						</p>
					)}
				</div>

				{/* AMOUNT */}
				<div className={`flex flex-col items-center ${sourceFieldsLocked ? 'opacity-60 pointer-events-none' : ''}`}>
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
							readOnly={sourceFieldsLocked}
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
				<div className={sourceFieldsLocked ? 'opacity-60 pointer-events-none' : ''}>
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
				<div>
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-2 px-1"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Category
					</p>
					{categoriesForType.length === 0 ? (
						<p className="text-xs px-1" style={{ color: 'var(--ledger-muted)' }}>
							No {type} categories available
						</p>
					) : (
						<div className="grid grid-rows-2 grid-flow-col auto-cols-max gap-2 overflow-x-auto pb-2 -mb-2 no-scrollbar px-1">
							{categoriesForType.map((cat) => (
								<button
									key={cat.id}
									onClick={() => {
										if (selectedParentId !== cat.id) {
											setSelectedParentId(cat.id);
											setSelectedSubId('');
										}
									}}
									className="rounded-full px-4 py-2 text-xs font-medium transition-colors whitespace-nowrap"
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
					<div>
						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em] mb-2 px-1"
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
				<div>
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-2 px-1"
						style={{ color: 'var(--ledger-muted)' }}
					>
						Account
					</p>
					{accounts.length === 0 ? (
						<p className="text-xs px-1" style={{ color: 'var(--ledger-muted)' }}>
							No accounts available
						</p>
					) : (
						<div className="grid grid-rows-2 grid-flow-col auto-cols-max gap-2 overflow-x-auto pb-2 -mb-2 no-scrollbar px-1">
							{accounts.map((acc) => (
								<button
									key={acc.id}
									onClick={() => setSelectedAccountId(selectedAccountId === acc.id ? '' : acc.id)}
									className="rounded-full px-4 py-2 text-xs font-medium transition-colors whitespace-nowrap"
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

				{mode === 'edit' && editingTransaction && (isBankDetected || hasMerchantIdentity) && (
					<MerchantIdentityCard transaction={editingTransaction} />
				)}

				{mode === 'edit' && editingTransaction && (
					<section aria-labelledby="monthly-plan-heading">
						<p id="monthly-plan-heading" className="text-[10px] font-bold uppercase tracking-[0.2em] mb-2 px-1" style={{ color: 'var(--ledger-muted)' }}>Monthly plan</p>
						<div className="rounded-[10px] p-3.5" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
							<div className="grid grid-cols-3 gap-1 rounded-[9px] p-1" style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)' }} role="group" aria-label="Monthly plan treatment">
								{([
									['AUTO', 'Automatic'],
									['INCLUDE', 'Count'],
									['EXCLUDE', 'Outside'],
								] as const).map(([value, label]) => (
									<button key={value} type="button" onClick={() => setBudgetTreatment(value)} aria-pressed={budgetTreatment === value} className="rounded-[7px] px-1 py-2 text-[10px] font-bold transition-colors" style={budgetTreatment === value ? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' } : { color: 'var(--ledger-muted)' }}>{label}</button>
								))}
							</div>
							<p className="text-[11px] leading-relaxed mt-3" style={{ color: 'var(--ledger-muted)' }}>
								{budgetTreatment === 'INCLUDE'
									? editingTransaction.type === 'income' ? 'This credit will reduce the month’s used budget.' : 'This transaction will count against the month’s limit.'
									: budgetTreatment === 'EXCLUDE'
										? 'This remains in cash flow and Analytics, but stays outside the budget.'
										: editingTransaction.effectiveBudgetTreatment === 'INCLUDE'
											? `Currently counted automatically by ${editingTransaction.budgetTreatmentSource === 'REFUND_RELATIONSHIP' ? 'its linked refund or purchase' : editingTransaction.budgetTreatmentSource === 'RECURRING_RULE' ? 'a recurring rule' : editingTransaction.budgetTreatmentSource === 'CATEGORY_RULE' ? 'its category rule' : 'the default expense rule'}.`
											: 'Currently outside the budget automatically. Transfers and ordinary income have no effect by default.'}
							</p>
							{editingTransaction.budgetRelationship && !relationshipRemoved && (
								<div className="mt-3 pt-3 flex items-start justify-between gap-3" style={{ borderTop: '1px solid var(--ledger-rule)' }}>
									<div className="min-w-0"><p className="text-[10px] font-bold uppercase tracking-[0.14em]" style={{ color: 'var(--ledger-accent)' }}>{editingTransaction.budgetRelationship.role === 'TARGET' && editingTransaction.budgetRelationship.related_count > 1 ? `${editingTransaction.budgetRelationship.related_count} linked refunds` : editingTransaction.budgetRelationship.kind === 'REFUND' ? 'Linked refund' : 'Linked reimbursement'}</p><p className="text-xs truncate mt-1" style={{ color: 'var(--ledger-fg)' }}>{editingTransaction.budgetRelationship.role === 'SOURCE' ? 'For' : editingTransaction.budgetRelationship.related_count > 1 ? 'Latest from' : 'From'} {editingTransaction.budgetRelationship.related_label}</p><p className="text-[9px] mt-1" style={{ color: 'var(--ledger-muted)' }}>{editingTransaction.budgetRelationship.created_by === 'AUTO' ? 'Matched automatically' : 'Linked by you'}</p></div>
									<div className="text-right shrink-0"><span className="text-xs tnum block" style={{ color: 'var(--ledger-muted)' }}>₹{(editingTransaction.budgetRelationship.related_amount_in_paise / 100).toLocaleString('en-IN', { maximumFractionDigits: 2 })}</span><button type="button" disabled={isUnlinking} onClick={() => { const relationshipId = editingTransaction.budgetRelationship?.id; if (!relationshipId) return; void unlinkRelationship(relationshipId).then(() => setRelationshipRemoved(true)).catch(cause => setSubmitError(cause instanceof Error ? cause.message : 'Could not unlink refund.')); }} className="text-[10px] font-bold mt-1" style={{ color: 'var(--ledger-negative)' }}>{editingTransaction.budgetRelationship.role === 'TARGET' && editingTransaction.budgetRelationship.related_count > 1 ? 'Unlink this' : 'Undo link'}</button></div>
								</div>
							)}
							{editingTransaction.type === 'income'
								&& (!editingTransaction.budgetRelationship || relationshipRemoved)
								&& !relationshipAddedLabel && (
								<div className="mt-3 pt-3" style={{ borderTop: '1px solid var(--ledger-rule)' }}>
									<button type="button" onClick={() => setLinkPickerOpen(open => !open)} className="w-full flex items-center justify-between gap-3 text-left"><span className="flex items-center gap-2 text-xs font-bold" style={{ color: 'var(--ledger-accent)' }}><Link2 size={14} /> Link to an expense</span><span className="text-[10px]" style={{ color: 'var(--ledger-muted)' }}>{linkPickerOpen ? 'Close' : 'Refund or payback'}</span></button>
									{linkPickerOpen && (
										<div className="mt-3">
											<div className="grid grid-cols-2 gap-1 rounded-[8px] p-1 mb-2" style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)' }}>
												{(['REFUND', 'REIMBURSEMENT'] as const).map(kind => <button key={kind} type="button" onClick={() => setLinkKind(kind)} className="rounded-[6px] py-1.5 text-[10px] font-bold" style={linkKind === kind ? { background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' } : { color: 'var(--ledger-muted)' }}>{kind === 'REFUND' ? 'Purchase refund' : 'Paid back'}</button>)}
											</div>
											{targetsLoading ? <div className="py-4 flex justify-center"><Loader2 size={15} className="animate-spin" style={{ color: 'var(--ledger-accent)' }} /></div> : refundTargets.length === 0 ? <p className="text-[11px] py-2" style={{ color: 'var(--ledger-muted)' }}>No eligible expense in the previous 120 days.</p> : <div className="max-h-48 overflow-y-auto rounded-[8px]" style={{ border: '1px solid var(--ledger-rule)' }}>{refundTargets.map((target, index) => <button key={target.transaction_id} type="button" disabled={isLinking} onClick={() => { const sourceId = editingTransaction.id; void linkRelationship(sourceId, target.transaction_id, linkKind).then(() => { setRelationshipAddedLabel(target.label); setLinkPickerOpen(false); }).catch(cause => setSubmitError(cause instanceof Error ? cause.message : 'Could not link transaction.')); }} className="w-full px-3 py-2.5 flex items-center justify-between gap-3 text-left" style={index < refundTargets.length - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}><span className="min-w-0"><span className="block text-xs font-semibold truncate">{target.label}</span><span className="block text-[10px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>{new Date(target.txn_date).toLocaleDateString('en-IN', { day: 'numeric', month: 'short' })} · ₹{(target.remaining_in_paise / 100).toLocaleString('en-IN', { maximumFractionDigits: 2 })} available</span></span><span className="text-xs font-bold tnum shrink-0">₹{(target.amount_in_paise / 100).toLocaleString('en-IN', { maximumFractionDigits: 2 })}</span></button>)}</div>}
										</div>
									)}
								</div>
							)}
							{relationshipAddedLabel && <p className="mt-3 pt-3 text-[11px]" style={{ borderTop: '1px solid var(--ledger-rule)', color: 'var(--ledger-positive)' }}>Linked to {relationshipAddedLabel}. The budget has been recalculated.</p>}
						</div>
					</section>
				)}

				{/* NOTE */}
				<div>
					<p
						className="text-[10px] font-bold uppercase tracking-[0.2em] mb-2 px-1"
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

				{mode === 'edit' && editingTransaction && (
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
			</main>

			{/* Pinned action bar: the form scrolls underneath, save stays put. */}
			<div
				className="shrink-0 px-5 pt-3 pb-4 z-20"
				style={{
					background: 'var(--ledger-bg)',
					borderTop: '1px solid var(--ledger-rule)',
					boxShadow: '0 -12px 24px -12px rgba(0, 0, 0, 0.12)',
				}}
			>
				{(submitError || unlinkError || targetsError) && (
					<p className="text-xs mb-2" style={{ color: 'var(--ledger-negative)' }}>{submitError ?? unlinkError ?? targetsError}</p>
				)}
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
			</div>
		</div>
	);
}
