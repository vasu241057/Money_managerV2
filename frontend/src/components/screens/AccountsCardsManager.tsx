import { useMemo, useState } from 'react';
import {
	ChevronDown, ChevronLeft, ChevronUp, Trash2, Plus, Pencil, Loader2, Check, X,
} from 'lucide-react';
import { useAccounts } from '../../hooks/useAccounts';
import { useAnalyticsSummary } from '../../hooks/useAnalyticsSummary';
import { useCardStatements } from '../../hooks/useCardStatements';
import { useCreditCards } from '../../hooks/useCreditCards';
import { useTransactions } from '../../hooks/useTransactions';
import { paiseToRupees, rupeesToPaise } from '../../lib/money';
import type { AccountType, CardNetwork } from '../../../../shared/types';

const TYPE_COLOR: Record<AccountType, string> = {
	cash: 'var(--ledger-positive)',
	bank: 'var(--ledger-accent)',
	card: 'var(--ledger-amber)',
	other: 'var(--ledger-muted)',
};

const TYPE_CODE: Record<AccountType, string> = {
	cash: 'CASH',
	bank: 'BANK',
	card: 'CARD',
	other: 'OTHER',
};

const NETWORK_OPTIONS: { label: string; value: CardNetwork }[] = [
	{ label: 'Visa', value: 'visa' },
	{ label: 'Mastercard', value: 'mastercard' },
	{ label: 'RuPay', value: 'rupay' },
	{ label: 'Amex', value: 'amex' },
	{ label: 'Diners', value: 'diners' },
	{ label: 'Other', value: 'other' },
];

const inputStyle: React.CSSProperties = {
	background: 'var(--ledger-bg)',
	border: '1px solid var(--ledger-rule)',
	color: 'var(--ledger-fg)',
	fontSize: '16px',
};

const cardStyle: React.CSSProperties = {
	background: 'var(--ledger-card)',
	border: '1px solid var(--ledger-rule)',
};

function digitsOnly(value: string, max = 4): string {
	return value.replace(/\D/g, '').slice(0, max);
}

// For "type/paste your account number" fields: keep only the trailing 4
// digits — the backend accepts exactly 4 digits or null.
function keepLast4Digits(value: string): string {
	return value.replace(/\D/g, '').slice(-4);
}

interface AccountDraft {
	name: string;
	type: AccountType;
	last4: string;
}

interface CardDraft {
	label: string;
	issuer: string;
	network: CardNetwork;
	first4: string;
	last4: string;
	accountId: string;
	isActive: boolean;
	// The real password never reaches the browser; a stored one shows as a
	// sentinel of the same length (server reveals only the length). Only a
	// value different from the sentinel is submitted.
	pdfPassword: string;
	pdfPasswordSentinel: string;
	billingCycleDay: string;
}

function pdfPasswordSentinelFor(card: { has_pdf_password?: boolean; pdf_password_length?: number | null }): string {
	if (!card.has_pdf_password) return '';
	return '•'.repeat(card.pdf_password_length ?? 8);
}

interface BillDraft {
	total: string;
	dueDate: string;
}

function formatDueDate(dateOnly: string): string {
	return new Date(dateOnly + 'T12:00:00').toLocaleDateString('en-IN', {
		day: 'numeric',
		month: 'short',
	});
}

function formatRupees(paise: number): string {
	const rupees = paiseToRupees(paise);
	const isNegative = rupees < 0;
	const abs = Math.abs(rupees);
	const formatted = abs.toLocaleString('en-IN', {
		minimumFractionDigits: 2,
		maximumFractionDigits: 2,
	});
	return isNegative ? `−₹${formatted}` : `₹${formatted}`;
}

interface AccountsCardsManagerProps {
	onBack: () => void;
}

export function AccountsCardsManager({ onBack }: AccountsCardsManagerProps) {
	const {
		accounts,
		addAccount,
		updateAccount,
		deleteAccount,
		isLoading: accountsLoading,
		error: accountsError,
	} = useAccounts();
	const {
		creditCards,
		addCreditCard,
		updateCreditCard,
		deleteCreditCard,
		isLoading: cardsLoading,
		error: cardsError,
	} = useCreditCards();
	const { summary, isLoading: summaryLoading } = useAnalyticsSummary();
	const { latestByCard, saveStatement, isLoading: statementsLoading } = useCardStatements();
	// Feed loads in the background (shared cache) — only tracked-spend uses it.
	const { transactions } = useTransactions();
	const [billEntryCardId, setBillEntryCardId] = useState<string | null>(null);
	const [billDraft, setBillDraft] = useState<BillDraft>({ total: '', dueDate: '' });

	const [filter, setFilter] = useState<'accounts' | 'cards'>('accounts');
	const [editingAccountId, setEditingAccountId] = useState<string | null>(null);
	const [addingAccount, setAddingAccount] = useState(false);
	const [newAccountName, setNewAccountName] = useState('');
	const [newAccountLast4, setNewAccountLast4] = useState('');
	const [editingCardId, setEditingCardId] = useState<string | null>(null);
	const [addingCard, setAddingCard] = useState(false);
	const [newCardLabel, setNewCardLabel] = useState('');
	const [newCardIssuer, setNewCardIssuer] = useState('');
	const [newCardNetwork, setNewCardNetwork] = useState<CardNetwork>('visa');
	const [newCardFirst4, setNewCardFirst4] = useState('');
	const [newCardLast4, setNewCardLast4] = useState('');
	const [newCardAccountId, setNewCardAccountId] = useState('');
	const [relinkNote, setRelinkNote] = useState<string | null>(null);
	const [editAccountDraft, setEditAccountDraft] = useState<AccountDraft | null>(null);
	const [editCardDraft, setEditCardDraft] = useState<CardDraft | null>(null);
	const [confirmDeleteAccountId, setConfirmDeleteAccountId] = useState<string | null>(null);

	const cardAccounts = useMemo(
		() => accounts.filter((a) => a.type === 'card'),
		[accounts],
	);

	// Card accounts live in the Cards tab (as the card's spend figure); the
	// Accounts tab shows only non-card folios.
	const visibleAccounts = useMemo(
		() => accounts.filter((a) => a.type !== 'card'),
		[accounts],
	);

	// Card-type accounts with no registered card identity: the engine can't
	// link their transactions by last4 until the card is registered here.
	const unregisteredCardAccounts = useMemo(() => {
		const registeredAccountIds = new Set(creditCards.map((c) => c.account_id));
		return cardAccounts.filter((a) => !registeredAccountIds.has(a.id));
	}, [cardAccounts, creditCards]);

	// Balances are derived purely from transactions (income adds, expense
	// subtracts, transfers neutral) — computed server-side in the analytics
	// summary so this screen never waits on the full feed.
	const deltaByAccount = useMemo(() => {
		const map = new Map<string, number>();
		for (const account of summary?.accounts ?? []) {
			map.set(
				account.account_id,
				paiseToRupees(account.income_in_paise - account.expense_in_paise),
			);
		}
		return map;
	}, [summary]);

	const netActivity = visibleAccounts.reduce((acc, a) => acc + (deltaByAccount.get(a.id) ?? 0), 0);
	const isLoading = accountsLoading || cardsLoading || summaryLoading;
	const error = accountsError ?? cardsError;

	const newAccountLast4Valid = newAccountLast4 === '' || newAccountLast4.length === 4;

	// Spend inside a statement period on the card's backing account — the
	// number to hold against the billed total. Approximate by design: the
	// bill also carries carried-forward balance, fees, and interest.
	const trackedForPeriod = (
		accountId: string,
		periodStart: string | null,
		periodEnd: string | null,
	): number | null => {
		if (!periodStart || !periodEnd) return null;
		let total = 0;
		for (const t of transactions) {
			if (t.accountId !== accountId || t.type !== 'expense') continue;
			const day = t.date.toLocaleDateString('en-CA');
			if (day >= periodStart && day <= periodEnd) total += t.amount;
		}
		return total;
	};

	const handleSaveBill = async (creditCardId: string) => {
		const totalRupees = Number.parseFloat(billDraft.total);
		if (!Number.isFinite(totalRupees) || totalRupees <= 0) return;
		try {
			await saveStatement({
				credit_card_id: creditCardId,
				total_due_in_paise: rupeesToPaise(totalRupees),
				due_date: billDraft.dueDate || null,
			});
			setBillEntryCardId(null);
			setBillDraft({ total: '', dueDate: '' });
		} catch {
			// hook error state renders; keep the form open
		}
	};

	const handleAddAccount = async () => {
		if (!newAccountName.trim() || !newAccountLast4Valid) return;
		// Only bank folios are user-created here: cards get theirs from the
		// Cards tab, and the Cash folio is seeded once.
		await addAccount({
			name: newAccountName.trim(),
			type: 'bank',
			instrument_last4: newAccountLast4 || null,
		});
		setNewAccountName('');
		setNewAccountLast4('');
		setAddingAccount(false);
	};

	const handleSaveAccountEdit = async () => {
		if (!editingAccountId || !editAccountDraft) return;
		if (!editAccountDraft.name.trim()) return;
		if (editAccountDraft.last4 !== '' && editAccountDraft.last4.length !== 4) return;
		try {
			await updateAccount(editingAccountId, {
				name: editAccountDraft.name.trim(),
				type: editAccountDraft.type,
				instrument_last4: editAccountDraft.last4 || null,
			});
			setEditingAccountId(null);
			setEditAccountDraft(null);
		} catch {
			// hook error state renders; keep the form open
		}
	};

	const handleSaveCardEdit = async () => {
		if (!editingCardId || !editCardDraft) return;
		if (
			!editCardDraft.label.trim() ||
			(editCardDraft.first4 !== '' && editCardDraft.first4.length !== 4) ||
			editCardDraft.last4.length !== 4 ||
			!editCardDraft.accountId
		) {
			return;
		}
		const cycleDay = editCardDraft.billingCycleDay
			? Number.parseInt(editCardDraft.billingCycleDay, 10)
			: null;
		if (cycleDay !== null && (!Number.isInteger(cycleDay) || cycleDay < 1 || cycleDay > 31)) return;
		try {
			const { relinkedTransactionCount } = await updateCreditCard(editingCardId, {
				account_id: editCardDraft.accountId,
				card_label: editCardDraft.label.trim(),
				issuer_name: editCardDraft.issuer.trim() || null,
				network: editCardDraft.network,
				first4: editCardDraft.first4 || null,
				last4: editCardDraft.last4,
				is_active: editCardDraft.isActive,
				// Only sent when actually retyped — the untouched sentinel (or a
				// cleared field) never overwrites the stored password.
				...(editCardDraft.pdfPassword &&
				editCardDraft.pdfPassword !== editCardDraft.pdfPasswordSentinel
					? { pdf_password: editCardDraft.pdfPassword }
					: {}),
				...(editCardDraft.billingCycleDay !== '' ? { billing_cycle_day: cycleDay } : {}),
			});
			setRelinkNote(
				relinkedTransactionCount > 0
					? `Linked ${relinkedTransactionCount} past transaction${relinkedTransactionCount !== 1 ? 's' : ''} to this card.`
					: null,
			);
			setEditingCardId(null);
			setEditCardDraft(null);
		} catch {
			// hook error state renders; keep the form open
		}
	};

	const handleAddCard = async () => {
		if (!newCardLabel.trim() || (newCardFirst4 !== '' && newCardFirst4.length !== 4) || newCardLast4.length !== 4) return;
		// Register buttons prefill the existing folio; a fresh card omits it
		// and the backend adopts-or-creates the backing folio itself.
		const { relinkedTransactionCount } = await addCreditCard({
			account_id: newCardAccountId || undefined,
			card_label: newCardLabel.trim(),
			issuer_name: newCardIssuer.trim() || null,
			network: newCardNetwork,
			first4: newCardFirst4 || null,
			last4: newCardLast4.trim(),
		});
		setRelinkNote(
			relinkedTransactionCount > 0
				? `Linked ${relinkedTransactionCount} past transaction${relinkedTransactionCount !== 1 ? 's' : ''} to this card.`
				: null,
		);
		setNewCardLabel('');
		setNewCardIssuer('');
		setNewCardFirst4('');
		setNewCardLast4('');
		setNewCardAccountId('');
		setAddingCard(false);
	};

	return (
		<div
			className="flex-1 flex flex-col min-h-0 relative"
			style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}
		>
			<header
				className="h-[56px] shrink-0 flex items-center justify-between px-5 z-10"
				style={{ borderBottom: '1px solid var(--ledger-rule)' }}
			>
				<button
					onClick={onBack}
					className="w-8 h-8 rounded-full flex items-center justify-center transition-colors"
					style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
				>
					<ChevronLeft size={20} />
				</button>
				<h1
					className="font-bold text-lg tracking-tight"
					style={{ color: 'var(--ledger-fg)' }}
				>
					Accounts & Cards
				</h1>
				<div className="w-8 h-8" />
			</header>

			<main className="flex-1 overflow-y-auto px-5 pt-5 pb-24 min-h-0">
				<div className="flex gap-2 mb-6">
					{(['accounts', 'cards'] as const).map((f) => (
						<button
							key={f}
							onClick={() => setFilter(f)}
							className="rounded-full px-4 py-1.5 text-xs font-medium transition-colors"
							style={
								filter === f
									? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
									: { background: 'var(--ledger-card)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
							}
						>
							{f === 'accounts' ? 'Accounts' : 'Cards'}
						</button>
					))}
				</div>

				{error && (
					<p className="text-xs mb-4" style={{ color: 'var(--ledger-negative)' }}>{error}</p>
				)}

				{filter === 'accounts' && (
					<>
						<section
							className="relative overflow-hidden rounded-[16px] mb-6"
							style={{
								...cardStyle,
								borderLeft: '4px solid var(--ledger-accent)',
								boxShadow: '0 12px 30px rgba(43, 38, 31, 0.05)',
							}}
						>
							<div className="flex items-center justify-between px-4 pt-4 pb-3">
								<div>
									<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
										Account register
									</p>
									<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>
										Recorded income less spending
									</p>
								</div>
								<span
									className="rounded-full px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.12em] shrink-0"
									style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}
								>
									{visibleAccounts.length} folio{visibleAccounts.length !== 1 ? 's' : ''}
								</span>
							</div>
							<div className="px-4 py-4" style={{ borderTop: '1px solid var(--ledger-rule)' }}>
								<p className="text-[9px] font-bold uppercase tracking-[0.18em] mb-2" style={{ color: 'var(--ledger-muted)' }}>
									Net activity
								</p>
								{isLoading && accounts.length === 0 ? (
									<div className="flex items-center gap-2 py-1">
										<Loader2 size={16} className="animate-spin" style={{ color: 'var(--ledger-muted)' }} />
										<span className="text-sm" style={{ color: 'var(--ledger-muted)' }}>Loading…</span>
									</div>
								) : (
									<p
										className="text-[34px] leading-none font-bold tnum tracking-tight"
										style={{ color: netActivity < 0 ? 'var(--ledger-negative)' : 'var(--ledger-fg)' }}
									>
										{formatRupees(Math.round(netActivity * 100))}
									</p>
								)}
							</div>
						</section>

						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3"
							style={{ color: 'var(--ledger-muted)' }}
						>
							Your folios
						</p>

						{visibleAccounts.length > 0 ? (
							<div className="space-y-3 mb-4">
								{visibleAccounts.map((account) => {
									const isEditing = editingAccountId === account.id;
									const balance = deltaByAccount.get(account.id) ?? 0;
									const isNegative = balance < 0;

									return (
										<div
											key={account.id}
											className="overflow-hidden rounded-[14px]"
											style={{
												...cardStyle,
												borderLeft: `4px solid ${TYPE_COLOR[account.type]}`,
											}}
										>
											<div className="px-4 pt-3.5 pb-4">
												<div className="flex items-center justify-between gap-3">
													<p
														className="text-[9px] font-bold uppercase tracking-[0.18em]"
														style={{ color: TYPE_COLOR[account.type] }}
													>
														{TYPE_CODE[account.type]} folio
													</p>
													{account.instrument_last4 && (
														<span
															className="rounded-full px-2 py-0.5 text-[10px] font-bold tnum tracking-[0.12em]"
															style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-muted)' }}
														>
															···· {account.instrument_last4}
														</span>
													)}
											</div>
											<p className="text-lg font-bold leading-tight truncate mt-2" style={{ color: 'var(--ledger-fg)' }}>
												{account.name}
											</p>
												<div className="flex items-end justify-between gap-4 mt-5 pt-3" style={{ borderTop: '1px solid var(--ledger-rule)' }}>
													<p className="text-[9px] font-bold uppercase tracking-[0.18em]" style={{ color: 'var(--ledger-muted)' }}>
														Net activity
													</p>
													<p
														className="text-xl leading-none font-bold tnum shrink-0"
														style={{ color: isNegative ? 'var(--ledger-negative)' : 'var(--ledger-fg)' }}
													>
														{formatRupees(Math.round(balance * 100))}
													</p>
												</div>
											</div>

											<button
												onClick={() => {
													setEditingAccountId(isEditing ? null : account.id);
													setEditAccountDraft(null);
													setConfirmDeleteAccountId(null);
												}}
												className="w-full flex items-center justify-between px-4 py-2.5 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset"
												style={{ borderTop: '1px solid var(--ledger-rule)', color: 'var(--ledger-muted)' }}
												aria-expanded={isEditing}
											>
												<span className="text-[10px] font-bold uppercase tracking-[0.14em]">
													{isEditing ? 'Close' : 'Manage account'}
												</span>
												{isEditing ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
											</button>

											{isEditing && !editAccountDraft && confirmDeleteAccountId !== account.id && (
												<div
													className="px-4 pb-3.5 flex items-center justify-between"
													style={{ borderTop: '1px solid var(--ledger-rule)' }}
												>
													<button
														onClick={() =>
															setEditAccountDraft({
																name: account.name,
																type: account.type,
																last4: account.instrument_last4 ?? '',
															})
														}
														className="flex items-center gap-1.5 text-xs font-medium"
														style={{ color: 'var(--ledger-muted)' }}
													>
														<Pencil size={12} /> Edit details
													</button>
													<button
														onClick={() => setConfirmDeleteAccountId(account.id)}
														className="flex items-center gap-1.5 text-xs font-semibold px-3 py-1.5 rounded-full transition-colors"
														style={{
															color: 'var(--ledger-negative)',
															background: 'rgba(196, 83, 54, 0.08)',
														}}
													>
														<Trash2 size={12} /> Delete
													</button>
												</div>
											)}

											{isEditing && editAccountDraft && (
												<div
													className="px-4 pb-4 pt-3 flex flex-col gap-2"
													style={{ borderTop: '1px solid var(--ledger-rule)' }}
												>
													<div className="flex gap-2 overflow-x-auto pb-1 no-scrollbar">
														{(['cash', 'bank', 'card', 'other'] as const).map((t) => (
															<button
																key={t}
																onClick={() => setEditAccountDraft({ ...editAccountDraft, type: t })}
																className="shrink-0 rounded-full px-3 py-1.5 text-xs font-medium uppercase tracking-wider transition-colors"
																style={
																	editAccountDraft.type === t
																		? { background: TYPE_COLOR[t], color: 'var(--ledger-bg)' }
																		: { background: 'var(--ledger-bg)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
																}
															>
																{t}
															</button>
														))}
													</div>
													<input
														type="text"
														placeholder="Account name"
														value={editAccountDraft.name}
														onChange={(e) => setEditAccountDraft({ ...editAccountDraft, name: e.target.value })}
														className="w-full rounded-[8px] px-3 py-2.5 text-sm focus:outline-none transition-all"
														style={inputStyle}
													/>
													<input
														type="text"
														inputMode="numeric"
														placeholder="A/C or card number (only last 4 kept)"
														value={editAccountDraft.last4}
														onChange={(e) =>
															setEditAccountDraft({ ...editAccountDraft, last4: keepLast4Digits(e.target.value) })
														}
														className="w-full rounded-[8px] px-3 py-2.5 text-sm tnum focus:outline-none transition-all"
														style={inputStyle}
													/>
													<div className="flex items-center justify-end gap-3 mt-1">
														<button
															onClick={() => setEditAccountDraft(null)}
															className="flex items-center gap-1 text-xs font-medium"
															style={{ color: 'var(--ledger-muted)' }}
														>
															<X size={13} /> Cancel
														</button>
														<button
															onClick={() => { void handleSaveAccountEdit(); }}
															disabled={!editAccountDraft.name.trim() || accountsLoading}
															className="flex items-center gap-1 text-xs font-bold px-3 py-1.5 rounded-full"
															style={{
																background: 'var(--ledger-accent)',
																color: 'var(--ledger-bg)',
																opacity: (!editAccountDraft.name.trim() || accountsLoading) ? 0.5 : 1,
															}}
														>
															<Check size={13} /> Save
														</button>
													</div>
												</div>
											)}

											{isEditing && confirmDeleteAccountId === account.id && (
												<div
													className="mx-4 mb-3.5 rounded-[10px] px-3 py-2.5 flex items-center justify-between gap-3"
													style={{ background: 'var(--ledger-amber-bg)', border: '1px solid rgba(184, 115, 42, 0.2)' }}
												>
													<p className="text-xs leading-snug" style={{ color: 'var(--ledger-amber)' }}>
														Delete "{account.name}"? Credit cards registered to it are deleted too. Accounts with transactions can't be deleted.
													</p>
													<span className="flex items-center gap-2 shrink-0">
														<button
															onClick={() => {
																void deleteAccount(account.id);
																setConfirmDeleteAccountId(null);
																setEditingAccountId(null);
															}}
															className="text-xs font-bold px-2.5 py-1 rounded-full"
															style={{ color: '#fff', background: 'var(--ledger-negative)' }}
														>
															Delete
														</button>
														<button
															onClick={() => setConfirmDeleteAccountId(null)}
															className="text-xs font-medium"
															style={{ color: 'var(--ledger-muted)' }}
														>
															Cancel
														</button>
													</span>
												</div>
											)}
										</div>
									);
								})}
							</div>
						) : (
							!isLoading && (
								<p className="text-sm mb-4" style={{ color: 'var(--ledger-muted)' }}>
									No accounts yet
								</p>
							)
						)}

						{!addingAccount ? (
							<button
								onClick={() => setAddingAccount(true)}
								className="w-full rounded-[10px] py-4 flex items-center justify-center gap-2 text-xs font-semibold transition-colors"
								style={{
									border: '1px dashed var(--ledger-rule)',
									color: 'var(--ledger-muted)',
								}}
							>
								<Plus size={14} /> Add account
							</button>
						) : (
							<div className="rounded-[10px] p-5" style={cardStyle}>
								<div className="flex items-center justify-between mb-4">
									<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
										New folio
									</p>
									<button
										onClick={() => { setAddingAccount(false); setNewAccountName(''); setNewAccountLast4(''); }}
										className="text-xs font-medium"
										style={{ color: 'var(--ledger-muted)' }}
									>
										Cancel
									</button>
								</div>

								<input
									type="text"
									placeholder="Bank account name"
									value={newAccountName}
									onChange={(e) => setNewAccountName(e.target.value)}
									className="w-full rounded-[8px] px-3 py-2.5 text-sm mb-2 focus:outline-none transition-all"
									style={inputStyle}
								/>
								<input
									type="text"
									inputMode="numeric"
									placeholder="A/C number (only last 4 kept, optional)"
									value={newAccountLast4}
									onChange={(e) => setNewAccountLast4(keepLast4Digits(e.target.value))}
									className="w-full rounded-[8px] px-3 py-2.5 text-sm mb-4 tnum focus:outline-none transition-all"
									style={inputStyle}
								/>

								<button
									onClick={() => { void handleAddAccount(); }}
									disabled={!newAccountName.trim() || !newAccountLast4Valid || accountsLoading}
									className="w-full h-11 rounded-[10px] text-sm font-bold flex items-center justify-center gap-1.5 active:scale-[0.98] transition-all"
									style={{
										background: 'var(--ledger-accent)',
										color: 'var(--ledger-bg)',
										boxShadow: '0 8px 24px -8px var(--ledger-accent)',
										opacity: (!newAccountName.trim() || !newAccountLast4Valid || accountsLoading) ? 0.5 : 1,
									}}
								>
									{accountsLoading ? <Loader2 size={16} className="animate-spin" /> : <Plus size={16} />}
									File folio
								</button>
							</div>
						)}
					</>
				)}

				{filter === 'cards' && (
					<>
						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em] mb-4"
							style={{ color: 'var(--ledger-muted)' }}
						>
							Plastic
						</p>

						{relinkNote && (
							<p className="text-xs mb-3" style={{ color: 'var(--ledger-positive)' }}>
								{relinkNote}
							</p>
						)}

						{isLoading && creditCards.length === 0 ? (
							<div className="flex items-center gap-2 py-4">
								<Loader2 size={16} className="animate-spin" style={{ color: 'var(--ledger-muted)' }} />
								<span className="text-sm" style={{ color: 'var(--ledger-muted)' }}>Loading…</span>
							</div>
						) : creditCards.length > 0 ? (
							<div className="space-y-5 mb-4">
								{creditCards.map((card) => {
									const isEditing = editingCardId === card.id;
									const maskedNumber = card.first4 ? `${card.first4} ······ ${card.last4}` : `···· ······ ${card.last4}`;
									const networkLabel = (card.network ?? 'other').toUpperCase();
									const issuer = card.issuer_name ?? 'Issuer not set';
									// Spend rides the card's backing account: expenses minus
									// refunds/cashback, same transaction sums as the folio list.
									const spentPaise = Math.round(-(deltaByAccount.get(card.account_id) ?? 0) * 100);
									const statement = latestByCard.get(card.id);
									const trackedRupees = statement
										? trackedForPeriod(card.account_id, statement.period_start, statement.period_end)
										: null;
									const billsMatch =
										trackedRupees !== null && statement?.total_due_in_paise != null
											? Math.abs(rupeesToPaise(trackedRupees) - statement.total_due_in_paise) <= 10000
											: null;
									const hasBill = Boolean(
										statement && (statement.total_due_in_paise != null || statement.due_date),
									);
									return (
										<div key={card.id}>
											<div
												className="relative w-full overflow-hidden rounded-[18px] p-5 flex flex-col justify-between"
												style={{
													aspectRatio: '1.586 / 1',
													background: card.is_active
														? 'var(--ledger-accent)'
														: 'linear-gradient(145deg, var(--ledger-rule), var(--ledger-card))',
													border: card.is_active ? 'none' : '1px dashed var(--ledger-muted)',
													color: card.is_active ? 'var(--ledger-bg)' : 'var(--ledger-fg)',
													boxShadow: card.is_active
														? '0 18px 38px rgba(13, 92, 75, 0.18)'
														: 'none',
												}}
											>
												<div className="flex items-start justify-between gap-4">
													<p className="text-[11px] font-bold uppercase tracking-[0.17em] truncate" style={{ opacity: 0.78 }}>
														{issuer}
													</p>
													<p className="text-[11px] font-extrabold uppercase tracking-[0.13em] shrink-0">
														{networkLabel}
													</p>
												</div>

												<div>
													<div
														className="relative w-9 h-7 rounded-[6px] mb-4 overflow-hidden"
														style={{
															background: card.is_active ? '#D6BE7A' : 'var(--ledger-muted)',
															opacity: card.is_active ? 0.9 : 0.55,
														}}
														aria-hidden="true"
													>
														<span className="absolute inset-x-0 top-1/2" style={{ borderTop: '1px solid rgba(43,38,31,0.35)' }} />
														<span className="absolute inset-y-0 left-1/2" style={{ borderLeft: '1px solid rgba(43,38,31,0.35)' }} />
													</div>
													<p className="text-[16px] font-bold tnum tracking-[0.13em]" style={{ opacity: 0.94 }}>
														{maskedNumber}
													</p>
												</div>

												<div className="flex items-end justify-between gap-4">
													<p className="text-base font-bold leading-tight truncate">{card.card_label}</p>
													<span className="flex items-center gap-1.5 text-[9px] font-bold uppercase tracking-[0.12em] shrink-0">
														<span
															className="w-1.5 h-1.5 rounded-full"
															style={{ background: card.is_active ? 'var(--ledger-bg)' : 'var(--ledger-muted)' }}
														/>
														{card.is_active ? 'Active' : 'Inactive'}
													</span>
												</div>
											</div>

											<div className="mt-2 overflow-hidden rounded-[14px]" style={cardStyle}>
												<div className="px-4 pt-4 pb-3.5">
													<div className="grid grid-cols-[minmax(0,1fr)_auto] gap-5 items-start">
														<div className="min-w-0">
															<p className="text-[9px] font-bold uppercase tracking-[0.18em]" style={{ color: 'var(--ledger-muted)' }}>
																Current bill
															</p>
															{hasBill && statement ? (
																<>
																	<p className="text-[26px] leading-none font-bold tnum tracking-tight mt-2" style={{ color: 'var(--ledger-fg)' }}>
																		{statement.total_due_in_paise != null
																			? formatRupees(statement.total_due_in_paise)
																			: '—'}
																	</p>
																	<p className="text-[11px] font-medium mt-2" style={{ color: 'var(--ledger-muted)' }}>
																		{statement.due_date ? `Due ${formatDueDate(statement.due_date)}` : 'Due date unavailable'}
																	</p>
																</>
															) : (
																<>
																	<p className="text-sm font-semibold mt-2" style={{ color: 'var(--ledger-fg)' }}>No bill yet</p>
																	<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>Add one from Manage card</p>
																</>
															)}
														</div>

														<div className="text-right shrink-0">
															<p className="text-[9px] font-bold uppercase tracking-[0.18em]" style={{ color: 'var(--ledger-muted)' }}>
																All-time spend
															</p>
															<p className="text-sm font-bold tnum mt-2" style={{ color: 'var(--ledger-fg)' }}>
																{formatRupees(spentPaise)}
															</p>
														</div>
													</div>

													{trackedRupees !== null && (
														<div
															className="flex items-center justify-between gap-3 mt-4 pt-3"
															style={{ borderTop: '1px solid var(--ledger-rule)' }}
														>
															<span className="flex items-center gap-1.5 text-[10px] font-medium" style={{ color: 'var(--ledger-muted)' }}>
																<span
																	className="w-1.5 h-1.5 rounded-full shrink-0"
																	style={{ background: billsMatch === false ? 'var(--ledger-amber)' : 'var(--ledger-positive)' }}
																/>
																Statement-period activity
															</span>
															<span className="text-[11px] font-bold tnum" style={{ color: 'var(--ledger-fg)' }}>
																{formatRupees(Math.round(trackedRupees * 100))}
															</span>
														</div>
													)}
												</div>

												<button
													onClick={() => {
														setEditingCardId(isEditing ? null : card.id);
														setEditCardDraft(null);
														setBillEntryCardId(null);
													}}
													className="w-full flex items-center justify-between px-4 py-2.5 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset"
													style={{ borderTop: '1px solid var(--ledger-rule)', color: 'var(--ledger-muted)' }}
													aria-expanded={isEditing}
													aria-label={`${isEditing ? 'Close' : 'Manage'} ${card.card_label}`}
												>
													<span className="text-[10px] font-bold uppercase tracking-[0.14em]">
														{isEditing ? 'Close' : 'Manage card'}
													</span>
													{isEditing ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
												</button>
											</div>

											{isEditing && !editCardDraft && billEntryCardId === card.id && (
												<div className="rounded-[14px] p-4 mt-2 flex flex-col gap-3" style={cardStyle}>
													<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
														Enter bill
													</p>
													<div className="flex gap-2">
														<input
															type="text"
															inputMode="decimal"
															placeholder="Total due (₹)"
															value={billDraft.total}
															onChange={(e) => setBillDraft({ ...billDraft, total: e.target.value })}
															className="w-1/2 rounded-[8px] px-3 py-2.5 text-sm tnum focus:outline-none"
															style={inputStyle}
														/>
														<input
															type="date"
															value={billDraft.dueDate}
															onChange={(e) => setBillDraft({ ...billDraft, dueDate: e.target.value })}
															className="w-1/2 rounded-[8px] px-3 py-2.5 text-sm tnum focus:outline-none"
															style={inputStyle}
														/>
													</div>
													<div className="flex items-center justify-end gap-3">
														<button
															onClick={() => { setBillEntryCardId(null); setBillDraft({ total: '', dueDate: '' }); }}
															className="flex items-center gap-1 text-xs font-medium"
															style={{ color: 'var(--ledger-muted)' }}
														>
															<X size={13} /> Cancel
														</button>
														<button
															onClick={() => { void handleSaveBill(card.id); }}
															disabled={
																!Number.isFinite(Number.parseFloat(billDraft.total)) ||
																Number.parseFloat(billDraft.total) <= 0 ||
																statementsLoading
															}
															className="flex items-center gap-1 text-xs font-bold px-3 py-1.5 rounded-full"
															style={{
																background: 'var(--ledger-accent)',
																color: 'var(--ledger-bg)',
																opacity:
																	(!Number.isFinite(Number.parseFloat(billDraft.total)) ||
																		Number.parseFloat(billDraft.total) <= 0 ||
																		statementsLoading) ? 0.5 : 1,
															}}
														>
															<Check size={13} /> Save bill
														</button>
													</div>
												</div>
											)}

											{isEditing && !editCardDraft && billEntryCardId !== card.id && (
												<div className="flex items-center justify-end gap-3 px-1 pt-2">
													<button
														onClick={() => {
															setBillEntryCardId(card.id);
															setBillDraft({
																total: statement?.total_due_in_paise != null
																	? paiseToRupees(statement.total_due_in_paise).toFixed(2)
																	: '',
																dueDate: statement?.due_date ?? '',
															});
														}}
														className="flex items-center gap-1 text-xs font-medium"
														style={{ color: 'var(--ledger-muted)' }}
													>
														<Plus size={12} /> Bill
													</button>
													<button
														onClick={() =>
															setEditCardDraft({
																label: card.card_label,
																issuer: card.issuer_name ?? '',
																network: card.network ?? 'other',
																first4: card.first4 ?? '',
																last4: card.last4,
																accountId: card.account_id,
																isActive: card.is_active,
																pdfPassword: pdfPasswordSentinelFor(card),
																pdfPasswordSentinel: pdfPasswordSentinelFor(card),
																billingCycleDay: card.billing_cycle_day?.toString() ?? '',
															})
														}
														className="flex items-center gap-1 text-xs font-medium"
														style={{ color: 'var(--ledger-muted)' }}
													>
														<Pencil size={12} /> Edit
													</button>
													<button
														onClick={() => { void deleteCreditCard(card.id); setEditingCardId(null); }}
														className="flex items-center gap-1 text-xs font-semibold"
														style={{ color: 'var(--ledger-negative)' }}
													>
														<Trash2 size={12} /> Delete
													</button>
												</div>
											)}

											{isEditing && editCardDraft && (
												<div className="rounded-[14px] p-4 mt-2 flex flex-col gap-3" style={cardStyle}>
													<input
														type="text"
														placeholder="Card label"
														value={editCardDraft.label}
														onChange={(e) => setEditCardDraft({ ...editCardDraft, label: e.target.value })}
														className="w-full rounded-[8px] px-3 py-2.5 text-sm focus:outline-none transition-all"
														style={inputStyle}
													/>
													<input
														type="text"
														placeholder="Issuer name (optional)"
														value={editCardDraft.issuer}
														onChange={(e) => setEditCardDraft({ ...editCardDraft, issuer: e.target.value })}
														className="w-full rounded-[8px] px-3 py-2.5 text-sm focus:outline-none transition-all"
														style={inputStyle}
													/>
													<div className="flex gap-2 overflow-x-auto pb-1 no-scrollbar">
														{NETWORK_OPTIONS.map((n) => (
															<button
																key={n.value}
																onClick={() => setEditCardDraft({ ...editCardDraft, network: n.value })}
																className="shrink-0 rounded-full px-3 py-1.5 text-xs font-medium transition-colors"
																style={
																	editCardDraft.network === n.value
																		? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
																		: { background: 'var(--ledger-bg)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
																}
															>
																{n.label}
															</button>
														))}
													</div>
													<div className="flex gap-2">
														<input
															type="text"
															inputMode="numeric"
															placeholder="First 4 (optional)"
															value={editCardDraft.first4}
															onChange={(e) => setEditCardDraft({ ...editCardDraft, first4: digitsOnly(e.target.value) })}
															className="w-1/2 rounded-[8px] px-3 py-2.5 text-sm tnum focus:outline-none transition-all"
															style={inputStyle}
														/>
														<input
															type="text"
															inputMode="numeric"
															placeholder="Last 4"
															value={editCardDraft.last4}
															onChange={(e) => setEditCardDraft({ ...editCardDraft, last4: digitsOnly(e.target.value) })}
															className="w-1/2 rounded-[8px] px-3 py-2.5 text-sm tnum focus:outline-none transition-all"
															style={inputStyle}
														/>
													</div>
													<div className="flex gap-2">
														<input
															type="password"
															placeholder="Statement PDF password"
															value={editCardDraft.pdfPassword}
															onChange={(e) => setEditCardDraft({ ...editCardDraft, pdfPassword: e.target.value })}
															onFocus={() => {
																if (editCardDraft.pdfPassword === editCardDraft.pdfPasswordSentinel) {
																	setEditCardDraft({ ...editCardDraft, pdfPassword: '' });
																}
															}}
															onBlur={() => {
																if (editCardDraft.pdfPassword === '') {
																	setEditCardDraft({ ...editCardDraft, pdfPassword: editCardDraft.pdfPasswordSentinel });
																}
															}}
															autoComplete="off"
															className="w-2/3 rounded-[8px] px-3 py-2.5 text-sm focus:outline-none transition-all"
															style={inputStyle}
														/>
														<input
															type="text"
															inputMode="numeric"
															placeholder="Cycle day"
															value={editCardDraft.billingCycleDay}
															onChange={(e) =>
																setEditCardDraft({
																	...editCardDraft,
																	billingCycleDay: e.target.value.replace(/\D/g, '').slice(0, 2),
																})
															}
															className="w-1/3 rounded-[8px] px-3 py-2.5 text-sm tnum focus:outline-none transition-all"
															style={inputStyle}
														/>
													</div>
													<p className="text-[10px] px-1" style={{ color: 'var(--ledger-muted)' }}>
														The PDF password unlocks statement PDFs locally when the email hides bill
														details (HDFC). Cycle day is auto-learned from statements when possible.
													</p>
													<div className="flex items-center justify-between px-1">
														<span className="text-xs font-medium" style={{ color: 'var(--ledger-muted)' }}>
															Card active
														</span>
														<button
															onClick={() => setEditCardDraft({ ...editCardDraft, isActive: !editCardDraft.isActive })}
															className="relative shrink-0 w-11 h-6 rounded-full transition-colors duration-200 ease-in-out"
															style={{ background: editCardDraft.isActive ? 'var(--ledger-accent)' : 'var(--ledger-rule)' }}
															aria-label="Toggle card active"
														>
															<div
																className={`absolute left-1 top-1 w-4 h-4 rounded-full transition-transform duration-200 ease-in-out ${editCardDraft.isActive ? 'translate-x-5' : 'translate-x-0'}`}
																style={{ background: 'var(--ledger-bg)' }}
															/>
														</button>
													</div>
													<div className="flex items-center justify-end gap-3 mt-1">
														<button
															onClick={() => setEditCardDraft(null)}
															className="flex items-center gap-1 text-xs font-medium"
															style={{ color: 'var(--ledger-muted)' }}
														>
															<X size={13} /> Cancel
														</button>
														<button
															onClick={() => { void handleSaveCardEdit(); }}
															disabled={
																!editCardDraft.label.trim() ||
																(editCardDraft.first4 !== '' && editCardDraft.first4.length !== 4) ||
																editCardDraft.last4.length !== 4 ||
																!editCardDraft.accountId ||
																cardsLoading
															}
															className="flex items-center gap-1 text-xs font-bold px-3 py-1.5 rounded-full"
															style={{
																background: 'var(--ledger-accent)',
																color: 'var(--ledger-bg)',
																opacity:
																	(!editCardDraft.label.trim() ||
																		(editCardDraft.first4 !== '' && editCardDraft.first4.length !== 4) ||
																		editCardDraft.last4.length !== 4 ||
																		!editCardDraft.accountId ||
																		cardsLoading) ? 0.5 : 1,
															}}
														>
															{cardsLoading ? <Loader2 size={13} className="animate-spin" /> : <Check size={13} />}
															{cardsLoading ? 'Saving…' : 'Save'}
														</button>
													</div>
												</div>
											)}
										</div>
									);
								})}
							</div>
						) : (
							!isLoading && (
								<p className="text-sm mb-4" style={{ color: 'var(--ledger-muted)' }}>
									No cards registered
								</p>
							)
						)}

						{unregisteredCardAccounts.length > 0 && (
							<div className="mb-4">
								<p
									className="text-[10px] font-bold uppercase tracking-[0.2em] mb-2"
									style={{ color: 'var(--ledger-muted)' }}
								>
									Card accounts without a registered card
								</p>
								<p className="text-[11px] mb-3" style={{ color: 'var(--ledger-muted)' }}>
									Register these so bank alerts link to the right card automatically.
								</p>
								<div className="rounded-[10px] overflow-hidden" style={cardStyle}>
									{unregisteredCardAccounts.map((account, i) => (
										<div
											key={account.id}
											className="px-4 py-3 flex items-center justify-between gap-3"
											style={
												i < unregisteredCardAccounts.length - 1
													? { borderBottom: '1px solid var(--ledger-rule)' }
													: undefined
											}
										>
											<div className="min-w-0">
												<p className="text-sm font-semibold truncate" style={{ color: 'var(--ledger-fg)' }}>
													{account.name}
												</p>
												<p className="text-[10px] tnum mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
													{[
														account.instrument_last4 ? `····${account.instrument_last4}` : null,
														`${formatRupees(Math.round(-(deltaByAccount.get(account.id) ?? 0) * 100))} spent`,
													].filter(Boolean).join(' · ')}
												</p>
											</div>
											<button
												onClick={() => {
													setNewCardLabel(account.name);
													setNewCardLast4(account.instrument_last4 ?? '');
													setNewCardFirst4('');
													setNewCardIssuer('');
													setNewCardAccountId(account.id);
													setAddingCard(true);
												}}
												className="shrink-0 text-xs font-bold px-3 py-1.5 rounded-full active:scale-95 transition-all"
												style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }}
											>
												Register
											</button>
										</div>
									))}
								</div>
							</div>
						)}

						{!addingCard ? (
							<button
								onClick={() => setAddingCard(true)}
								className="w-full rounded-[14px] py-8 flex items-center justify-center gap-2 text-xs font-semibold transition-colors"
								style={{
									border: '1px dashed var(--ledger-rule)',
									color: 'var(--ledger-muted)',
									minHeight: '60px',
								}}
							>
								<Plus size={14} /> Add card
							</button>
						) : (
							<div className="rounded-[14px] p-5" style={cardStyle}>
								<div className="flex items-center justify-between mb-4">
									<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
										New card
									</p>
									<button
										onClick={() => {
											setAddingCard(false);
											setNewCardLabel(''); setNewCardIssuer(''); setNewCardFirst4(''); setNewCardLast4(''); setNewCardAccountId('');
										}}
										className="text-xs font-medium"
										style={{ color: 'var(--ledger-muted)' }}
									>
										Cancel
									</button>
								</div>

								<div className="flex flex-col gap-3 mb-4">
									<input
										type="text"
										placeholder="Card label (e.g. Amazon ICICI)"
										value={newCardLabel}
										onChange={(e) => setNewCardLabel(e.target.value)}
										className="w-full rounded-[8px] px-3 py-2.5 text-sm focus:outline-none transition-all"
										style={inputStyle}
									/>
									<input
										type="text"
										placeholder="Issuer name (optional)"
										value={newCardIssuer}
										onChange={(e) => setNewCardIssuer(e.target.value)}
										className="w-full rounded-[8px] px-3 py-2.5 text-sm focus:outline-none transition-all"
										style={inputStyle}
									/>

									<div className="flex gap-2 overflow-x-auto pb-1 no-scrollbar">
										{NETWORK_OPTIONS.map((n) => (
											<button
												key={n.value}
												onClick={() => setNewCardNetwork(n.value)}
												className="shrink-0 rounded-full px-3 py-1.5 text-xs font-medium transition-colors"
												style={
													newCardNetwork === n.value
														? { background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }
														: { background: 'var(--ledger-bg)', color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }
												}
											>
												{n.label}
											</button>
										))}
									</div>

									<div className="flex gap-2">
										<input
											type="text"
											inputMode="numeric"
											placeholder="First 4 (optional)"
											value={newCardFirst4}
											onChange={(e) => setNewCardFirst4(digitsOnly(e.target.value))}
											className="w-1/2 rounded-[8px] px-3 py-2.5 text-sm tnum focus:outline-none transition-all"
											style={inputStyle}
										/>
										<input
											type="text"
											inputMode="numeric"
											placeholder="Last 4"
											value={newCardLast4}
											onChange={(e) => setNewCardLast4(digitsOnly(e.target.value))}
											className="w-1/2 rounded-[8px] px-3 py-2.5 text-sm tnum focus:outline-none transition-all"
											style={inputStyle}
										/>
									</div>

									<p className="text-[11px] px-1" style={{ color: 'var(--ledger-muted)' }}>
										Its folio is set up automatically — spend and history live there.
									</p>
								</div>

								<button
									onClick={() => { void handleAddCard(); }}
									disabled={!newCardLabel.trim() || (newCardFirst4 !== '' && newCardFirst4.length !== 4) || newCardLast4.length !== 4 || cardsLoading}
									className="w-full h-11 rounded-[10px] text-sm font-bold flex items-center justify-center gap-1.5 active:scale-[0.98] transition-all"
									style={{
										background: 'var(--ledger-accent)',
										color: 'var(--ledger-bg)',
										boxShadow: '0 8px 24px -8px var(--ledger-accent)',
										opacity: (!newCardLabel.trim() || (newCardFirst4 !== '' && newCardFirst4.length !== 4) || newCardLast4.length !== 4 || cardsLoading) ? 0.5 : 1,
									}}
								>
									{cardsLoading ? <Loader2 size={16} className="animate-spin" /> : <Plus size={16} />}
									Add card
								</button>
							</div>
						)}
					</>
				)}
			</main>
		</div>
	);
}
