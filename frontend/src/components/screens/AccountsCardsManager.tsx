import { useMemo, useState } from 'react';
import {
	ChevronLeft, Trash2, Plus, Pencil, Loader2, Check, X,
} from 'lucide-react';
import { useAccounts } from '../../hooks/useAccounts';
import { useCreditCards } from '../../hooks/useCreditCards';
import { useTransactions } from '../../hooks/useTransactions';
import { paiseToRupees } from '../../lib/money';
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
	const { transactions } = useTransactions();

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

	// Balances are derived purely from transactions: income adds, expense
	// subtracts, transfers are neutral (single account_id — no pair to move
	// between). Unattributed transactions (no account) appear nowhere here.
	const deltaByAccount = useMemo(() => {
		const map = new Map<string, number>();
		for (const t of transactions) {
			if (!t.accountId) continue;
			if (t.type === 'income') {
				map.set(t.accountId, (map.get(t.accountId) ?? 0) + t.amount);
			} else if (t.type === 'expense') {
				map.set(t.accountId, (map.get(t.accountId) ?? 0) - t.amount);
			}
		}
		return map;
	}, [transactions]);

	const netWorth = visibleAccounts.reduce((acc, a) => acc + (deltaByAccount.get(a.id) ?? 0), 0);
	const isLoading = accountsLoading || cardsLoading;
	const error = accountsError ?? cardsError;

	const newAccountLast4Valid = newAccountLast4 === '' || newAccountLast4.length === 4;

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
		try {
			const { relinkedTransactionCount } = await updateCreditCard(editingCardId, {
				account_id: editCardDraft.accountId,
				card_label: editCardDraft.label.trim(),
				issuer_name: editCardDraft.issuer.trim() || null,
				network: editCardDraft.network,
				first4: editCardDraft.first4 || null,
				last4: editCardDraft.last4,
				is_active: editCardDraft.isActive,
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
						<section className="mb-6">
							<p
								className="text-[10px] font-bold uppercase tracking-[0.2em] mb-2"
								style={{ color: 'var(--ledger-muted)' }}
							>
								Net across {visibleAccounts.length} account{visibleAccounts.length !== 1 ? 's' : ''}
							</p>
							{isLoading && accounts.length === 0 ? (
								<div className="flex items-center gap-2 py-2">
									<Loader2 size={16} className="animate-spin" style={{ color: 'var(--ledger-muted)' }} />
									<span className="text-sm" style={{ color: 'var(--ledger-muted)' }}>Loading…</span>
								</div>
							) : (
								<div className="flex items-baseline gap-1">
									<span className="text-xl font-medium" style={{ color: 'var(--ledger-muted)' }}>₹</span>
									<span
										className="text-4xl font-bold tnum tracking-tight"
										style={{ color: netWorth < 0 ? 'var(--ledger-negative)' : 'var(--ledger-fg)' }}
									>
										{Math.floor(Math.round(Math.abs(netWorth) * 100) / 100).toLocaleString('en-IN')}
									</span>
									<span className="text-xl font-medium tnum" style={{ color: 'var(--ledger-muted)' }}>
										.{(Math.round(Math.abs(netWorth) * 100) % 100).toString().padStart(2, '0')}
									</span>
								</div>
							)}
						</section>

						<p
							className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3"
							style={{ color: 'var(--ledger-muted)' }}
						>
							Folios
						</p>

						{visibleAccounts.length > 0 ? (
							<div className="rounded-[10px] overflow-hidden mb-4" style={cardStyle}>
								{visibleAccounts.map((account, index) => {
									const isEditing = editingAccountId === account.id;
									const balance = deltaByAccount.get(account.id) ?? 0;
									const isNegative = balance < 0;
									const isLast = index === visibleAccounts.length - 1;

									const meta = [
										TYPE_CODE[account.type],
										account.instrument_last4 && `····${account.instrument_last4}`,
									].filter(Boolean).join(' · ');

									return (
										<div
											key={account.id}
											style={{
												borderLeft: `3px solid ${TYPE_COLOR[account.type]}`,
												...(!isLast ? { borderBottom: '1px solid var(--ledger-rule)' } : {}),
											}}
										>
											<button
												onClick={() => {
													setEditingAccountId(isEditing ? null : account.id);
													setEditAccountDraft(null);
													setConfirmDeleteAccountId(null);
												}}
												className="w-full flex items-center justify-between px-4 py-3.5 text-left transition-colors"
											>
												<div className="min-w-0">
													<p className="text-sm font-semibold truncate" style={{ color: 'var(--ledger-fg)' }}>
														{account.name}
													</p>
													<p
														className="text-[10px] font-bold uppercase tracking-[0.15em] mt-1 tnum"
														style={{ color: 'var(--ledger-muted)' }}
													>
														{meta}
													</p>
												</div>
												<p
													className="text-base font-bold tnum shrink-0 ml-3"
													style={{ color: isNegative ? 'var(--ledger-negative)' : 'var(--ledger-fg)' }}
												>
													{formatRupees(Math.round(balance * 100))}
												</p>
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
							<div className="space-y-3 mb-4">
								{creditCards.map((card) => {
									const isEditing = editingCardId === card.id;
									const maskedNumber = card.first4 ? `${card.first4} ······ ${card.last4}` : `···· ······ ${card.last4}`;
									const networkLabel = (card.network ?? 'other').toUpperCase();
									const issuer = card.issuer_name ?? 'Unknown issuer';
									// Spend rides the card's backing account: expenses minus
									// refunds/cashback, same transaction sums as the folio list.
									const spentPaise = Math.round(-(deltaByAccount.get(card.account_id) ?? 0) * 100);

									return (
										<div key={card.id}>
											{card.is_active ? (
												<button
													onClick={() => { setEditingCardId(isEditing ? null : card.id); setEditCardDraft(null); }}
													className="w-full text-left rounded-[14px] p-5 transition-transform active:scale-[0.98]"
													style={{
														background: 'var(--ledger-accent)',
														color: 'var(--ledger-bg)',
														minHeight: '170px',
														display: 'flex',
														flexDirection: 'column',
														justifyContent: 'space-between',
													}}
												>
													<div className="flex items-start justify-between">
														<span className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ opacity: 0.7 }}>
															{issuer}
														</span>
														<span
															className="text-[10px] font-bold tracking-[0.15em] px-2 py-0.5 rounded uppercase"
															style={{ background: 'rgba(255,255,255,0.18)' }}
														>
															{networkLabel}
														</span>
													</div>
													<p className="text-lg font-bold tnum tracking-[0.15em]" style={{ letterSpacing: '0.1em' }}>
														{maskedNumber}
													</p>
													<div className="flex items-end justify-between">
														<div className="min-w-0">
															<p className="text-[9px] font-bold uppercase tracking-[0.2em]" style={{ opacity: 0.7 }}>
																Total spent
															</p>
															<p className="text-xl font-bold tnum">{formatRupees(spentPaise)}</p>
															<p className="text-sm font-semibold mt-1.5 truncate">{card.card_label}</p>
														</div>
														<span className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wider shrink-0">
															<span className="w-1.5 h-1.5 rounded-full" style={{ background: 'var(--ledger-bg)' }} />
															Active
														</span>
													</div>
												</button>
											) : (
												<button
													onClick={() => { setEditingCardId(isEditing ? null : card.id); setEditCardDraft(null); }}
													className="w-full text-left rounded-[14px] p-5 transition-colors"
													style={{
														background: 'var(--ledger-card)',
														border: '1px dashed var(--ledger-rule)',
														color: 'var(--ledger-muted)',
														minHeight: '170px',
														display: 'flex',
														flexDirection: 'column',
														justifyContent: 'space-between',
														opacity: 0.7,
													}}
												>
													<div className="flex items-start justify-between">
														<span className="text-[10px] font-bold uppercase tracking-[0.2em]">{issuer}</span>
														<span
															className="text-[10px] font-bold tracking-[0.15em] px-2 py-0.5 rounded uppercase"
															style={{ border: '1px solid var(--ledger-rule)' }}
														>
															{networkLabel}
														</span>
													</div>
													<p className="text-lg font-bold tnum tracking-[0.1em]">{maskedNumber}</p>
													<div className="flex items-end justify-between">
														<div className="min-w-0">
															<p className="text-[9px] font-bold uppercase tracking-[0.2em]" style={{ opacity: 0.7 }}>
																Total spent
															</p>
															<p className="text-xl font-bold tnum">{formatRupees(spentPaise)}</p>
															<p className="text-sm font-semibold mt-1.5 truncate">{card.card_label}</p>
														</div>
														<span className="text-[10px] font-bold uppercase tracking-wider shrink-0">Inactive</span>
													</div>
												</button>
											)}

											{isEditing && !editCardDraft && (
												<div className="flex items-center justify-end gap-3 px-1 pt-2">
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
															<Check size={13} /> Save
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
