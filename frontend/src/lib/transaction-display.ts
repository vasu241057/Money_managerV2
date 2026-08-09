import type { Transaction } from '../types/domain';

function nonEmpty(value: string | null | undefined): string | null {
	const normalized = value?.trim();
	return normalized ? normalized : null;
}

export function transactionIdentityName(transaction: Transaction): string | null {
	return nonEmpty(transaction.merchantName) ?? nonEmpty(transaction.counterpartyRaw);
}

export function transactionPrimaryLabel(transaction: Transaction): string {
	return nonEmpty(transaction.description)
		?? transactionIdentityName(transaction)
		?? nonEmpty(transaction.subCategory)
		?? nonEmpty(transaction.category)
		?? 'Transaction';
}

// Person transfers never carry a brand logo — a person is not a brand, and
// uploading one would put a private individual in a globally shared catalogue.
// Their artwork is drawn from the name itself instead: initials on a tint that
// is stable for the same person, so two payees stay visually distinct.
const PERSON_TINTS = [
	'var(--ledger-accent)',
	'var(--ledger-category-blue)',
	'var(--ledger-category-plum)',
	'var(--ledger-category-rose)',
	'var(--ledger-amber)',
];

/**
 * First and last initials of a person's name, or null when the name carries no
 * letters at all (a masked or numeric payee) and a glyph must stand in.
 */
export function personInitials(name: string | null | undefined): string | null {
	const words = (name ?? '')
		.split(/[^\p{L}\p{N}]+/u)
		.filter(word => /\p{L}/u.test(word));
	if (words.length === 0) return null;
	const first = [...words[0]][0];
	const last = words.length > 1 ? [...words[words.length - 1]][0] : '';
	return `${first}${last}`.toUpperCase();
}

export function personTint(name: string | null | undefined): string {
	const seed = name ?? '';
	let hash = 0;
	for (let index = 0; index < seed.length; index += 1) {
		hash = (hash * 31 + seed.charCodeAt(index)) >>> 0;
	}
	return PERSON_TINTS[hash % PERSON_TINTS.length];
}

/**
 * Money coming in, for artwork that shows direction. The bank's own debit/credit
 * marker wins where it exists; transfers have no income/expense type to read.
 */
export function isIncomingTransaction(transaction: Transaction): boolean {
	if (transaction.direction) return transaction.direction === 'credit';
	return transaction.type === 'income';
}

function paymentSourceLabel(transaction: Transaction): string | null {
	if (transaction.creditCardLabel) {
		return transaction.instrumentLast4
			? `${transaction.creditCardLabel} ·· ${transaction.instrumentLast4}`
			: transaction.creditCardLabel;
	}
	return transaction.accountName ?? null;
}

export function transactionMetaLabel(
	transaction: Transaction,
	viewMode: 'daily' | 'monthly',
): string {
	const primaryLabel = transactionPrimaryLabel(transaction);
	const merchantContext = nonEmpty(transaction.description)
		? transactionIdentityName(transaction)
		: null;
	const categoryLabel = transaction.subCategory ?? transaction.category;
	const paymentSource = paymentSourceLabel(transaction);
	const transactionDay = viewMode === 'monthly'
		? transaction.date.toLocaleDateString('en-IN', { day: 'numeric', month: 'short' })
		: null;

	return [
		merchantContext !== primaryLabel ? merchantContext : null,
		paymentSource,
		categoryLabel !== primaryLabel ? categoryLabel : null,
		transactionDay,
	].filter(Boolean).join(' · ');
}
