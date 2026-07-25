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
