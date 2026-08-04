import type {
	BudgetEffectKind,
	BudgetTreatment,
	BudgetTreatmentSource,
	EffectiveBudgetTreatment,
	EventDirection,
	TransactionType,
} from '../../../shared/types';

export interface BudgetCalculationTransaction {
	id: string;
	amountInPaise: number;
	type: TransactionType;
	direction: EventDirection;
	treatment: BudgetTreatment;
	categoryTreatment: EffectiveBudgetTreatment | null;
	recurringTreatment: BudgetTreatment | null;
}

export interface BudgetCalculationRelationship {
	sourceTransactionId: string;
	targetTransactionId: string;
}

export interface ResolvedBudgetTransaction {
	id: string;
	effectiveTreatment: EffectiveBudgetTreatment;
	treatmentSource: BudgetTreatmentSource;
	effect: BudgetEffectKind;
	effectInPaise: number;
}

export interface BudgetCalculationResult {
	countedSpendingInPaise: number;
	adjustmentsInPaise: number;
	usedInPaise: number;
	outsidePlanInPaise: number;
	transactions: Map<string, ResolvedBudgetTransaction>;
}

function systemDefault(transaction: BudgetCalculationTransaction): EffectiveBudgetTreatment {
	return transaction.type === 'expense' ? 'INCLUDE' : 'EXCLUDE';
}

function includedEffect(transaction: BudgetCalculationTransaction): BudgetEffectKind {
	if (transaction.type === 'expense') return 'SPEND';
	if (transaction.type === 'income') return 'ADJUSTMENT';
	return transaction.direction === 'debit' ? 'SPEND' : 'ADJUSTMENT';
}

/**
 * Resolves one month in a single place so the Budget screen, Ledger badges,
 * and transaction detail never invent slightly different rules.
 *
 * Precedence: transaction override > confirmed relationship > recurring rule
 * > category rule > system default.
 */
export function calculateBudget(
	transactions: BudgetCalculationTransaction[],
	relationships: BudgetCalculationRelationship[],
): BudgetCalculationResult {
	const transactionById = new Map(transactions.map((transaction) => [transaction.id, transaction]));
	const relationshipBySource = new Map(
		relationships.map((relationship) => [relationship.sourceTransactionId, relationship]),
	);
	const resolved = new Map<string, ResolvedBudgetTransaction>();

	const resolve = (
		transaction: BudgetCalculationTransaction,
		visiting: Set<string> = new Set(),
	): ResolvedBudgetTransaction => {
		const cached = resolved.get(transaction.id);
		if (cached) return cached;

		let effectiveTreatment: EffectiveBudgetTreatment;
		let treatmentSource: BudgetTreatmentSource;
		if (transaction.treatment !== 'AUTO') {
			effectiveTreatment = transaction.treatment;
			treatmentSource = 'TRANSACTION_OVERRIDE';
		} else {
			const relationship = relationshipBySource.get(transaction.id);
			const target = relationship
				? transactionById.get(relationship.targetTransactionId)
				: undefined;
			if (target && !visiting.has(target.id)) {
				const nextVisiting = new Set(visiting).add(transaction.id);
				effectiveTreatment = resolve(target, nextVisiting).effectiveTreatment;
				treatmentSource = 'REFUND_RELATIONSHIP';
			} else if (
				transaction.recurringTreatment
				&& transaction.recurringTreatment !== 'AUTO'
			) {
				effectiveTreatment = transaction.recurringTreatment;
				treatmentSource = 'RECURRING_RULE';
			} else if (transaction.categoryTreatment) {
				effectiveTreatment = transaction.categoryTreatment;
				treatmentSource = 'CATEGORY_RULE';
			} else {
				effectiveTreatment = systemDefault(transaction);
				treatmentSource = 'SYSTEM_DEFAULT';
			}
		}

		const effect = effectiveTreatment === 'INCLUDE'
			? includedEffect(transaction)
			: 'NONE';
		const result: ResolvedBudgetTransaction = {
			id: transaction.id,
			effectiveTreatment,
			treatmentSource,
			effect,
			effectInPaise: effect === 'NONE' ? 0 : transaction.amountInPaise,
		};
		resolved.set(transaction.id, result);
		return result;
	};

	for (const transaction of transactions) resolve(transaction);

	// A purchase can receive multiple partial refunds, but adjustments never
	// exceed the original purchase. Allocation order stays deterministic.
	const refundableRemaining = new Map<string, number>();
	for (const relationship of relationships) {
		const target = transactionById.get(relationship.targetTransactionId);
		if (target) refundableRemaining.set(target.id, target.amountInPaise);
	}

	let countedSpendingInPaise = 0;
	let adjustmentsInPaise = 0;
	let outsidePlanInPaise = 0;
	for (const transaction of transactions) {
		const item = resolved.get(transaction.id);
		if (!item) continue;

		if (item.effect === 'SPEND') {
			countedSpendingInPaise += item.effectInPaise;
		} else if (item.effect === 'ADJUSTMENT') {
			const relationship = relationshipBySource.get(transaction.id);
			if (relationship) {
				const remaining = refundableRemaining.get(relationship.targetTransactionId) ?? 0;
				item.effectInPaise = Math.min(item.effectInPaise, remaining);
				refundableRemaining.set(
					relationship.targetTransactionId,
					Math.max(0, remaining - item.effectInPaise),
				);
			}
			adjustmentsInPaise += item.effectInPaise;
		}

		if (transaction.type === 'expense' && item.effectiveTreatment === 'EXCLUDE') {
			outsidePlanInPaise += transaction.amountInPaise;
		}
	}

	return {
		countedSpendingInPaise,
		adjustmentsInPaise,
		usedInPaise: Math.max(0, countedSpendingInPaise - adjustmentsInPaise),
		outsidePlanInPaise,
		transactions: resolved,
	};
}
