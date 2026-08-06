import type { Transaction } from '../types/domain';

export type LedgerFilter =
	| 'all'
	| 'income'
	| 'expense'
	| 'review'
	| 'in-budget'
	| 'outside-budget'
	| 'adjustments';

export type SwipeOutcome =
	| 'CLOSE'
	| 'OPEN_LEFT'
	| 'OPEN_RIGHT'
	| 'COMMIT_RIGHT';

const OPEN_DISTANCE = 42;
const OPEN_VELOCITY = 0.32;
const COMMIT_RATIO = 0.52;

export function resolveSwipeOutcome(
	offset: number,
	velocityX: number,
	rowWidth: number,
): SwipeOutcome {
	const distance = Math.abs(offset);
	if (offset === 0 || rowWidth <= 0) return 'CLOSE';
	const isRight = offset > 0;
	if (isRight && distance >= rowWidth * COMMIT_RATIO) {
		return 'COMMIT_RIGHT';
	}

	const movingTowardAction = isRight
		? velocityX > OPEN_VELOCITY
		: velocityX < -OPEN_VELOCITY;
	if (distance >= OPEN_DISTANCE || (distance >= 20 && movingTowardAction)) {
		return isRight ? 'OPEN_RIGHT' : 'OPEN_LEFT';
	}
	return 'CLOSE';
}

export function matchesLedgerFilter(transaction: Transaction, filter: LedgerFilter): boolean {
	if (filter === 'income') return transaction.type === 'income';
	if (filter === 'expense') return transaction.type === 'expense';
	if (filter === 'review') return transaction.status === 'NEEDS_REVIEW';
	if (filter === 'all') return true;

	// Budget chips mirror exactly what the monthly plan counts, and the plan
	// counts unreviewed records too (the overview query filters on event status,
	// never review status). Hiding them here made an over-budget month
	// impossible to explain from the ledger: the rows driving the overage were
	// the ones the filter dropped. The row keeps its amber "Review" badge, so
	// an uncertain record still reads as uncertain.
	if (filter === 'in-budget') return transaction.budgetEffect !== 'NONE';
	if (filter === 'outside-budget') return transaction.effectiveBudgetTreatment === 'EXCLUDE';
	return transaction.budgetEffect === 'ADJUSTMENT';
}
