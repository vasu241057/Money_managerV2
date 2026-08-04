export interface RefundMatchTransaction {
	id: string;
	amountInPaise: number;
	date: string;
	cardId: string | null;
	merchantId: string | null;
	searchKey: string | null;
}

export interface RefundTargetCandidate extends RefundMatchTransaction {
	remainingInPaise: number;
	label: string;
}

export interface RankedRefundMatch {
	target: RefundTargetCandidate;
	confidence: number;
	canAutoLink: boolean;
}

function hasSameMerchant(
	left: RefundMatchTransaction,
	right: RefundMatchTransaction,
): boolean {
	return (
		left.merchantId !== null
		&& right.merchantId !== null
		&& left.merchantId === right.merchantId
	) || (
		left.searchKey !== null
		&& right.searchKey !== null
		&& left.searchKey === right.searchKey
	);
}

function daysBetween(left: string, right: string): number {
	return Math.floor((new Date(left).getTime() - new Date(right).getTime()) / 86_400_000);
}

/** Strict, deterministic policy shared by suggestion tests and ingestion SQL. */
export function rankRefundMatches(
	source: RefundMatchTransaction,
	candidates: RefundTargetCandidate[],
): RankedRefundMatch[] {
	const eligible = candidates.filter(candidate => {
		const ageDays = daysBetween(source.date, candidate.date);
		return ageDays >= 0
			&& ageDays <= 120
			&& source.amountInPaise <= candidate.remainingInPaise;
	});
	const strict = eligible.filter(candidate =>
		source.cardId !== null
		&& candidate.cardId === source.cardId
		&& hasSameMerchant(source, candidate),
	);

	return eligible
		.map(candidate => {
			const sameCard = source.cardId !== null && source.cardId === candidate.cardId;
			const sameMerchant = hasSameMerchant(source, candidate);
			const exactAmount = source.amountInPaise === candidate.remainingInPaise;
			let confidence = 0;
			if (sameCard && sameMerchant) confidence = exactAmount ? 0.995 : 0.94;
			else if (sameMerchant && exactAmount) confidence = 0.82;
			else if (exactAmount) confidence = 0.62;
			const ageDays = daysBetween(source.date, candidate.date);
			if (confidence > 0 && ageDays <= 14) confidence = Math.min(0.999, confidence + 0.01);
			return {
				target: candidate,
				confidence,
				canAutoLink: sameCard && sameMerchant && strict.length === 1,
			};
		})
		.filter(match => match.confidence >= 0.8)
		.sort((left, right) =>
			right.confidence - left.confidence
			|| new Date(right.target.date).getTime() - new Date(left.target.date).getTime()
			|| left.target.id.localeCompare(right.target.id),
		)
		.slice(0, 3);
}
