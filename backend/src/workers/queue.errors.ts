export class PoisonMessageError extends Error {
	readonly kind = 'POISON_MESSAGE' as const;

	constructor(message: string) {
		super(message);
		this.name = 'PoisonMessageError';
	}
}

export class TransientMessageError extends Error {
	readonly kind = 'TRANSIENT_MESSAGE' as const;

	constructor(message: string) {
		super(message);
		this.name = 'TransientMessageError';
	}
}
