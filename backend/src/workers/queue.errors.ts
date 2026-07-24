export class PoisonMessageError extends Error {
	readonly kind = 'POISON_MESSAGE' as const;

	constructor(message: string) {
		super(message);
		this.name = 'PoisonMessageError';
	}
}

export class TransientMessageError extends Error {
	readonly kind = 'TRANSIENT_MESSAGE' as const;
	readonly retryAfterSeconds: number | null;
	readonly retryJitterSeconds: number;

	constructor(
		message: string,
		options: {
			retryAfterSeconds?: number;
			retryJitterSeconds?: number;
		} = {},
	) {
		super(message);
		this.name = 'TransientMessageError';
		this.retryAfterSeconds =
			typeof options.retryAfterSeconds === 'number' &&
			Number.isFinite(options.retryAfterSeconds) &&
			options.retryAfterSeconds >= 0
				? Math.ceil(options.retryAfterSeconds)
				: null;
		this.retryJitterSeconds =
			typeof options.retryJitterSeconds === 'number' &&
			Number.isFinite(options.retryJitterSeconds) &&
			options.retryJitterSeconds >= 0
				? Math.floor(options.retryJitterSeconds)
				: 0;
	}
}
