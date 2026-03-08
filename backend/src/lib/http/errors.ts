export interface HttpErrorPayload {
	error: string;
	message: string;
	details?: string;
}

export class HttpError extends Error {
	readonly statusCode: number;
	readonly errorCode: string;
	readonly details?: string;

	constructor(statusCode: number, errorCode: string, message: string, details?: string) {
		super(message);
		this.name = 'HttpError';
		this.statusCode = statusCode;
		this.errorCode = errorCode;
		this.details = details;
	}
}

interface PostgresErrorLike {
	code?: string;
	message?: string;
	detail?: string;
	constraint?: string;
}

function isPostgresErrorLike(error: unknown): error is PostgresErrorLike {
	return typeof error === 'object' && error !== null && 'code' in error;
}

export function badRequest(errorCode: string, message: string, details?: string): HttpError {
	return new HttpError(400, errorCode, message, details);
}

export function unauthorized(errorCode: string, message: string, details?: string): HttpError {
	return new HttpError(401, errorCode, message, details);
}

export function notFound(errorCode: string, message: string, details?: string): HttpError {
	return new HttpError(404, errorCode, message, details);
}

export function conflict(errorCode: string, message: string, details?: string): HttpError {
	return new HttpError(409, errorCode, message, details);
}

export function serviceUnavailable(errorCode: string, message: string, details?: string): HttpError {
	return new HttpError(503, errorCode, message, details);
}

export function toHttpError(error: unknown): HttpError {
	if (error instanceof HttpError) {
		return error;
	}

	if (isPostgresErrorLike(error)) {
		switch (error.code) {
			case '23505':
				return conflict(
					'CONFLICT',
					'Resource conflict while persisting data',
					error.constraint ?? error.detail,
				);
			case '23503':
				return conflict(
					'REFERENTIAL_CONFLICT',
					'Referenced resource is missing or not accessible',
					error.constraint ?? error.detail,
				);
			case '23514':
			case '23502':
			case '22P02':
				return badRequest(
					'VALIDATION_ERROR',
					'Payload violates database validation rules',
					error.detail ?? error.message,
				);
			default:
				break;
		}
	}

	if (error instanceof Error && error.message === 'SUPABASE_POOLER_URL is not configured') {
		return serviceUnavailable('DB_UNAVAILABLE', 'Database is not configured');
	}

	return new HttpError(500, 'INTERNAL_SERVER_ERROR', 'Internal server error');
}

export function toHttpPayload(error: HttpError): HttpErrorPayload {
	if (error.details) {
		return {
			error: error.errorCode,
			message: error.message,
			details: error.details,
		};
	}

	return {
		error: error.errorCode,
		message: error.message,
	};
}
