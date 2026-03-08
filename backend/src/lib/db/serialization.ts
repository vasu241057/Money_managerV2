import { badRequest } from '../http/errors';

type Primitive = string | number | boolean | null | Date;

function isPrimitive(value: unknown): value is Primitive {
	return (
		typeof value === 'string' ||
		typeof value === 'number' ||
		typeof value === 'boolean' ||
		value === null ||
		value instanceof Date
	);
}

export function toIsoDateTime(value: unknown, fieldName: string): string {
	if (value instanceof Date) {
		return value.toISOString();
	}

	if (typeof value === 'string') {
		const parsed = new Date(value);
		if (Number.isNaN(parsed.getTime())) {
			throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not a valid datetime`);
		}
		return parsed.toISOString();
	}

	throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not a datetime value`);
}

export function toSafeInteger(value: unknown, fieldName: string): number {
	if (typeof value === 'number') {
		if (!Number.isSafeInteger(value)) {
			throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not a safe integer`);
		}
		return value;
	}

	if (typeof value === 'string') {
		const parsed = Number(value);
		if (!Number.isSafeInteger(parsed)) {
			throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not a safe integer`);
		}
		return parsed;
	}

	throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not an integer`);
}

export function toNullableNumber(value: unknown, fieldName: string): number | null {
	if (value === null || value === undefined) {
		return null;
	}

	if (typeof value === 'number') {
		if (!Number.isFinite(value)) {
			throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not finite`);
		}
		return value;
	}

	if (typeof value === 'string') {
		const parsed = Number(value);
		if (!Number.isFinite(parsed)) {
			throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not finite`);
		}
		return parsed;
	}

	throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not numeric`);
}

export function toNullableString(value: unknown, fieldName: string): string | null {
	if (value === null || value === undefined) {
		return null;
	}

	if (typeof value === 'string') {
		return value;
	}

	throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not a string`);
}

export function toRequiredString(value: unknown, fieldName: string): string {
	if (typeof value !== 'string' || value.length === 0) {
		throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not a non-empty string`);
	}

	return value;
}

export function toBoolean(value: unknown, fieldName: string): boolean {
	if (typeof value !== 'boolean') {
		throw badRequest('SERIALIZATION_ERROR', `${fieldName} is not boolean`);
	}

	return value;
}

export function isPlainPrimitive(value: unknown): value is Primitive {
	return isPrimitive(value);
}
