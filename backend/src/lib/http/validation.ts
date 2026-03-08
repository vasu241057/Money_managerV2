import {
	TRANSACTION_STATUS_TRANSITIONS,
	type TransactionStatus,
	type UUID,
} from '../../../../shared/types';
import { badRequest } from './errors';

const UUID_REGEX =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const LAST4_REGEX = /^[0-9]{4}$/;

export function asRecord(value: unknown, errorMessage = 'Request body must be a JSON object'): Record<string, unknown> {
	if (typeof value !== 'object' || value === null || Array.isArray(value)) {
		throw badRequest('INVALID_PAYLOAD', errorMessage);
	}

	return value as Record<string, unknown>;
}

export function parseRequiredString(value: unknown, fieldName: string): string {
	if (typeof value !== 'string') {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a string`);
	}

	const normalized = value.trim();
	if (normalized.length === 0) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} cannot be empty`);
	}

	return normalized;
}

export function parseNullableString(value: unknown, fieldName: string): string | null | undefined {
	if (value === undefined) {
		return undefined;
	}

	if (value === null) {
		return null;
	}

	return parseRequiredString(value, fieldName);
}

export function parseUuid(value: unknown, fieldName: string): UUID {
	if (typeof value !== 'string' || !UUID_REGEX.test(value)) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a valid UUID`);
	}

	return value;
}

export function parseOptionalUuid(value: unknown, fieldName: string): UUID | undefined {
	if (value === undefined) {
		return undefined;
	}

	return parseUuid(value, fieldName);
}

export function parseNullableUuid(value: unknown, fieldName: string): UUID | null | undefined {
	if (value === undefined) {
		return undefined;
	}

	if (value === null) {
		return null;
	}

	return parseUuid(value, fieldName);
}

export function parseFiniteNumber(value: unknown, fieldName: string): number {
	if (typeof value !== 'number' || !Number.isFinite(value)) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a finite number`);
	}

	return value;
}

export function parseOptionalFiniteNumber(value: unknown, fieldName: string): number | undefined {
	if (value === undefined) {
		return undefined;
	}

	return parseFiniteNumber(value, fieldName);
}

export function parsePositiveInteger(value: unknown, fieldName: string): number {
	const parsed = parseFiniteNumber(value, fieldName);
	if (!Number.isInteger(parsed) || parsed <= 0) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a positive integer`);
	}

	return parsed;
}

export function parseNonNegativeInteger(value: unknown, fieldName: string): number {
	const parsed = parseFiniteNumber(value, fieldName);
	if (!Number.isInteger(parsed) || parsed < 0) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a non-negative integer`);
	}

	return parsed;
}

export function parseOptionalNonNegativeInteger(
	value: unknown,
	fieldName: string,
): number | undefined {
	if (value === undefined) {
		return undefined;
	}

	return parseNonNegativeInteger(value, fieldName);
}

export function parseIsoDateTime(value: unknown, fieldName: string): string {
	if (typeof value !== 'string') {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be an ISO-8601 string`);
	}

	const parsed = new Date(value);
	if (Number.isNaN(parsed.getTime())) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a valid ISO-8601 datetime`);
	}

	return parsed.toISOString();
}

export function parseOptionalIsoDateTime(value: unknown, fieldName: string): string | undefined {
	if (value === undefined) {
		return undefined;
	}

	return parseIsoDateTime(value, fieldName);
}

export function parseBoolean(value: unknown, fieldName: string): boolean {
	if (typeof value !== 'boolean') {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a boolean`);
	}

	return value;
}

export function parseOptionalBoolean(value: unknown, fieldName: string): boolean | undefined {
	if (value === undefined) {
		return undefined;
	}

	return parseBoolean(value, fieldName);
}

export function parseLast4(value: unknown, fieldName: string): string | null | undefined {
	const normalized = parseNullableString(value, fieldName);
	if (normalized === undefined || normalized === null) {
		return normalized;
	}

	if (!LAST4_REGEX.test(normalized)) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a 4-digit numeric string`);
	}

	return normalized;
}

export function parsePage(value: unknown): number {
	if (value === undefined) {
		return 1;
	}

	let parsed: number;
	if (typeof value === 'number') {
		parsed = value;
	} else if (typeof value === 'string') {
		const normalized = value.trim();
		if (!/^[0-9]+$/.test(normalized)) {
			throw badRequest('INVALID_QUERY', 'page must be a positive integer');
		}
		parsed = Number.parseInt(normalized, 10);
	} else {
		throw badRequest('INVALID_QUERY', 'page must be a positive integer');
	}

	if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
		throw badRequest('INVALID_QUERY', 'page must be a positive integer');
	}

	return parsed;
}

export function parseLimit(value: unknown, maxLimit = 100): number {
	if (value === undefined) {
		return 25;
	}

	let parsed: number;
	if (typeof value === 'number') {
		parsed = value;
	} else if (typeof value === 'string') {
		const normalized = value.trim();
		if (!/^[0-9]+$/.test(normalized)) {
			throw badRequest('INVALID_QUERY', 'limit must be a positive integer');
		}
		parsed = Number.parseInt(normalized, 10);
	} else {
		throw badRequest('INVALID_QUERY', 'limit must be a positive integer');
	}

	if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
		throw badRequest('INVALID_QUERY', 'limit must be a positive integer');
	}

	if (parsed > maxLimit) {
		throw badRequest('INVALID_QUERY', `limit must be less than or equal to ${maxLimit}`);
	}

	return parsed;
}

export function parseEmail(value: unknown, fieldName: string): string {
	const email = parseRequiredString(value, fieldName).toLowerCase();
	if (!email.includes('@') || email.startsWith('@') || email.endsWith('@')) {
		throw badRequest('INVALID_PAYLOAD', `${fieldName} must be a valid email address`);
	}

	return email;
}

export function validateTransactionStatusTransition(
	currentStatus: TransactionStatus,
	nextStatus: TransactionStatus,
): void {
	if (currentStatus === nextStatus) {
		return;
	}

	const allowedTransitions = TRANSACTION_STATUS_TRANSITIONS[currentStatus];
	if (!allowedTransitions.includes(nextStatus)) {
		throw badRequest(
			'INVALID_STATUS_TRANSITION',
			`Transaction status transition ${currentStatus} -> ${nextStatus} is not allowed`,
		);
	}
}
