// Shared secret guard for /internal/* routes. (The REQUIRES_AI webhook and
// its AI_CLASSIFICATION queue were retired in July 2026; the secret header
// contract lives on for internal mutation endpoints like /internal/requeue.)
import { unauthorized } from '../lib/http/errors';

const AI_REQUIRES_WEBHOOK_SECRET_HEADER = 'x-ai-webhook-secret';

function normalizeHeaderValue(value: string | undefined): string | null {
	if (value === undefined) {
		return null;
	}

	const normalized = value.trim();
	return normalized.length > 0 ? normalized : null;
}

function isTimingSafeStringMatch(expected: string, actual: string): boolean {
	const expectedBytes = new TextEncoder().encode(expected);
	const actualBytes = new TextEncoder().encode(actual);
	if (expectedBytes.length !== actualBytes.length) {
		return false;
	}

	let mismatch = 0;
	for (let index = 0; index < expectedBytes.length; index += 1) {
		mismatch |= expectedBytes[index] ^ actualBytes[index];
	}

	return mismatch === 0;
}

export function assertAiRequiresWebhookSecret(
	providedSecretHeader: string | undefined,
	expectedSecret: string,
): void {
	const providedSecret = normalizeHeaderValue(providedSecretHeader);
	if (!providedSecret || !isTimingSafeStringMatch(expectedSecret, providedSecret)) {
		throw unauthorized('UNAUTHORIZED', 'Invalid AI webhook secret');
	}
}

export function getAiRequiresWebhookSecretHeaderName(): string {
	return AI_REQUIRES_WEBHOOK_SECRET_HEADER;
}
