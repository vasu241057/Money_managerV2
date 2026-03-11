import type {
	AiRequiresWebhookRequest,
	AiRequiresWebhookResponse,
} from '../../../shared/types';
import { badRequest, unauthorized } from '../lib/http/errors';
import { asRecord, parseOptionalIsoDateTime, parseUuid } from '../lib/http/validation';
import { resolveAiQueueDelaySeconds } from '../lib/ai';
import { buildAiClassificationJob } from '../workers/queue.messages';

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

export function parseAiRequiresWebhookRequest(payload: unknown): AiRequiresWebhookRequest {
	const body = asRecord(payload);
	const jobType = body.job_type;
	if (jobType !== undefined && jobType !== 'REQUIRES_AI') {
		throw badRequest('INVALID_PAYLOAD', 'job_type must be REQUIRES_AI when provided');
	}

	return {
		...(jobType === undefined ? {} : { job_type: 'REQUIRES_AI' as const }),
		transaction_id: parseUuid(body.transaction_id, 'transaction_id'),
		requested_at: parseOptionalIsoDateTime(body.requested_at, 'requested_at'),
	};
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

export async function enqueueAiRequiresWebhookJob(
	env: Env,
	input: AiRequiresWebhookRequest,
): Promise<AiRequiresWebhookResponse> {
	const queuedAt = input.requested_at ?? new Date().toISOString();
	const delaySeconds = resolveAiQueueDelaySeconds(env);
	const payload = buildAiClassificationJob(input.transaction_id, queuedAt);

	await env.AI_CLASSIFICATION_QUEUE.send(payload, {
		contentType: 'json',
		delaySeconds,
	});

	return {
		accepted: true,
		queued_at: queuedAt,
	};
}
