const DEFAULT_AI_QUEUE_DELAY_SECONDS = 30;
const MAX_AI_QUEUE_DELAY_SECONDS = 12 * 60 * 60;
const DEFAULT_OPENROUTER_MODEL = 'openai/gpt-4o-mini';
const DEFAULT_AI_AUTO_VERIFY_MIN_CONFIDENCE = 0.92;
const MIN_AI_AUTO_VERIFY_MIN_CONFIDENCE = 0.5;
const MAX_AI_AUTO_VERIFY_MIN_CONFIDENCE = 1;

function normalizeText(value: string | undefined): string | null {
	if (value === undefined) {
		return null;
	}

	const normalized = value.trim();
	return normalized.length > 0 ? normalized : null;
}

export function resolveOpenRouterApiKey(env: Env): string | null {
	return normalizeText(env.OPENROUTER_API_KEY);
}

export function resolveOpenRouterModel(env: Env): string {
	return normalizeText(env.OPENROUTER_MODEL) ?? DEFAULT_OPENROUTER_MODEL;
}

export function resolveAiWebhookSecret(env: Env): string | null {
	return normalizeText(env.AI_REQUIRES_WEBHOOK_SECRET);
}

export function resolveAiQueueDelaySeconds(env: Env): number {
	const raw = normalizeText(env.AI_QUEUE_DELAY_SECONDS);
	if (!raw) {
		return DEFAULT_AI_QUEUE_DELAY_SECONDS;
	}

	const parsed = Number.parseInt(raw, 10);
	if (!Number.isFinite(parsed) || parsed < 0) {
		return DEFAULT_AI_QUEUE_DELAY_SECONDS;
	}

	return Math.min(parsed, MAX_AI_QUEUE_DELAY_SECONDS);
}

export function resolveAiAutoVerifyMinConfidence(env: Env): number {
	const raw = normalizeText(env.AI_AUTO_VERIFY_MIN_CONFIDENCE);
	if (!raw) {
		return DEFAULT_AI_AUTO_VERIFY_MIN_CONFIDENCE;
	}

	const parsed = Number.parseFloat(raw);
	if (!Number.isFinite(parsed)) {
		return DEFAULT_AI_AUTO_VERIFY_MIN_CONFIDENCE;
	}

	if (parsed < MIN_AI_AUTO_VERIFY_MIN_CONFIDENCE) {
		return MIN_AI_AUTO_VERIFY_MIN_CONFIDENCE;
	}

	if (parsed > MAX_AI_AUTO_VERIFY_MIN_CONFIDENCE) {
		return MAX_AI_AUTO_VERIFY_MIN_CONFIDENCE;
	}

	return parsed;
}
