import type { NormalizationEngine, NormalizerTierConfig } from '../../../shared/types';

const DEFAULT_AI_QUEUE_DELAY_SECONDS = 30;
const MAX_AI_QUEUE_DELAY_SECONDS = 12 * 60 * 60;
const DEFAULT_OPENCODE_MODEL = 'deepseek-v4-flash';
const DEFAULT_AI_AUTO_VERIFY_MIN_CONFIDENCE = 0.92;
const MIN_AI_AUTO_VERIFY_MIN_CONFIDENCE = 0.5;
const MAX_AI_AUTO_VERIFY_MIN_CONFIDENCE = 1;

const DEFAULT_NORMALIZER_MODEL_TIERS = [
	'deepseek-v4-flash',
	'deepseek-v4-pro',
	'qwen3.7-plus',
];
const DEFAULT_NORMALIZER_ROTATE_FAILURES = 3;
const DEFAULT_NORMALIZER_COOLDOWN_MS = 300_000;
const DEFAULT_NORMALIZER_MAX_RETRIES = 6;
const DEFAULT_NORMALIZER_TIMEOUT_MS = 45_000;
const DEFAULT_NORMALIZER_TEMPERATURE = 0;
const DEFAULT_NORMALIZER_MAX_TOKENS = 0;
const DEFAULT_NORMALIZER_AUTO_VERIFY_CONFIDENCE = 0.85;
const DEFAULT_NORMALIZER_MIN_CONFIDENCE = 0.3;
const DEFAULT_NORMALIZER_BATCH_SIZE = 4;
const MAX_NORMALIZER_BATCH_SIZE = 10;

function normalizeText(value: string | undefined): string | null {
	if (value === undefined) {
		return null;
	}

	const normalized = value.trim();
	return normalized.length > 0 ? normalized : null;
}

export function resolveOpenCodeGoApiKey(env: Env): string | null {
	return normalizeText(env.OPENCODE_GO_API_KEY);
}

export function resolveOpenCodeGoModel(env: Env): string {
	return normalizeText(env.OPENCODE_GO_MODEL) ?? DEFAULT_OPENCODE_MODEL;
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

export function resolveNormalizationEngine(env: Env): NormalizationEngine {
	const raw = normalizeText(env.NORMALIZATION_ENGINE);
	if (raw === 'deterministic') return 'deterministic';
	if (raw === 'ai') return 'ai';
	// Default: deterministic first, AI fallback for what it can't extract.
	return 'hybrid';
}

function parseCsvList(value: string | undefined, fallback: string[]): string[] {
	const raw = normalizeText(value);
	if (!raw) return fallback;
	return raw
		.split(',')
		.map(s => s.trim())
		.filter(s => s.length > 0);
}

function parseEnvInteger(value: string | undefined, fallback: number, min: number): number {
	const raw = normalizeText(value);
	if (!raw) return fallback;
	const parsed = Number.parseInt(raw, 10);
	if (!Number.isFinite(parsed) || parsed < min) return fallback;
	return parsed;
}

function parseEnvFloat(
	value: string | undefined,
	fallback: number,
	min: number,
	max: number,
): number {
	const raw = normalizeText(value);
	if (!raw) return fallback;
	const parsed = Number.parseFloat(raw);
	if (!Number.isFinite(parsed) || parsed < min || parsed > max) return fallback;
	return parsed;
}

function parseEnvBoolean(value: string | undefined, fallback: boolean): boolean {
	const raw = normalizeText(value);
	if (!raw) return fallback;
	const lower = raw.toLowerCase();
	if (lower === 'true' || lower === '1' || lower === 'yes') return true;
	if (lower === 'false' || lower === '0' || lower === 'no') return false;
	return fallback;
}

export function resolveNormalizerTierConfig(env: Env): NormalizerTierConfig {
	return {
		models: parseCsvList(env.AI_NORMALIZER_TIERS, DEFAULT_NORMALIZER_MODEL_TIERS),
		rotateFailures: parseEnvInteger(
			env.AI_NORMALIZER_TIER_ROTATE_FAILURES,
			DEFAULT_NORMALIZER_ROTATE_FAILURES,
			1,
		),
		cooldownMs: parseEnvInteger(
			env.AI_NORMALIZER_TIER_COOLDOWN_MS,
			DEFAULT_NORMALIZER_COOLDOWN_MS,
			1000,
		),
		maxRetries: parseEnvInteger(
			env.AI_NORMALIZER_MAX_RETRIES,
			DEFAULT_NORMALIZER_MAX_RETRIES,
			1,
		),
		timeoutMs: parseEnvInteger(
			env.AI_NORMALIZER_TIMEOUT_MS,
			DEFAULT_NORMALIZER_TIMEOUT_MS,
			1000,
		),
		temperature: parseEnvFloat(
			env.AI_NORMALIZER_TEMPERATURE,
			DEFAULT_NORMALIZER_TEMPERATURE,
			0,
			2,
		),
		maxTokens: parseEnvInteger(
			env.AI_NORMALIZER_MAX_TOKENS,
			DEFAULT_NORMALIZER_MAX_TOKENS,
			0,
		),
		autoVerifyConfidence: parseEnvFloat(
			env.AI_NORMALIZER_AUTO_VERIFY_CONFIDENCE,
			DEFAULT_NORMALIZER_AUTO_VERIFY_CONFIDENCE,
			0,
			1,
		),
		minConfidence: parseEnvFloat(
			env.AI_NORMALIZER_MIN_CONFIDENCE,
			DEFAULT_NORMALIZER_MIN_CONFIDENCE,
			0,
			1,
		),
		maskNumbers: parseEnvBoolean(
			(env as unknown as Record<string, string | undefined>).AI_NORMALIZER_MASK_NUMBERS,
			true,
		),
		batchSize: Math.min(
			parseEnvInteger(
				(env as unknown as Record<string, string | undefined>).AI_NORMALIZER_BATCH_SIZE,
				DEFAULT_NORMALIZER_BATCH_SIZE,
				1,
			),
			MAX_NORMALIZER_BATCH_SIZE,
		),
	};
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
