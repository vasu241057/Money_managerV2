import type { NormalizerTierConfig } from '../../../shared/types';

const DEFAULT_OPENCODE_MODEL = 'deepseek-v4-flash';

const DEFAULT_NORMALIZER_MODEL_TIERS = [
	'deepseek-v4-flash',
	'deepseek-v4-pro',
	'qwen3.7-plus',
];
const DEFAULT_NORMALIZER_ROTATE_FAILURES = 3;
const DEFAULT_NORMALIZER_COOLDOWN_MS = 300_000;
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

// Template matching + learning is on by default; EMAIL_TEMPLATES=off forces
// every email through the AI path (debug/rollback escape hatch only —
// templates never see a provider, so there is no privacy reason to disable).
export function resolveEmailTemplatesEnabled(env: Env): boolean {
	const raw = normalizeText(
		(env as unknown as Record<string, string | undefined>).EMAIL_TEMPLATES,
	)?.toLowerCase();
	return raw !== 'off' && raw !== 'false' && raw !== '0';
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
	const models = parseCsvList(env.AI_NORMALIZER_TIERS, DEFAULT_NORMALIZER_MODEL_TIERS);
	const rotateFailures = parseEnvInteger(
		env.AI_NORMALIZER_TIER_ROTATE_FAILURES,
		DEFAULT_NORMALIZER_ROTATE_FAILURES,
		1,
	);
	return {
		models,
		rotateFailures,
		cooldownMs: parseEnvInteger(
			env.AI_NORMALIZER_TIER_COOLDOWN_MS,
			DEFAULT_NORMALIZER_COOLDOWN_MS,
			1000,
		),
		// The default retry budget covers the FULL tier chain — otherwise the
		// last tier can never be reached (rotate=3 × 3 tiers needs 9, not 6).
		maxRetries: parseEnvInteger(
			env.AI_NORMALIZER_MAX_RETRIES,
			rotateFailures * models.length,
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

