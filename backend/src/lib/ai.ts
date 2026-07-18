import type { NormalizerTierConfig } from '../../../shared/types';

const DEFAULT_OPENCODE_MODEL = 'deepseek-v4-flash';

const OPENCODE_CHAT_URL = 'https://opencode.ai/zen/go/v1/chat/completions';
const OPENROUTER_CHAT_URL = 'https://openrouter.ai/api/v1/chat/completions';

// OpenRouter free-tier model ids (":free" suffix). The free roster churns —
// verify current ids at openrouter.ai/models before relying on these.
const DEFAULT_OPENROUTER_MODEL = 'nvidia/llama-3.1-nemotron-70b-instruct:free';
const DEFAULT_OPENROUTER_NORMALIZER_TIERS = [
	'deepseek/deepseek-r1:free',
	'nvidia/llama-3.1-nemotron-70b-instruct:free',
	'meta-llama/llama-3.3-70b-instruct:free',
];

const DEFAULT_NORMALIZER_MODEL_TIERS = [
	'deepseek-v4-flash',
	'deepseek-v4-pro',
	'qwen3.7-plus',
];
// Per-tier attempt budget: 2 on the primary, 2 on the second, 1 on the last
// (5 total). Shorter schedules pad with their last value across extra tiers.
const DEFAULT_NORMALIZER_TIER_ATTEMPTS = [2, 2, 1];
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

export type AiProviderName = 'opencode' | 'openrouter';

export interface AiProviderConfig {
	provider: AiProviderName;
	chatUrl: string;
	apiKey: string | null;
	/** Which env var carries the key — for skip/error messages. */
	apiKeyEnvVar: string;
	/** Model for the daily batch jobs (sender classifier + merchant mapper). */
	dailyModel: string;
}

// AI_PROVIDER selects the whole provider stack (URL + key + model defaults).
// Both providers' keys can be configured at once; flipping this one var is
// the switch (an admin-dashboard toggle will front it later). Anything else
// defaults to 'opencode'.
export function resolveAiProvider(env: Env): AiProviderName {
	const raw = normalizeText(
		(env as unknown as Record<string, string | undefined>).AI_PROVIDER,
	)?.toLowerCase();
	return raw === 'openrouter' ? 'openrouter' : 'opencode';
}

export function resolveAiProviderConfig(env: Env): AiProviderConfig {
	const record = env as unknown as Record<string, string | undefined>;
	if (resolveAiProvider(env) === 'openrouter') {
		return {
			provider: 'openrouter',
			chatUrl: OPENROUTER_CHAT_URL,
			apiKey: normalizeText(record.OPENROUTER_API_KEY),
			apiKeyEnvVar: 'OPENROUTER_API_KEY',
			dailyModel: normalizeText(record.OPENROUTER_MODEL) ?? DEFAULT_OPENROUTER_MODEL,
		};
	}

	return {
		provider: 'opencode',
		chatUrl: OPENCODE_CHAT_URL,
		apiKey: resolveOpenCodeGoApiKey(env),
		apiKeyEnvVar: 'OPENCODE_GO_API_KEY',
		dailyModel: resolveOpenCodeGoModel(env),
	};
}

export function resolveAiWebhookSecret(env: Env): string | null {
	return normalizeText(env.AI_REQUIRES_WEBHOOK_SECRET);
}

// Local-dev pipeline tracing (default OFF). When on, the normalizer logs one
// structured line per transaction with the full resolve outcome (amount,
// date, search key, category + why, verify verdict). Content-bearing —
// never enable in production.
export function resolveEmailPipelineTraceEnabled(env: Env): boolean {
	const raw = normalizeText(
		(env as unknown as Record<string, string | undefined>).EMAIL_PIPELINE_TRACE,
	)?.toLowerCase();
	return raw === 'true' || raw === '1' || raw === 'on';
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

// Resolves the per-tier attempt schedule, fitted to the tier count.
// Precedence: AI_NORMALIZER_TIER_ATTEMPTS (csv, e.g. "2,2,1") >
// AI_NORMALIZER_TIER_ROTATE_FAILURES (legacy uniform per-tier value) >
// default [2,2,1]. Schedules shorter than the chain pad with their last
// value; longer ones truncate.
function resolveTierAttempts(env: Env, tierCount: number): number[] {
	const explicit = normalizeText(
		(env as unknown as Record<string, string | undefined>).AI_NORMALIZER_TIER_ATTEMPTS,
	);

	let schedule: number[] | null = null;
	if (explicit) {
		const parsed = explicit
			.split(',')
			.map(s => Number.parseInt(s.trim(), 10))
			.filter(n => Number.isFinite(n) && n >= 1);
		if (parsed.length > 0) {
			schedule = parsed;
		}
	}

	if (!schedule && normalizeText(env.AI_NORMALIZER_TIER_ROTATE_FAILURES)) {
		const uniform = parseEnvInteger(env.AI_NORMALIZER_TIER_ROTATE_FAILURES, 0, 1);
		if (uniform >= 1) {
			schedule = new Array<number>(tierCount).fill(uniform);
		}
	}

	if (!schedule) {
		schedule = DEFAULT_NORMALIZER_TIER_ATTEMPTS;
	}

	const fitted: number[] = [];
	for (let i = 0; i < tierCount; i++) {
		fitted.push(schedule[i] ?? schedule[schedule.length - 1] ?? 1);
	}
	return fitted;
}

export function resolveNormalizerTierConfig(env: Env): NormalizerTierConfig {
	const providerDefaultTiers =
		resolveAiProvider(env) === 'openrouter'
			? DEFAULT_OPENROUTER_NORMALIZER_TIERS
			: DEFAULT_NORMALIZER_MODEL_TIERS;
	// AI_NORMALIZER_TIERS overrides for whichever provider is active — its
	// entries must be model ids the active provider understands.
	const models = parseCsvList(env.AI_NORMALIZER_TIERS, providerDefaultTiers);
	const tierAttempts = resolveTierAttempts(env, models.length);
	return {
		models,
		tierAttempts,
		cooldownMs: parseEnvInteger(
			env.AI_NORMALIZER_TIER_COOLDOWN_MS,
			DEFAULT_NORMALIZER_COOLDOWN_MS,
			1000,
		),
		// The default retry budget is exactly the schedule's total (2+2+1 = 5
		// for the default chain), so the last tier is always reachable and the
		// whole budget is spent walking the chain once.
		maxRetries: parseEnvInteger(
			env.AI_NORMALIZER_MAX_RETRIES,
			tierAttempts.reduce((sum, n) => sum + n, 0),
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

