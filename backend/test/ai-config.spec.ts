import { describe, expect, it, vi } from 'vitest';

import {
	resolveAiProvider,
	resolveAiProviderConfig,
	resolveEmailPipelineTraceEnabled,
	resolveNormalizerTierConfig,
} from '../src/lib/ai';

describe('resolveNormalizerTierConfig retry budget', () => {
	it('defaults to the 2/2/1 per-tier schedule with maxRetries = 5', () => {
		const config = resolveNormalizerTierConfig({} as unknown as Env);
		expect(config.models.length).toBe(3);
		expect(config.tierAttempts).toEqual([2, 2, 1]);
		expect(config.maxRetries).toBe(5);
	});

	it('fits the schedule to the configured chain (truncate short chains)', () => {
		const config = resolveNormalizerTierConfig({
			AI_NORMALIZER_TIERS: 'model-a,model-b',
		} as unknown as Env);
		expect(config.tierAttempts).toEqual([2, 2]);
		expect(config.maxRetries).toBe(4);
	});

	it('pads longer chains with the last schedule value', () => {
		const config = resolveNormalizerTierConfig({
			AI_NORMALIZER_TIERS: 'a,b,c,d',
		} as unknown as Env);
		expect(config.tierAttempts).toEqual([2, 2, 1, 1]);
		expect(config.maxRetries).toBe(6);
	});

	it('honours an explicit AI_NORMALIZER_TIER_ATTEMPTS schedule', () => {
		const config = resolveNormalizerTierConfig({
			AI_NORMALIZER_TIER_ATTEMPTS: '3,1,1',
		} as unknown as Env);
		expect(config.tierAttempts).toEqual([3, 1, 1]);
		expect(config.maxRetries).toBe(5);
	});

	it('keeps the legacy AI_NORMALIZER_TIER_ROTATE_FAILURES as a uniform schedule', () => {
		const config = resolveNormalizerTierConfig({
			AI_NORMALIZER_TIERS: 'model-a,model-b',
			AI_NORMALIZER_TIER_ROTATE_FAILURES: '2',
		} as unknown as Env);
		expect(config.tierAttempts).toEqual([2, 2]);
		expect(config.maxRetries).toBe(4);
	});

	it('lets an explicit AI_NORMALIZER_MAX_RETRIES override the computed default', () => {
		const config = resolveNormalizerTierConfig({
			AI_NORMALIZER_MAX_RETRIES: '4',
		} as unknown as Env);
		expect(config.maxRetries).toBe(4);
	});
});

describe('resolveAiProviderConfig', () => {
	it('defaults to opencode with the existing key/model vars', () => {
		const config = resolveAiProviderConfig({
			OPENCODE_GO_API_KEY: 'oc-key',
		} as unknown as Env);
		expect(config.provider).toBe('opencode');
		expect(config.chatUrl).toContain('opencode.ai');
		expect(config.apiKey).toBe('oc-key');
		expect(config.apiKeyEnvVar).toBe('OPENCODE_GO_API_KEY');
		expect(config.dailyModel).toBe('deepseek-v4-flash');
	});

	it('switches the whole stack when AI_PROVIDER=openrouter', () => {
		const config = resolveAiProviderConfig({
			AI_PROVIDER: 'openrouter',
			OPENCODE_GO_API_KEY: 'oc-key',
			OPENROUTER_API_KEY: 'or-key',
		} as unknown as Env);
		expect(config.provider).toBe('openrouter');
		expect(config.chatUrl).toBe('https://openrouter.ai/api/v1/chat/completions');
		expect(config.apiKey).toBe('or-key');
		expect(config.apiKeyEnvVar).toBe('OPENROUTER_API_KEY');
		expect(config.dailyModel).toBe('deepseek/deepseek-v4-flash');
	});

	it('falls back to opencode on unknown provider values', () => {
		expect(resolveAiProvider({ AI_PROVIDER: 'gibberish' } as unknown as Env)).toBe('opencode');
	});

	it('uses DeepSeek followed by the two free OpenRouter fallbacks', () => {
		const config = resolveNormalizerTierConfig({
			AI_PROVIDER: 'openrouter',
		} as unknown as Env);
		expect(config.models).toEqual([
			'deepseek/deepseek-v4-flash',
			'nvidia/nemotron-3-ultra-550b-a55b:free',
			'openai/gpt-oss-120b:free',
		]);
		expect(config.models.slice(1).every(model => model.endsWith(':free'))).toBe(true);
		// AI_NORMALIZER_TIERS still overrides regardless of provider.
		const overridden = resolveNormalizerTierConfig({
			AI_PROVIDER: 'openrouter',
			AI_NORMALIZER_TIERS: 'custom/model',
		} as unknown as Env);
		expect(overridden.models).toEqual(['custom/model']);
	});
});

describe('resolveEmailPipelineTraceEnabled', () => {
	it('is off unless the flag is set', () => {
		expect(resolveEmailPipelineTraceEnabled({} as Env)).toBe(false);
		expect(resolveEmailPipelineTraceEnabled({ EMAIL_PIPELINE_TRACE: 'off' } as unknown as Env)).toBe(false);
	});

	it('turns on in development when the flag is set', () => {
		for (const value of ['true', '1', 'on']) {
			expect(
				resolveEmailPipelineTraceEnabled({
					EMAIL_PIPELINE_TRACE: value,
					NODE_ENV: 'development',
				} as unknown as Env),
			).toBe(true);
		}
	});

	it('refuses to trace in production even with the flag set', () => {
		// The trace prints counterparty names and amounts (Task 7 log audit).
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

		expect(
			resolveEmailPipelineTraceEnabled({
				EMAIL_PIPELINE_TRACE: 'true',
				NODE_ENV: 'production',
			} as unknown as Env),
		).toBe(false);
		expect(warn).toHaveBeenCalledWith('EMAIL_PIPELINE_TRACE_REFUSED', { nodeEnv: 'production' });
		warn.mockRestore();
	});
});
