import { describe, expect, it } from 'vitest';

import { resolveNormalizerTierConfig } from '../src/lib/ai';

describe('resolveNormalizerTierConfig retry budget', () => {
	it('defaults maxRetries to rotateFailures × tier count so every tier is reachable', () => {
		// 3 default tiers × 3 rotate failures = 9; the old fixed default of 6
		// made the third tier unreachable.
		const config = resolveNormalizerTierConfig({} as unknown as Env);
		expect(config.models.length).toBe(3);
		expect(config.rotateFailures).toBe(3);
		expect(config.maxRetries).toBe(9);
	});

	it('scales the default with the configured chain', () => {
		const config = resolveNormalizerTierConfig({
			AI_NORMALIZER_TIERS: 'model-a,model-b',
			AI_NORMALIZER_TIER_ROTATE_FAILURES: '2',
		} as unknown as Env);
		expect(config.maxRetries).toBe(4);
	});

	it('lets an explicit AI_NORMALIZER_MAX_RETRIES override the computed default', () => {
		const config = resolveNormalizerTierConfig({
			AI_NORMALIZER_MAX_RETRIES: '4',
		} as unknown as Env);
		expect(config.maxRetries).toBe(4);
	});
});
