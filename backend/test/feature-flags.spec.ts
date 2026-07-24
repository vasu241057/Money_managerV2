import { describe, expect, it } from 'vitest';

import { isMerchantMappingEnabled } from '../src/lib/feature-flags';

describe('merchant mapping feature flag', () => {
	it.each(['1', 'on', 'true', 'yes', ' TRUE '])(
		'enables only for an explicit supported value (%s)',
		(value) => {
			expect(
				isMerchantMappingEnabled({ MERCHANT_MAPPING_ENABLED: value } as Env),
			).toBe(true);
		},
	);

	it.each([undefined, '', '0', 'off', 'false', 'no', 'unexpected'])(
		'fails closed for %s',
		(value) => {
			expect(
				isMerchantMappingEnabled({ MERCHANT_MAPPING_ENABLED: value } as Env),
			).toBe(false);
		},
	);
});
