import { describe, expect, it } from 'vitest';

import { isMerchantMappingKillSwitched } from '../src/lib/feature-flags';

describe('merchant mapping kill switch', () => {
	it.each(['0', 'off', 'false', 'no', ' FALSE '])(
		'forces the mapper off for an explicit disabled value (%s)',
		(value) => {
			expect(
				isMerchantMappingKillSwitched({ MERCHANT_MAPPING_ENABLED: value } as Env),
			).toBe(true);
		},
	);

	// Anything else defers to the schedule, which ships disabled — so an absent
	// or unrecognized value still means "does not run" until it is turned on.
	it.each([undefined, '', '1', 'on', 'true', 'yes', 'unexpected'])(
		'defers to the schedule for %s',
		(value) => {
			expect(
				isMerchantMappingKillSwitched({ MERCHANT_MAPPING_ENABLED: value } as Env),
			).toBe(false);
		},
	);
});
