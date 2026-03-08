import { describe, expect, it } from 'vitest';

import { getVersionPayload } from '../src/services/version.service';

describe('version.service', () => {
	it('returns correct version payload', () => {
		const config = {
			appName: 'money-manager-backend',
			appVersion: '0.1.0',
			nodeEnv: 'development',
		};

		const result = getVersionPayload(config);

		expect(result).toEqual({
			name: 'money-manager-backend',
			version: '0.1.0',
			environment: 'development',
			runtime: 'cloudflare-workers',
		});
	});
});
