import { describe, expect, it } from 'vitest';

import { toBackendRequest } from '../src/lib/api-request';

describe('toBackendRequest', () => {
	it('removes the deployed /api prefix while preserving request details', async () => {
		const request = new Request('https://money.example/api/transactions?status=ACTIVE', {
			method: 'POST',
			headers: { 'content-type': 'application/json', 'x-test': 'kept' },
			body: JSON.stringify({ note: 'Lunch' }),
		});

		const mapped = toBackendRequest(request);

		expect(mapped.url).toBe('https://money.example/transactions?status=ACTIVE');
		expect(mapped.method).toBe('POST');
		expect(mapped.headers.get('x-test')).toBe('kept');
		expect(await mapped.json()).toEqual({ note: 'Lunch' });
	});

	it('maps the exact /api path to the backend root', () => {
		const mapped = toBackendRequest(new Request('https://money.example/api'));
		expect(mapped.url).toBe('https://money.example/');
	});

	it('leaves direct backend routes unchanged', () => {
		const request = new Request('https://money.example/health');
		expect(toBackendRequest(request)).toBe(request);
	});
});
