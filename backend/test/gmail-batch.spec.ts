import { describe, expect, it, vi } from 'vitest';

import {
	executeGmailBatch,
	GMAIL_BATCH_MAX_CALLS,
	GmailBatchProtocolError,
} from '../src/workers/gmail-batch';

function buildBatchPart(input: {
	boundary: string;
	contentId: string;
	status: number;
	body: unknown;
}): string {
	return [
		`--${input.boundary}`,
		'Content-Type: application/http',
		`Content-ID: <response-${input.contentId}>`,
		'',
		`HTTP/1.1 ${input.status} Status`,
		'Content-Type: application/json',
		'',
		JSON.stringify(input.body),
	].join('\r\n');
}

describe('gmail-batch', () => {
	it('sends one multipart request and matches out-of-order responses by Content-ID', async () => {
		const responseBoundary = 'response_boundary';
		const fetcher = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
			const requestBody = String(init?.body ?? '');
			expect(requestBody).toContain('Content-ID: <gmail-0>');
			expect(requestBody).toContain('Content-ID: <gmail-1>');
			expect(requestBody).toContain(
				'GET /gmail/v1/users/me/messages/m1?format=metadata HTTP/1.1',
			);
			expect(requestBody).toContain(
				'GET /gmail/v1/users/me/messages/m2?format=metadata HTTP/1.1',
			);

			const body = [
				buildBatchPart({
					boundary: responseBoundary,
					contentId: 'gmail-1',
					status: 404,
					body: { error: { message: 'gone' } },
				}),
				buildBatchPart({
					boundary: responseBoundary,
					contentId: 'gmail-0',
					status: 200,
					body: { id: 'm1' },
				}),
				`--${responseBoundary}--`,
				'',
			].join('\r\n');
			return new Response(body, {
				status: 200,
				headers: {
					'content-type': `multipart/mixed; boundary="${responseBoundary}"`,
				},
			});
		});

		const result = await executeGmailBatch(
			'access-token',
			[
				{
					id: 'm1',
					path: '/gmail/v1/users/me/messages/m1?format=metadata',
				},
				{
					id: 'm2',
					path: '/gmail/v1/users/me/messages/m2?format=metadata',
				},
			],
			fetcher,
		);

		expect(fetcher).toHaveBeenCalledTimes(1);
		expect(result.parts).toEqual([
			{ id: 'm2', status: 404, body: { error: { message: 'gone' } } },
			{ id: 'm1', status: 200, body: { id: 'm1' } },
		]);
	});

	it('rejects incomplete multipart responses so the queue retries the full page', async () => {
		const responseBoundary = 'response_boundary';
		const fetcher = vi.fn().mockResolvedValue(
			new Response(
				[
					buildBatchPart({
						boundary: responseBoundary,
						contentId: 'gmail-0',
						status: 200,
						body: { id: 'm1' },
					}),
					`--${responseBoundary}--`,
					'',
				].join('\r\n'),
				{
					status: 200,
					headers: {
						'content-type': `multipart/mixed; boundary=${responseBoundary}`,
					},
				},
			),
		);

		await expect(
			executeGmailBatch(
				'access-token',
				[
					{ id: 'm1', path: '/gmail/v1/users/me/messages/m1?format=full' },
					{ id: 'm2', path: '/gmail/v1/users/me/messages/m2?format=full' },
				],
				fetcher,
			),
		).rejects.toThrow('returned 1 parts for 2 requests');
	});

	it('rejects an unknown response Content-ID instead of matching it by position', async () => {
		const responseBoundary = 'response_boundary';
		const fetcher = vi.fn().mockResolvedValue(
			new Response(
				[
					buildBatchPart({
						boundary: responseBoundary,
						contentId: 'unknown-request',
						status: 200,
						body: { id: 'm1' },
					}),
					`--${responseBoundary}--`,
					'',
				].join('\r\n'),
				{
					status: 200,
					headers: {
						'content-type': `multipart/mixed; boundary=${responseBoundary}`,
					},
				},
			),
		);

		await expect(
			executeGmailBatch(
				'access-token',
				[{ id: 'm1', path: '/gmail/v1/users/me/messages/m1?format=full' }],
				fetcher,
			),
		).rejects.toThrow('cannot be matched to a request');
	});

	it('enforces the recommended maximum of 50 inner Gmail calls', async () => {
		const requests = Array.from({ length: GMAIL_BATCH_MAX_CALLS + 1 }, (_, index) => ({
			id: `m${index}`,
			path: `/gmail/v1/users/me/messages/m${index}?format=full`,
		}));

		await expect(
			executeGmailBatch('access-token', requests, vi.fn()),
		).rejects.toBeInstanceOf(GmailBatchProtocolError);
	});
});
