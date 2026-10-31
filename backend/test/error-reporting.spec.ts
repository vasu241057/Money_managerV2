import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
	buildErrorReportPayload,
	reportError,
	reportErrorInBackground,
	resetErrorReportingState,
} from '../src/lib/error-reporting';

const WEBHOOK = 'https://alerts.example.com/hook';

function envWith(overrides: Record<string, string> = {}) {
	return {
		APP_NAME: 'money-manager-backend',
		APP_VERSION: '0.1.0',
		NODE_ENV: 'test',
		ERROR_REPORTING_WEBHOOK_URL: WEBHOOK,
		...overrides,
	};
}

function okResponse() {
	return { ok: true, status: 200 } as Response;
}

describe('error reporting', () => {
	let fetchMock: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		resetErrorReportingState();
		fetchMock = vi.fn().mockResolvedValue(okResponse());
		vi.stubGlobal('fetch', fetchMock);
		vi.spyOn(console, 'warn').mockImplementation(() => undefined);
	});

	afterEach(() => {
		vi.unstubAllGlobals();
		vi.restoreAllMocks();
	});

	it('stays off when no webhook is configured', async () => {
		const delivered = await reportError({ NODE_ENV: 'test' }, { event: 'X' });

		expect(delivered).toBe(false);
		expect(fetchMock).not.toHaveBeenCalled();
	});

	it('posts one JSON object with the event, context and environment', async () => {
		const delivered = await reportError(envWith(), {
			event: 'DLQ_MESSAGE',
			message: 'two dead letters',
			context: { queue: 'money-manager-nightly', deadLetterCount: 2 },
		});

		expect(delivered).toBe(true);
		expect(fetchMock).toHaveBeenCalledTimes(1);
		const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
		expect(url).toBe(WEBHOOK);
		expect(init.method).toBe('POST');
		expect((init.headers as Record<string, string>)['content-type']).toBe('application/json');
		expect((init.headers as Record<string, string>).authorization).toBeUndefined();
		expect(init.signal).toBeDefined();

		const body = JSON.parse(init.body as string) as Record<string, unknown>;
		expect(body.event).toBe('DLQ_MESSAGE');
		expect(body.severity).toBe('error');
		expect(body.environment).toBe('test');
		expect(body.service).toBe('money-manager-backend');
		expect(body.context).toEqual({ queue: 'money-manager-nightly', deadLetterCount: 2 });
		expect(typeof body.occurred_at).toBe('string');
	});

	it('sends the bearer token when the sink needs one', async () => {
		await reportError(envWith({ ERROR_REPORTING_WEBHOOK_TOKEN: 's3cret' }), { event: 'X' });

		const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
		expect((init.headers as Record<string, string>).authorization).toBe('Bearer s3cret');
	});

	// An error report over plaintext to an arbitrary host is a leak, so the
	// only http allowed is loopback, for `wrangler dev`.
	it.each([
		['http://alerts.example.com/hook', false],
		['ftp://alerts.example.com/hook', false],
		['not-a-url', false],
		['http://localhost:9000/hook', true],
		['http://127.0.0.1:9000/hook', true],
	])('accepts %s: %s', async (url, expected) => {
		const delivered = await reportError(envWith({ ERROR_REPORTING_WEBHOOK_URL: url }), {
			event: 'X',
		});

		expect(delivered).toBe(expected);
		expect(fetchMock.mock.calls.length).toBe(expected ? 1 : 0);
	});

	it('warns once per isolate about a bad URL, not once per error', async () => {
		const env = envWith({ ERROR_REPORTING_WEBHOOK_URL: 'http://evil.example.com' });
		await reportError(env, { event: 'A' });
		await reportError(env, { event: 'B' });

		const warnings = (console.warn as unknown as ReturnType<typeof vi.fn>).mock.calls.filter(
			(call) => call[0] === 'ERROR_REPORTING_URL_INVALID',
		);
		expect(warnings).toHaveLength(1);
	});

	it('truncates the message and every context value', async () => {
		const payload = buildErrorReportPayload(envWith(), {
			event: 'X',
			message: 'm'.repeat(900),
			context: { long: 'c'.repeat(900), nested: { a: 1 }, nothing: null, flag: true },
		});

		expect((payload.message as string).length).toBe(501);
		const context = payload.context as Record<string, unknown>;
		expect((context.long as string).length).toBe(201);
		expect(context.nested).toBe('{"a":1}');
		expect(context.nothing).toBeNull();
		expect(context.flag).toBe(true);
	});

	it('caps the number of context keys', () => {
		const context: Record<string, number> = {};
		for (let index = 0; index < 50; index += 1) {
			context[`k${index}`] = index;
		}

		const payload = buildErrorReportPayload(envWith(), { event: 'X', context });
		expect(Object.keys(payload.context as Record<string, unknown>)).toHaveLength(20);
	});

	it('reports a non-2xx sink without throwing or recursing', async () => {
		fetchMock.mockResolvedValue({ ok: false, status: 500 } as Response);

		await expect(reportError(envWith(), { event: 'X' })).resolves.toBe(false);
		expect(fetchMock).toHaveBeenCalledTimes(1);
	});

	it('swallows a network failure', async () => {
		fetchMock.mockRejectedValue(new Error('connect ECONNREFUSED'));

		await expect(reportError(envWith(), { event: 'X' })).resolves.toBe(false);
	});

	it('spends at most the per-minute budget, then suppresses', async () => {
		const env = envWith({ ERROR_REPORTING_MAX_PER_MINUTE: '2' });

		const outcomes: boolean[] = [];
		for (let index = 0; index < 5; index += 1) {
			outcomes.push(await reportError(env, { event: `E${index}` }));
		}

		expect(outcomes).toEqual([true, true, false, false, false]);
		expect(fetchMock).toHaveBeenCalledTimes(2);
	});

	it('hands the delivery to waitUntil when a context is available', () => {
		const waitUntil = vi.fn();
		reportErrorInBackground(envWith(), { event: 'X' }, { waitUntil });

		expect(waitUntil).toHaveBeenCalledTimes(1);
		expect(waitUntil.mock.calls[0][0]).toBeInstanceOf(Promise);
	});

	it('never rejects from the background wrapper', async () => {
		fetchMock.mockRejectedValue(new Error('down'));
		const waitUntil = vi.fn();
		reportErrorInBackground(envWith(), { event: 'X' }, { waitUntil });

		await expect(waitUntil.mock.calls[0][0] as Promise<unknown>).resolves.toBe(false);
	});
});
