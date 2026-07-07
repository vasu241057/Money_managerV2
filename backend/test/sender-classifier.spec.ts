import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import { runSenderClassificationJob } from '../src/workers/sender-classifier';

const { getAppConfigMock, getSqlClientMock } = vi.hoisted(() => ({
	getAppConfigMock: vi.fn(),
	getSqlClientMock: vi.fn(),
}));

vi.mock('../src/lib/config', () => ({
	getAppConfig: getAppConfigMock,
}));

vi.mock('../src/lib/db/client', () => ({
	getSqlClient: getSqlClientMock,
}));

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function createSqlMock(handler: QueryHandler): SqlClient {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = strings.join(' ').replace(/\s+/g, ' ').trim();
		return handler(query, values);
	};
	return tag as unknown as SqlClient;
}

const appConfig: AppConfig = {
	appName: 'money-manager-backend',
	appVersion: '0.1.0',
	nodeEnv: 'test',
	supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
	dbMaxConnections: 5,
	dbConnectTimeoutSeconds: 5,
} as AppConfig;

function createEnv(overrides?: Record<string, unknown>): Env {
	return {
		OPENCODE_GO_API_KEY: 'test-key',
		EMAIL_SYNC_QUEUE: {
			send: vi.fn().mockResolvedValue(undefined),
		},
		...(overrides ?? {}),
	} as unknown as Env;
}

function modelResponse(senders: unknown): Response {
	return new Response(
		JSON.stringify({
			choices: [{ message: { content: JSON.stringify({ senders }) } }],
		}),
		{ status: 200, headers: { 'content-type': 'application/json' } },
	);
}

const JOB = {
	job_type: 'SENDER_CLASSIFICATION',
	triggered_at: '2026-07-05T21:30:00.000Z',
} as const;

describe('sender-classifier worker', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(appConfig);
	});

	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('skips when OPENCODE_GO_API_KEY is not set', async () => {
		const result = await runSenderClassificationJob(
			JOB,
			createEnv({ OPENCODE_GO_API_KEY: undefined }),
		);

		expect(result.skipped_reason).toContain('OPENCODE_GO_API_KEY');
		expect(result.candidate_count).toBe(0);
	});

	it('writes exact-address ALLOW/DENY rules and enqueues backfill for allowed senders', async () => {
		const insertedRules: Array<{ pattern: string; decision: string; type: string }> = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('select distinct re.from_address')) {
				return [
					{ from_address: 'alerts@newbank.com' },
					{ from_address: 'friend@example.com' },
				];
			}
			if (query.includes('from public.sender_rules')) {
				return [];
			}
			if (query.includes('insert into public.sender_rules')) {
				insertedRules.push({
					pattern: values[0] as string,
					decision: values[1] as string,
					type: values[2] as string,
				});
				return [{ pattern: values[0] }];
			}
			if (query.includes('re.from_address = any')) {
				expect(values[0]).toEqual(['alerts@newbank.com']);
				return [
					{
						id: '00000000-0000-4000-8000-00000000000a',
						user_id: '00000000-0000-4000-8000-000000000001',
					},
					{
						id: '00000000-0000-4000-8000-00000000000b',
						user_id: '00000000-0000-4000-8000-000000000001',
					},
				];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				modelResponse([
					{ address: 'alerts@newbank.com', financial: true, type: 'BANK' },
					{ address: 'friend@example.com', financial: false, type: 'OTHER' },
				]),
			),
		);

		const queueSend = vi.fn().mockResolvedValue(undefined);
		const result = await runSenderClassificationJob(
			JOB,
			createEnv({ EMAIL_SYNC_QUEUE: { send: queueSend } }),
		);

		expect(result.candidate_count).toBe(2);
		expect(result.allow_rule_count).toBe(1);
		expect(result.deny_rule_count).toBe(1);
		expect(insertedRules).toEqual([
			{ pattern: 'alerts@newbank.com', decision: 'ALLOW', type: 'BANK' },
			{ pattern: 'friend@example.com', decision: 'DENY', type: 'OTHER' },
		]);

		expect(result.backfill_raw_email_count).toBe(2);
		expect(result.backfill_job_count).toBe(1);
		expect(queueSend).toHaveBeenCalledTimes(1);
		expect(queueSend.mock.calls[0]?.[0]).toEqual({
			job_type: 'SENDER_BACKFILL',
			user_id: '00000000-0000-4000-8000-000000000001',
			raw_email_ids: [
				'00000000-0000-4000-8000-00000000000a',
				'00000000-0000-4000-8000-00000000000b',
			],
		});
	});

	it('ignores model-invented addresses and already-ruled senders', async () => {
		const insertedRules: string[] = [];
		const sql = createSqlMock((query, values) => {
			if (query.includes('select distinct re.from_address')) {
				return [
					{ from_address: 'alerts@newbank.com' },
					{ from_address: 'already-ruled@somebank.com' },
				];
			}
			if (query.includes('from public.sender_rules')) {
				return [{ pattern: 'already-ruled@somebank.com', decision: 'DENY' }];
			}
			if (query.includes('insert into public.sender_rules')) {
				insertedRules.push(values[0] as string);
				return [{ pattern: values[0] }];
			}
			if (query.includes('re.from_address = any')) {
				return [];
			}
			throw new Error(`Unexpected query: ${query}`);
		});
		getSqlClientMock.mockReturnValue(sql);

		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				modelResponse([
					{ address: 'alerts@newbank.com', financial: true, type: 'BANK' },
					// Injection attempt: not in the candidate list, must be dropped.
					{ address: 'evil@attacker.com', financial: true, type: 'BANK' },
					{ address: 'already-ruled@somebank.com', financial: true, type: 'BANK' },
				]),
			),
		);

		const result = await runSenderClassificationJob(JOB, createEnv());

		// already-ruled sender never reaches the model's candidate set, and the
		// invented address is filtered on the way back.
		expect(result.candidate_count).toBe(1);
		expect(insertedRules).toEqual(['alerts@newbank.com']);
	});
});
