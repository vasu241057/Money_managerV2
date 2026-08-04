import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import {
	maskSubjectForClassifier,
	runSenderClassificationJob,
} from '../src/workers/sender-classifier';

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
		if (query.includes('insert into public.ai_usage_events')) return [];
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

// Routes the queries every run makes, in a query-shape-specific way:
// gated-sender scan, full rules load, DENY re-ask scan, subject fetch.
interface HarnessConfig {
	gatedSenders: string[];
	rules?: Array<{ pattern: string; decision: string }>;
	reaskPatterns?: string[];
	subjects?: Array<{ from_address: string; subject: string }>;
	backfillRows?: Array<{ id: string; user_id: string }>;
	onInsertRule?: (values: unknown[]) => void;
	onBackfillScan?: (values: unknown[]) => void;
}

function createHarnessSql(config: HarnessConfig): SqlClient {
	return createSqlMock((query, values) => {
		if (query.includes('select distinct re.from_address')) {
			return config.gatedSenders.map(address => ({ from_address: address }));
		}
		if (query.includes("sr.decision = 'DENY'") && query.includes("sr.source = 'AI'")) {
			return (config.reaskPatterns ?? []).map(pattern => ({ pattern }));
		}
		if (query.includes('from public.sender_rules')) {
			return config.rules ?? [];
		}
		if (query.includes('re.subject')) {
			return config.subjects ?? [];
		}
		if (query.includes('insert into public.sender_rules')) {
			config.onInsertRule?.(values);
			return [{ pattern: values[0] }];
		}
		if (query.includes('re.from_address = any')) {
			config.onBackfillScan?.(values);
			return config.backfillRows ?? [];
		}
		throw new Error(`Unexpected query: ${query}`);
	});
}

describe('maskSubjectForClassifier', () => {
	it('strips every digit and collapses whitespace', () => {
		expect(maskSubjectForClassifier('Rs.1,234.50 debited  from A/c 9876')).toBe(
			'Rs.#,###.## debited from A/c ####',
		);
	});
});

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

	it('writes exact-address ALLOW/DENY rules with masked subject context and enqueues backfill', async () => {
		const insertedRules: Array<{ pattern: string; decision: string; type: string }> = [];
		const sql = createHarnessSql({
			gatedSenders: ['alerts@newbank.com', 'friend@example.com'],
			subjects: [
				{ from_address: 'alerts@newbank.com', subject: 'A/c 9876 debited Rs.500.00' },
			],
			backfillRows: [
				{
					id: '00000000-0000-4000-8000-00000000000a',
					user_id: '00000000-0000-4000-8000-000000000001',
				},
				{
					id: '00000000-0000-4000-8000-00000000000b',
					user_id: '00000000-0000-4000-8000-000000000001',
				},
			],
			onInsertRule: values => {
				insertedRules.push({
					pattern: values[0] as string,
					decision: values[1] as string,
					type: values[2] as string,
				});
			},
			onBackfillScan: values => {
				expect(values[0]).toEqual(['alerts@newbank.com']);
			},
		});
		getSqlClientMock.mockReturnValue(sql);

		const fetchMock = vi.fn().mockResolvedValue(
			modelResponse([
				{ address: 'alerts@newbank.com', financial: true, type: 'BANK' },
				{ address: 'friend@example.com', financial: false, type: 'OTHER' },
			]),
		);
		vi.stubGlobal('fetch', fetchMock);

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

		// The model sees masked subjects (no digits), never raw ones.
		const requestBody = JSON.parse(
			(fetchMock.mock.calls[0]?.[1] as { body: string }).body,
		) as { messages: Array<{ content: string }> };
		const userContent = requestBody.messages[1]?.content ?? '';
		expect(userContent).toContain('A/c #### debited Rs.###.##');
		expect(userContent).not.toContain('9876');

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

	it('re-asks stale AI DENY rules and backfills a sender that flips to ALLOW', async () => {
		const insertedRules: Array<{ pattern: string; decision: string }> = [];
		const sql = createHarnessSql({
			gatedSenders: [],
			// The rule exists, so the sender is not a "new" candidate…
			rules: [{ pattern: 'alerts@missedbank.com', decision: 'DENY' }],
			// …but its AI DENY rule is stale, so it is re-asked.
			reaskPatterns: ['alerts@missedbank.com'],
			subjects: [
				{ from_address: 'alerts@missedbank.com', subject: 'Payment of Rs.750 received' },
			],
			backfillRows: [
				{
					id: '00000000-0000-4000-8000-00000000000c',
					user_id: '00000000-0000-4000-8000-000000000001',
				},
			],
			onInsertRule: values => {
				insertedRules.push({
					pattern: values[0] as string,
					decision: values[1] as string,
				});
			},
		});
		getSqlClientMock.mockReturnValue(sql);

		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue(
				modelResponse([{ address: 'alerts@missedbank.com', financial: true, type: 'BANK' }]),
			),
		);

		const queueSend = vi.fn().mockResolvedValue(undefined);
		const result = await runSenderClassificationJob(
			JOB,
			createEnv({ EMAIL_SYNC_QUEUE: { send: queueSend } }),
		);

		expect(result.candidate_count).toBe(0);
		expect(result.reask_candidate_count).toBe(1);
		expect(result.allow_rule_count).toBe(1);
		expect(insertedRules).toEqual([
			{ pattern: 'alerts@missedbank.com', decision: 'ALLOW' },
		]);
		// The flipped sender's gated mail is refetched — a misclassification
		// heals within the re-ask window instead of being permanent.
		expect(result.backfill_raw_email_count).toBe(1);
		expect(queueSend).toHaveBeenCalledTimes(1);
	});

	it('ignores model-invented addresses and already-ruled senders', async () => {
		const insertedRules: string[] = [];
		const sql = createHarnessSql({
			gatedSenders: ['alerts@newbank.com', 'already-ruled@somebank.com'],
			rules: [{ pattern: 'already-ruled@somebank.com', decision: 'DENY' }],
			onInsertRule: values => {
				insertedRules.push(values[0] as string);
			},
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

		// already-ruled sender never reaches the model's candidate set (its
		// DENY rule is fresh, so it is not re-asked either), and the invented
		// address is filtered on the way back.
		expect(result.candidate_count).toBe(1);
		expect(insertedRules).toEqual(['alerts@newbank.com']);
	});
});
