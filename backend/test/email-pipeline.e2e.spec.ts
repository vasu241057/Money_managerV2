import { beforeEach, describe, expect, it, vi } from 'vitest';

import type {
	EmailSyncJobPayload,
	EmailSyncUserJobPayload,
	RawEmailStatus,
	TransactionStatus,
	UUID,
} from '../../shared/types';
import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import { runNormalizeRawEmailsJob } from '../src/workers/email-normalizer';
import { dispatchEmailSyncUsers } from '../src/workers/email-sync-dispatcher';
import { runEmailSyncUserJob } from '../src/workers/email-sync-fetcher';

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

const TEST_APP_CONFIG: AppConfig = {
	appName: 'money-manager-backend',
	appVersion: '0.1.0',
	nodeEnv: 'test',
	supabasePoolerUrl: 'postgres://postgres:postgres@localhost:6543/postgres',
	dbMaxConnections: 5,
	dbConnectTimeoutSeconds: 5,
};

interface SqlMock extends SqlClient {
	begin: (cb: (tx: SqlClient) => Promise<unknown>) => Promise<unknown>;
}

interface UserState {
	id: UUID;
	last_app_open_date: string;
	created_at: string;
}

interface OAuthConnectionState {
	id: UUID;
	user_id: UUID;
	provider: 'google';
	email_address: string;
	access_token: string | null;
	refresh_token: string | null;
	last_sync_timestamp: number;
	sync_status: 'ACTIVE' | 'DORMANT' | 'AUTH_REVOKED' | 'ERROR_PAUSED';
	created_at: string;
	updated_at: string;
}

interface RawEmailState {
	id: UUID;
	user_id: UUID;
	oauth_connection_id: UUID | null;
	source_id: string;
	internal_date: string;
	clean_text: string;
	status: RawEmailStatus;
	created_at: string;
}

interface FinancialEventState {
	id: UUID;
	user_id: UUID;
	raw_email_id: UUID | null;
	extraction_index: number;
	direction: 'debit' | 'credit';
	amount_in_paise: number;
	currency: 'INR';
	txn_timestamp: string;
	payment_method: 'upi' | 'credit_card' | 'debit_card' | 'netbanking' | 'cash' | 'unknown';
	instrument_id: string | null;
	counterparty_raw: string | null;
	search_key: string | null;
	status: 'ACTIVE' | 'REVERSED';
	created_at: string;
}

interface TransactionState {
	id: UUID;
	user_id: UUID;
	financial_event_id: UUID;
	account_id: UUID | null;
	category_id: UUID | null;
	merchant_id: UUID | null;
	credit_card_id: UUID | null;
	amount_in_paise: number;
	type: 'income' | 'expense' | 'transfer';
	txn_date: string;
	user_note: string | null;
	status: TransactionStatus;
	classification_source: 'USER' | 'SYSTEM_DEFAULT' | 'HEURISTIC' | 'AI';
	ai_confidence_score: number | null;
	created_at: string;
	updated_at: string;
}

interface GlobalAliasState {
	search_key: string;
	merchant_id: UUID;
	default_category_id: UUID | null;
	merchant_type: 'MERCHANT' | 'TRANSFER_INSTITUTION' | 'AGGREGATOR' | 'P2P';
}

interface PipelineState {
	now_iso: string;
	user: UserState;
	connection: OAuthConnectionState;
	system_categories: Array<{ id: UUID; type: 'income' | 'expense' | 'transfer' }>;
	global_aliases: Map<string, GlobalAliasState>;
	raw_emails_by_id: Map<UUID, RawEmailState>;
	raw_email_id_by_user_source: Map<string, UUID>;
	financial_events_by_id: Map<UUID, FinancialEventState>;
	financial_event_id_by_fact_key: Map<string, UUID>;
	transactions_by_id: Map<UUID, TransactionState>;
	transaction_id_by_financial_event_id: Map<UUID, UUID>;
	next_uuid_counter: number;
}

interface PipelineScenarioInput {
	merchant_alias?: { search_key: string; merchant_id: UUID; category_id: UUID } | null;
}

interface GmailMessageFixture {
	id: string;
	internal_date_ms: number;
	text: string;
}

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim().toLowerCase();
}

function buildUuid(index: number): UUID {
	return `00000000-0000-4000-8000-${index.toString(16).padStart(12, '0')}`;
}

function nextUuid(state: PipelineState): UUID {
	state.next_uuid_counter += 1;
	return buildUuid(state.next_uuid_counter);
}

function rawEmailLookupKey(userId: UUID, sourceId: string): string {
	return `${userId}:${sourceId}`;
}

function financialEventFactKey(rawEmailId: UUID, extractionIndex: number): string {
	return `${rawEmailId}:${extractionIndex}`;
}

function createPipelineState(input: PipelineScenarioInput = {}): PipelineState {
	const userId = buildUuid(1);
	const connectionId = buildUuid(2);

	const state: PipelineState = {
		now_iso: '2026-03-11T00:00:00.000Z',
		user: {
			id: userId,
			last_app_open_date: '2026-03-11T00:00:00.000Z',
			created_at: '2026-03-01T00:00:00.000Z',
		},
		connection: {
			id: connectionId,
			user_id: userId,
			provider: 'google',
			email_address: 'pipeline-user@example.com',
			access_token: null,
			refresh_token: 'plain-refresh-token',
			last_sync_timestamp: 1_700_000_000_000,
			sync_status: 'ACTIVE',
			created_at: '2026-03-02T00:00:00.000Z',
			updated_at: '2026-03-02T00:00:00.000Z',
		},
		system_categories: [
			{ id: buildUuid(101), type: 'income' },
			{ id: buildUuid(102), type: 'expense' },
			{ id: buildUuid(103), type: 'transfer' },
		],
		global_aliases: new Map<string, GlobalAliasState>(),
		raw_emails_by_id: new Map<UUID, RawEmailState>(),
		raw_email_id_by_user_source: new Map<string, UUID>(),
		financial_events_by_id: new Map<UUID, FinancialEventState>(),
		financial_event_id_by_fact_key: new Map<string, UUID>(),
		transactions_by_id: new Map<UUID, TransactionState>(),
		transaction_id_by_financial_event_id: new Map<UUID, UUID>(),
		next_uuid_counter: 2_000,
	};

	if (input.merchant_alias) {
		state.global_aliases.set(input.merchant_alias.search_key, {
			search_key: input.merchant_alias.search_key,
			merchant_id: input.merchant_alias.merchant_id,
			default_category_id: input.merchant_alias.category_id,
			merchant_type: 'MERCHANT',
		});
	}

	return state;
}

function mapOAuthConnectionToRow(connection: OAuthConnectionState) {
	return {
		id: connection.id,
		user_id: connection.user_id,
		provider: connection.provider,
		email_address: connection.email_address,
		access_token: connection.access_token,
		refresh_token: connection.refresh_token,
		last_sync_timestamp: connection.last_sync_timestamp,
		sync_status: connection.sync_status,
		created_at: connection.created_at,
		updated_at: connection.updated_at,
	};
}

function mapRawEmailToRow(rawEmail: RawEmailState) {
	return {
		id: rawEmail.id,
		user_id: rawEmail.user_id,
		internal_date: rawEmail.internal_date,
		clean_text: rawEmail.clean_text,
		status: rawEmail.status,
	};
}

function createSqlHarness(state: PipelineState): SqlMock {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);

		if (query.includes('select max(oc.user_id) as max_user_id')) {
			const snapshotTimeMs = Number(values[0]);
			const snapshotIso = new Date(snapshotTimeMs).toISOString();
			const eligible =
				state.connection.provider === 'google' &&
				(state.connection.sync_status === 'ACTIVE' || state.connection.sync_status === 'DORMANT') &&
				state.connection.created_at <= snapshotIso
					? [state.connection.user_id]
					: [];
			return [{ max_user_id: eligible[0] ?? null }];
		}

		if (
			query.includes('from public.oauth_connections as oc') &&
			query.includes('join public.users as u') &&
			query.includes('group by u.id, u.last_app_open_date') &&
			query.includes('order by u.id asc') &&
			query.includes('limit') &&
			query.includes('offset')
		) {
			const hasUpperBound = query.includes('and u.id <=');
			const offsetIndex = hasUpperBound ? 4 : 3;
			const limitIndex = hasUpperBound ? 3 : 2;
			const snapshotTimeMs = Number(values[0]);
			const snapshotIso = new Date(snapshotTimeMs).toISOString();
			const scanUpperUserId = hasUpperBound ? (values[2] as UUID) : null;
			const limit = Number(values[limitIndex]);
			const offset = Number(values[offsetIndex]);

			const candidateRows = [] as Array<{
				user_id: UUID;
				last_sync_timestamp: number;
				last_app_open_date: string;
				has_active_connections: boolean;
				has_dormant_connections: boolean;
			}>;

			const isConnectionEligible =
				state.connection.provider === 'google' &&
				(state.connection.sync_status === 'ACTIVE' || state.connection.sync_status === 'DORMANT') &&
				state.connection.created_at <= snapshotIso &&
				state.user.created_at <= snapshotIso &&
				(scanUpperUserId ? state.user.id <= scanUpperUserId : true);

			if (isConnectionEligible) {
				candidateRows.push({
					user_id: state.user.id,
					last_sync_timestamp: state.connection.last_sync_timestamp,
					last_app_open_date: state.user.last_app_open_date,
					has_active_connections: state.connection.sync_status === 'ACTIVE',
					has_dormant_connections: state.connection.sync_status === 'DORMANT',
				});
			}

			candidateRows.sort((left, right) => left.user_id.localeCompare(right.user_id));
			return candidateRows.slice(offset, offset + limit);
		}

		if (query.includes("set sync_status = 'dormant'")) {
			const userIds = (values[0] as UUID[]) ?? [];
			if (
				userIds.includes(state.connection.user_id) &&
				state.connection.provider === 'google' &&
				state.connection.sync_status === 'ACTIVE'
			) {
				state.connection.sync_status = 'DORMANT';
				state.connection.updated_at = state.now_iso;
			}
			return [];
		}

		if (query.includes("set sync_status = 'active'")) {
			const userIds = (values[0] as UUID[]) ?? [];
			if (
				userIds.includes(state.connection.user_id) &&
				state.connection.provider === 'google' &&
				state.connection.sync_status === 'DORMANT'
			) {
				state.connection.sync_status = 'ACTIVE';
				state.connection.updated_at = state.now_iso;
			}
			return [];
		}

		if (
			query.includes('from public.oauth_connections as oc') &&
			query.includes("sync_status in ('active', 'error_paused')") &&
			query.includes('order by oc.updated_at desc')
		) {
			const userId = values[0] as UUID;
			if (
				state.connection.user_id === userId &&
				state.connection.provider === 'google' &&
				(state.connection.sync_status === 'ACTIVE' ||
					state.connection.sync_status === 'ERROR_PAUSED')
			) {
				return [mapOAuthConnectionToRow(state.connection)];
			}

			return [];
		}

		if (
			query.includes('set access_token =') &&
			query.includes('refresh_token =') &&
			query.includes('oc.access_token is not distinct from')
		) {
			const encryptedAccessToken = values[0] as string;
			const encryptedRefreshToken = values[1] as string;
			const connectionId = values[2] as UUID;
			const userId = values[3] as UUID;
			const expectedAccessToken = values[4] as string | null;
			const expectedRefreshToken = values[5] as string | null;

			const occMatch =
				state.connection.id === connectionId &&
				state.connection.user_id === userId &&
				state.connection.access_token === expectedAccessToken &&
				state.connection.refresh_token === expectedRefreshToken &&
				(state.connection.sync_status === 'ACTIVE' ||
					state.connection.sync_status === 'ERROR_PAUSED');

			if (!occMatch) {
				return [];
			}

			state.connection.access_token = encryptedAccessToken;
			state.connection.refresh_token = encryptedRefreshToken;
			state.connection.sync_status = 'ACTIVE';
			state.connection.updated_at = state.now_iso;

			return [mapOAuthConnectionToRow(state.connection)];
		}

		if (
			query.includes('from public.oauth_connections as oc') &&
			query.includes('where oc.id =') &&
			query.includes('and oc.user_id =') &&
			query.includes('limit 1')
		) {
			const connectionId = values[0] as UUID;
			const userId = values[1] as UUID;
			if (state.connection.id === connectionId && state.connection.user_id === userId) {
				return [mapOAuthConnectionToRow(state.connection)];
			}
			return [];
		}

		if (
			query.includes('from public.raw_emails as re') &&
			query.includes('re.source_id = any')
		) {
			const userId = values[0] as UUID;
			const sourceIds = (values[1] as string[]) ?? [];

			return sourceIds
				.map((sourceId) => {
					const rawEmailId = state.raw_email_id_by_user_source.get(
						rawEmailLookupKey(userId, sourceId),
					);
					if (!rawEmailId) {
						return null;
					}

					const rawEmail = state.raw_emails_by_id.get(rawEmailId);
					if (!rawEmail) {
						return null;
					}

					return {
						source_id: rawEmail.source_id,
						status: rawEmail.status,
						internal_date: rawEmail.internal_date,
					};
				})
				.filter((row): row is { source_id: string; status: RawEmailStatus; internal_date: string } => Boolean(row));
		}

		if (query.includes('insert into public.raw_emails')) {
			const userId = values[0] as UUID;
			const connectionId = values[1] as UUID;
			const sourceId = values[2] as string;
			const internalDate = values[3] as string;
			const cleanText = values[4] as string;
			const lookup = rawEmailLookupKey(userId, sourceId);
			const existingId = state.raw_email_id_by_user_source.get(lookup);

			if (!existingId) {
				const id = nextUuid(state);
				const record: RawEmailState = {
					id,
					user_id: userId,
					oauth_connection_id: connectionId,
					source_id: sourceId,
					internal_date: internalDate,
					clean_text: cleanText,
					status: 'PENDING_EXTRACTION',
					created_at: state.now_iso,
				};
				state.raw_emails_by_id.set(id, record);
				state.raw_email_id_by_user_source.set(lookup, id);
				return [];
			}

			const existing = state.raw_emails_by_id.get(existingId);
			if (existing && existing.status === 'FAILED') {
				existing.oauth_connection_id = connectionId;
				existing.internal_date = internalDate;
				existing.clean_text = cleanText;
				existing.status = 'PENDING_EXTRACTION';
			}

			return [];
		}

		if (query.includes('last_sync_timestamp = greatest')) {
			const maxInternalTimestamp = Number(values[0]);
			const connectionId = values[1] as UUID;
			const userId = values[2] as UUID;

			if (
				state.connection.id === connectionId &&
				state.connection.user_id === userId &&
				(state.connection.sync_status === 'ACTIVE' ||
					state.connection.sync_status === 'ERROR_PAUSED')
			) {
				state.connection.last_sync_timestamp = Math.max(
					state.connection.last_sync_timestamp,
					maxInternalTimestamp,
				);
				if (state.connection.sync_status === 'ERROR_PAUSED') {
					state.connection.sync_status = 'ACTIVE';
				}
				state.connection.updated_at = state.now_iso;
			}

			return [];
		}

		if (
			query.includes('from public.raw_emails as re') &&
			query.includes('where re.id = any')
		) {
			const ids = (values[0] as UUID[]) ?? [];
			return ids
				.map((id) => state.raw_emails_by_id.get(id))
				.filter((entry): entry is RawEmailState => Boolean(entry))
				.map(mapRawEmailToRow);
		}

		if (
			query.includes('from public.categories as c') &&
			query.includes('c.user_id is null')
		) {
			return state.system_categories.map((row) => ({
				id: row.id,
				type: row.type,
			}));
		}

		if (query.includes('from public.user_merchant_rules as umr')) {
			return [];
		}

		if (query.includes('from public.global_merchant_aliases as gma')) {
			const searchKeys = new Set<string>(((values[0] as string[]) ?? []).map((key) => key));
			return Array.from(state.global_aliases.values())
				.filter((alias) => searchKeys.has(alias.search_key))
				.map((alias) => ({
					search_key: alias.search_key,
					merchant_id: alias.merchant_id,
					default_category_id: alias.default_category_id,
					merchant_type: alias.merchant_type,
				}));
		}

		if (query.includes('for update') && query.includes('where re.id =')) {
			const rawEmailId = values[0] as UUID;
			const rawEmail = state.raw_emails_by_id.get(rawEmailId);
			return rawEmail ? [mapRawEmailToRow(rawEmail)] : [];
		}

		if (query.includes('insert into public.financial_events')) {
			const userId = values[0] as UUID;
			const rawEmailId = values[1] as UUID;
			const extractionIndex = Number(values[2]);
			const key = financialEventFactKey(rawEmailId, extractionIndex);
			const existingId = state.financial_event_id_by_fact_key.get(key);
			if (existingId) {
				return [];
			}

			const id = nextUuid(state);
			const record: FinancialEventState = {
				id,
				user_id: userId,
				raw_email_id: rawEmailId,
				extraction_index: extractionIndex,
				direction: values[3] as FinancialEventState['direction'],
				amount_in_paise: Number(values[4]),
				currency: 'INR',
				txn_timestamp: values[5] as string,
				payment_method: values[6] as FinancialEventState['payment_method'],
				instrument_id: (values[7] as string | null) ?? null,
				counterparty_raw: (values[8] as string | null) ?? null,
				search_key: (values[9] as string | null) ?? null,
				status: 'ACTIVE',
				created_at: state.now_iso,
			};

			state.financial_events_by_id.set(id, record);
			state.financial_event_id_by_fact_key.set(key, id);
			return [{ id }];
		}

		if (
			query.includes('select fe.id') &&
			query.includes('from public.financial_events as fe') &&
			query.includes('where fe.raw_email_id =')
		) {
			const rawEmailId = values[0] as UUID;
			const extractionIndex = Number(values[1]);
			const existingId = state.financial_event_id_by_fact_key.get(
				financialEventFactKey(rawEmailId, extractionIndex),
			);

			return existingId ? [{ id: existingId }] : [];
		}

		if (query.includes('insert into public.transactions')) {
			const userId = values[0] as UUID;
			const financialEventId = values[1] as UUID;
			const existingTransactionId =
				state.transaction_id_by_financial_event_id.get(financialEventId);
			if (existingTransactionId) {
				return [];
			}

			const id = nextUuid(state);
			const record: TransactionState = {
				id,
				user_id: userId,
				financial_event_id: financialEventId,
				account_id: null,
				category_id: (values[2] as UUID | null) ?? null,
				merchant_id: (values[3] as UUID | null) ?? null,
				credit_card_id: null,
				amount_in_paise: Number(values[4]),
				type: values[5] as TransactionState['type'],
				txn_date: values[6] as string,
				user_note: null,
				status: values[7] as TransactionStatus,
				classification_source: values[8] as TransactionState['classification_source'],
				ai_confidence_score: null,
				created_at: state.now_iso,
				updated_at: state.now_iso,
			};

			state.transactions_by_id.set(id, record);
			state.transaction_id_by_financial_event_id.set(financialEventId, id);
			return [{ id, status: record.status }];
		}

		if (query.includes("set status = 'processed'")) {
			const rawEmailId = values[0] as UUID;
			const userId = values[1] as UUID;
			const record = state.raw_emails_by_id.get(rawEmailId);
			if (record && record.user_id === userId && record.status === 'PENDING_EXTRACTION') {
				record.status = 'PROCESSED';
			}
			return [];
		}

		if (query.includes("set status = 'failed'")) {
			const rawEmailId = values[0] as UUID;
			const userId = values[1] as UUID;
			const record = state.raw_emails_by_id.get(rawEmailId);
			if (record && record.user_id === userId && record.status === 'PENDING_EXTRACTION') {
				record.status = 'FAILED';
			}
			return [];
		}

		if (
			query.includes('update public.raw_emails as re') &&
			query.includes('set status =') &&
			query.includes('and re.status = \'pending_extraction\'')
		) {
			const terminalStatus = String(values[0]) as RawEmailStatus;
			const rawEmailId = values[1] as UUID;
			const userId = values[2] as UUID;
			const record = state.raw_emails_by_id.get(rawEmailId);
			if (record && record.user_id === userId && record.status === 'PENDING_EXTRACTION') {
				record.status = terminalStatus;
			}
			return [];
		}

		throw new Error(`Unexpected query execution: ${query}`);
	};

	const begin = async (cb: (tx: SqlClient) => Promise<unknown>): Promise<unknown> =>
		cb(tag as unknown as SqlClient);

	return Object.assign(tag, { begin }) as unknown as SqlMock;
}

function createEmailSyncQueueCapture() {
	const userJobs: EmailSyncUserJobPayload[] = [];

	const queue = {
		sendBatch: vi.fn(async (messages: Array<{ body: EmailSyncJobPayload }>) => {
			for (const message of messages) {
				const body = message.body;
				if (body.job_type === 'EMAIL_SYNC_USER') {
					userJobs.push(body);
				}
			}
		}),
		send: vi.fn().mockResolvedValue(undefined),
	} as unknown as Queue<EmailSyncJobPayload>;

	return { queue, userJobs };
}

function createEnv(params: {
	emailSyncQueue: Queue<EmailSyncJobPayload>;
	aiQueueSend?: ReturnType<typeof vi.fn>;
	aiQueueDelaySeconds?: string;
}): Env {
	const aiSend = params.aiQueueSend ?? vi.fn().mockResolvedValue(undefined);

	return {
		APP_NAME: 'money-manager-backend',
		APP_VERSION: '0.1.0',
		NODE_ENV: 'test',
		SUPABASE_POOLER_URL: 'postgres://postgres:postgres@localhost:6543/postgres',
		DB_MAX_CONNECTIONS: '5',
		DB_CONNECT_TIMEOUT_SECONDS: '5',
		GOOGLE_CLIENT_ID: 'google-client-id',
		GOOGLE_CLIENT_SECRET: 'google-client-secret',
		GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY: btoa('0123456789abcdef0123456789abcdef'),
		EMAIL_SYNC_QUEUE: params.emailSyncQueue,
		AI_CLASSIFICATION_QUEUE: {
			send: aiSend,
		},
		AI_QUEUE_DELAY_SECONDS: params.aiQueueDelaySeconds,
	} as unknown as Env;
}

function createGoogleFetcher(message: GmailMessageFixture) {
	return vi.fn(async (input: RequestInfo | URL) => {
		const url = typeof input === 'string' ? input : input.toString();

		if (url.includes('oauth2.googleapis.com/token')) {
			return new Response(JSON.stringify({ access_token: 'fresh-access-token' }), {
				status: 200,
				headers: { 'content-type': 'application/json' },
			});
		}

		if (url.includes('/gmail/v1/users/me/messages?')) {
			return new Response(JSON.stringify({ messages: [{ id: message.id }] }), {
				status: 200,
				headers: { 'content-type': 'application/json' },
			});
		}

		if (url.includes(`/gmail/v1/users/me/messages/${encodeURIComponent(message.id)}?format=full`)) {
			return new Response(
				JSON.stringify({
					id: message.id,
					internalDate: String(message.internal_date_ms),
					snippet: message.text,
				}),
				{
					status: 200,
					headers: { 'content-type': 'application/json' },
				},
			);
		}

		throw new Error(`Unexpected URL: ${url}`);
	});
}

describe('email sync pipeline e2e', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(TEST_APP_CONFIG);
	});

	it('processes dispatch -> fetcher -> normalizer into VERIFIED transaction for known alias', async () => {
		const state = createPipelineState({
			merchant_alias: {
				search_key: 'AMAZON',
				merchant_id: buildUuid(900),
				category_id: buildUuid(102),
			},
		});
		const sql = createSqlHarness(state);
		getSqlClientMock.mockReturnValue(sql);

		const { queue: emailSyncQueue, userJobs } = createEmailSyncQueueCapture();
		const aiSend = vi.fn().mockResolvedValue(undefined);
		const env = createEnv({
			emailSyncQueue,
			aiQueueSend: aiSend,
		});

		const dispatchResult = await dispatchEmailSyncUsers(
			sql,
			emailSyncQueue,
			Date.parse('2026-03-11T00:00:00.000Z'),
			{
				scan_upper_user_id: state.user.id,
			},
		);

		expect(dispatchResult.enqueued_user_job_count).toBe(1);
		expect(userJobs).toHaveLength(1);

		const fetchResult = await runEmailSyncUserJob(
			userJobs[0] as EmailSyncUserJobPayload,
			env,
			createGoogleFetcher({
				id: 'gmail-msg-1',
				internal_date_ms: 1_700_000_300_000,
				text: 'Rs 123.45 debited to Amazon via UPI ref AXIS123456',
			}),
		);

		expect(fetchResult.fetched_message_count).toBe(1);
		expect(fetchResult.inserted_or_retried_raw_email_count).toBe(1);

		const rawEmailId = state.raw_email_id_by_user_source.get(
			rawEmailLookupKey(state.user.id, 'gmail-msg-1'),
		);
		expect(rawEmailId).toBeDefined();

		const normalizeResult = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: [rawEmailId as UUID],
			},
			env,
		);

		expect(normalizeResult.processed_raw_email_count).toBe(1);
		expect(normalizeResult.created_transaction_count).toBe(1);
		expect(normalizeResult.needs_review_transaction_count).toBe(0);
		expect(normalizeResult.ai_enqueued_count).toBe(0);

		expect(state.transactions_by_id.size).toBe(1);
		const createdTransaction = Array.from(state.transactions_by_id.values())[0];
		expect(createdTransaction?.status).toBe('VERIFIED');
		expect(createdTransaction?.classification_source).toBe('HEURISTIC');
		expect(aiSend).not.toHaveBeenCalled();

		const persistedRawEmail = state.raw_emails_by_id.get(rawEmailId as UUID);
		expect(persistedRawEmail?.status).toBe('PROCESSED');
	});

	it('keeps pipeline non-blocking for unknown merchants by creating NEEDS_REVIEW and enqueueing AI', async () => {
		const state = createPipelineState();
		const sql = createSqlHarness(state);
		getSqlClientMock.mockReturnValue(sql);

		const { queue: emailSyncQueue, userJobs } = createEmailSyncQueueCapture();
		const aiSend = vi.fn().mockResolvedValue(undefined);
		const env = createEnv({
			emailSyncQueue,
			aiQueueSend: aiSend,
		});

		await dispatchEmailSyncUsers(
			sql,
			emailSyncQueue,
			Date.parse('2026-03-11T00:00:00.000Z'),
			{
				scan_upper_user_id: state.user.id,
			},
		);
		expect(userJobs).toHaveLength(1);

		await runEmailSyncUserJob(
			userJobs[0] as EmailSyncUserJobPayload,
			env,
			createGoogleFetcher({
				id: 'gmail-msg-2',
				internal_date_ms: 1_700_000_400_000,
				text: 'Rs 799 debited to UnknownShop ref X12345',
			}),
		);

		const rawEmailId = state.raw_email_id_by_user_source.get(
			rawEmailLookupKey(state.user.id, 'gmail-msg-2'),
		);
		expect(rawEmailId).toBeDefined();

		const normalizeResult = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: [rawEmailId as UUID],
			},
			env,
		);

		expect(normalizeResult.processed_raw_email_count).toBe(1);
		expect(normalizeResult.created_transaction_count).toBe(1);
		expect(normalizeResult.needs_review_transaction_count).toBe(1);
		expect(normalizeResult.ai_enqueued_count).toBe(1);
		expect(aiSend).toHaveBeenCalledTimes(1);
		expect(aiSend).toHaveBeenCalledWith(
			expect.objectContaining({
				job_type: 'AI_CLASSIFICATION',
			}),
			{ contentType: 'json', delaySeconds: 30 },
		);

		const createdTransaction = Array.from(state.transactions_by_id.values())[0];
		expect(createdTransaction?.status).toBe('NEEDS_REVIEW');
		expect(createdTransaction?.classification_source).toBe('SYSTEM_DEFAULT');
	});

	it('does not fail normalization when AI enqueue retries are exhausted', async () => {
		const state = createPipelineState();
		const sql = createSqlHarness(state);
		getSqlClientMock.mockReturnValue(sql);

		const { queue: emailSyncQueue, userJobs } = createEmailSyncQueueCapture();
		const aiSend = vi.fn().mockRejectedValue(new Error('queue unavailable'));
		const env = createEnv({
			emailSyncQueue,
			aiQueueSend: aiSend,
		});

		await dispatchEmailSyncUsers(
			sql,
			emailSyncQueue,
			Date.parse('2026-03-11T00:00:00.000Z'),
			{
				scan_upper_user_id: state.user.id,
			},
		);

		await runEmailSyncUserJob(
			userJobs[0] as EmailSyncUserJobPayload,
			env,
			createGoogleFetcher({
				id: 'gmail-msg-3',
				internal_date_ms: 1_700_000_500_000,
				text: 'Rs 450 debited to UnknownMerchant ref Y12345',
			}),
		);

		const rawEmailId = state.raw_email_id_by_user_source.get(
			rawEmailLookupKey(state.user.id, 'gmail-msg-3'),
		);
		expect(rawEmailId).toBeDefined();

		const normalizeResult = await runNormalizeRawEmailsJob(
			{
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: [rawEmailId as UUID],
			},
			env,
		);

		expect(normalizeResult.processed_raw_email_count).toBe(1);
		expect(normalizeResult.created_transaction_count).toBe(1);
		expect(normalizeResult.needs_review_transaction_count).toBe(1);
		expect(normalizeResult.ai_enqueued_count).toBe(0);
		expect(aiSend).toHaveBeenCalledTimes(3);
		expect(Array.from(state.transactions_by_id.values())[0]?.status).toBe('NEEDS_REVIEW');
	});
});
