// End-to-end pipeline: dispatch → fetch (sender gate) → normalize.
// Normalization is the single template→AI→learn engine: the first email of
// a format pays one masked model call and teaches a template; structurally
// identical emails after that are resolved locally with zero AI calls.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type {
	EmailSyncJobPayload,
	EmailSyncUserJobPayload,
	RawEmailStatus,
	TransactionStatus,
	UUID,
} from '../../shared/types';
import type { AppConfig } from '../src/lib/config';
import type { SqlClient } from '../src/lib/db/client';
import { runNormalizeRawEmailsJob } from '../src/workers/email-ai-normalizer';
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
	from_address: string | null;
	subject: string | null;
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

interface EmailTemplateState {
	id: UUID;
	from_address: string;
	skeleton_hash: string;
	skeleton: string;
	slot_map_json: string;
	status: 'ACTIVE' | 'DISABLED';
	hit_count: number;
	failure_count: number;
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
	email_templates_by_key: Map<string, EmailTemplateState>;
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
		email_templates_by_key: new Map<string, EmailTemplateState>(),
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
		from_address: rawEmail.from_address,
		status: rawEmail.status,
	};
}

function createSqlHarness(state: PipelineState): SqlMock {
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);

		if (query.includes('select oc.user_id as max_user_id')) {
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
			const fromAddress = (values[5] as string | null) ?? null;
			const subject = (values[6] as string | null) ?? null;
			const insertStatus = (values[7] as RawEmailStatus | undefined) ?? 'PENDING_EXTRACTION';
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
					from_address: fromAddress,
					subject,
					status: insertStatus,
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
				existing.from_address = fromAddress;
				existing.subject = subject;
				existing.status = insertStatus;
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

		// User-scoped categories for the AI prompt context (none in fixtures).
		if (
			query.includes('from public.categories as c') &&
			query.includes('c.is_system = false')
		) {
			return [];
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

		if (query.includes('from public.accounts as a')) {
			return [];
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

		if (query.includes('insert into public.email_templates')) {
			const fromAddress = values[0] as string;
			const skeletonHash = values[1] as string;
			const key = `${fromAddress}:${skeletonHash}`;
			if (state.email_templates_by_key.has(key)) {
				return [];
			}
			const id = nextUuid(state);
			state.email_templates_by_key.set(key, {
				id,
				from_address: fromAddress,
				skeleton_hash: skeletonHash,
				skeleton: values[2] as string,
				slot_map_json: values[3] as string,
				status: 'ACTIVE',
				hit_count: 0,
				failure_count: 0,
			});
			return [{ id }];
		}

		if (query.includes('delete from public.email_templates')) {
			return [];
		}

		if (query.includes('set hit_count = et.hit_count + 1')) {
			const templateId = values[0] as UUID;
			for (const template of state.email_templates_by_key.values()) {
				if (template.id === templateId) {
					template.hit_count += 1;
					template.failure_count = 0;
				}
			}
			return [];
		}

		if (query.includes('set failure_count = et.failure_count + 1')) {
			const templateId = values[1] as UUID;
			for (const template of state.email_templates_by_key.values()) {
				if (template.id === templateId) {
					template.failure_count += 1;
					if (template.failure_count >= 3) {
						template.status = 'DISABLED';
					}
				}
			}
			return [];
		}

		if (query.includes('from public.email_templates')) {
			const senders = new Set<string>((values[0] as string[]) ?? []);
			return Array.from(state.email_templates_by_key.values())
				.filter((template) => template.status === 'ACTIVE' && senders.has(template.from_address))
				.map((template) => ({
					id: template.id,
					from_address: template.from_address,
					skeleton: template.skeleton,
					slot_map: JSON.parse(template.slot_map_json) as unknown,
					hit_count: template.hit_count,
					failure_count: template.failure_count,
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

		// AI-engine transactions insert layout:
		// 0 user_id, 1 financial_event_id, 2 account_id, 3 category_id,
		// 4 merchant_id, 5 amount, 6 type, 7 txn_date, 8 status,
		// 9 classification_source, 10 ai_confidence_score.
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
				account_id: (values[2] as UUID | null) ?? null,
				category_id: (values[3] as UUID | null) ?? null,
				merchant_id: (values[4] as UUID | null) ?? null,
				credit_card_id: null,
				amount_in_paise: Number(values[5]),
				type: values[6] as TransactionState['type'],
				txn_date: values[7] as string,
				user_note: null,
				status: values[8] as TransactionStatus,
				classification_source: values[9] as TransactionState['classification_source'],
				ai_confidence_score: typeof values[10] === 'number' ? values[10] : null,
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

		if (query.includes("set status = 'ignored'")) {
			const rawEmailId = values[0] as UUID;
			const userId = values[1] as UUID;
			const record = state.raw_emails_by_id.get(rawEmailId);
			if (record && record.user_id === userId && record.status === 'PENDING_EXTRACTION') {
				record.status = 'IGNORED';
			}
			return [];
		}

		if (query.includes('from public.sender_rules')) {
			return [{ pattern: 'hdfc' }, { pattern: 'icici' }];
		}

		// Cross-email dedup probe: no duplicates in these scenarios.
		if (query.includes('fe.amount_in_paise =')) {
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

function createEnv(params: { emailSyncQueue: Queue<EmailSyncJobPayload> }): Env {
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
		OPENCODE_GO_API_KEY: 'test-key',
		EMAIL_SYNC_QUEUE: params.emailSyncQueue,
	} as unknown as Env;
}

const GMAIL_METADATA_SUFFIX = '?format=metadata&metadataHeaders=From&metadataHeaders=Subject';

// The fetcher requests headers (format=metadata) before deciding whether to
// fetch the body. Fixtures serve the same message JSON for both phases, so
// metadata URLs are rewritten to their format=full equivalent here.
function normalizeGmailGetUrl(url: string): string {
	if (url.endsWith(GMAIL_METADATA_SUFFIX)) {
		return `${url.slice(0, -GMAIL_METADATA_SUFFIX.length)}?format=full`;
	}
	return url;
}

function createGoogleFetcher(message: GmailMessageFixture) {
	return vi.fn(async (input: RequestInfo | URL) => {
		const url = normalizeGmailGetUrl(typeof input === 'string' ? input : input.toString());

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
					payload: {
						// Allowlisted bank sender so the sender gate admits the email.
						headers: [
							{ name: 'From', value: 'HDFC Bank InstaAlerts <alerts@hdfcbank.net>' },
							{ name: 'Subject', value: 'Transaction alert' },
						],
					},
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

function aiJsonResponse(payload: unknown): Response {
	return new Response(
		JSON.stringify({
			choices: [{ message: { content: JSON.stringify(payload) } }],
		}),
		{ status: 200, headers: { 'content-type': 'application/json' } },
	);
}

// Syncs one Gmail fixture into raw_emails and returns its raw email id.
async function syncEmail(
	state: PipelineState,
	sql: SqlMock,
	env: Env,
	fixture: GmailMessageFixture,
): Promise<UUID> {
	const { queue: emailSyncQueue, userJobs } = createEmailSyncQueueCapture();
	await dispatchEmailSyncUsers(sql, emailSyncQueue, Date.parse(state.now_iso), {
		scan_upper_user_id: state.user.id,
	});
	expect(userJobs).toHaveLength(1);

	await runEmailSyncUserJob(
		userJobs[0] as EmailSyncUserJobPayload,
		env,
		createGoogleFetcher(fixture),
	);

	const rawEmailId = state.raw_email_id_by_user_source.get(
		rawEmailLookupKey(state.user.id, fixture.id),
	);
	expect(rawEmailId).toBeDefined();
	return rawEmailId as UUID;
}

describe('email sync pipeline e2e (template → AI → learn)', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		getAppConfigMock.mockReturnValue(TEST_APP_CONFIG);
	});

	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('pays one AI call for a new format, learns a template, and resolves the next email locally', async () => {
		const state = createPipelineState();
		const sql = createSqlHarness(state);
		getSqlClientMock.mockReturnValue(sql);

		const { queue: emailSyncQueue } = createEmailSyncQueueCapture();
		const env = createEnv({ emailSyncQueue });

		// Masking order for email 1: N1=123.45, N2=9876, N3=05, N4=07, N5=26,
		// N6=5544332211 (ref), N7=18002586161 (call helpline).
		const aiFetch = vi.fn().mockResolvedValue(
			aiJsonResponse({
				transactions: [
					{
						direction: 'debit',
						amount_token: 'N1',
						date_text: '<N3>-<N4>-<N5>',
						payment_method: 'upi',
						counterparty_raw: 'zepto@ybl',
						instrument_token: 'N2',
						category_id: null,
						sub_category_id: null,
						confidence: 0.9,
					},
				],
			}),
		);
		vi.stubGlobal('fetch', aiFetch);

		const rawEmailId1 = await syncEmail(state, sql, env, {
			id: 'gmail-msg-1',
			internal_date_ms: 1_700_000_300_000,
			text: 'Dear Customer, Rs.123.45 has been debited from your account **9876 on 05-07-26 to VPA zepto@ybl. UPI Ref No 5544332211. Not you? Call 18002586161.',
		});

		const firstResult = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [rawEmailId1] },
			env,
		);

		expect(aiFetch).toHaveBeenCalledTimes(1);
		expect(firstResult.processed_raw_email_count).toBe(1);
		expect(firstResult.created_transaction_count).toBe(1);
		expect(firstResult.template_matched_count).toBe(0);
		expect(firstResult.template_learned_count).toBe(1);
		expect(state.email_templates_by_key.size).toBe(1);

		// The learned skeleton keeps bank boilerplate literal and wildcards
		// the merchant span, so a different merchant still matches.
		const learnedTemplate = Array.from(state.email_templates_by_key.values())[0];
		expect(learnedTemplate?.from_address).toBe('alerts@hdfcbank.net');
		expect(learnedTemplate?.skeleton).toContain('{{M}}');
		expect(learnedTemplate?.skeleton).toContain('has been debited from your account');

		// Same format, different merchant/amount/date/ref: no AI call.
		const rawEmailId2 = await syncEmail(state, sql, env, {
			id: 'gmail-msg-2',
			internal_date_ms: 1_700_000_400_000,
			text: 'Dear Customer, Rs.999.00 has been debited from your account **9876 on 06-07-26 to VPA swiggy@icici. UPI Ref No 9988776655. Not you? Call 18002586161.',
		});

		const secondResult = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [rawEmailId2] },
			env,
		);

		expect(aiFetch).toHaveBeenCalledTimes(1);
		expect(secondResult.processed_raw_email_count).toBe(1);
		expect(secondResult.created_transaction_count).toBe(1);
		expect(secondResult.template_matched_count).toBe(1);
		expect(secondResult.template_learned_count).toBe(0);
		expect(learnedTemplate?.hit_count).toBe(1);

		const transactions = Array.from(state.transactions_by_id.values());
		expect(transactions).toHaveLength(2);
		expect(transactions.map(txn => txn.amount_in_paise).sort((a, b) => a - b)).toEqual([
			12_345, 99_900,
		]);

		const events = Array.from(state.financial_events_by_id.values());
		const templateEvent = events.find(event => event.amount_in_paise === 99_900);
		expect(templateEvent?.counterparty_raw).toBe('swiggy@icici');
		expect(templateEvent?.instrument_id).toBe('9876');
		// 06-07-26 (DD-MM-YY) midnight IST.
		expect(templateEvent?.txn_timestamp).toBe('2026-07-05T18:30:00.000Z');

		// No identity signals → both stay NEEDS_REVIEW regardless of engine.
		expect(new Set(transactions.map(txn => txn.status))).toEqual(new Set(['NEEDS_REVIEW']));
		expect(state.raw_emails_by_id.get(rawEmailId1)?.status).toBe('PROCESSED');
		expect(state.raw_emails_by_id.get(rawEmailId2)?.status).toBe('PROCESSED');
	});

	it('auto-verifies via global alias on the AI path and records AI classification', async () => {
		const merchantId = buildUuid(900);
		const aliasCategoryId = buildUuid(555);
		const state = createPipelineState({
			merchant_alias: {
				search_key: 'ZEPTOMARKETPLACEPRIVATELIMITED',
				merchant_id: merchantId,
				category_id: aliasCategoryId,
			},
		});
		const sql = createSqlHarness(state);
		getSqlClientMock.mockReturnValue(sql);

		const { queue: emailSyncQueue } = createEmailSyncQueueCapture();
		const env = createEnv({ emailSyncQueue });

		// Masking order: N1=450.00, N2=9876, N3=05, N4=07, N5=26, N6=ref.
		const aiFetch = vi.fn().mockResolvedValue(
			aiJsonResponse({
				transactions: [
					{
						direction: 'debit',
						amount_token: 'N1',
						date_text: '<N3>-<N4>-<N5>',
						payment_method: 'upi',
						counterparty_raw: 'ZEPTO MARKETPLACE PRIVATE LIMITED',
						instrument_token: 'N2',
						category_id: null,
						sub_category_id: null,
						// Deliberately low: VERIFIED must come from the alias hit,
						// never from model confidence.
						confidence: 0.55,
					},
				],
			}),
		);
		vi.stubGlobal('fetch', aiFetch);

		const rawEmailId = await syncEmail(state, sql, env, {
			id: 'gmail-msg-3',
			internal_date_ms: 1_700_000_500_000,
			text: 'Dear Customer, Rs.450.00 has been debited from your account **9876 on 05-07-26 to ZEPTO MARKETPLACE PRIVATE LIMITED. UPI Ref No 1122334455.',
		});

		const result = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [rawEmailId] },
			env,
		);

		expect(result.processed_raw_email_count).toBe(1);
		expect(result.created_transaction_count).toBe(1);
		expect(result.needs_review_transaction_count).toBe(0);

		const transaction = Array.from(state.transactions_by_id.values())[0];
		expect(transaction?.status).toBe('VERIFIED');
		expect(transaction?.classification_source).toBe('AI');
		expect(transaction?.merchant_id).toBe(merchantId);
		expect(transaction?.category_id).toBe(aliasCategoryId);

		const event = Array.from(state.financial_events_by_id.values())[0];
		expect(event?.search_key).toBe('ZEPTOMARKETPLACEPRIVATELIMITED');
	});

	it('learns an IGNORE template from a no-transaction answer and gates the next OTP email without AI', async () => {
		const state = createPipelineState();
		const sql = createSqlHarness(state);
		getSqlClientMock.mockReturnValue(sql);

		const { queue: emailSyncQueue } = createEmailSyncQueueCapture();
		const env = createEnv({ emailSyncQueue });

		const aiFetch = vi.fn().mockResolvedValue(aiJsonResponse({ transactions: [] }));
		vi.stubGlobal('fetch', aiFetch);

		const rawEmailId1 = await syncEmail(state, sql, env, {
			id: 'gmail-otp-1',
			internal_date_ms: 1_700_000_600_000,
			text: 'Dear Customer, your OTP for netbanking login is 482913. It is valid for 10 minutes. Do not share it with anyone for safety reasons.',
		});

		const firstResult = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [rawEmailId1] },
			env,
		);

		expect(aiFetch).toHaveBeenCalledTimes(1);
		expect(firstResult.ignored_raw_email_count).toBe(1);
		expect(firstResult.template_learned_count).toBe(1);
		expect(state.raw_emails_by_id.get(rawEmailId1)?.status).toBe('IGNORED');

		const rawEmailId2 = await syncEmail(state, sql, env, {
			id: 'gmail-otp-2',
			internal_date_ms: 1_700_000_700_000,
			text: 'Dear Customer, your OTP for netbanking login is 771122. It is valid for 10 minutes. Do not share it with anyone for safety reasons.',
		});

		const secondResult = await runNormalizeRawEmailsJob(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: [rawEmailId2] },
			env,
		);

		// The recurring OTP format never reaches the model again.
		expect(aiFetch).toHaveBeenCalledTimes(1);
		expect(secondResult.ignored_raw_email_count).toBe(1);
		expect(secondResult.template_matched_count).toBe(1);
		expect(state.raw_emails_by_id.get(rawEmailId2)?.status).toBe('IGNORED');
		expect(state.transactions_by_id.size).toBe(0);
	});
});
