import { NORMALIZE_RAW_EMAILS_MAX_IDS } from '../../../shared/types';
import type {
	EmailSyncUserJobPayload,
	OauthConnectionRow,
	RawEmailStatus,
	SenderBackfillJobPayload,
	UUID,
} from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableString,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';
import {
	isAllowedSender,
	loadSenderRuleEntries,
	parseFromAddress,
	resolveSenderGateConfig,
} from '../lib/sender-gate';
import {
	executeGmailBatch,
	GmailBatchProtocolError,
} from './gmail-batch';
import { isMissingUserReferenceError } from '../lib/db/errors';
import {
	claimPendingRequeueSlot,
	listStalePendingRawEmailIds,
	loadUserQuotaStatus,
	PENDING_REQUEUE_STALE_MINUTES,
	recordUserUsage,
	releasePendingRequeueSlot,
	resolveDefaultQuotaCaps,
	type UserQuotaCaps,
	type UserUsageIncrement,
} from '../services/user-quotas.service';
import { PoisonMessageError, TransientMessageError } from './queue.errors';
import { buildEmailSyncUserJob } from './queue.messages';

const GOOGLE_OAUTH_PROVIDER = 'google';
const GOOGLE_OAUTH_TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token';
const GMAIL_API_MESSAGES_LIST_ENDPOINT = 'https://gmail.googleapis.com/gmail/v1/users/me/messages';
const GMAIL_API_MESSAGES_GET_ENDPOINT = 'https://gmail.googleapis.com/gmail/v1/users/me/messages';
export const ENCRYPTED_TOKEN_PREFIX = 'enc:v1:';
const GMAIL_QUERY_OVERLAP_MS = 15 * 60 * 1000;
// A worst-case page makes one list call plus one metadata and one full-message
// call per item. At Gmail's documented costs, 25 items consume about 1,005
// quota units and leave enough room inside the 6,000 units/minute/user limit.
const GMAIL_LIST_MAX_RESULTS = 25;
const GMAIL_CONTINUATION_DELAY_SECONDS = 30;
const GMAIL_RATE_LIMIT_RETRY_SECONDS = 65;
const GMAIL_RATE_LIMIT_RETRY_JITTER_SECONDS = 5;
const TOKEN_REFRESH_OCC_MAX_ATTEMPTS = 3;
const CONTINUATION_ENQUEUE_MAX_ATTEMPTS = 3;
const EMAIL_SYNC_LEASE_MINUTES = 20;
const MAX_CLEAN_TEXT_LENGTH = 50_000;
// First sync of a connection is capped to a bounded lookback instead of
// walking the entire mailbox history.
const DEFAULT_INITIAL_LOOKBACK_DAYS = 90;
const DAY_MS = 24 * 60 * 60 * 1000;

function resolveInitialLookbackDays(env: Env): number {
	const raw = (env as unknown as Record<string, unknown>).EMAIL_SYNC_INITIAL_LOOKBACK_DAYS;
	if (typeof raw !== 'string' || raw.trim().length === 0) {
		return DEFAULT_INITIAL_LOOKBACK_DAYS;
	}
	const parsed = Number.parseInt(raw.trim(), 10);
	if (!Number.isFinite(parsed) || parsed < 1) {
		return DEFAULT_INITIAL_LOOKBACK_DAYS;
	}
	return parsed;
}

interface OAuthConnectionFetchRowRaw {
	id: unknown;
	user_id: unknown;
	provider: unknown;
	email_address: unknown;
	access_token: unknown;
	refresh_token: unknown;
	last_sync_timestamp: unknown;
	sync_status: unknown;
	created_at: unknown;
	updated_at: unknown;
}

interface ExistingRawEmailRowRaw {
	id: unknown;
	source_id: unknown;
	status: unknown;
	internal_date: unknown;
}

interface GmailListMessageRef {
	id?: unknown;
}

interface GmailListResponse {
	messages?: unknown;
	nextPageToken?: unknown;
	resultSizeEstimate?: unknown;
}

interface GmailMessagePartBody {
	data?: unknown;
}

interface GmailMessagePart {
	mimeType?: unknown;
	body?: unknown;
	parts?: unknown;
}

interface GmailMessageResponse {
	id?: unknown;
	internalDate?: unknown;
	snippet?: unknown;
	payload?: unknown;
}

interface OAuthRefreshResponse {
	access_token?: unknown;
	refresh_token?: unknown;
	error?: unknown;
}

interface GoogleApiErrorResponse {
	error?: {
		errors?: Array<{
			reason?: unknown;
			message?: unknown;
		}>;
		message?: unknown;
		code?: unknown;
		status?: unknown;
	} | null;
}

const MAX_SUBJECT_LENGTH = 500;

interface RawEmailUpsertRecord {
	source_id: string;
	internal_date_iso: string;
	clean_text: string;
	from_address: string | null;
	subject: string | null;
	status: 'PENDING_EXTRACTION' | 'IGNORED';
}

interface ExistingRawEmailStatus {
	id: UUID | null;
	status: RawEmailStatus;
	internal_date_timestamp: number;
}

interface EmailSyncConnectionResult {
	connection_id: UUID;
	email_address: string;
	fetched_message_count: number;
	inserted_or_retried_raw_email_count: number;
	skipped_existing_raw_email_count: number;
	sender_gated_raw_email_count: number;
	revoked: boolean;
	paused: boolean;
	continuation_enqueued: boolean;
	/** The daily fetch cap stopped this mailbox before any Gmail call (Task 6). */
	quota_deferred: boolean;
}

interface EmailSyncContinuationState {
	connection_id: UUID;
	page_token: string;
	after_seconds: number;
	max_internal_timestamp_seen: number;
	sync_run_id: UUID | null;
}

export interface EmailSyncUserResult {
	user_id: UUID;
	connection_count: number;
	processed_connection_count: number;
	revoked_connection_count: number;
	fetched_message_count: number;
	inserted_or_retried_raw_email_count: number;
	skipped_existing_raw_email_count: number;
	sender_gated_raw_email_count: number;
	/** Mailboxes deferred to tomorrow by the daily fetch cap (Task 6). */
	quota_deferred_connection_count: number;
	/** Stale PENDING rows this job handed back to the normalizer (Task 6 self-heal). */
	pending_requeued_count: number;
}

interface FetchLike {
	(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

const GOOGLE_403_RETRYABLE_REASONS = new Set<string>([
	'rateLimitExceeded',
	'userRateLimitExceeded',
	'quotaExceeded',
	'dailyLimitExceeded',
	'dailyLimitExceededUnreg',
	'backendError',
	'RESOURCE_EXHAUSTED',
]);
const GOOGLE_403_PAUSE_REASONS = new Set<string>([
	'insufficientPermissions',
	'accessNotConfigured',
	'forbidden',
	'forbiddenForServiceAccounts',
	'domainPolicy',
	'accessDenied',
	'PERMISSION_DENIED',
]);

function normalizeText(value: string | undefined): string | null {
	if (value === undefined) {
		return null;
	}

	const normalized = value.trim();
	return normalized.length > 0 ? normalized : null;
}

function parseBase64SecretBytes(value: string, fieldName: string): Uint8Array {
	const normalized = value.trim();
	if (normalized.length === 0) {
		throw new TransientMessageError(`${fieldName} is not configured`);
	}

	try {
		const base64 = normalized.replace(/-/g, '+').replace(/_/g, '/');
		const padded = base64.padEnd(Math.ceil(base64.length / 4) * 4, '=');
		const binary = atob(padded);
		const bytes = new Uint8Array(binary.length);
		for (let index = 0; index < binary.length; index += 1) {
			bytes[index] = binary.charCodeAt(index);
		}
		return bytes;
	} catch {
		throw new TransientMessageError(
			`${fieldName} must be a valid base64/base64url-encoded secret`,
		);
	}
}

export function getGoogleRefreshConfig(env: Env): {
	clientId: string;
	clientSecret: string;
	tokenEncryptionKey: Uint8Array;
} {
	const clientId = normalizeText(env.GOOGLE_CLIENT_ID);
	const clientSecret = normalizeText(env.GOOGLE_CLIENT_SECRET);
	const tokenKeyEncoded = normalizeText(env.GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY);

	if (!clientId || !clientSecret || !tokenKeyEncoded) {
		throw new TransientMessageError('Google OAuth refresh configuration is incomplete');
	}

	const tokenEncryptionKey = parseBase64SecretBytes(
		tokenKeyEncoded,
		'GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY',
	);
	if (tokenEncryptionKey.byteLength !== 32) {
		throw new TransientMessageError(
			'GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY must decode to exactly 32 bytes',
		);
	}

	return {
		clientId,
		clientSecret,
		tokenEncryptionKey,
	};
}

function toBase64Url(bytes: Uint8Array): string {
	const base64 = btoa(String.fromCharCode(...bytes));
	return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function fromBase64Url(value: string): Uint8Array {
	const base64 = value.replace(/-/g, '+').replace(/_/g, '/');
	const padded = base64.padEnd(Math.ceil(base64.length / 4) * 4, '=');
	const binary = atob(padded);
	const bytes = new Uint8Array(binary.length);
	for (let index = 0; index < binary.length; index += 1) {
		bytes[index] = binary.charCodeAt(index);
	}
	return bytes;
}

async function importAesKey(rawKey: Uint8Array): Promise<CryptoKey> {
	return crypto.subtle.importKey('raw', rawKey, 'AES-GCM', false, ['encrypt', 'decrypt']);
}

function isEncryptedToken(value: string | null): boolean {
	return Boolean(value && value.startsWith(ENCRYPTED_TOKEN_PREFIX));
}

export async function decryptTokenValue(value: string, keyBytes: Uint8Array): Promise<string> {
	if (!isEncryptedToken(value)) {
		return value;
	}

	const payload = value.slice(ENCRYPTED_TOKEN_PREFIX.length);
	let combinedBytes: Uint8Array;
	try {
		combinedBytes = fromBase64Url(payload);
	} catch {
		throw new TransientMessageError('Encrypted OAuth token payload is malformed');
	}
	if (combinedBytes.byteLength <= 12) {
		throw new TransientMessageError('Encrypted OAuth token payload is invalid');
	}

	const iv = combinedBytes.slice(0, 12);
	const cipherBytes = combinedBytes.slice(12);
	try {
		const cryptoKey = await importAesKey(keyBytes);
		const plainBuffer = await crypto.subtle.decrypt(
			{ name: 'AES-GCM', iv },
			cryptoKey,
			cipherBytes,
		);
		const plainText = new TextDecoder().decode(plainBuffer);
		if (plainText.trim().length === 0) {
			throw new Error('empty');
		}
		return plainText;
	} catch {
		throw new TransientMessageError('Unable to decrypt OAuth token');
	}
}

export async function encryptTokenValue(plainText: string, keyBytes: Uint8Array): Promise<string> {
	const iv = crypto.getRandomValues(new Uint8Array(12));
	const cryptoKey = await importAesKey(keyBytes);
	const cipherBuffer = await crypto.subtle.encrypt(
		{ name: 'AES-GCM', iv },
		cryptoKey,
		new TextEncoder().encode(plainText),
	);
	const cipherBytes = new Uint8Array(cipherBuffer);
	const output = new Uint8Array(iv.length + cipherBytes.length);
	output.set(iv, 0);
	output.set(cipherBytes, iv.length);
	return `${ENCRYPTED_TOKEN_PREFIX}${toBase64Url(output)}`;
}

function mapOauthConnectionRow(row: OAuthConnectionFetchRowRaw): OauthConnectionRow {
	return {
		id: toRequiredString(row.id, 'oauth_connections.id'),
		user_id: toRequiredString(row.user_id, 'oauth_connections.user_id'),
		provider: toRequiredString(row.provider, 'oauth_connections.provider') as OauthConnectionRow['provider'],
		email_address: toRequiredString(row.email_address, 'oauth_connections.email_address'),
		access_token: toNullableString(row.access_token, 'oauth_connections.access_token'),
		refresh_token: toNullableString(row.refresh_token, 'oauth_connections.refresh_token'),
		last_sync_timestamp: toSafeInteger(
			row.last_sync_timestamp,
			'oauth_connections.last_sync_timestamp',
		),
		sync_status: toRequiredString(row.sync_status, 'oauth_connections.sync_status') as OauthConnectionRow['sync_status'],
		created_at: toIsoDateTime(row.created_at, 'oauth_connections.created_at'),
		updated_at: toIsoDateTime(row.updated_at, 'oauth_connections.updated_at'),
	};
}

export async function listEligibleConnectionsForUser(
	sql: SqlClient,
	userId: UUID,
): Promise<OauthConnectionRow[]> {
	const rows = await sql<OAuthConnectionFetchRowRaw[]>`
		select
			oc.id,
			oc.user_id,
			oc.provider,
			oc.email_address,
			oc.access_token,
			oc.refresh_token,
			oc.last_sync_timestamp,
			oc.sync_status,
			oc.created_at,
			oc.updated_at
		from public.oauth_connections as oc
		where oc.user_id = ${userId}
			and oc.provider = ${GOOGLE_OAUTH_PROVIDER}
			and oc.sync_status in ('ACTIVE', 'ERROR_PAUSED')
		order by oc.updated_at desc, oc.created_at desc, oc.id asc
	`;

	return rows.map(mapOauthConnectionRow);
}

async function loadConnectionById(
	sql: SqlClient,
	connectionId: UUID,
	userId: UUID,
): Promise<OauthConnectionRow | null> {
	const rows = await sql<OAuthConnectionFetchRowRaw[]>`
		select
			oc.id,
			oc.user_id,
			oc.provider,
			oc.email_address,
			oc.access_token,
			oc.refresh_token,
			oc.last_sync_timestamp,
			oc.sync_status,
			oc.created_at,
			oc.updated_at
		from public.oauth_connections as oc
		where oc.id = ${connectionId}
			and oc.user_id = ${userId}
		limit 1
	`;

	return rows[0] ? mapOauthConnectionRow(rows[0]) : null;
}

async function markConnectionRevoked(
	sql: SqlClient,
	connectionId: UUID,
	userId: UUID,
	reason: string,
): Promise<void> {
	await sql`
		update public.oauth_connections as oc
		set
			access_token = null,
			refresh_token = null,
			sync_status = 'AUTH_REVOKED'
		where oc.id = ${connectionId}
			and oc.user_id = ${userId}
	`;

	console.warn('Marked oauth connection AUTH_REVOKED during EMAIL_SYNC_USER processing', {
		connectionId,
		userId,
		reason,
	});
}

async function markConnectionErrorPaused(
	sql: SqlClient,
	connectionId: UUID,
	userId: UUID,
	reason: string,
): Promise<void> {
	await sql`
		update public.oauth_connections as oc
		set sync_status = 'ERROR_PAUSED'
		where oc.id = ${connectionId}
			and oc.user_id = ${userId}
			and oc.sync_status in ('ACTIVE', 'ERROR_PAUSED')
	`;

	console.warn('Marked oauth connection ERROR_PAUSED during EMAIL_SYNC_USER processing', {
		connectionId,
		userId,
		reason,
	});
}

function asRecord(value: unknown): Record<string, unknown> | null {
	return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : null;
}

function parseRefreshErrorCode(payload: unknown): string | null {
	const record = asRecord(payload);
	const code = record?.error;
	return typeof code === 'string' && code.length > 0 ? code : null;
}

interface GoogleApiErrorDetails {
	reason: string | null;
	message: string | null;
}

function normalizeGoogleApiErrorText(value: unknown): string | null {
	if (typeof value !== 'string') {
		return null;
	}
	const normalized = value.replace(/\s+/g, ' ').trim();
	if (normalized.length === 0) {
		return null;
	}
	return normalized.length <= 300 ? normalized : `${normalized.slice(0, 297)}...`;
}

function parseGoogleApiErrorDetails(payload: unknown): GoogleApiErrorDetails {
	const record = asRecord(payload) as GoogleApiErrorResponse | null;
	const errors = record?.error?.errors;
	let reason: string | null = null;
	let message: string | null = null;
	if (Array.isArray(errors) && errors.length > 0) {
		reason = normalizeGoogleApiErrorText(errors[0]?.reason);
		message = normalizeGoogleApiErrorText(errors[0]?.message);
	}

	const status = record?.error?.status;
	reason ??= normalizeGoogleApiErrorText(status);
	message ??= normalizeGoogleApiErrorText(record?.error?.message);

	return { reason, message };
}

function parseGoogleApiErrorReason(payload: unknown): string | null {
	return parseGoogleApiErrorDetails(payload).reason;
}

function formatGoogleApiErrorDetails(payload: unknown): string {
	const details = parseGoogleApiErrorDetails(payload);
	return `reason=${details.reason ?? 'unknown_reason'}, message=${details.message ?? 'unknown_message'}`;
}

function createGmailRateLimitError(
	scope: string,
	status: number,
	payload: unknown,
): TransientMessageError {
	return new TransientMessageError(
		`${scope} retryable status ${status} (${formatGoogleApiErrorDetails(payload)})`,
		{
			retryAfterSeconds: GMAIL_RATE_LIMIT_RETRY_SECONDS,
			retryJitterSeconds: GMAIL_RATE_LIMIT_RETRY_JITTER_SECONDS,
		},
	);
}

function classifyGoogle403Reason(reason: string | null): 'retry' | 'pause' | 'unknown' {
	if (!reason) {
		return 'unknown';
	}
	if (GOOGLE_403_RETRYABLE_REASONS.has(reason)) {
		return 'retry';
	}
	if (GOOGLE_403_PAUSE_REASONS.has(reason)) {
		return 'pause';
	}
	return 'unknown';
}

function parseGmailMessageRefs(payload: unknown): string[] {
	if (!Array.isArray(payload)) {
		return [];
	}

	const messageIds: string[] = [];
	for (const entry of payload as GmailListMessageRef[]) {
		if (typeof entry?.id === 'string' && entry.id.length > 0) {
			messageIds.push(entry.id);
		}
	}
	return messageIds;
}

function parseGmailNextPageToken(value: unknown): string | null {
	if (typeof value !== 'string') {
		return null;
	}
	const normalized = value.trim();
	return normalized.length > 0 ? normalized : null;
}

function getJobContinuationStateForConnection(
	job: EmailSyncUserJobPayload,
	connectionId: UUID,
): EmailSyncContinuationState | null {
	if (job.continuation_connection_id !== connectionId) {
		return null;
	}
	if (
		typeof job.continuation_page_token !== 'string' ||
		job.continuation_page_token.length === 0 ||
		typeof job.continuation_after_seconds !== 'number' ||
		!Number.isSafeInteger(job.continuation_after_seconds) ||
		job.continuation_after_seconds < 0
	) {
		return null;
	}

	const maxSeenRaw = job.continuation_max_internal_timestamp_seen;
	const maxSeen =
		typeof maxSeenRaw === 'number' && Number.isSafeInteger(maxSeenRaw) && maxSeenRaw >= 0
			? maxSeenRaw
			: 0;

	return {
		connection_id: connectionId,
		page_token: job.continuation_page_token,
		after_seconds: job.continuation_after_seconds,
		max_internal_timestamp_seen: maxSeen,
		sync_run_id: job.sync_run_id ?? null,
	};
}

async function enqueueEmailSyncContinuation(
	queue: Queue,
	userId: UUID,
	lastSyncTimestamp: number,
	continuation: EmailSyncContinuationState,
): Promise<void> {
	for (let attempt = 1; attempt <= CONTINUATION_ENQUEUE_MAX_ATTEMPTS; attempt += 1) {
		try {
			await queue.send(
				buildEmailSyncUserJob(
					userId,
					lastSyncTimestamp,
					continuation.sync_run_id ?? undefined,
					{
						continuation_connection_id: continuation.connection_id,
						continuation_page_token: continuation.page_token,
						continuation_after_seconds: continuation.after_seconds,
						continuation_max_internal_timestamp_seen:
							continuation.max_internal_timestamp_seen,
					},
				),
				{
					contentType: 'json',
					delaySeconds: GMAIL_CONTINUATION_DELAY_SECONDS,
				},
			);
			return;
		} catch (error) {
			console.warn('Failed to enqueue EMAIL_SYNC_USER continuation', {
				attempt,
				attemptsMax: CONTINUATION_ENQUEUE_MAX_ATTEMPTS,
				userId,
				connectionId: continuation.connection_id,
				error: error instanceof Error ? error.message : 'Unknown enqueue error',
			});
		}
	}

	throw new TransientMessageError('Failed to enqueue EMAIL_SYNC_USER continuation');
}

async function claimEmailSyncLease(
	sql: SqlClient,
	connectionId: UUID,
	userId: UUID,
	syncRunId: UUID,
): Promise<boolean> {
	const rows = await sql<{ id: UUID }[]>`
		update public.oauth_connections as oc
		set
			email_sync_lease_id = ${syncRunId},
			email_sync_lease_expires_at =
				now() + make_interval(mins => ${EMAIL_SYNC_LEASE_MINUTES})
		where oc.id = ${connectionId}
			and oc.user_id = ${userId}
			and oc.sync_status in ('ACTIVE', 'ERROR_PAUSED')
			and (
				oc.email_sync_lease_id = ${syncRunId}
				or oc.email_sync_lease_expires_at is null
				or oc.email_sync_lease_expires_at <= now()
			)
		returning oc.id
	`;
	return rows.length > 0;
}

async function releaseEmailSyncLease(
	sql: SqlClient,
	connectionId: UUID,
	userId: UUID,
	syncRunId: UUID,
): Promise<void> {
	const rows = await sql<{ id: UUID }[]>`
		update public.oauth_connections as oc
		set
			email_sync_lease_id = null,
			email_sync_lease_expires_at = null
		where oc.id = ${connectionId}
			and oc.user_id = ${userId}
			and oc.email_sync_lease_id = ${syncRunId}
		returning id
	`;
	if (rows.length > 0) {
		console.info('EMAIL_SYNC_CONNECTION_LEASE_RELEASED', {
			connectionId,
			userId,
			syncRunId,
		});
	}
}

async function enqueueNormalizeRawEmailsJobs(queue: Queue, rawEmailIds: UUID[]): Promise<void> {
	for (let i = 0; i < rawEmailIds.length; i += NORMALIZE_RAW_EMAILS_MAX_IDS) {
		const batch = rawEmailIds.slice(i, i + NORMALIZE_RAW_EMAILS_MAX_IDS);
		await queue.send(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: batch },
			{ contentType: 'json' },
		);
	}
}

// Gmail refused the page token itself (HTTP 400). Distinct so a token parked
// on the connection by a quota stop can be forgotten instead of wedging the
// mailbox on every retry.
class GmailPageTokenInvalidError extends TransientMessageError {
	constructor(message: string) {
		super(message);
		this.name = 'GmailPageTokenInvalidError';
	}
}

interface StoredSyncContinuation {
	page_token: string;
	after_seconds: number;
	max_internal_timestamp_seen: number;
}

interface StoredSyncContinuationRowRaw {
	email_sync_continuation_token: unknown;
	email_sync_continuation_after_seconds: unknown;
	email_sync_continuation_max_seen: unknown;
}

// A quota stop parks the Gmail page token on the connection (Task 6, A.3 §3)
// so the next day's job resumes mid-listing rather than re-listing from the
// cursor. The after: window travels with it: a token is only valid for the
// exact query that produced it.
async function loadStoredContinuation(
	sql: SqlClient,
	connectionId: UUID,
	userId: UUID,
): Promise<StoredSyncContinuation | null> {
	const rows = await sql<StoredSyncContinuationRowRaw[]>`
		select
			oc.email_sync_continuation_token,
			oc.email_sync_continuation_after_seconds,
			oc.email_sync_continuation_max_seen
		from public.oauth_connections as oc
		where oc.id = ${connectionId}
			and oc.user_id = ${userId}
		limit 1
	`;
	const row = rows[0];
	if (!row || typeof row.email_sync_continuation_token !== 'string' || row.email_sync_continuation_token.length === 0) {
		return null;
	}
	const afterSeconds = Number(row.email_sync_continuation_after_seconds);
	if (!Number.isSafeInteger(afterSeconds) || afterSeconds < 0) {
		return null;
	}
	const maxSeenRaw = Number(row.email_sync_continuation_max_seen);
	return {
		page_token: row.email_sync_continuation_token,
		after_seconds: afterSeconds,
		max_internal_timestamp_seen:
			Number.isSafeInteger(maxSeenRaw) && maxSeenRaw > 0 ? maxSeenRaw : 0,
	};
}

async function storeContinuation(
	sql: SqlClient,
	connectionId: UUID,
	userId: UUID,
	state: StoredSyncContinuation,
): Promise<void> {
	await sql`
		update public.oauth_connections as oc
		set
			email_sync_continuation_token = ${state.page_token},
			email_sync_continuation_after_seconds = ${state.after_seconds},
			email_sync_continuation_max_seen = ${state.max_internal_timestamp_seen}
		where oc.id = ${connectionId}
			and oc.user_id = ${userId}
	`;
}

async function clearStoredContinuation(
	sql: SqlClient,
	connectionId: UUID,
	userId: UUID,
): Promise<void> {
	await sql`
		update public.oauth_connections as oc
		set
			email_sync_continuation_token = null,
			email_sync_continuation_after_seconds = null,
			email_sync_continuation_max_seen = null
		where oc.id = ${connectionId}
			and oc.user_id = ${userId}
			and oc.email_sync_continuation_token is not null
	`;
}

/**
 * Self-heal (Task 6, A.1 §5 / A.2 §8): re-enqueues this user's
 * PENDING_EXTRACTION rows that nothing has touched for half an hour — the
 * ones a lost NORMALIZE_RAW_EMAILS job or yesterday's AI cap left behind.
 * Bounded to one NORMALIZE job per user per hour by an atomic claim, and
 * skipped while the user is over the AI cap (the normalizer would only defer
 * them again).
 */
async function requeueStalePendingEmails(
	sql: SqlClient,
	env: Env,
	userId: UUID,
	quotaDefaults: UserQuotaCaps,
): Promise<number> {
	const quota = await loadUserQuotaStatus(sql, userId, quotaDefaults);
	if (quota.over_ai_call_cap) {
		return 0;
	}
	const claimed = await claimPendingRequeueSlot(sql, userId);
	if (!claimed) {
		return 0;
	}
	const rawEmailIds = await listStalePendingRawEmailIds(
		sql,
		userId,
		PENDING_REQUEUE_STALE_MINUTES,
		NORMALIZE_RAW_EMAILS_MAX_IDS,
	);
	if (rawEmailIds.length === 0) {
		return 0;
	}
	try {
		await enqueueNormalizeRawEmailsJobs(env.EMAIL_SYNC_QUEUE, rawEmailIds);
	} catch (error) {
		// Hand the hour back so the next job retries instead of waiting it out.
		await releasePendingRequeueSlot(sql, userId).catch(() => undefined);
		console.warn('EMAIL_SYNC_PENDING_REQUEUE_FAILED', {
			userId,
			rawEmailCount: rawEmailIds.length,
			error: error instanceof Error ? error.message : 'Unknown enqueue error',
		});
		return 0;
	}
	console.info('EMAIL_SYNC_PENDING_REQUEUED', { userId, rawEmailCount: rawEmailIds.length });
	return rawEmailIds.length;
}

export async function refreshAccessTokenWithOcc(
	sql: SqlClient,
	connection: OauthConnectionRow,
	config: ReturnType<typeof getGoogleRefreshConfig>,
	fetcher: FetchLike,
): Promise<{
	connection: OauthConnectionRow;
	accessToken: string;
	revoked: boolean;
	inactive: boolean;
}> {
	let currentConnection = connection;

	for (let attempt = 1; attempt <= TOKEN_REFRESH_OCC_MAX_ATTEMPTS; attempt += 1) {
		if (currentConnection.sync_status === 'AUTH_REVOKED') {
			return { connection: currentConnection, accessToken: '', revoked: true, inactive: false };
		}
		if (
			currentConnection.sync_status !== 'ACTIVE' &&
			currentConnection.sync_status !== 'ERROR_PAUSED'
		) {
			return { connection: currentConnection, accessToken: '', revoked: false, inactive: true };
		}
		if (!currentConnection.refresh_token) {
			await markConnectionRevoked(
				sql,
				currentConnection.id,
				currentConnection.user_id,
				'refresh_token_missing',
			);
			return { connection: currentConnection, accessToken: '', revoked: true, inactive: false };
		}

		const refreshTokenPlain = await decryptTokenValue(
			currentConnection.refresh_token,
			config.tokenEncryptionKey,
		);
		const requestBody = new URLSearchParams({
			client_id: config.clientId,
			client_secret: config.clientSecret,
			refresh_token: refreshTokenPlain,
			grant_type: 'refresh_token',
		});

		let response: Response;
		try {
			response = await fetcher(GOOGLE_OAUTH_TOKEN_ENDPOINT, {
				method: 'POST',
				headers: {
					'content-type': 'application/x-www-form-urlencoded',
					accept: 'application/json',
				},
				body: requestBody.toString(),
			});
		} catch {
			throw new TransientMessageError('Failed to reach Google OAuth token endpoint');
		}

		let parsedResponse: OAuthRefreshResponse | null = null;
		try {
			parsedResponse = (await response.json()) as OAuthRefreshResponse;
		} catch {
			parsedResponse = null;
		}

		if (!response.ok) {
			const refreshErrorCode = parseRefreshErrorCode(parsedResponse);
			if (response.status === 400 && refreshErrorCode === 'invalid_grant') {
				await markConnectionRevoked(
					sql,
					currentConnection.id,
					currentConnection.user_id,
					'google_invalid_grant',
				);
				return { connection: currentConnection, accessToken: '', revoked: true, inactive: false };
			}

			if (response.status === 429 || response.status >= 500) {
				throw new TransientMessageError(
					`Google OAuth token refresh failed with retryable status ${response.status}`,
				);
			}

			throw new TransientMessageError(
				`Google OAuth token refresh failed with status ${response.status}`,
			);
		}

		const accessToken =
			typeof parsedResponse?.access_token === 'string' ? parsedResponse.access_token.trim() : '';
		if (accessToken.length === 0) {
			throw new TransientMessageError('Google OAuth token refresh missing access_token');
		}
		const responseRefreshTokenRaw =
			typeof parsedResponse?.refresh_token === 'string'
				? parsedResponse.refresh_token.trim()
				: '';
		const nextRefreshToken =
			responseRefreshTokenRaw.length > 0 ? responseRefreshTokenRaw : refreshTokenPlain;

		const encryptedAccessToken = await encryptTokenValue(accessToken, config.tokenEncryptionKey);
		const encryptedRefreshToken = await encryptTokenValue(
			nextRefreshToken,
			config.tokenEncryptionKey,
		);

			const updatedRows = await sql<OAuthConnectionFetchRowRaw[]>`
				update public.oauth_connections as oc
				set
					access_token = ${encryptedAccessToken},
					refresh_token = ${encryptedRefreshToken},
					sync_status = 'ACTIVE'
				where oc.id = ${currentConnection.id}
					and oc.user_id = ${currentConnection.user_id}
					and oc.access_token is not distinct from ${currentConnection.access_token}
					and oc.refresh_token is not distinct from ${currentConnection.refresh_token}
					and oc.sync_status in ('ACTIVE', 'ERROR_PAUSED')
				returning
				oc.id,
				oc.user_id,
				oc.provider,
				oc.email_address,
				oc.access_token,
				oc.refresh_token,
				oc.last_sync_timestamp,
				oc.sync_status,
				oc.created_at,
				oc.updated_at
		`;

		if (updatedRows.length > 0) {
				return {
					connection: mapOauthConnectionRow(updatedRows[0]),
					accessToken,
					revoked: false,
					inactive: false,
				};
			}

		const reloaded = await loadConnectionById(
			sql,
			currentConnection.id,
			currentConnection.user_id,
		);
		if (!reloaded) {
			return { connection: currentConnection, accessToken: '', revoked: true, inactive: false };
		}
		if (reloaded.sync_status === 'AUTH_REVOKED') {
			return { connection: reloaded, accessToken: '', revoked: true, inactive: false };
		}
		if (reloaded.sync_status !== 'ACTIVE' && reloaded.sync_status !== 'ERROR_PAUSED') {
			return { connection: reloaded, accessToken: '', revoked: false, inactive: true };
		}

		currentConnection = reloaded;
	}

	throw new TransientMessageError('OAuth refresh OCC retry budget exhausted');
}

async function gmailListMessages(
	accessToken: string,
	afterSeconds: number,
	pageToken: string | null,
	useSearchFilter: boolean,
	fetcher: FetchLike,
	maxResults = GMAIL_LIST_MAX_RESULTS,
): Promise<{
	messageIds: string[];
	nextPageToken: string | null;
	resultSizeEstimate: number | null;
	revoked: boolean;
	paused: boolean;
}> {
	const url = new URL(GMAIL_API_MESSAGES_LIST_ENDPOINT);
	url.searchParams.set('maxResults', String(Math.min(GMAIL_LIST_MAX_RESULTS, maxResults)));
	url.searchParams.set('includeSpamTrash', 'true');
	if (useSearchFilter) {
		if (afterSeconds > 0) {
			url.searchParams.set('q', `in:anywhere after:${afterSeconds}`);
		} else {
			url.searchParams.set('q', 'in:anywhere');
		}
	}
	if (pageToken) {
		url.searchParams.set('pageToken', pageToken);
	}

	let response: Response;
	try {
		response = await fetcher(url, {
			method: 'GET',
			headers: {
				authorization: `Bearer ${accessToken}`,
				accept: 'application/json',
			},
		});
	} catch {
		throw new TransientMessageError('Failed to reach Gmail list API');
	}

	if (response.status === 401) {
		return {
			messageIds: [],
			nextPageToken: null,
			resultSizeEstimate: null,
			revoked: true,
			paused: false,
		};
	}
	let parsedError: GoogleApiErrorResponse | null = null;
	if (!response.ok) {
		try {
			parsedError = (await response.json()) as GoogleApiErrorResponse;
		} catch {
			parsedError = null;
		}
	}
	if (response.status === 429) {
		throw createGmailRateLimitError('Gmail list API', response.status, parsedError);
	}
	if (response.status >= 500) {
		throw new TransientMessageError(`Gmail list API retryable status ${response.status}`);
	}
	if (response.status === 403) {
		const reason = parseGoogleApiErrorReason(parsedError);
		const classification = classifyGoogle403Reason(reason);
		if (classification === 'retry' || classification === 'unknown') {
			throw new TransientMessageError(
				`Gmail list API retryable 403 (${reason ?? 'unknown_reason'})`,
			);
		}
		return {
			messageIds: [],
			nextPageToken: null,
			resultSizeEstimate: null,
			revoked: false,
			paused: true,
		};
	}
	if (response.status === 400 && pageToken) {
		throw new GmailPageTokenInvalidError('Gmail list API rejected the page token (400)');
	}
	if (!response.ok) {
		throw new TransientMessageError(`Gmail list API failed with status ${response.status}`);
	}

	let parsed: GmailListResponse;
	try {
		parsed = (await response.json()) as GmailListResponse;
	} catch {
		throw new TransientMessageError('Gmail list API returned invalid JSON');
	}

	return {
		messageIds: parseGmailMessageRefs(parsed.messages),
		nextPageToken: parseGmailNextPageToken(parsed.nextPageToken),
		resultSizeEstimate:
			typeof parsed.resultSizeEstimate === 'number' &&
			Number.isSafeInteger(parsed.resultSizeEstimate)
				? parsed.resultSizeEstimate
				: (() => {
						if (typeof parsed.resultSizeEstimate !== 'string') {
							return null;
						}
						const parsedEstimate = Number(parsed.resultSizeEstimate);
						return Number.isSafeInteger(parsedEstimate) ? parsedEstimate : null;
					})(),
		revoked: false,
		paused: false,
	};
}

function decodeBase64UrlToText(value: string): string {
	try {
		const bytes = fromBase64Url(value);
		return new TextDecoder().decode(bytes);
	} catch {
		return '';
	}
}

function stripHtml(value: string): string {
	return value
		.replace(/<script[\s\S]*?<\/script>/gi, ' ')
		.replace(/<style[\s\S]*?<\/style>/gi, ' ')
		.replace(/<[^>]+>/g, ' ')
		.replace(/&nbsp;/gi, ' ')
		.replace(/&amp;/gi, '&')
		.replace(/&lt;/gi, '<')
		.replace(/&gt;/gi, '>');
}

function sanitizeText(value: string): string {
	const withoutControls = value
		.replace(/\u0000/g, ' ')
		.replace(/[\u0001-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, ' ')
		.replace(/\r\n?/g, '\n');

	const normalized = withoutControls
		.replace(/[ \t]+\n/g, '\n')
		.replace(/\n{3,}/g, '\n\n')
		.replace(/[ \t]{2,}/g, ' ')
		.trim();

	return truncateWithoutSplittingSurrogates(normalized, MAX_CLEAN_TEXT_LENGTH);
}

// Truncates by UTF-16 length without leaving a dangling high surrogate at
// the cut. A lone surrogate (an emoji split at the boundary) is invalid
// UTF-8: Postgres rejects it, the whole page's persist transaction aborts,
// the cursor never advances, and the connection wedges on one email forever.
export function truncateWithoutSplittingSurrogates(
	value: string,
	maxLength: number,
): string {
	if (value.length <= maxLength) {
		return value;
	}
	const truncated = value.slice(0, maxLength);
	const lastCode = truncated.charCodeAt(truncated.length - 1);
	return lastCode >= 0xd800 && lastCode <= 0xdbff ? truncated.slice(0, -1) : truncated;
}

function collectMessageBodyText(
	part: GmailMessagePart,
	plainParts: string[],
	htmlParts: string[],
): void {
	const mimeType = typeof part.mimeType === 'string' ? part.mimeType.toLowerCase() : '';
	const bodyRecord = asRecord(part.body) as GmailMessagePartBody | null;
	const data = typeof bodyRecord?.data === 'string' ? bodyRecord.data : null;

	if (data) {
		const decoded = decodeBase64UrlToText(data);
		if (mimeType.includes('text/plain')) {
			plainParts.push(decoded);
		} else if (mimeType.includes('text/html')) {
			htmlParts.push(decoded);
		}
	}

	if (Array.isArray(part.parts)) {
		for (const child of part.parts as GmailMessagePart[]) {
			const childRecord = asRecord(child);
			if (!childRecord) {
				continue;
			}
			collectMessageBodyText(childRecord as GmailMessagePart, plainParts, htmlParts);
		}
	}
}

function extractMessageHeader(
	message: GmailMessageResponse,
	headerName: string,
): string | null {
	const payloadRecord = asRecord(message.payload);
	const headers = payloadRecord?.headers;
	if (!Array.isArray(headers)) {
		return null;
	}

	const target = headerName.toLowerCase();
	for (const entry of headers) {
		const record = asRecord(entry);
		const name = typeof record?.name === 'string' ? record.name.toLowerCase() : null;
		const value = typeof record?.value === 'string' ? record.value.trim() : null;
		if (name === target && value && value.length > 0) {
			return value;
		}
	}
	return null;
}

function buildSanitizedEmailText(message: GmailMessageResponse): string {
	const plainParts: string[] = [];
	const htmlParts: string[] = [];

	const payloadRecord = asRecord(message.payload);
	if (payloadRecord) {
		collectMessageBodyText(payloadRecord as GmailMessagePart, plainParts, htmlParts);
	}

	const joinedPlain = plainParts.join('\n');
	if (joinedPlain.trim().length > 0) {
		return sanitizeText(joinedPlain);
	}

	const joinedHtml = htmlParts.join('\n');
	if (joinedHtml.trim().length > 0) {
		return sanitizeText(stripHtml(joinedHtml));
	}

	if (typeof message.snippet === 'string') {
		return sanitizeText(message.snippet);
	}

	return '';
}

// Fetches one attachment's bytes (base64url in the response). Same error
// classification as message GET.
export async function gmailGetAttachment(
	accessToken: string,
	messageId: string,
	attachmentId: string,
	fetcher: FetchLike,
): Promise<{ revoked: boolean; paused: boolean; data: string | null }> {
	const url = new URL(
		`${GMAIL_API_MESSAGES_GET_ENDPOINT}/${encodeURIComponent(messageId)}/attachments/${encodeURIComponent(attachmentId)}`,
	);

	let response: Response;
	try {
		response = await fetcher(url, {
			method: 'GET',
			headers: {
				authorization: `Bearer ${accessToken}`,
				accept: 'application/json',
			},
		});
	} catch {
		throw new TransientMessageError('Failed to reach Gmail attachment API');
	}

	if (response.status === 401) {
		return { revoked: true, paused: false, data: null };
	}
	if (response.status === 404) {
		return { revoked: false, paused: false, data: null };
	}
	if (response.status === 429) {
		let parsedError: GoogleApiErrorResponse | null = null;
		try {
			parsedError = (await response.json()) as GoogleApiErrorResponse;
		} catch {
			parsedError = null;
		}
		throw createGmailRateLimitError('Gmail attachment API', response.status, parsedError);
	}
	if (response.status >= 500) {
		throw new TransientMessageError(
			`Gmail attachment API retryable status ${response.status}`,
		);
	}
	if (response.status === 403) {
		return { revoked: false, paused: true, data: null };
	}
	if (!response.ok) {
		throw new TransientMessageError(
			`Gmail attachment API failed with status ${response.status}`,
		);
	}

	try {
		const parsed = (await response.json()) as { data?: string };
		return { revoked: false, paused: false, data: parsed.data ?? null };
	} catch {
		throw new TransientMessageError('Gmail attachment API returned invalid JSON');
	}
}

export async function gmailGetMessage(
	accessToken: string,
	messageId: string,
	fetcher: FetchLike,
	format: 'metadata' | 'full',
): Promise<{ revoked: boolean; paused: boolean; message: GmailMessageResponse | null }> {
	const url = new URL(`${GMAIL_API_MESSAGES_GET_ENDPOINT}/${encodeURIComponent(messageId)}`);
	url.searchParams.set('format', format);
	if (format === 'metadata') {
		url.searchParams.append('metadataHeaders', 'From');
		url.searchParams.append('metadataHeaders', 'Subject');
	}

	let response: Response;
	try {
		response = await fetcher(url, {
			method: 'GET',
			headers: {
				authorization: `Bearer ${accessToken}`,
				accept: 'application/json',
			},
		});
	} catch {
		throw new TransientMessageError('Failed to reach Gmail message API');
	}

	if (response.status === 401) {
		return { revoked: true, paused: false, message: null };
	}
	if (response.status === 404) {
		return { revoked: false, paused: false, message: null };
	}
	let parsedError: GoogleApiErrorResponse | null = null;
	if (!response.ok) {
		try {
			parsedError = (await response.json()) as GoogleApiErrorResponse;
		} catch {
			parsedError = null;
		}
	}
	if (response.status === 429) {
		throw createGmailRateLimitError('Gmail message API', response.status, parsedError);
	}
	if (response.status >= 500) {
		throw new TransientMessageError(`Gmail message API retryable status ${response.status}`);
	}
	if (response.status === 403) {
		const reason = parseGoogleApiErrorReason(parsedError);
		const classification = classifyGoogle403Reason(reason);
		if (classification === 'retry' || classification === 'unknown') {
			throw new TransientMessageError(
				`Gmail message API retryable 403 (${reason ?? 'unknown_reason'})`,
			);
		}
		return { revoked: false, paused: true, message: null };
	}
	if (!response.ok) {
		throw new TransientMessageError(`Gmail message API failed with status ${response.status}`);
	}

	try {
		return {
			revoked: false,
			paused: false,
			message: (await response.json()) as GmailMessageResponse,
		};
	} catch {
		throw new TransientMessageError('Gmail message API returned invalid JSON');
	}
}

interface GmailBatchGetMessagesResult {
	revoked: boolean;
	paused: boolean;
	messages: Map<string, GmailMessageResponse | null>;
}

function buildGmailMessageBatchPath(
	messageId: string,
	format: 'metadata' | 'full',
): string {
	const params = new URLSearchParams({ format });
	if (format === 'metadata') {
		params.append('metadataHeaders', 'From');
		params.append('metadataHeaders', 'Subject');
	}
	return `/gmail/v1/users/me/messages/${encodeURIComponent(messageId)}?${params.toString()}`;
}

function toBatchErrorMessage(error: unknown): string {
	if (error instanceof Error) {
		return error.message;
	}
	return typeof error === 'string' ? error : 'Unknown error';
}

async function gmailBatchGetMessages(
	accessToken: string,
	messageIds: string[],
	fetcher: FetchLike,
	format: 'metadata' | 'full',
): Promise<GmailBatchGetMessagesResult> {
	if (messageIds.length === 0) {
		return {
			revoked: false,
			paused: false,
			messages: new Map(),
		};
	}

	let batchResponse;
	try {
		batchResponse = await executeGmailBatch(
			accessToken,
			messageIds.map(messageId => ({
				id: messageId,
				path: buildGmailMessageBatchPath(messageId, format),
			})),
			fetcher,
		);
	} catch (error) {
		const prefix =
			error instanceof GmailBatchProtocolError
				? 'Gmail message batch protocol failure'
				: 'Failed to reach Gmail message batch API';
		throw new TransientMessageError(`${prefix}: ${toBatchErrorMessage(error)}`);
	}

	if (batchResponse.status === 401) {
		return { revoked: true, paused: false, messages: new Map() };
	}
	if (batchResponse.status === 429) {
		throw createGmailRateLimitError(
			'Gmail message batch API',
			batchResponse.status,
			batchResponse.body,
		);
	}
	if (batchResponse.status >= 500) {
		throw new TransientMessageError(
			`Gmail message batch API retryable status ${batchResponse.status}`,
		);
	}
	if (batchResponse.status === 403) {
		const reason = parseGoogleApiErrorReason(batchResponse.body);
		const classification = classifyGoogle403Reason(reason);
		if (classification === 'pause') {
			return { revoked: false, paused: true, messages: new Map() };
		}
		throw new TransientMessageError(
			`Gmail message batch API retryable 403 (${reason ?? 'unknown_reason'})`,
		);
	}
	if (batchResponse.status < 200 || batchResponse.status >= 300) {
		throw new TransientMessageError(
			`Gmail message batch API failed with status ${batchResponse.status}`,
		);
	}

	const messages = new Map<string, GmailMessageResponse | null>();
	for (const part of batchResponse.parts) {
		if (part.status === 401) {
			return { revoked: true, paused: false, messages: new Map() };
		}
		if (part.status === 404) {
			messages.set(part.id, null);
			continue;
		}
		if (part.status === 429) {
			throw createGmailRateLimitError(
				`Gmail message batch part for ${part.id}`,
				part.status,
				part.body,
			);
		}
		if (part.status >= 500) {
			throw new TransientMessageError(
				`Gmail message batch part retryable status ${part.status} for ${part.id}`,
			);
		}
		if (part.status === 403) {
			const reason = parseGoogleApiErrorReason(part.body);
			const classification = classifyGoogle403Reason(reason);
			if (classification === 'pause') {
				return { revoked: false, paused: true, messages: new Map() };
			}
			throw new TransientMessageError(
				`Gmail message batch part retryable 403 (${reason ?? 'unknown_reason'}) for ${part.id}`,
			);
		}
		if (part.status < 200 || part.status >= 300) {
			throw new TransientMessageError(
				`Gmail message batch part failed with status ${part.status} for ${part.id}`,
			);
		}

		const message = asRecord(part.body);
		if (!message) {
			throw new TransientMessageError(
				`Gmail message batch part returned invalid JSON for ${part.id}`,
			);
		}
		messages.set(part.id, message as GmailMessageResponse);
	}

	for (const messageId of messageIds) {
		if (!messages.has(messageId)) {
			throw new TransientMessageError(
				`Gmail message batch response omitted ${messageId}`,
			);
		}
	}

	return { revoked: false, paused: false, messages };
}

async function listExistingRawEmailStatuses(
	sql: SqlClient,
	userId: UUID,
	sourceIds: string[],
): Promise<Map<string, ExistingRawEmailStatus>> {
	if (sourceIds.length === 0) {
		return new Map<string, ExistingRawEmailStatus>();
	}

	const rows = await sql<ExistingRawEmailRowRaw[]>`
		select
			re.id,
			re.source_id,
			re.status,
			re.internal_date
		from public.raw_emails as re
		where re.user_id = ${userId}
			and re.source_id = any(${sourceIds}::text[])
	`;

	const map = new Map<string, ExistingRawEmailStatus>();
	for (const row of rows) {
		const sourceId = toRequiredString(row.source_id, 'raw_emails.source_id');
		const status = toRequiredString(row.status, 'raw_emails.status') as RawEmailStatus;
		const id =
			typeof row.id === 'string' && row.id.length > 0
				? (row.id as UUID)
				: null;
		if (status === 'PENDING_EXTRACTION' && !id) {
			throw new TransientMessageError(
				`Existing PENDING raw email ${sourceId} is missing its id`,
			);
		}
		const internalDateIso = toIsoDateTime(row.internal_date, 'raw_emails.internal_date');
		const internalDateTimestamp = Date.parse(internalDateIso);
		map.set(sourceId, {
			id,
			status,
			internal_date_timestamp: Number.isFinite(internalDateTimestamp)
				? internalDateTimestamp
				: 0,
		});
	}
	return map;
}

async function persistRawEmailsAndCursor(
	sql: SqlClient,
	connectionId: UUID,
	userId: UUID,
	records: RawEmailUpsertRecord[],
	maxInternalTimestamp: number,
	advanceCursor: boolean,
	usage?: UserUsageIncrement,
): Promise<UUID[]> {
	const insertedIds: UUID[] = [];
	await sql.begin(async (tx) => {
		const txSql = tx as unknown as SqlClient;
		if (usage) await recordUserUsage(txSql, userId, usage);

		for (const record of records) {
			const result = await txSql<{ id: UUID }[]>`
				insert into public.raw_emails (
					user_id,
					oauth_connection_id,
					source_id,
					internal_date,
					clean_text,
					from_address,
					subject,
					status
				)
				values (
					${userId},
					${connectionId},
					${record.source_id},
					${record.internal_date_iso},
					${record.clean_text},
					${record.from_address},
					${record.subject},
					${record.status}
				)
				on conflict (user_id, source_id) do update
				set
					oauth_connection_id = excluded.oauth_connection_id,
					internal_date = excluded.internal_date,
					clean_text = excluded.clean_text,
					from_address = excluded.from_address,
					subject = excluded.subject,
					status = excluded.status,
					processing_route = null,
					ai_call_id = null,
					processing_completed_at = null
				where raw_emails.status = 'FAILED'
				returning id
			`;

			// Sender-gated rows are persisted for dedupe/auditing but must
			// never be handed to the normalizers.
			if (result.length > 0 && record.status === 'PENDING_EXTRACTION') {
				insertedIds.push(result[0].id);
			}
		}

		if (advanceCursor) {
			await txSql`
				update public.oauth_connections as oc
				set
					last_sync_timestamp = greatest(oc.last_sync_timestamp, ${maxInternalTimestamp}),
					sync_status = case
						when oc.sync_status = 'ERROR_PAUSED' then 'ACTIVE'
						else oc.sync_status
					end
				where oc.id = ${connectionId}
					and oc.user_id = ${userId}
					and oc.sync_status in ('ACTIVE', 'ERROR_PAUSED')
			`;
		}
	});

	return insertedIds;
}

function parseMessageInternalTimestamp(value: unknown): number | null {
	const timestamp =
		typeof value === 'number'
			? value
			: typeof value === 'string'
				? Number(value)
				: Number.NaN;
	if (!Number.isSafeInteger(timestamp) || timestamp < 0) {
		return null;
	}
	return timestamp;
}

function emptyConnectionResult(
	connection: Pick<OauthConnectionRow, 'id' | 'email_address'>,
	flags: Pick<EmailSyncConnectionResult, 'revoked' | 'paused' | 'quota_deferred'>,
): EmailSyncConnectionResult {
	return {
		connection_id: connection.id,
		email_address: connection.email_address,
		fetched_message_count: 0,
		inserted_or_retried_raw_email_count: 0,
		skipped_existing_raw_email_count: 0,
		sender_gated_raw_email_count: 0,
		continuation_enqueued: false,
		...flags,
	};
}

async function processConnection(
	sql: SqlClient,
	job: EmailSyncUserJobPayload,
	connection: OauthConnectionRow,
	env: Env,
	fetcher: FetchLike,
	quotaDefaults: UserQuotaCaps = resolveDefaultQuotaCaps({}),
): Promise<EmailSyncConnectionResult> {
	const jobContinuation = getJobContinuationStateForConnection(job, connection.id);

	// Fair use (Task 6): checked before every page and before any Google
	// call. Over the cap the cursor stays put; a page token this job carried
	// is parked on the connection so tomorrow resumes exactly here (A.3 §3).
	const quota = await loadUserQuotaStatus(sql, job.user_id, quotaDefaults);
	if (quota.over_email_fetch_cap) {
		if (jobContinuation) {
			await storeContinuation(sql, connection.id, job.user_id, {
				page_token: jobContinuation.page_token,
				after_seconds: jobContinuation.after_seconds,
				max_internal_timestamp_seen: jobContinuation.max_internal_timestamp_seen,
			});
		}
		console.warn('EMAIL_SYNC_QUOTA_DEFERRED', {
			connectionId: connection.id,
			userId: job.user_id,
			day: quota.day,
			bodiesFetched: quota.bodies_fetched,
			dailyEmailFetchCap: quota.daily_email_fetch_cap,
			pageTokenStored: Boolean(jobContinuation),
		});
		return emptyConnectionResult(connection, { revoked: false, paused: false, quota_deferred: true });
	}
	// A fresh job resumes a listing an earlier quota stop parked, if any.
	const storedContinuation = jobContinuation
		? null
		: await loadStoredContinuation(sql, connection.id, job.user_id);
	const resume = jobContinuation ?? storedContinuation;

	const config = getGoogleRefreshConfig(env);
	const refreshOutcome = await refreshAccessTokenWithOcc(sql, connection, config, fetcher);
	if (refreshOutcome.revoked) {
		return emptyConnectionResult(connection, { revoked: true, paused: false, quota_deferred: false });
	}
	if (refreshOutcome.inactive) {
		return emptyConnectionResult(refreshOutcome.connection, {
			revoked: false,
			paused: false,
			quota_deferred: false,
		});
	}

	const accessToken = refreshOutcome.accessToken;
	const senderGate = resolveSenderGateConfig(env);
	let senderGateEntries: readonly string[] = senderGate.entries;
	if (senderGate.enabled) {
		// sender_rules is the exhaustive allowlist. Without it the gate would
		// silently drop every bank email's body, so a load failure must retry
		// the sync rather than proceed.
		try {
			const dbEntries = await loadSenderRuleEntries(sql);
			senderGateEntries = [...senderGate.entries, ...dbEntries];
		} catch (error) {
			throw new TransientMessageError(
				`Failed to load sender_rules allowlist: ${
					error instanceof Error ? error.message : 'Unknown error'
				}`,
			);
		}
	}
	let senderGatedCount = 0;
	const baseCursor = Math.min(
		Math.max(0, job.last_sync_timestamp),
		Math.max(0, refreshOutcome.connection.last_sync_timestamp),
	);
	// A zero cursor means this connection has never synced; cap the initial
	// backfill instead of walking the whole mailbox history.
	const effectiveCursor =
		baseCursor > 0
			? baseCursor
			: Date.now() - resolveInitialLookbackDays(env) * DAY_MS;
	const afterSeconds =
		resume?.after_seconds ??
		Math.max(0, Math.floor((effectiveCursor - GMAIL_QUERY_OVERLAP_MS) / 1000));

	let fetchedMessageCount = 0;
	let bodiesFetchedCount = 0;
	let insertedOrRetriedCount = 0;
	let skippedExistingCount = 0;
	const pageToken = resume?.page_token ?? null;
	let maxInternalTimestampSeen = Math.max(
		baseCursor,
		resume?.max_internal_timestamp_seen ?? 0,
	);
	const searchFilterApplied = afterSeconds > 0;

	const buildInterruptedResult = (
		revoked: boolean,
		paused: boolean,
	): EmailSyncConnectionResult => ({
		connection_id: connection.id,
		email_address: connection.email_address,
		fetched_message_count: fetchedMessageCount,
		inserted_or_retried_raw_email_count: insertedOrRetriedCount,
		skipped_existing_raw_email_count: skippedExistingCount,
		sender_gated_raw_email_count: senderGatedCount,
		revoked,
		paused,
		continuation_enqueued: false,
		quota_deferred: false,
	});

	let listResult: Awaited<ReturnType<typeof gmailListMessages>>;
	try {
		listResult = await gmailListMessages(
			accessToken,
			afterSeconds,
			pageToken,
			searchFilterApplied,
			fetcher,
			quota.email_fetch_remaining,
		);
	} catch (error) {
		if (storedContinuation && error instanceof GmailPageTokenInvalidError) {
			// A parked token Gmail no longer accepts must not wedge the mailbox:
			// forget it, and the retry lists from the cursor instead.
			await clearStoredContinuation(sql, connection.id, job.user_id);
			console.warn('EMAIL_SYNC_STORED_CONTINUATION_INVALID', {
				connectionId: connection.id,
				userId: job.user_id,
			});
		}
		throw error;
	}
	console.info('EMAIL_SYNC_GMAIL_LIST_PAGE', {
		connectionId: connection.id,
		userId: connection.user_id,
		afterSeconds,
		pageTokenPresent: Boolean(pageToken),
		messageIdCount: listResult.messageIds.length,
		nextPageTokenPresent: Boolean(listResult.nextPageToken),
		resultSizeEstimate: listResult.resultSizeEstimate,
		searchFilterApplied,
	});
	if (listResult.messageIds.length === 0 && !listResult.nextPageToken) {
		console.warn('EMAIL_SYNC_GMAIL_EMPTY_RESULT', {
			connectionId: connection.id,
			userId: connection.user_id,
			afterSeconds,
		});
	}
	if (listResult.revoked) {
		await markConnectionRevoked(sql, connection.id, connection.user_id, 'gmail_list_unauthorized');
		return buildInterruptedResult(true, false);
	}
	if (listResult.paused) {
		await markConnectionErrorPaused(
			sql,
			connection.id,
			connection.user_id,
			'gmail_list_permission_denied',
		);
		return buildInterruptedResult(false, true);
	}

	const existingStatusMap = await listExistingRawEmailStatuses(
		sql,
		job.user_id,
		listResult.messageIds,
	);
	const idsToFetch: string[] = [];
	const normalizationIds = new Set<UUID>();
	for (const messageId of listResult.messageIds) {
		const existing = existingStatusMap.get(messageId);
		if (existing && existing.status !== 'FAILED') {
			skippedExistingCount += 1;
			maxInternalTimestampSeen = Math.max(
				maxInternalTimestampSeen,
				existing.internal_date_timestamp,
			);
			if (existing.status === 'PENDING_EXTRACTION' && existing.id) {
				// Re-enqueueing is intentional. It repairs the case where a prior
				// attempt committed raw rows but failed before sending normalize jobs.
				normalizationIds.add(existing.id);
			}
			continue;
		}

		idsToFetch.push(messageId);
	}

	// One multipart request returns headers for the whole page. We deliberately
	// fetch bodies only after the sender gate passes, preserving the privacy
	// property that denied email bodies never leave Gmail.
	const metadataBatch = await gmailBatchGetMessages(
		accessToken,
		idsToFetch,
		fetcher,
		'metadata',
	);
	if (metadataBatch.revoked) {
		await markConnectionRevoked(sql, connection.id, connection.user_id, 'gmail_unauthorized');
		return buildInterruptedResult(true, false);
	}
	if (metadataBatch.paused) {
		await markConnectionErrorPaused(
			sql,
			connection.id,
			connection.user_id,
			'gmail_get_permission_denied',
		);
		return buildInterruptedResult(false, true);
	}

	const recordsToPersist: RawEmailUpsertRecord[] = [];
	const allowedMessageState = new Map<
		string,
		{
			sourceId: string;
			internalDateIso: string;
			fromAddress: string | null;
			subject: string | null;
		}
	>();
	for (const messageId of idsToFetch) {
		const metaMessage = metadataBatch.messages.get(messageId);
		if (!metaMessage) {
			continue;
		}
		fetchedMessageCount += 1;

		const sourceId = typeof metaMessage.id === 'string' ? metaMessage.id : messageId;
		const internalTimestamp = parseMessageInternalTimestamp(metaMessage.internalDate);
		if (internalTimestamp === null) {
			continue;
		}

		maxInternalTimestampSeen = Math.max(maxInternalTimestampSeen, internalTimestamp);

		const fromHeader = extractMessageHeader(metaMessage, 'From');
		const subjectHeader = extractMessageHeader(metaMessage, 'Subject');
		const fromAddress = parseFromAddress(fromHeader);
		const senderAllowed =
			!senderGate.enabled || isAllowedSender(fromHeader, fromAddress, senderGateEntries);
		const internalDateIso = new Date(internalTimestamp).toISOString();
		const subject = subjectHeader
			? truncateWithoutSplittingSurrogates(subjectHeader, MAX_SUBJECT_LENGTH)
			: null;

		if (!senderAllowed) {
			senderGatedCount += 1;
			recordsToPersist.push({
				source_id: sourceId,
				internal_date_iso: internalDateIso,
				clean_text: '',
				from_address: fromAddress,
				subject,
				status: 'IGNORED',
			});
			continue;
		}

		allowedMessageState.set(messageId, {
			sourceId,
			internalDateIso,
			fromAddress,
			subject,
		});
	}

	const fullBatch = await gmailBatchGetMessages(
		accessToken,
		[...allowedMessageState.keys()],
		fetcher,
		'full',
	);
	if (fullBatch.revoked) {
		await markConnectionRevoked(sql, connection.id, connection.user_id, 'gmail_unauthorized');
		return buildInterruptedResult(true, false);
	}
	if (fullBatch.paused) {
		await markConnectionErrorPaused(
			sql,
			connection.id,
			connection.user_id,
			'gmail_get_permission_denied',
		);
		return buildInterruptedResult(false, true);
	}

	for (const [messageId, messageState] of allowedMessageState) {
		const fullMessage = fullBatch.messages.get(messageId);
		if (!fullMessage) {
			continue;
		}
		bodiesFetchedCount += 1;
		recordsToPersist.push({
			source_id: messageState.sourceId,
			internal_date_iso: messageState.internalDateIso,
			clean_text: buildSanitizedEmailText(fullMessage),
			from_address: messageState.fromAddress,
			subject: messageState.subject,
			status: 'PENDING_EXTRACTION',
		});
	}

	{
		const insertedIds = await persistRawEmailsAndCursor(
			sql,
			connection.id,
			job.user_id,
			recordsToPersist,
			maxInternalTimestampSeen,
			false,
			{ messages_listed: listResult.messageIds.length, bodies_fetched: bodiesFetchedCount },
		);
		for (const insertedId of insertedIds) {
			normalizationIds.add(insertedId);
		}
		insertedOrRetriedCount += recordsToPersist.length;
	}

	// Queue all downstream work before moving either pagination or timestamp
	// pointers. If this send fails, the queue retries the same page; existing
	// PENDING rows above are deliberately re-enqueued on that retry.
	if (normalizationIds.size > 0) {
		await enqueueNormalizeRawEmailsJobs(env.EMAIL_SYNC_QUEUE, [...normalizationIds]);
	}

	const quotaDeferred = listResult.nextPageToken !== null && Math.max(bodiesFetchedCount, listResult.messageIds.length) >= quota.email_fetch_remaining;
	const continuationEnqueued = listResult.nextPageToken !== null && !quotaDeferred;
	if (listResult.nextPageToken && quotaDeferred) {
		await storeContinuation(sql, connection.id, job.user_id, {
			page_token: listResult.nextPageToken,
			after_seconds: afterSeconds,
			max_internal_timestamp_seen: maxInternalTimestampSeen,
		});
		console.warn('EMAIL_SYNC_QUOTA_DEFERRED', { userId: job.user_id, connectionId: connection.id, day: quota.day });
	} else if (listResult.nextPageToken) {
		await enqueueEmailSyncContinuation(
			env.EMAIL_SYNC_QUEUE,
			job.user_id,
			baseCursor,
			{
				connection_id: connection.id,
				page_token: listResult.nextPageToken,
				after_seconds: afterSeconds,
				max_internal_timestamp_seen: maxInternalTimestampSeen,
				sync_run_id: job.sync_run_id ?? null,
			},
		);
		console.info('EMAIL_SYNC_GMAIL_PAGE_COMMITTED', {
			connectionId: connection.id,
			userId: job.user_id,
			messageIdCount: listResult.messageIds.length,
			nextPageTokenPresent: true,
		});
	} else {
		// The durable timestamp watermark moves only after the final page has
		// been fetched, stored, and handed to downstream normalization.
		await persistRawEmailsAndCursor(
			sql,
			connection.id,
			job.user_id,
			[],
			maxInternalTimestampSeen,
			true,
		);
	}

	// The parked token has been consumed: the chain now carries its own
	// continuation (or finished). A later quota stop parks a fresh one.
	if (storedContinuation && !quotaDeferred) {
		await clearStoredContinuation(sql, connection.id, job.user_id);
	}

	return {
		connection_id: connection.id,
		email_address: connection.email_address,
		fetched_message_count: fetchedMessageCount,
		inserted_or_retried_raw_email_count: insertedOrRetriedCount,
		skipped_existing_raw_email_count: skippedExistingCount,
		sender_gated_raw_email_count: senderGatedCount,
		revoked: false,
		paused: false,
		continuation_enqueued: continuationEnqueued,
		quota_deferred: quotaDeferred,
	};
}

interface BackfillRawEmailRowRaw {
	id: unknown;
	source_id: unknown;
	oauth_connection_id: unknown;
}

export interface SenderBackfillResult {
	user_id: UUID;
	requested_count: number;
	refetched_count: number;
	skipped_count: number;
	normalize_enqueued: boolean;
}

// Refetches full bodies for metadata-only gated raw emails whose sender was
// later allowlisted (by the daily AI classification job or manually), then
// re-enters them into the extraction pipeline.
export async function runSenderBackfillJob(
	job: SenderBackfillJobPayload,
	env: Env,
	fetcher: FetchLike = fetch,
): Promise<SenderBackfillResult> {
	try {
		return await runSenderBackfillJobUnguarded(job, env, fetcher);
	} catch (error) {
		if (isMissingUserReferenceError(error)) {
			throw new PoisonMessageError(`SENDER_BACKFILL user ${job.user_id} no longer exists`);
		}
		throw error;
	}
}

async function runSenderBackfillJobUnguarded(
	job: SenderBackfillJobPayload,
	env: Env,
	fetcher: FetchLike,
): Promise<SenderBackfillResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);

	const result: SenderBackfillResult = {
		user_id: job.user_id,
		requested_count: job.raw_email_ids.length,
		refetched_count: 0,
		skipped_count: 0,
		normalize_enqueued: false,
	};

	const rows = await sql<BackfillRawEmailRowRaw[]>`
		select re.id, re.source_id, re.oauth_connection_id
		from public.raw_emails as re
		where re.id = any(${job.raw_email_ids}::uuid[])
			and re.user_id = ${job.user_id}
			and re.status = 'IGNORED'
			and re.clean_text = ''
			and re.body_purged_at is null
	`;
	if (rows.length === 0) {
		result.skipped_count = job.raw_email_ids.length;
		return result;
	}

	const googleConfig = getGoogleRefreshConfig(env);
	const connections = await listEligibleConnectionsForUser(sql, job.user_id);
	if (connections.length === 0) {
		result.skipped_count = job.raw_email_ids.length;
		return result;
	}

	// Each row is refetched ONLY through its own original connection: Gmail
	// message ids are per-mailbox, so another connection would 404 (or worse,
	// fetch an unrelated user email). Rows whose connection is gone or
	// currently ineligible are skipped — they stay IGNORED with empty
	// clean_text, so a later run can still promote them if the connection
	// comes back.
	const connectionById = new Map<UUID, OauthConnectionRow>();
	for (const connection of connections) {
		connectionById.set(connection.id, connection);
	}
	const accessTokenByConnectionId = new Map<UUID, string>();
	const updatedRawEmailIds: UUID[] = [];
	let unresolvableConnectionCount = 0;
	const quotaDefaults = resolveDefaultQuotaCaps(config);
	let remainingBudget = Number.POSITIVE_INFINITY;

	for (const [index, row] of rows.entries()) {
		const quota = await loadUserQuotaStatus(sql, job.user_id, quotaDefaults);
		remainingBudget = Math.min(remainingBudget, quota.email_fetch_remaining);
		if (remainingBudget <= 0) {
			// The rows remain gated until tomorrow. A delayed job preserves this
			// backlog even when the sender will never be classified again.
			const deferredIds = rows.slice(index).map(pending => toRequiredString(pending.id, 'raw_emails.id'));
			const nextDay = Date.parse(`${quota.day}T00:00:00+05:30`) + DAY_MS;
			await env.EMAIL_SYNC_QUEUE.send(
				{ ...job, raw_email_ids: deferredIds },
				{ contentType: 'json', delaySeconds: Math.max(1, Math.min(86400, Math.ceil((nextDay - Date.now()) / 1000))) },
			);
			result.skipped_count += deferredIds.length;
			console.warn('SENDER_BACKFILL_QUOTA_DEFERRED', { userId: job.user_id, deferredCount: deferredIds.length, day: quota.day });
			break;
		}
		const rawEmailId = toRequiredString(row.id, 'raw_emails.id');
		const sourceId = toRequiredString(row.source_id, 'raw_emails.source_id');
		const connectionId = toNullableString(
			row.oauth_connection_id,
			'raw_emails.oauth_connection_id',
		);
		const connection = connectionId ? connectionById.get(connectionId) : undefined;
		if (!connection) {
			unresolvableConnectionCount += 1;
			result.skipped_count += 1;
			continue;
		}

		let accessToken = accessTokenByConnectionId.get(connection.id);
		if (!accessToken) {
			const refreshOutcome = await refreshAccessTokenWithOcc(
				sql,
				connection,
				googleConfig,
				fetcher,
			);
			if (refreshOutcome.revoked || refreshOutcome.inactive) {
				result.skipped_count += 1;
				continue;
			}
			accessToken = refreshOutcome.accessToken;
			accessTokenByConnectionId.set(connection.id, accessToken);
		}

		const getResult = await gmailGetMessage(accessToken, sourceId, fetcher, 'full');
		if (getResult.revoked || getResult.paused || !getResult.message) {
			result.skipped_count += 1;
			continue;
		}

		const cleanText = buildSanitizedEmailText(getResult.message);
		remainingBudget -= 1;
		const updated = await sql.begin(async tx => {
			const txSql = tx as unknown as SqlClient;
			await recordUserUsage(txSql, job.user_id, { messages_listed: 0, bodies_fetched: 1 });
			return txSql<{ id: UUID }[]>`
			update public.raw_emails as re
			set
				clean_text = ${cleanText},
				status = 'PENDING_EXTRACTION'
			where re.id = ${rawEmailId}
				and re.user_id = ${job.user_id}
				and re.status = 'IGNORED'
				and re.clean_text = ''
				and re.body_purged_at is null
			returning re.id
			`;
		}) as unknown as { id: UUID }[];
		if (updated.length > 0) {
			result.refetched_count += 1;
			updatedRawEmailIds.push(updated[0].id);
		} else {
			result.skipped_count += 1;
		}
	}

	result.skipped_count += rows.length < job.raw_email_ids.length
		? job.raw_email_ids.length - rows.length
		: 0;

	if (unresolvableConnectionCount > 0) {
		// Structural, not benign: these rows cannot be promoted until their
		// original connection is restored (or the row is re-pointed manually).
		console.warn('SENDER_BACKFILL_CONNECTION_UNRESOLVABLE', {
			userId: job.user_id,
			rowCount: unresolvableConnectionCount,
		});
	}

	if (updatedRawEmailIds.length > 0) {
		await enqueueNormalizeRawEmailsJobs(env.EMAIL_SYNC_QUEUE, updatedRawEmailIds);
		result.normalize_enqueued = true;
	}

	return result;
}

// ── Internal ops: sender-metadata backfill + sync probe ─────────
// Dev/ops tooling behind /internal/* (secret-guarded). Neither touches the
// sync watermark; both are safe to run repeatedly.

export interface SenderMetadataBackfillResult {
	scanned_count: number;
	updated_count: number;
	skipped_count: number;
	/** Rows still missing from_address after this run (0 = done). */
	remaining_count: number;
	skip_reasons: {
		no_connection_id: number;
		connection_missing: number;
		token_refresh_failed: number;
		message_gone: number;
		update_no_match: number;
	};
}

interface MetadataBackfillRowRaw {
	id: unknown;
	user_id: unknown;
	source_id: unknown;
	oauth_connection_id: unknown;
}

// One-time repair for rows ingested before from_address/subject were stored
// (pre-July-2026): re-fetches HEADERS ONLY by stored Gmail id, via each
// row's own connection. With sender addresses present, the backlog becomes
// template-learnable — the whole point of running this before a drain.
export async function runSenderMetadataBackfill(
	limit: number,
	env: Env,
	fetcher: FetchLike = fetch,
): Promise<SenderMetadataBackfillResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const result: SenderMetadataBackfillResult = {
		scanned_count: 0,
		updated_count: 0,
		skipped_count: 0,
		remaining_count: 0,
		skip_reasons: {
			no_connection_id: 0,
			connection_missing: 0,
			token_refresh_failed: 0,
			message_gone: 0,
			update_no_match: 0,
		},
	};

	const rows = await sql<MetadataBackfillRowRaw[]>`
		select re.id, re.user_id, re.source_id, re.oauth_connection_id
		from public.raw_emails as re
		where re.from_address is null
		order by re.created_at asc
		limit ${limit}
	`;
	result.scanned_count = rows.length;

	const googleConfig = getGoogleRefreshConfig(env);
	const connectionById = new Map<UUID, OauthConnectionRow>();
	const accessTokenByConnectionId = new Map<UUID, string>();

	for (const row of rows) {
		const rawEmailId = toRequiredString(row.id, 'raw_emails.id') as UUID;
		const userId = toRequiredString(row.user_id, 'raw_emails.user_id') as UUID;
		const sourceId = toRequiredString(row.source_id, 'raw_emails.source_id');
		const connectionId = toNullableString(
			row.oauth_connection_id,
			'raw_emails.oauth_connection_id',
		) as UUID | null;
		if (!connectionId) {
			result.skipped_count += 1;
			result.skip_reasons.no_connection_id += 1;
			continue;
		}

		let connection = connectionById.get(connectionId);
		if (!connection) {
			const loaded = await loadConnectionById(sql, connectionId, userId);
			if (!loaded) {
				result.skipped_count += 1;
				result.skip_reasons.connection_missing += 1;
				continue;
			}
			connection = loaded;
			connectionById.set(connectionId, connection);
		}

		let accessToken = accessTokenByConnectionId.get(connectionId);
		if (!accessToken) {
			const refreshOutcome = await refreshAccessTokenWithOcc(
				sql,
				connection,
				googleConfig,
				fetcher,
			);
			if (refreshOutcome.revoked || refreshOutcome.inactive) {
				result.skipped_count += 1;
				result.skip_reasons.token_refresh_failed += 1;
				continue;
			}
			accessToken = refreshOutcome.accessToken;
			accessTokenByConnectionId.set(connectionId, accessToken);
		}

		const getResult = await gmailGetMessage(accessToken, sourceId, fetcher, 'metadata');
		if (getResult.revoked || getResult.paused || !getResult.message) {
			// Deleted messages 404 forever: mark the sender as unknown-but-tried
			// so the row stops being re-scanned every run.
			await sql`
				update public.raw_emails as re
				set from_address = ''
				where re.id = ${rawEmailId} and re.from_address is null
			`;
			result.skipped_count += 1;
			result.skip_reasons.message_gone += 1;
			continue;
		}

		const fromHeader = extractMessageHeader(getResult.message, 'From');
		const subjectHeader = extractMessageHeader(getResult.message, 'Subject');
		const fromAddress = parseFromAddress(fromHeader) ?? '';
		const subject = subjectHeader
			? truncateWithoutSplittingSurrogates(subjectHeader, MAX_SUBJECT_LENGTH)
			: null;

		const updated = await sql<{ id: UUID }[]>`
			update public.raw_emails as re
			set
				from_address = ${fromAddress},
				subject = coalesce(re.subject, ${subject})
			where re.id = ${rawEmailId}
				and re.from_address is null
			returning re.id
		`;
		if (updated.length > 0) {
			result.updated_count += 1;
		} else {
			result.skipped_count += 1;
			result.skip_reasons.update_no_match += 1;
		}
	}

	const remainingRows = await sql<{ remaining: unknown }[]>`
		select count(*)::int as remaining
		from public.raw_emails as re
		where re.from_address is null
	`;
	result.remaining_count = Number(remainingRows[0]?.remaining ?? 0);

	return result;
}

export interface RetroSenderGateResult {
	gate_enabled: boolean;
	dry_run: boolean;
	scanned_count: number;
	/** Senders that pass the gate — these stay PENDING and are worth AI. */
	allowed_count: number;
	/** Senders that fail the gate — flipped IGNORED + body purged (unless dry run). */
	gated_count: number;
	/** Rows with no usable sender (deleted messages) — left PENDING untouched. */
	unknown_sender_count: number;
	remaining_pending_count: number;
}

// One-time retro-application of the sender gate to rows ingested before the
// gate existed (full bodies, no gating). Requires the metadata backfill to
// have run first. Denied rows enter the same shape as live-gated mail
// (IGNORED, clean_text='') so the sender-classifier backfill can still
// promote them later if their sender turns out to be a bank. Slightly
// stricter than the live gate: only the parsed address is stored, so brand
// tokens can't match display names.
export async function runRetroactiveSenderGate(
	limit: number,
	dryRun: boolean,
	env: Env,
): Promise<RetroSenderGateResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);

	const senderGate = resolveSenderGateConfig(env);
	let senderGateEntries: readonly string[] = senderGate.entries;
	if (senderGate.enabled) {
		const dbEntries = await loadSenderRuleEntries(sql);
		senderGateEntries = [...senderGate.entries, ...dbEntries];
	}

	const result: RetroSenderGateResult = {
		gate_enabled: senderGate.enabled,
		dry_run: dryRun,
		scanned_count: 0,
		allowed_count: 0,
		gated_count: 0,
		unknown_sender_count: 0,
		remaining_pending_count: 0,
	};

	const rows = await sql<Array<{ id: unknown; from_address: unknown }>>`
		select re.id, re.from_address
		from public.raw_emails as re
		where re.status = 'PENDING_EXTRACTION'
			and re.body_purged_at is null
		order by re.created_at asc
		limit ${limit}
	`;
	result.scanned_count = rows.length;

	const deniedIds: UUID[] = [];
	for (const row of rows) {
		const id = toRequiredString(row.id, 'raw_emails.id') as UUID;
		const fromAddress = toNullableString(row.from_address, 'raw_emails.from_address');
		if (!fromAddress) {
			result.unknown_sender_count += 1;
			continue;
		}
		const allowed =
			!senderGate.enabled || isAllowedSender(fromAddress, fromAddress, senderGateEntries);
		if (allowed) {
			result.allowed_count += 1;
		} else {
			result.gated_count += 1;
			deniedIds.push(id);
		}
	}

	if (!dryRun && deniedIds.length > 0) {
		await sql`
			update public.raw_emails as re
			set status = 'IGNORED', clean_text = ''
			where re.id = any(${deniedIds}::uuid[])
				and re.status = 'PENDING_EXTRACTION'
		`;
	}

	const remainingRows = await sql<{ remaining: unknown }[]>`
		select count(*)::int as remaining
		from public.raw_emails as re
		where re.status = 'PENDING_EXTRACTION'
	`;
	result.remaining_pending_count = Number(remainingRows[0]?.remaining ?? 0);

	return result;
}

export interface SyncProbeMessageReport {
	source_id: string;
	internal_date: string | null;
	from_address: string | null;
	subject: string | null;
	verdict: 'allowed' | 'gated' | 'already_known' | 'skipped';
}

export interface SyncProbeResult {
	user_id: UUID | null;
	connection_email: string | null;
	listed_count: number;
	enqueued_pending_count: number;
	reports: SyncProbeMessageReport[];
}

// Fetches ONLY the newest `limit` Gmail messages through the normal
// metadata-first + sender-gate flow, logging and returning each message's
// metadata + gate verdict. Persists exactly like the real fetcher but NEVER
// advances last_sync_timestamp, so the full historical sync stays intact.
export async function runEmailSyncProbe(
	limit: number,
	env: Env,
	fetcher: FetchLike = fetch,
): Promise<SyncProbeResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const result: SyncProbeResult = {
		user_id: null,
		connection_email: null,
		listed_count: 0,
		enqueued_pending_count: 0,
		reports: [],
	};

	const connectionRows = await sql<OAuthConnectionFetchRowRaw[]>`
		select
			oc.id,
			oc.user_id,
			oc.provider,
			oc.email_address,
			oc.access_token,
			oc.refresh_token,
			oc.last_sync_timestamp,
			oc.sync_status,
			oc.created_at,
			oc.updated_at
		from public.oauth_connections as oc
		where oc.provider = ${GOOGLE_OAUTH_PROVIDER}
			and oc.sync_status in ('ACTIVE', 'ERROR_PAUSED')
		order by oc.updated_at desc
		limit 1
	`;
	if (connectionRows.length === 0) {
		return result;
	}
	const connection = mapOauthConnectionRow(connectionRows[0]);
	result.user_id = connection.user_id;
	result.connection_email = connection.email_address;

	const googleConfig = getGoogleRefreshConfig(env);
	const refreshOutcome = await refreshAccessTokenWithOcc(sql, connection, googleConfig, fetcher);
	if (refreshOutcome.revoked || refreshOutcome.inactive) {
		return result;
	}
	const accessToken = refreshOutcome.accessToken;

	const senderGate = resolveSenderGateConfig(env);
	let senderGateEntries: readonly string[] = senderGate.entries;
	if (senderGate.enabled) {
		const dbEntries = await loadSenderRuleEntries(sql);
		senderGateEntries = [...senderGate.entries, ...dbEntries];
	}

	// No search filter, no pagination: Gmail lists newest-first, one page.
	const listResult = await gmailListMessages(accessToken, 0, null, false, fetcher);
	if (listResult.revoked || listResult.paused) {
		return result;
	}
	const messageIds = listResult.messageIds.slice(0, limit);
	result.listed_count = messageIds.length;

	const existingBySourceId = await listExistingRawEmailStatuses(
		sql,
		connection.user_id,
		messageIds,
	);

	const recordsToPersist: RawEmailUpsertRecord[] = [];
	for (const messageId of messageIds) {
		if (existingBySourceId.has(messageId)) {
			result.reports.push({
				source_id: messageId,
				internal_date: null,
				from_address: null,
				subject: null,
				verdict: 'already_known',
			});
			continue;
		}

		const metaResult = await gmailGetMessage(accessToken, messageId, fetcher, 'metadata');
		if (metaResult.revoked || metaResult.paused || !metaResult.message) {
			result.reports.push({
				source_id: messageId,
				internal_date: null,
				from_address: null,
				subject: null,
				verdict: 'skipped',
			});
			continue;
		}
		const metaMessage = metaResult.message;
		const internalTimestamp = parseMessageInternalTimestamp(metaMessage.internalDate);
		if (internalTimestamp === null) {
			result.reports.push({
				source_id: messageId,
				internal_date: null,
				from_address: null,
				subject: null,
				verdict: 'skipped',
			});
			continue;
		}

		const fromHeader = extractMessageHeader(metaMessage, 'From');
		const subjectHeader = extractMessageHeader(metaMessage, 'Subject');
		const fromAddress = parseFromAddress(fromHeader);
		const subject = subjectHeader
			? truncateWithoutSplittingSurrogates(subjectHeader, MAX_SUBJECT_LENGTH)
			: null;
		const internalDateIso = new Date(internalTimestamp).toISOString();
		const senderAllowed =
			!senderGate.enabled || isAllowedSender(fromHeader, fromAddress, senderGateEntries);

		const report: SyncProbeMessageReport = {
			source_id: messageId,
			internal_date: internalDateIso,
			from_address: fromAddress,
			subject,
			verdict: senderAllowed ? 'allowed' : 'gated',
		};
		result.reports.push(report);
		// Content-bearing log; the probe is dev/ops-only by design.
		console.info('SYNC_PROBE_MESSAGE', report);

		if (!senderAllowed) {
			recordsToPersist.push({
				source_id: messageId,
				internal_date_iso: internalDateIso,
				clean_text: '',
				from_address: fromAddress,
				subject,
				status: 'IGNORED',
			});
			continue;
		}

		const fullResult = await gmailGetMessage(accessToken, messageId, fetcher, 'full');
		if (fullResult.revoked || fullResult.paused || !fullResult.message) {
			report.verdict = 'skipped';
			continue;
		}
		recordsToPersist.push({
			source_id: messageId,
			internal_date_iso: internalDateIso,
			clean_text: buildSanitizedEmailText(fullResult.message),
			from_address: fromAddress,
			subject,
			status: 'PENDING_EXTRACTION',
		});
	}

	if (recordsToPersist.length > 0) {
		// advanceCursor: false — the probe must never move the watermark.
		const insertedIds = await persistRawEmailsAndCursor(
			sql,
			connection.id,
			connection.user_id,
			recordsToPersist,
			0,
			false,
		);
		if (insertedIds.length > 0) {
			await enqueueNormalizeRawEmailsJobs(env.EMAIL_SYNC_QUEUE, insertedIds);
			result.enqueued_pending_count = insertedIds.length;
		}
	}

	return result;
}

// A sync for a user who deleted their account while it ran must ack, not
// retry into the dead-letter queue: the cascade already removed everything
// the job would write (Task 5, A.2 §10). Once the row is gone the next run
// finds no connections and does nothing.
export async function runEmailSyncUserJob(
	job: EmailSyncUserJobPayload,
	env: Env,
	fetcher: FetchLike = fetch,
): Promise<EmailSyncUserResult> {
	try {
		return await runEmailSyncUserJobUnguarded(job, env, fetcher);
	} catch (error) {
		if (isMissingUserReferenceError(error)) {
			throw new PoisonMessageError(`EMAIL_SYNC_USER user ${job.user_id} no longer exists`);
		}
		throw error;
	}
}

async function runEmailSyncUserJobUnguarded(
	job: EmailSyncUserJobPayload,
	env: Env,
	fetcher: FetchLike,
): Promise<EmailSyncUserResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const quotaDefaults = resolveDefaultQuotaCaps(config);
	const allConnections = await listEligibleConnectionsForUser(sql, job.user_id);
	const connections =
		job.continuation_connection_id !== undefined
			? allConnections.filter(connection => connection.id === job.continuation_connection_id)
			: allConnections;

	const result: EmailSyncUserResult = {
		user_id: job.user_id,
		connection_count: connections.length,
		processed_connection_count: 0,
		revoked_connection_count: 0,
		fetched_message_count: 0,
		inserted_or_retried_raw_email_count: 0,
		skipped_existing_raw_email_count: 0,
		sender_gated_raw_email_count: 0,
		quota_deferred_connection_count: 0,
		pending_requeued_count: 0,
	};

	for (const connection of connections) {
		const syncRunId = job.sync_run_id;
		if (syncRunId) {
			const leaseAcquired = await claimEmailSyncLease(
				sql,
				connection.id,
				job.user_id,
				syncRunId,
			);
			if (!leaseAcquired) {
				console.info('EMAIL_SYNC_CONNECTION_LEASE_BUSY', {
					connectionId: connection.id,
					userId: job.user_id,
					syncRunId,
				});
				continue;
			}
			console.info('EMAIL_SYNC_CONNECTION_LEASE_CLAIMED_OR_RENEWED', {
				connectionId: connection.id,
				userId: job.user_id,
				syncRunId,
				continuation: Boolean(job.continuation_page_token),
				leaseMinutes: EMAIL_SYNC_LEASE_MINUTES,
			});
		}

		// If processing throws, keep this chain's lease while Cloudflare retries
		// the same queue message. A later cron cannot overlap it, and the expiry
		// still provides crash/DLQ recovery if no retry completes the chain.
		const connectionResult = await processConnection(
			sql,
			job,
			connection,
			env,
			fetcher,
			quotaDefaults,
		);

		if (syncRunId && !connectionResult.continuation_enqueued) {
			await releaseEmailSyncLease(sql, connection.id, job.user_id, syncRunId);
		}

		result.processed_connection_count += 1;
		result.fetched_message_count += connectionResult.fetched_message_count;
		result.inserted_or_retried_raw_email_count +=
			connectionResult.inserted_or_retried_raw_email_count;
		result.skipped_existing_raw_email_count += connectionResult.skipped_existing_raw_email_count;
		result.sender_gated_raw_email_count += connectionResult.sender_gated_raw_email_count;
		if (connectionResult.revoked) {
			result.revoked_connection_count += 1;
		}
		if (connectionResult.quota_deferred) {
			result.quota_deferred_connection_count += 1;
			// The cap is per user: every other mailbox would defer the same way.
			break;
		}
		if (connectionResult.continuation_enqueued) {
			break;
		}
	}

	// Self-heal once per chain start; continuation pages skip it.
	if (job.continuation_connection_id === undefined) {
		result.pending_requeued_count = await requeueStalePendingEmails(
			sql,
			env,
			job.user_id,
			quotaDefaults,
		);
	}

	return result;
}

export async function releaseEmailSyncJobLeases(
	job: EmailSyncUserJobPayload,
	env: Env,
): Promise<number> {
	if (!job.sync_run_id) {
		return 0;
	}

	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const rows = await sql<{ id: UUID }[]>`
		update public.oauth_connections as oc
		set
			email_sync_lease_id = null,
			email_sync_lease_expires_at = null
		where oc.user_id = ${job.user_id}
			and oc.email_sync_lease_id = ${job.sync_run_id}
		returning id
	`;

	console.info('EMAIL_SYNC_JOB_LEASES_RELEASED_FOR_DLQ', {
		userId: job.user_id,
		syncRunId: job.sync_run_id,
		releasedConnectionCount: rows.length,
	});
	return rows.length;
}

export interface StatementHarvestResult {
	dry_run: boolean;
	matched_count: number;
	flipped_count: number;
	enqueued_batches: number;
	by_sender: Record<string, number>;
}

const STATEMENT_HARVEST_BATCH_SIZE = 250;

// One-time (idempotent) harvest of historical statement emails: candidates
// are matched permissively by subject/sender wording, flipped back to
// PENDING_EXTRACTION, and re-run through the normalizer — which now
// classifies them as statements (fields captured, PDF fallback when the
// text is bare) instead of transactions. Safe to re-run: emails that
// already produced a card_statements row are excluded, true statements
// yield zero transactions under the current prompt, and cross-email dedup
// guards any non-statement that slips through the match.
export async function runStatementHarvest(
	limit: number,
	dryRun: boolean,
	env: Env,
	windowDays: number | null = null,
): Promise<StatementHarvestResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);

	const rows = await sql<{ id: unknown; from_address: unknown }[]>`
		select re.id, re.from_address
		from public.raw_emails as re
		where re.status in ('IGNORED', 'PROCESSED')
			and re.body_purged_at is null
			and (re.subject ~* 'statement' or re.from_address ~* 'statement')
			and (
				${windowDays}::int is null
				or re.internal_date >= now() - (${windowDays} || ' days')::interval
			)
			and not exists (
				select 1
				from public.card_statements as cs
				where cs.source_raw_email_id = re.id
			)
		order by re.internal_date desc
		limit ${limit}
	`;

	const result: StatementHarvestResult = {
		dry_run: dryRun,
		matched_count: rows.length,
		flipped_count: 0,
		enqueued_batches: 0,
		by_sender: {},
	};
	const ids: UUID[] = [];
	for (const row of rows) {
		ids.push(toRequiredString(row.id, 'raw_emails.id'));
		const sender = toNullableString(row.from_address, 'raw_emails.from_address') ?? '<unknown>';
		result.by_sender[sender] = (result.by_sender[sender] ?? 0) + 1;
	}

	if (dryRun || ids.length === 0) {
		return result;
	}

	const flipped = await sql<{ id: unknown }[]>`
		update public.raw_emails as re
		set status = 'PENDING_EXTRACTION'
		where re.id = any(${ids}::uuid[])
			and re.status in ('IGNORED', 'PROCESSED')
			and re.body_purged_at is null
		returning re.id
	`;
	result.flipped_count = flipped.length;

	const flippedIds = flipped.map(row => toRequiredString(row.id, 'raw_emails.id'));
	for (let i = 0; i < flippedIds.length; i += STATEMENT_HARVEST_BATCH_SIZE) {
		const batch = flippedIds.slice(i, i + STATEMENT_HARVEST_BATCH_SIZE);
		await env.EMAIL_SYNC_QUEUE.send(
			{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: batch },
			{ contentType: 'json' },
		);
		result.enqueued_batches += 1;
	}

	return result;
}
