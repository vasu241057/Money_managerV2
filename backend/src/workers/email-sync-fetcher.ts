import type { EmailSyncUserJobPayload, OauthConnectionRow, RawEmailStatus, UUID } from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableString,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';
import { TransientMessageError } from './queue.errors';
import { buildEmailSyncUserJob } from './queue.messages';

const GOOGLE_OAUTH_PROVIDER = 'google';
const GOOGLE_OAUTH_TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token';
const GMAIL_API_MESSAGES_LIST_ENDPOINT = 'https://gmail.googleapis.com/gmail/v1/users/me/messages';
const GMAIL_API_MESSAGES_GET_ENDPOINT = 'https://gmail.googleapis.com/gmail/v1/users/me/messages';
const ENCRYPTED_TOKEN_PREFIX = 'enc:v1:';
const GMAIL_QUERY_OVERLAP_MS = 15 * 60 * 1000;
const GMAIL_LIST_MAX_RESULTS = 100;
const GMAIL_LIST_MAX_PAGES = 1000;
const TOKEN_REFRESH_OCC_MAX_ATTEMPTS = 3;
const CONTINUATION_ENQUEUE_MAX_ATTEMPTS = 3;
const MAX_CLEAN_TEXT_LENGTH = 50_000;

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
		}>;
		status?: unknown;
	} | null;
}

interface RawEmailUpsertRecord {
	source_id: string;
	internal_date_iso: string;
	clean_text: string;
}

interface ExistingRawEmailStatus {
	status: RawEmailStatus;
	internal_date_timestamp: number;
}

interface EmailSyncConnectionResult {
	connection_id: UUID;
	email_address: string;
	fetched_message_count: number;
	inserted_or_retried_raw_email_count: number;
	skipped_existing_raw_email_count: number;
	revoked: boolean;
	paused: boolean;
	continuation_enqueued: boolean;
}

interface EmailSyncContinuationState {
	connection_id: UUID;
	page_token: string;
	after_seconds: number;
	max_internal_timestamp_seen: number;
}

export interface EmailSyncUserResult {
	user_id: UUID;
	connection_count: number;
	processed_connection_count: number;
	revoked_connection_count: number;
	fetched_message_count: number;
	inserted_or_retried_raw_email_count: number;
	skipped_existing_raw_email_count: number;
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

function getGoogleRefreshConfig(env: Env): {
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

async function decryptTokenValue(value: string, keyBytes: Uint8Array): Promise<string> {
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

async function encryptTokenValue(plainText: string, keyBytes: Uint8Array): Promise<string> {
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

async function listEligibleConnectionsForUser(
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

function parseGoogleApiErrorReason(payload: unknown): string | null {
	const record = asRecord(payload) as GoogleApiErrorResponse | null;
	const errors = record?.error?.errors;
	if (Array.isArray(errors) && errors.length > 0) {
		const reason = errors[0]?.reason;
		if (typeof reason === 'string' && reason.length > 0) {
			return reason;
		}
	}

	const status = record?.error?.status;
	if (typeof status === 'string' && status.length > 0) {
		return status;
	}

	return null;
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
				buildEmailSyncUserJob(userId, lastSyncTimestamp, {
					continuation_connection_id: continuation.connection_id,
					continuation_page_token: continuation.page_token,
					continuation_after_seconds: continuation.after_seconds,
					continuation_max_internal_timestamp_seen:
						continuation.max_internal_timestamp_seen,
				}),
				{ contentType: 'json' },
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

async function refreshAccessTokenWithOcc(
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
	fetcher: FetchLike,
): Promise<{ messageIds: string[]; nextPageToken: string | null; revoked: boolean; paused: boolean }> {
	const url = new URL(GMAIL_API_MESSAGES_LIST_ENDPOINT);
	url.searchParams.set('maxResults', String(GMAIL_LIST_MAX_RESULTS));
	url.searchParams.set('q', `after:${afterSeconds}`);
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
		return { messageIds: [], nextPageToken: null, revoked: true, paused: false };
	}
	if (response.status === 429 || response.status >= 500) {
		throw new TransientMessageError(`Gmail list API retryable status ${response.status}`);
	}
	let parsedError: GoogleApiErrorResponse | null = null;
	if (!response.ok) {
		try {
			parsedError = (await response.json()) as GoogleApiErrorResponse;
		} catch {
			parsedError = null;
		}
	}
	if (response.status === 403) {
		const reason = parseGoogleApiErrorReason(parsedError);
		const classification = classifyGoogle403Reason(reason);
		if (classification === 'retry' || classification === 'unknown') {
			throw new TransientMessageError(
				`Gmail list API retryable 403 (${reason ?? 'unknown_reason'})`,
			);
		}
		return { messageIds: [], nextPageToken: null, revoked: false, paused: true };
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

	if (normalized.length <= MAX_CLEAN_TEXT_LENGTH) {
		return normalized;
	}

	return normalized.slice(0, MAX_CLEAN_TEXT_LENGTH);
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

async function gmailGetMessage(
	accessToken: string,
	messageId: string,
	fetcher: FetchLike,
): Promise<{ revoked: boolean; paused: boolean; message: GmailMessageResponse | null }> {
	const url = new URL(`${GMAIL_API_MESSAGES_GET_ENDPOINT}/${encodeURIComponent(messageId)}`);
	url.searchParams.set('format', 'full');

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
	if (response.status === 429 || response.status >= 500) {
		throw new TransientMessageError(`Gmail message API retryable status ${response.status}`);
	}
	let parsedError: GoogleApiErrorResponse | null = null;
	if (!response.ok) {
		try {
			parsedError = (await response.json()) as GoogleApiErrorResponse;
		} catch {
			parsedError = null;
		}
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
		const internalDateIso = toIsoDateTime(row.internal_date, 'raw_emails.internal_date');
		const internalDateTimestamp = Date.parse(internalDateIso);
		map.set(sourceId, {
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
): Promise<void> {
	await sql.begin(async (tx) => {
		const txSql = tx as unknown as SqlClient;

		for (const record of records) {
			await txSql`
				insert into public.raw_emails (
					user_id,
					oauth_connection_id,
					source_id,
					internal_date,
					clean_text,
					status
				)
				values (
					${userId},
					${connectionId},
					${record.source_id},
					${record.internal_date_iso},
					${record.clean_text},
					'PENDING_EXTRACTION'
				)
				on conflict (user_id, source_id) do update
				set
					oauth_connection_id = excluded.oauth_connection_id,
					internal_date = excluded.internal_date,
					clean_text = excluded.clean_text,
					status = 'PENDING_EXTRACTION'
				where raw_emails.status = 'FAILED'
			`;
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

async function processConnection(
	sql: SqlClient,
	job: EmailSyncUserJobPayload,
	connection: OauthConnectionRow,
	env: Env,
	fetcher: FetchLike,
): Promise<EmailSyncConnectionResult> {
	const config = getGoogleRefreshConfig(env);
	const refreshOutcome = await refreshAccessTokenWithOcc(sql, connection, config, fetcher);
	if (refreshOutcome.revoked) {
		return {
			connection_id: connection.id,
			email_address: connection.email_address,
			fetched_message_count: 0,
			inserted_or_retried_raw_email_count: 0,
			skipped_existing_raw_email_count: 0,
			revoked: true,
			paused: false,
			continuation_enqueued: false,
		};
	}
	if (refreshOutcome.inactive) {
		return {
			connection_id: refreshOutcome.connection.id,
			email_address: refreshOutcome.connection.email_address,
			fetched_message_count: 0,
			inserted_or_retried_raw_email_count: 0,
			skipped_existing_raw_email_count: 0,
			revoked: false,
			paused: false,
			continuation_enqueued: false,
		};
	}

	const accessToken = refreshOutcome.accessToken;
	const jobContinuation = getJobContinuationStateForConnection(job, connection.id);
	const baseCursor = Math.min(
		Math.max(0, job.last_sync_timestamp),
		Math.max(0, refreshOutcome.connection.last_sync_timestamp),
	);
	const afterSeconds =
		jobContinuation?.after_seconds ??
		Math.max(0, Math.floor((baseCursor - GMAIL_QUERY_OVERLAP_MS) / 1000));

	let fetchedMessageCount = 0;
	let insertedOrRetriedCount = 0;
	let skippedExistingCount = 0;
	let pageToken: string | null = jobContinuation?.page_token ?? null;
	let maxInternalTimestampSeen = Math.max(
		baseCursor,
		jobContinuation?.max_internal_timestamp_seen ?? 0,
	);
	let pageCount = 0;
	let hitPageCap = false;
	let continuationPageToken: string | null = null;

	while (true) {
		pageCount += 1;
		const listResult = await gmailListMessages(accessToken, afterSeconds, pageToken, fetcher);
		if (listResult.revoked) {
			await markConnectionRevoked(sql, connection.id, connection.user_id, 'gmail_list_unauthorized');
			return {
				connection_id: connection.id,
				email_address: connection.email_address,
				fetched_message_count: fetchedMessageCount,
				inserted_or_retried_raw_email_count: insertedOrRetriedCount,
				skipped_existing_raw_email_count: skippedExistingCount,
				revoked: true,
				paused: false,
				continuation_enqueued: false,
			};
		}
		if (listResult.paused) {
			await markConnectionErrorPaused(
				sql,
				connection.id,
				connection.user_id,
				'gmail_list_permission_denied',
			);
			return {
				connection_id: connection.id,
				email_address: connection.email_address,
				fetched_message_count: fetchedMessageCount,
				inserted_or_retried_raw_email_count: insertedOrRetriedCount,
				skipped_existing_raw_email_count: skippedExistingCount,
				revoked: false,
				paused: true,
				continuation_enqueued: false,
			};
		}

		const existingStatusMap = await listExistingRawEmailStatuses(
			sql,
			job.user_id,
			listResult.messageIds,
		);
		const idsToFetch: string[] = [];
		for (const messageId of listResult.messageIds) {
			const existing = existingStatusMap.get(messageId);
			if (existing && existing.status !== 'FAILED') {
				skippedExistingCount += 1;
				maxInternalTimestampSeen = Math.max(
					maxInternalTimestampSeen,
					existing.internal_date_timestamp,
				);
				continue;
			}

			idsToFetch.push(messageId);
		}

		const recordsToPersist: RawEmailUpsertRecord[] = [];
		let fetchedMessagesThisPage = 0;
		for (const messageId of idsToFetch) {
			const getResult = await gmailGetMessage(accessToken, messageId, fetcher);
			if (getResult.revoked) {
				await markConnectionRevoked(sql, connection.id, connection.user_id, 'gmail_unauthorized');
				return {
					connection_id: connection.id,
					email_address: connection.email_address,
					fetched_message_count: fetchedMessageCount,
					inserted_or_retried_raw_email_count: insertedOrRetriedCount,
					skipped_existing_raw_email_count: skippedExistingCount,
					revoked: true,
					paused: false,
					continuation_enqueued: false,
				};
			}
			if (getResult.paused) {
				await markConnectionErrorPaused(
					sql,
					connection.id,
					connection.user_id,
					'gmail_get_permission_denied',
				);
				return {
					connection_id: connection.id,
					email_address: connection.email_address,
					fetched_message_count: fetchedMessageCount,
					inserted_or_retried_raw_email_count: insertedOrRetriedCount,
					skipped_existing_raw_email_count: skippedExistingCount,
					revoked: false,
					paused: true,
					continuation_enqueued: false,
				};
			}
			const message = getResult.message;
			if (!message) {
				continue;
			}
			fetchedMessagesThisPage += 1;

			const sourceId = typeof message.id === 'string' ? message.id : messageId;
			const internalTimestamp = parseMessageInternalTimestamp(message.internalDate);
			if (internalTimestamp === null) {
				continue;
			}

			maxInternalTimestampSeen = Math.max(maxInternalTimestampSeen, internalTimestamp);
			const cleanText = buildSanitizedEmailText(message);
			recordsToPersist.push({
				source_id: sourceId,
				internal_date_iso: new Date(internalTimestamp).toISOString(),
				clean_text: cleanText,
			});
		}
		fetchedMessageCount += fetchedMessagesThisPage;

		if (recordsToPersist.length > 0) {
			await persistRawEmailsAndCursor(
				sql,
				connection.id,
				job.user_id,
				recordsToPersist,
				maxInternalTimestampSeen,
				false,
			);

			insertedOrRetriedCount += recordsToPersist.length;
		}

		if (!listResult.nextPageToken) {
			break;
		}
		if (pageCount >= GMAIL_LIST_MAX_PAGES) {
			hitPageCap = true;
			continuationPageToken = listResult.nextPageToken;
			break;
		}

		pageToken = listResult.nextPageToken;
	}

	if (hitPageCap) {
		if (!continuationPageToken) {
			throw new TransientMessageError('Gmail pagination cap reached without continuation token');
		}

		const continuation: EmailSyncContinuationState = {
			connection_id: connection.id,
			page_token: continuationPageToken,
			after_seconds: afterSeconds,
			max_internal_timestamp_seen: maxInternalTimestampSeen,
		};
		await enqueueEmailSyncContinuation(
			env.EMAIL_SYNC_QUEUE,
			job.user_id,
			baseCursor,
			continuation,
		);

		console.warn('EMAIL_SYNC_USER reached Gmail pagination cap; enqueued continuation', {
			connectionId: connection.id,
			userId: job.user_id,
			pageCount,
			pageCap: GMAIL_LIST_MAX_PAGES,
			afterSeconds,
			nextPageTokenPresent: true,
		});
	} else {
		await persistRawEmailsAndCursor(
			sql,
			connection.id,
			job.user_id,
			[],
			maxInternalTimestampSeen,
			true,
		);
	}

	return {
		connection_id: connection.id,
		email_address: connection.email_address,
		fetched_message_count: fetchedMessageCount,
		inserted_or_retried_raw_email_count: insertedOrRetriedCount,
		skipped_existing_raw_email_count: skippedExistingCount,
		revoked: false,
		paused: false,
		continuation_enqueued: hitPageCap,
	};
}

export async function runEmailSyncUserJob(
	job: EmailSyncUserJobPayload,
	env: Env,
	fetcher: FetchLike = fetch,
): Promise<EmailSyncUserResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);
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
	};

	for (const connection of connections) {
		const connectionResult = await processConnection(sql, job, connection, env, fetcher);

		result.processed_connection_count += 1;
		result.fetched_message_count += connectionResult.fetched_message_count;
		result.inserted_or_retried_raw_email_count +=
			connectionResult.inserted_or_retried_raw_email_count;
		result.skipped_existing_raw_email_count += connectionResult.skipped_existing_raw_email_count;
		if (connectionResult.revoked) {
			result.revoked_connection_count += 1;
		}
		if (connectionResult.continuation_enqueued) {
			break;
		}
	}

	return result;
}
