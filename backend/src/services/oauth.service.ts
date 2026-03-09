import type {
	GoogleOAuthCallbackRequest,
	GoogleOAuthConnectionResponse,
	GoogleOAuthConnectionStatusResponse,
	GoogleOAuthStartResponse,
	OauthConnectionRow,
	UUID,
} from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableString,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';
import { asRecord, parseRequiredString } from '../lib/http/validation';
import { badRequest, serviceUnavailable } from '../lib/http/errors';

const GOOGLE_OAUTH_PROVIDER = 'google';
const GOOGLE_OAUTH_SCOPE =
	'https://www.googleapis.com/auth/gmail.readonly openid email profile';
const GOOGLE_OAUTH_AUTHORIZE_ENDPOINT = 'https://accounts.google.com/o/oauth2/v2/auth';
const GOOGLE_OAUTH_TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token';
const GOOGLE_OAUTH_USERINFO_ENDPOINT = 'https://openidconnect.googleapis.com/v1/userinfo';
const OAUTH_STATE_VERSION = 1;
const OAUTH_STATE_TTL_SECONDS = 10 * 60;
const ENCRYPTED_TOKEN_PREFIX = 'enc:v1:';

interface OauthConnectionRowRaw {
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

interface GoogleOAuthConfig {
	clientId: string;
	clientSecret: string;
	redirectUri: string;
	stateSecret: string;
	tokenEncryptionKey: Uint8Array;
}

interface GoogleOAuthStatePayload {
	v: number;
	u: UUID;
	iat: number;
	exp: number;
	n: string;
}

interface GoogleTokenExchangeResponse {
	access_token?: unknown;
	refresh_token?: unknown;
}

interface GoogleUserInfoResponse {
	email?: unknown;
}

interface FetchLike {
	(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

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
		throw serviceUnavailable('OAUTH_UNAVAILABLE', `${fieldName} is not configured`);
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
		throw serviceUnavailable(
			'OAUTH_UNAVAILABLE',
			`${fieldName} must be a valid base64/base64url-encoded secret`,
		);
	}
}

function getGoogleOAuthConfig(env: Env): GoogleOAuthConfig {
	const clientId = normalizeText(env.GOOGLE_CLIENT_ID);
	const clientSecret = normalizeText(env.GOOGLE_CLIENT_SECRET);
	const redirectUri = normalizeText(env.GOOGLE_OAUTH_REDIRECT_URI);
	const stateSecret = normalizeText(env.GOOGLE_OAUTH_STATE_SECRET);
	const tokenKeyEncoded = normalizeText(env.GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY);

	if (!clientId || !clientSecret || !redirectUri || !stateSecret || !tokenKeyEncoded) {
		throw serviceUnavailable(
			'OAUTH_UNAVAILABLE',
			'Google OAuth is not fully configured on the backend',
		);
	}

	let parsedRedirectUri: URL;
	try {
		parsedRedirectUri = new URL(redirectUri);
	} catch {
		throw serviceUnavailable('OAUTH_UNAVAILABLE', 'GOOGLE_OAUTH_REDIRECT_URI must be a valid URL');
	}

	const isLocalhostRedirect =
		parsedRedirectUri.hostname === 'localhost' ||
		parsedRedirectUri.hostname === '127.0.0.1' ||
		parsedRedirectUri.hostname === '::1';
	if (parsedRedirectUri.protocol !== 'https:' && !isLocalhostRedirect) {
		throw serviceUnavailable(
			'OAUTH_UNAVAILABLE',
			'GOOGLE_OAUTH_REDIRECT_URI must use https:// (localhost is allowed for dev)',
		);
	}

	const tokenEncryptionKey = parseBase64SecretBytes(
		tokenKeyEncoded,
		'GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY',
	);
	if (tokenEncryptionKey.byteLength !== 32) {
		throw serviceUnavailable(
			'OAUTH_UNAVAILABLE',
			'GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY must decode to exactly 32 bytes',
		);
	}

	return {
		clientId,
		clientSecret,
		redirectUri: parsedRedirectUri.toString(),
		stateSecret,
		tokenEncryptionKey,
	};
}

function toBase64Url(bytes: Uint8Array): string {
	const base64 = btoa(String.fromCharCode(...bytes));
	return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function fromBase64Url(value: string, fieldName: string): Uint8Array {
	try {
		const base64 = value.replace(/-/g, '+').replace(/_/g, '/');
		const padded = base64.padEnd(Math.ceil(base64.length / 4) * 4, '=');
		const binary = atob(padded);
		const bytes = new Uint8Array(binary.length);

		for (let index = 0; index < binary.length; index += 1) {
			bytes[index] = binary.charCodeAt(index);
		}

		return bytes;
	} catch {
		throw badRequest('INVALID_OAUTH_STATE', `${fieldName} is malformed`);
	}
}

function timingSafeEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) {
		return false;
	}

	let diff = 0;
	for (let index = 0; index < a.length; index += 1) {
		diff |= a[index] ^ b[index];
	}

	return diff === 0;
}

async function hmacSha256(secret: string, message: string): Promise<Uint8Array> {
	const key = await crypto.subtle.importKey(
		'raw',
		new TextEncoder().encode(secret),
		{ name: 'HMAC', hash: 'SHA-256' },
		false,
		['sign'],
	);

	const signature = await crypto.subtle.sign(
		'HMAC',
		key,
		new TextEncoder().encode(message),
	);

	return new Uint8Array(signature);
}

async function signOAuthState(payload: GoogleOAuthStatePayload, stateSecret: string): Promise<string> {
	const payloadJson = JSON.stringify(payload);
	const payloadBytes = new TextEncoder().encode(payloadJson);
	const payloadEncoded = toBase64Url(payloadBytes);
	const signature = await hmacSha256(stateSecret, payloadEncoded);
	const signatureEncoded = toBase64Url(signature);
	return `${payloadEncoded}.${signatureEncoded}`;
}

export async function createGoogleOAuthState(
	userId: UUID,
	stateSecret: string,
	nowSeconds = Math.floor(Date.now() / 1000),
): Promise<string> {
	const nonceBytes = crypto.getRandomValues(new Uint8Array(16));
	const payload: GoogleOAuthStatePayload = {
		v: OAUTH_STATE_VERSION,
		u: userId,
		iat: nowSeconds,
		exp: nowSeconds + OAUTH_STATE_TTL_SECONDS,
		n: toBase64Url(nonceBytes),
	};

	return signOAuthState(payload, stateSecret);
}

export async function verifyGoogleOAuthState(
	state: string,
	expectedUserId: UUID,
	stateSecret: string,
	nowSeconds = Math.floor(Date.now() / 1000),
): Promise<void> {
	const normalized = state.trim();
	const [payloadSegment, signatureSegment] = normalized.split('.', 2);
	if (!payloadSegment || !signatureSegment) {
		throw badRequest('INVALID_OAUTH_STATE', 'OAuth state is malformed');
	}

	const expectedSignature = await hmacSha256(stateSecret, payloadSegment);
	const receivedSignature = fromBase64Url(signatureSegment, 'state signature');
	if (!timingSafeEqual(expectedSignature, receivedSignature)) {
		throw badRequest('INVALID_OAUTH_STATE', 'OAuth state signature is invalid');
	}

	const payloadBytes = fromBase64Url(payloadSegment, 'state payload');
	let payload: GoogleOAuthStatePayload;
	try {
		payload = JSON.parse(new TextDecoder().decode(payloadBytes)) as GoogleOAuthStatePayload;
	} catch {
		throw badRequest('INVALID_OAUTH_STATE', 'OAuth state payload is malformed');
	}

	if (
		payload.v !== OAUTH_STATE_VERSION ||
		typeof payload.u !== 'string' ||
		typeof payload.iat !== 'number' ||
		typeof payload.exp !== 'number' ||
		typeof payload.n !== 'string'
	) {
		throw badRequest('INVALID_OAUTH_STATE', 'OAuth state payload is invalid');
	}

	if (payload.u !== expectedUserId) {
		throw badRequest('INVALID_OAUTH_STATE', 'OAuth state user mismatch');
	}

	if (!Number.isFinite(payload.exp) || payload.exp < nowSeconds) {
		throw badRequest('INVALID_OAUTH_STATE', 'OAuth state has expired');
	}

	if (!Number.isFinite(payload.iat) || payload.iat > nowSeconds + 30) {
		throw badRequest('INVALID_OAUTH_STATE', 'OAuth state issued-at value is invalid');
	}
}

async function importAesKey(rawKey: Uint8Array): Promise<CryptoKey> {
	return crypto.subtle.importKey('raw', rawKey, 'AES-GCM', false, ['encrypt', 'decrypt']);
}

function isEncryptedToken(value: string | null): boolean {
	return Boolean(value && value.startsWith(ENCRYPTED_TOKEN_PREFIX));
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

function mapOauthConnectionRow(row: OauthConnectionRowRaw): OauthConnectionRow {
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

function redactConnectionTokens(connection: OauthConnectionRow): OauthConnectionRow {
	return {
		...connection,
		access_token: null,
		refresh_token: null,
	};
}

async function exchangeGoogleAuthorizationCode(
	config: GoogleOAuthConfig,
	code: string,
	fetcher: FetchLike,
): Promise<{ accessToken: string; refreshToken: string | null }> {
	const body = new URLSearchParams({
		code,
		client_id: config.clientId,
		client_secret: config.clientSecret,
		redirect_uri: config.redirectUri,
		grant_type: 'authorization_code',
	});

	let response: Response;
	try {
		response = await fetcher(GOOGLE_OAUTH_TOKEN_ENDPOINT, {
			method: 'POST',
			headers: {
				'content-type': 'application/x-www-form-urlencoded',
			},
			body: body.toString(),
		});
	} catch {
		throw serviceUnavailable('OAUTH_UNAVAILABLE', 'Failed to reach Google token endpoint');
	}

	if (!response.ok) {
		throw badRequest('OAUTH_TOKEN_ERROR', 'Google token exchange failed');
	}

	let parsed: GoogleTokenExchangeResponse;
	try {
		parsed = (await response.json()) as GoogleTokenExchangeResponse;
	} catch {
		throw badRequest('OAUTH_TOKEN_ERROR', 'Google token exchange returned invalid JSON');
	}

	if (typeof parsed.access_token !== 'string' || parsed.access_token.trim() === '') {
		throw badRequest('OAUTH_TOKEN_ERROR', 'Google token exchange did not return access_token');
	}

	const refreshToken =
		typeof parsed.refresh_token === 'string' && parsed.refresh_token.trim() !== ''
			? parsed.refresh_token
			: null;

	return {
		accessToken: parsed.access_token,
		refreshToken,
	};
}

async function fetchGoogleUserEmail(accessToken: string, fetcher: FetchLike): Promise<string> {
	let response: Response;
	try {
		response = await fetcher(GOOGLE_OAUTH_USERINFO_ENDPOINT, {
			method: 'GET',
			headers: {
				authorization: `Bearer ${accessToken}`,
				accept: 'application/json',
			},
		});
	} catch {
		throw serviceUnavailable('OAUTH_UNAVAILABLE', 'Failed to reach Google userinfo endpoint');
	}

	if (!response.ok) {
		throw badRequest('OAUTH_USERINFO_ERROR', 'Google userinfo request failed');
	}

	let parsed: GoogleUserInfoResponse;
	try {
		parsed = (await response.json()) as GoogleUserInfoResponse;
	} catch {
		throw badRequest('OAUTH_USERINFO_ERROR', 'Google userinfo response was invalid JSON');
	}

	return parseRequiredString(parsed.email, 'google_userinfo.email').toLowerCase();
}

async function listGoogleConnections(sql: SqlClient, userId: UUID): Promise<OauthConnectionRow[]> {
	const rows = await sql<OauthConnectionRowRaw[]>`
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
		order by oc.updated_at desc, oc.created_at desc, oc.id asc
	`;

	return rows.map(mapOauthConnectionRow);
}

async function updateDuplicateGoogleConnectionsToRevoked(
	sql: SqlClient,
	userId: UUID,
	keepConnectionId: string,
): Promise<void> {
	await sql`
		update public.oauth_connections as oc
		set
			access_token = null,
			refresh_token = null,
			sync_status = 'AUTH_REVOKED'
		where oc.user_id = ${userId}
			and oc.provider = ${GOOGLE_OAUTH_PROVIDER}
			and oc.id <> ${keepConnectionId}
	`;
}

async function upsertGoogleConnectionByEmail(
	sql: SqlClient,
	userId: UUID,
	email: string,
	accessTokenEncrypted: string,
	refreshTokenEncrypted: string,
): Promise<OauthConnectionRow> {
	const rows = await sql<OauthConnectionRowRaw[]>`
		insert into public.oauth_connections (
			user_id,
			provider,
			email_address,
			access_token,
			refresh_token,
			sync_status
		)
		values (
			${userId},
			${GOOGLE_OAUTH_PROVIDER},
			${email},
			${accessTokenEncrypted},
			${refreshTokenEncrypted},
			'ACTIVE'
		)
		on conflict (user_id, email_address) do update
		set
			provider = excluded.provider,
			access_token = excluded.access_token,
			refresh_token = excluded.refresh_token,
			sync_status = 'ACTIVE'
		returning
			id,
			user_id,
			provider,
			email_address,
			access_token,
			refresh_token,
			last_sync_timestamp,
			sync_status,
			created_at,
			updated_at
	`;

	if (rows.length === 0) {
		throw badRequest('OAUTH_PERSISTENCE_ERROR', 'Unable to persist oauth connection');
	}

	return mapOauthConnectionRow(rows[0]);
}

function buildGoogleAuthUrl(config: GoogleOAuthConfig, state: string): string {
	const params = new URLSearchParams({
		client_id: config.clientId,
		redirect_uri: config.redirectUri,
		response_type: 'code',
		scope: GOOGLE_OAUTH_SCOPE,
		access_type: 'offline',
		include_granted_scopes: 'true',
		prompt: 'consent',
		state,
	});

	return `${GOOGLE_OAUTH_AUTHORIZE_ENDPOINT}?${params.toString()}`;
}

export function parseGoogleOAuthCallbackRequest(payload: unknown): GoogleOAuthCallbackRequest {
	const body = asRecord(payload);

	return {
		code: parseRequiredString(body.code, 'code'),
		state: parseRequiredString(body.state, 'state'),
	};
}

export async function startGoogleOAuth(
	userId: UUID,
	env: Env,
): Promise<GoogleOAuthStartResponse> {
	const config = getGoogleOAuthConfig(env);
	const state = await createGoogleOAuthState(userId, config.stateSecret);
	return {
		auth_url: buildGoogleAuthUrl(config, state),
	};
}

export async function completeGoogleOAuthCallback(
	sql: SqlClient,
	userId: UUID,
	request: GoogleOAuthCallbackRequest,
	env: Env,
	fetcher: FetchLike = fetch,
): Promise<GoogleOAuthConnectionResponse> {
	const config = getGoogleOAuthConfig(env);

	await verifyGoogleOAuthState(request.state, userId, config.stateSecret);
	const tokenResponse = await exchangeGoogleAuthorizationCode(config, request.code, fetcher);
	const email = await fetchGoogleUserEmail(tokenResponse.accessToken, fetcher);

	const existingConnections = await listGoogleConnections(sql, userId);
	const fallbackConnectionWithRefreshToken =
		existingConnections.find(
			connection =>
				connection.email_address.toLowerCase() === email &&
				Boolean(connection.refresh_token),
		) ?? null;

	let refreshTokenSource: string | null = tokenResponse.refreshToken;
	if (!refreshTokenSource && fallbackConnectionWithRefreshToken?.refresh_token) {
		refreshTokenSource = fallbackConnectionWithRefreshToken.refresh_token;
	}

	if (!refreshTokenSource) {
		throw badRequest(
			'OAUTH_TOKEN_ERROR',
			'Google did not return a refresh token. Disconnect and reconnect Gmail to continue.',
		);
	}

	const encryptedAccessToken = await encryptTokenValue(
		tokenResponse.accessToken,
		config.tokenEncryptionKey,
	);
	const encryptedRefreshToken = isEncryptedToken(refreshTokenSource)
		? refreshTokenSource
		: await encryptTokenValue(refreshTokenSource, config.tokenEncryptionKey);

	const persistedConnection = await upsertGoogleConnectionByEmail(
		sql,
		userId,
		email,
		encryptedAccessToken,
		encryptedRefreshToken,
	);

	await updateDuplicateGoogleConnectionsToRevoked(sql, userId, persistedConnection.id);

	return {
		connection: redactConnectionTokens(persistedConnection),
	};
}

export async function getGoogleOAuthConnectionStatus(
	sql: SqlClient,
	userId: UUID,
): Promise<GoogleOAuthConnectionStatusResponse> {
	const connections = await listGoogleConnections(sql, userId);
	const latestConnection = connections[0] ?? null;

	if (!latestConnection) {
		return { connection: null };
	}

	return {
		connection: redactConnectionTokens(latestConnection),
	};
}

export async function disconnectGoogleOAuthConnection(
	sql: SqlClient,
	userId: UUID,
): Promise<void> {
	await sql`
		update public.oauth_connections as oc
		set
			access_token = null,
			refresh_token = null,
			sync_status = 'AUTH_REVOKED'
		where oc.user_id = ${userId}
			and oc.provider = ${GOOGLE_OAUTH_PROVIDER}
	`;
}
