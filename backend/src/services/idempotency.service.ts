// Replay protection for unsafe writes (Task 7). Opt-in: a client that sends
// `Idempotency-Key` gets exactly-once semantics for that key; a client that
// does not is unaffected.
//
// The problem it solves is the one retry that cannot be made safe on the
// client: a POST whose response was lost. The caller cannot tell whether the
// transaction was created, and retrying blind duplicates it.
//
// The ledger is (user_id, key):
//   * a row is CLAIMED before the handler runs, so two concurrent retries
//     cannot both execute;
//   * it is COMPLETED with the status and body actually sent, so the retry
//     replays that response instead of writing again;
//   * the body is pinned by hash, so reusing one key for a different payload
//     is a 409 rather than a wrong, silent replay;
//   * a claim abandoned mid-request (the worker died) is reclaimable after
//     IN_FLIGHT_RECLAIM_MINUTES, so a crash cannot wedge a key until the
//     nightly purge.
//
// Failed handlers release their claim: only a response that was actually
// sent is worth replaying, and a 500 should not be pinned to the key.

import type { Request, Response } from 'express';

import type { UUID } from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import { toRequiredString, toSafeInteger } from '../lib/db/serialization';
import { badRequest, conflict } from '../lib/http/errors';

export const IDEMPOTENCY_HEADER = 'Idempotency-Key';
/** Marks a response that came from the ledger rather than a fresh write. */
export const IDEMPOTENT_REPLAY_HEADER = 'Idempotent-Replay';
export const IDEMPOTENCY_KEY_MAX_LENGTH = 255;
/**
 * How long a claim may sit unfinished before another request may take it.
 *
 * Deliberately far longer than any request can live: a Worker invocation is
 * bounded well under a minute, so a claim this old belongs to a worker that
 * died, never to one still running. Shortening it would break that — two
 * live requests could then hold the same key, and the later one's response
 * would overwrite the earlier one's in the ledger.
 */
export const IN_FLIGHT_RECLAIM_MINUTES = 5;
/** Rows older than this are dropped by the nightly purge. */
export const IDEMPOTENCY_RETENTION_HOURS = 24;
/** Bounded like every other nightly loop. */
export const IDEMPOTENCY_PURGE_BATCH_SIZE = 1000;
export const IDEMPOTENCY_PURGE_MAX_BATCHES = 20;

/**
 * Printable ASCII, no spaces: UUIDs and ULIDs pass, and a header that cannot
 * round-trip through a log line or a URL never reaches the database.
 */
const KEY_PATTERN = /^[\x21-\x7e]+$/;

export function parseIdempotencyKey(raw: unknown): string | null {
	if (raw === undefined || raw === null) {
		return null;
	}

	const value = Array.isArray(raw) ? raw[0] : raw;
	if (typeof value !== 'string') {
		return null;
	}

	const normalized = value.trim();
	if (normalized.length === 0) {
		return null;
	}

	if (normalized.length > IDEMPOTENCY_KEY_MAX_LENGTH) {
		throw badRequest(
			'INVALID_IDEMPOTENCY_KEY',
			`${IDEMPOTENCY_HEADER} must be at most ${IDEMPOTENCY_KEY_MAX_LENGTH} characters`,
		);
	}

	if (!KEY_PATTERN.test(normalized)) {
		throw badRequest(
			'INVALID_IDEMPOTENCY_KEY',
			`${IDEMPOTENCY_HEADER} must contain only printable ASCII characters`,
		);
	}

	return normalized;
}

/**
 * Stable JSON: object keys sorted at every depth, so two semantically equal
 * bodies hash the same however the client ordered them. Array order is
 * meaningful and preserved.
 */
export function canonicalJson(value: unknown): string {
	if (value === null || typeof value !== 'object') {
		return JSON.stringify(value ?? null) ?? 'null';
	}

	if (Array.isArray(value)) {
		return `[${value.map((item) => canonicalJson(item)).join(',')}]`;
	}

	const entries = Object.entries(value as Record<string, unknown>)
		.filter(([, item]) => item !== undefined)
		.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
		.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`);
	return `{${entries.join(',')}}`;
}

export async function hashRequestBody(body: unknown): Promise<string> {
	const encoded = new TextEncoder().encode(canonicalJson(body));
	const digest = await crypto.subtle.digest('SHA-256', encoded);
	return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
}

export type IdempotencyClaim =
	/** This request owns the key and must run the handler. */
	| { kind: 'CLAIMED' }
	/** The same key and body already produced this response. */
	| { kind: 'REPLAY'; status: number; body: unknown }
	/** The same key was used for a different body. */
	| { kind: 'CONFLICT' }
	/** Another request holds the claim and is still running. */
	| { kind: 'IN_FLIGHT' };

interface ClaimRowRaw {
	request_hash: unknown;
	response_status: unknown;
	response_body: unknown;
	completed_at: unknown;
	reclaimable: unknown;
}

export async function claimIdempotencyKey(
	sql: SqlClient,
	userId: UUID,
	key: string,
	requestHash: string,
	retriesLeft = 1,
): Promise<IdempotencyClaim> {
	const inserted = await sql<{ key: string }[]>`
		insert into public.idempotency_keys (user_id, key, request_hash)
		values (${userId}, ${key}, ${requestHash})
		on conflict (user_id, key) do nothing
		returning key
	`;
	if (inserted.length > 0) {
		return { kind: 'CLAIMED' };
	}

	const rows = await sql<ClaimRowRaw[]>`
		select
			ik.request_hash,
			ik.response_status,
			ik.response_body,
			ik.completed_at,
			(
				ik.completed_at is null
				and ik.created_at < now() - (${IN_FLIGHT_RECLAIM_MINUTES} || ' minutes')::interval
			) as reclaimable
		from public.idempotency_keys as ik
		where ik.user_id = ${userId} and ik.key = ${key}
	`;
	const row = rows[0];
	if (!row) {
		// Purged between the two statements; the key is free again. One retry
		// is enough — a second miss means something else is deleting rows and
		// the caller should see the in-flight answer rather than spin.
		if (retriesLeft > 0) {
			return claimIdempotencyKey(sql, userId, key, requestHash, retriesLeft - 1);
		}
		return { kind: 'IN_FLIGHT' };
	}

	if (toRequiredString(row.request_hash, 'idempotency_keys.request_hash') !== requestHash) {
		return { kind: 'CONFLICT' };
	}

	if (row.completed_at !== null && row.completed_at !== undefined) {
		return {
			kind: 'REPLAY',
			status: toSafeInteger(row.response_status, 'idempotency_keys.response_status'),
			body: row.response_body ?? null,
		};
	}

	if (row.reclaimable === true) {
		// The previous holder never finished. Taking the claim is guarded by
		// the same predicate, so only one of several racing requests wins.
		const reclaimed = await sql<{ key: string }[]>`
			update public.idempotency_keys
			set created_at = now(), request_hash = ${requestHash}
			where user_id = ${userId}
				and key = ${key}
				and completed_at is null
				and created_at < now() - (${IN_FLIGHT_RECLAIM_MINUTES} || ' minutes')::interval
			returning key
		`;
		if (reclaimed.length > 0) {
			console.warn('IDEMPOTENCY_CLAIM_RECLAIMED', { userId, staleMinutes: IN_FLIGHT_RECLAIM_MINUTES });
			return { kind: 'CLAIMED' };
		}
	}

	return { kind: 'IN_FLIGHT' };
}

export async function completeIdempotencyKey(
	sql: SqlClient,
	userId: UUID,
	key: string,
	status: number,
	body: unknown,
): Promise<void> {
	await sql`
		update public.idempotency_keys
		set response_status = ${status},
			response_body = ${JSON.stringify(body ?? null)}::jsonb,
			completed_at = now()
		where user_id = ${userId} and key = ${key} and completed_at is null
	`;
}

export async function releaseIdempotencyKey(
	sql: SqlClient,
	userId: UUID,
	key: string,
): Promise<void> {
	await sql`
		delete from public.idempotency_keys
		where user_id = ${userId} and key = ${key} and completed_at is null
	`;
}

export interface IdempotencyPurgeOutcome {
	deleted_count: number;
	exhausted: boolean;
}

/**
 * Nightly cleanup; runs inside the existing retention job. Bounded the same
 * way as the body purge next door: a single batch could never drain a
 * backlog larger than itself, so the warning would repeat every night while
 * the table kept growing.
 */
export async function purgeExpiredIdempotencyKeys(
	sql: SqlClient,
	retentionHours: number = IDEMPOTENCY_RETENTION_HOURS,
	batchSize: number = IDEMPOTENCY_PURGE_BATCH_SIZE,
	maxBatches: number = IDEMPOTENCY_PURGE_MAX_BATCHES,
): Promise<IdempotencyPurgeOutcome> {
	const outcome: IdempotencyPurgeOutcome = { deleted_count: 0, exhausted: false };

	for (let batch = 0; batch < maxBatches; batch += 1) {
		const rows = await sql<{ key: string }[]>`
			with expired as (
				select ik.user_id, ik.key
				from public.idempotency_keys as ik
				where ik.created_at < now() - (${retentionHours} || ' hours')::interval
				order by ik.created_at asc
				limit ${batchSize}
			)
			delete from public.idempotency_keys as ik
			using expired
			where ik.user_id = expired.user_id and ik.key = expired.key
			returning ik.key
		`;
		outcome.deleted_count += rows.length;
		if (rows.length < batchSize) {
			return outcome;
		}
	}

	// Stopped at the bound with candidates still left for tomorrow.
	outcome.exhausted = true;
	return outcome;
}

export interface IdempotentResult {
	status: number;
	body: unknown;
}

/**
 * Runs `handler` under the caller's `Idempotency-Key`, or plainly when the
 * header is absent, and sends the response. Every database write the ledger
 * needs is awaited before the response goes out: a Worker is not guaranteed
 * to run anything after that.
 */
export async function withIdempotency(
	sql: SqlClient,
	userId: UUID,
	req: Request,
	res: Response,
	handler: () => Promise<IdempotentResult>,
): Promise<void> {
	const key = parseIdempotencyKey(req.header(IDEMPOTENCY_HEADER));
	if (key === null) {
		const result = await handler();
		res.status(result.status).json(result.body);
		return;
	}

	const requestHash = await hashRequestBody(req.body);
	const claim = await claimIdempotencyKey(sql, userId, key, requestHash);

	if (claim.kind === 'CONFLICT') {
		throw conflict(
			'IDEMPOTENCY_KEY_REUSED',
			`${IDEMPOTENCY_HEADER} was already used for a different request body`,
		);
	}

	if (claim.kind === 'IN_FLIGHT') {
		throw conflict(
			'IDEMPOTENCY_KEY_IN_FLIGHT',
			`A request with this ${IDEMPOTENCY_HEADER} is still in progress`,
		);
	}

	if (claim.kind === 'REPLAY') {
		res.setHeader(IDEMPOTENT_REPLAY_HEADER, 'true');
		res.status(claim.status).json(claim.body);
		return;
	}

	let result: IdempotentResult;
	try {
		result = await handler();
	} catch (error) {
		// Nothing was sent, so nothing is worth replaying; free the key so the
		// caller can retry the same request.
		await releaseIdempotencyKey(sql, userId, key).catch((releaseError: unknown) => {
			console.warn('IDEMPOTENCY_RELEASE_FAILED', {
				userId,
				message: releaseError instanceof Error ? releaseError.message : 'unknown',
			});
		});
		throw error;
	}

	// The write already happened. If recording it fails, the caller must
	// still get their 201 — turning a successful create into a 500 is the
	// one outcome guaranteed to make them retry and duplicate it. The claim
	// stays in place, so a retry inside the reclaim window is refused rather
	// than run twice.
	try {
		await completeIdempotencyKey(sql, userId, key, result.status, result.body);
	} catch (error) {
		console.error('IDEMPOTENCY_COMPLETE_FAILED', {
			userId,
			status: result.status,
			message: error instanceof Error ? error.message : 'unknown',
		});
	}

	res.status(result.status).json(result.body);
}
