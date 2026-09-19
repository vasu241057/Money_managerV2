// Account deletion (Task 5). Order matters:
//
//   1. Revoke the Google grant while the encrypted refresh tokens still
//      exist (best-effort: Google unreachable must not block deletion).
//   2. One transaction: drop the mapper's memory of counterparty keys only
//      this user ever paid, delete the user's PRIVATE templates (SHARED rows
//      carry no owner and stay), delete the users row so every user-owned
//      table cascades, and write the PII-free tombstone.
//   3. Delete the identity-provider user. Best-effort: when it fails the
//      person can sign in again and gets a fresh, empty account under the
//      same id until the nightly retry removes the provider user — or drops
//      the tombstone if they did come back.
//
// The database deletion comes before the provider deletion on purpose: the
// reverse order could leave data that nobody can sign in to delete.

import { DELETE_ACCOUNT_CONFIRMATION, type UUID } from '../../../shared/types';
import { deleteClerkUser, type FetchLike } from '../lib/clerk-admin';
import type { SqlClient } from '../lib/db/client';
import { toNullableString } from '../lib/db/serialization';
import { badRequest, notFound } from '../lib/http/errors';
import { asRecord } from '../lib/http/validation';
import { revokeGoogleOAuthTokens } from './oauth.service';

const AUTH_PROVIDER_CLERK = 'clerk';

export function parseDeleteAccountRequest(payload: unknown): void {
	const body = asRecord(
		payload,
		`Request body must be a JSON object with confirm: "${DELETE_ACCOUNT_CONFIRMATION}"`,
	);
	if (body.confirm !== DELETE_ACCOUNT_CONFIRMATION) {
		throw badRequest(
			'CONFIRMATION_REQUIRED',
			`Send { "confirm": "${DELETE_ACCOUNT_CONFIRMATION}" } to delete the account`,
		);
	}
}

export interface DeleteAccountOutcome {
	user_id: UUID;
	google_connection_count: number;
	google_revoked: boolean;
	private_template_count: number;
	mapping_attempt_count: number;
	clerk_deleted: boolean;
	clerk_error: string | null;
}

interface UserIdentityRowRaw {
	auth_provider: unknown;
	auth_subject: unknown;
}

export async function deleteAccount(
	sql: SqlClient,
	userId: UUID,
	env: Env,
	fetcher: FetchLike = fetch,
): Promise<DeleteAccountOutcome> {
	const identityRows = await sql<UserIdentityRowRaw[]>`
		select u.auth_provider, u.auth_subject
		from public.users as u
		where u.id = ${userId}
		limit 1
	`;
	const identity = identityRows[0];
	if (!identity) {
		throw notFound('USER_NOT_FOUND', 'Account does not exist');
	}
	const authProvider = toNullableString(identity.auth_provider, 'users.auth_provider');
	const authSubject = toNullableString(identity.auth_subject, 'users.auth_subject');

	const revocation = await revokeGoogleOAuthTokens(sql, userId, env, fetcher);
	const googleRevoked = revocation.failed_count === 0;
	if (!googleRevoked) {
		console.warn('ACCOUNT_DELETION_GOOGLE_REVOKE_INCOMPLETE', { userId, ...revocation });
	}

	const deleted = await sql.begin(async (tx) => {
		const txSql = tx as unknown as SqlClient;

		// Keys that only this user's events ever carried: the mapper's memory
		// of them (including a person's display name) has no one left to
		// serve. Keys shared with another user stay.
		const attempts = await txSql<{ search_key: string }[]>`
			delete from public.merchant_mapping_attempts as mma
			where exists (
					select 1 from public.financial_events as fe
					where fe.user_id = ${userId} and fe.search_key = mma.search_key
				)
				and not exists (
					select 1 from public.financial_events as other
					where other.user_id <> ${userId} and other.search_key = mma.search_key
				)
			returning mma.search_key
		`;

		const templates = await txSql<{ id: UUID }[]>`
			delete from public.email_templates as et
			where et.scope = 'PRIVATE' and et.learned_from_user_id = ${userId}
			returning et.id
		`;

		// user_merchant_rule_categories hangs off user_merchant_rules (no
		// user_id of its own) and its category_id FK is NO ACTION: during the
		// users cascade the categories rows go before the rules' children do,
		// and the check fires while these rows still exist, so the whole
		// cascade fails. Removing them first lets everything else cascade
		// (proven on the tenancy harness; one FK, no schema change).
		await txSql`
			delete from public.user_merchant_rule_categories as umrc
			where umrc.user_merchant_rule_id in (
				select umr.id from public.user_merchant_rules as umr where umr.user_id = ${userId}
			)
		`;

		const users = await txSql<{ id: UUID }[]>`
			delete from public.users as u
			where u.id = ${userId}
			returning u.id
		`;
		if (users.length === 0) {
			throw notFound('USER_NOT_FOUND', 'Account does not exist');
		}

		await txSql`
			insert into public.user_deletion_log (
				user_id, deleted_at, clerk_deleted, google_revoked, auth_provider, auth_subject
			)
			values (${userId}, now(), ${false}, ${googleRevoked}, ${authProvider}, ${authSubject})
			on conflict (user_id) do update
			set deleted_at = now(),
				clerk_deleted = false,
				google_revoked = excluded.google_revoked,
				auth_provider = excluded.auth_provider,
				auth_subject = excluded.auth_subject,
				clerk_delete_attempts = 0,
				clerk_last_attempt_at = null,
				clerk_last_error = null
		`;

		return { mappingAttemptCount: attempts.length, privateTemplateCount: templates.length };
	});

	let clerkDeleted = false;
	let clerkError: string | null = null;
	if (authProvider === AUTH_PROVIDER_CLERK && authSubject) {
		const outcome = await deleteClerkUser(env, authSubject, fetcher);
		if (outcome.ok) {
			clerkDeleted = true;
			await sql`
				update public.user_deletion_log
				set clerk_deleted = true,
					auth_subject = null,
					clerk_delete_attempts = 1,
					clerk_last_attempt_at = now(),
					clerk_last_error = null
				where user_id = ${userId}
			`;
		} else {
			clerkError = outcome.reason;
			await sql`
				update public.user_deletion_log
				set clerk_delete_attempts = 1,
					clerk_last_attempt_at = now(),
					clerk_last_error = ${outcome.reason}
				where user_id = ${userId}
			`;
			console.warn('ACCOUNT_DELETION_CLERK_FAILED', { userId, reason: outcome.reason });
		}
	} else {
		// Provisioned before Task 2 and never seen since: nothing to retry with.
		clerkError = 'no provider subject on record';
		console.warn('ACCOUNT_DELETION_CLERK_SUBJECT_MISSING', { userId });
	}

	return {
		user_id: userId,
		google_connection_count: revocation.connection_count,
		google_revoked: googleRevoked,
		private_template_count: deleted.privateTemplateCount,
		mapping_attempt_count: deleted.mappingAttemptCount,
		clerk_deleted: clerkDeleted,
		clerk_error: clerkError,
	};
}
