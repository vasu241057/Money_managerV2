// Clerk Backend API, used only to delete a user after their account data is
// gone (Task 5). Best-effort by design: the caller records the outcome in
// user_deletion_log and the nightly job retries what failed.

const CLERK_API_BASE_URL = 'https://api.clerk.com/v1';

export interface FetchLike {
	(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

export type ClerkUserDeletionOutcome =
	| { ok: true; already_deleted: boolean }
	| { ok: false; reason: string };

export function resolveClerkSecretKey(env: Env): string | null {
	const value = env.CLERK_SECRET_KEY?.trim();
	return value ? value : null;
}

export async function deleteClerkUser(
	env: Env,
	clerkUserId: string,
	fetcher: FetchLike = fetch,
): Promise<ClerkUserDeletionOutcome> {
	const secretKey = resolveClerkSecretKey(env);
	if (!secretKey) {
		return { ok: false, reason: 'CLERK_SECRET_KEY is not configured' };
	}

	const subject = clerkUserId.trim();
	if (subject.length === 0) {
		return { ok: false, reason: 'empty Clerk user id' };
	}

	let response: Response;
	try {
		response = await fetcher(`${CLERK_API_BASE_URL}/users/${encodeURIComponent(subject)}`, {
			method: 'DELETE',
			headers: {
				authorization: `Bearer ${secretKey}`,
				accept: 'application/json',
			},
		});
	} catch (error) {
		return {
			ok: false,
			reason: `network: ${error instanceof Error ? error.message : 'unknown'}`,
		};
	}

	if (response.ok) {
		return { ok: true, already_deleted: false };
	}
	// Already gone at Clerk (deleted from the dashboard, or a retry after a
	// success whose bookkeeping did not land): nothing left to do.
	if (response.status === 404) {
		return { ok: true, already_deleted: true };
	}
	return { ok: false, reason: `http ${response.status}` };
}
