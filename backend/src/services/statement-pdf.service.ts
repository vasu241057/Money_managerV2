// Statement-PDF fallback: when a statement email's text lacks bill fields
// (HDFC prints nothing in the body), the PDF attachment is fetched from
// Gmail, decrypted with the card's stored password, and field-matched IN
// CODE — the PDF's bytes and text never reach any AI and are never
// persisted; only the extracted fields are saved.

import { getDocumentProxy } from 'unpdf';

import type { UUID } from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import {
	decodeBase64Url,
	extractStatementFieldsFromItems,
	pickPdfAttachment,
	validatePdfFields,
	type GmailMessagePart,
	type PdfStatementFields,
	type PdfTextItem,
} from '../lib/pdf-statement';
import { toNullableString, toRequiredString } from '../lib/db/serialization';
import {
	decryptTokenValue,
	getGoogleRefreshConfig,
	gmailGetAttachment,
	gmailGetMessage,
	listEligibleConnectionsForUser,
	refreshAccessTokenWithOcc,
} from '../workers/email-sync-fetcher';

type FetchLike = typeof fetch;

const MAX_PDF_PAGES = 3;
// Every stored password is tried (matched card's first): with no last4 to
// prioritize (empty-body emails) the right one can be anywhere, and a wrong
// password fails in milliseconds.
const MAX_PASSWORD_ATTEMPTS = 20;

interface CardPasswordCandidate {
	id: string;
	last4: string;
	pdf_password_encrypted: string | null;
}

// Loads active cards carrying a stored PDF password; the matched card's
// password is tried first, then the rest (same-bank formulas differ per
// card, and a wrong password just fails fast).
async function loadPasswordCandidates(
	sql: SqlClient,
	userId: UUID,
	preferredLast4: string | null,
): Promise<CardPasswordCandidate[]> {
	const rows = await sql<{ id: unknown; last4: unknown; pdf_password: unknown }[]>`
		select ucc.id, ucc.last4, ucc.pdf_password
		from public.user_credit_cards as ucc
		where ucc.user_id = ${userId}
			and ucc.is_active = true
			and ucc.pdf_password is not null
		order by ucc.created_at asc
	`;
	const candidates = rows.map(row => ({
		id: toRequiredString(row.id, 'user_credit_cards.id'),
		last4: toRequiredString(row.last4, 'user_credit_cards.last4'),
		pdf_password_encrypted: toNullableString(row.pdf_password, 'user_credit_cards.pdf_password'),
	}));
	candidates.sort((a, b) =>
		Number(b.last4 === preferredLast4) - Number(a.last4 === preferredLast4),
	);
	return candidates.slice(0, MAX_PASSWORD_ATTEMPTS);
}

async function extractTextItems(
	pdfBytes: Uint8Array,
	password: string | undefined,
): Promise<PdfTextItem[] | 'bad_password' | null> {
	let pdf;
	try {
		pdf = await getDocumentProxy(pdfBytes, password ? { password } : {});
	} catch (error) {
		const name = (error as { name?: string })?.name ?? '';
		const message = error instanceof Error ? error.message : '';
		if (name === 'PasswordException' || /password/i.test(message)) {
			return 'bad_password';
		}
		return null;
	}

	try {
		const items: PdfTextItem[] = [];
		const pageCount = Math.min(pdf.numPages, MAX_PDF_PAGES);
		for (let pageNumber = 1; pageNumber <= pageCount; pageNumber++) {
			const page = await pdf.getPage(pageNumber);
			const content = await page.getTextContent();
			for (const raw of content.items) {
				const item = raw as { str?: string; transform?: number[] };
				if (typeof item.str !== 'string' || item.str.trim().length === 0) continue;
				const transform = Array.isArray(item.transform) ? item.transform : [];
				items.push({
					text: item.str,
					x: Number(transform[4] ?? 0),
					y: Number(transform[5] ?? 0),
					page: pageNumber,
				});
			}
		}
		return items;
	} catch {
		return null;
	} finally {
		try {
			await pdf.destroy();
		} catch {
			// best-effort cleanup
		}
	}
}

export interface PdfCompletionResult {
	fields: PdfStatementFields | null;
	reason:
		| 'ok'
		| 'no_connection'
		| 'no_attachment'
		| 'fetch_failed'
		| 'no_password'
		| 'bad_password'
		| 'parse_failed';
}

// Attempts to complete missing statement fields from the email's PDF.
// Every failure is soft: the statement row keeps whatever the email text
// yielded and the card surface offers manual entry.
export async function completeStatementFromPdf(
	sql: SqlClient,
	env: Env,
	rawEmailId: UUID,
	userId: UUID,
	internalDateIso: string,
	preferredLast4: string | null,
	fetcher: FetchLike = fetch,
): Promise<PdfCompletionResult> {
	const emailRows = await sql<{ source_id: unknown; oauth_connection_id: unknown }[]>`
		select re.source_id, re.oauth_connection_id
		from public.raw_emails as re
		where re.id = ${rawEmailId}
			and re.user_id = ${userId}
		limit 1
	`;
	const emailRow = emailRows[0];
	if (!emailRow) return { fields: null, reason: 'no_connection' };
	const sourceId = toRequiredString(emailRow.source_id, 'raw_emails.source_id');
	const connectionId = toNullableString(
		emailRow.oauth_connection_id,
		'raw_emails.oauth_connection_id',
	);

	// Only the row's own connection: Gmail ids are per-mailbox, and another
	// connection would 404 or fetch a different user's mail.
	const connections = await listEligibleConnectionsForUser(sql, userId);
	const connection = connections.find(candidate => candidate.id === connectionId);
	if (!connection) return { fields: null, reason: 'no_connection' };

	const googleConfig = getGoogleRefreshConfig(env);
	const refreshOutcome = await refreshAccessTokenWithOcc(sql, connection, googleConfig, fetcher);
	if (refreshOutcome.revoked || refreshOutcome.inactive) {
		return { fields: null, reason: 'no_connection' };
	}
	const accessToken = refreshOutcome.accessToken;

	const messageResult = await gmailGetMessage(accessToken, sourceId, fetcher, 'full');
	if (messageResult.revoked || messageResult.paused || !messageResult.message) {
		return { fields: null, reason: 'fetch_failed' };
	}
	const payload = (messageResult.message as { payload?: GmailMessagePart }).payload;
	const attachment = pickPdfAttachment(payload);
	if (!attachment) return { fields: null, reason: 'no_attachment' };

	const attachmentResult = await gmailGetAttachment(
		accessToken,
		sourceId,
		attachment.attachmentId,
		fetcher,
	);
	if (attachmentResult.revoked || attachmentResult.paused || !attachmentResult.data) {
		return { fields: null, reason: 'fetch_failed' };
	}
	const pdfBytes = decodeBase64Url(attachmentResult.data);

	// Unprotected first, then stored passwords (matched card's first).
	const unprotected = await extractTextItems(pdfBytes, undefined);
	let items: PdfTextItem[] | null = unprotected === 'bad_password' ? null : unprotected;
	let sawBadPassword = unprotected === 'bad_password';

	if (!items) {
		const candidates = await loadPasswordCandidates(sql, userId, preferredLast4);
		if (candidates.length === 0 && sawBadPassword) {
			return { fields: null, reason: 'no_password' };
		}
		for (const candidate of candidates) {
			if (!candidate.pdf_password_encrypted) continue;
			let password: string;
			try {
				password = await decryptTokenValue(
					candidate.pdf_password_encrypted,
					googleConfig.tokenEncryptionKey,
				);
			} catch {
				continue;
			}
			const attempt = await extractTextItems(pdfBytes, password);
			if (attempt === 'bad_password') {
				sawBadPassword = true;
				continue;
			}
			if (attempt) {
				items = attempt;
				break;
			}
		}
	}

	if (!items) {
		return { fields: null, reason: sawBadPassword ? 'bad_password' : 'parse_failed' };
	}

	const fields = validatePdfFields(
		extractStatementFieldsFromItems(items, internalDateIso),
		internalDateIso,
	);
	return { fields, reason: 'ok' };
}
