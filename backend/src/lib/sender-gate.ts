// Sender allowlist gate for the email sync pipeline.
//
// Only mail from known bank/fintech alert senders should ever be stored with
// its body or reach a normalization engine (and therefore a third-party AI
// call). Everything else is persisted as metadata-only (for dedupe and
// auditing) with status IGNORED.

import type { SqlClient } from './db/client';

// Entry kinds, decided by shape:
// - contains '@'  → exact full-address match against the parsed From address
// - contains '.'  → domain match (exact or subdomain suffix) against the
//                   parsed From address domain
// - otherwise     → brand token, substring-matched against the ENTIRE From
//                   header (display name + address), lowercased
//
// Brand tokens are deliberately short and permissive: banks rotate sender
// addresses and domains ("federal.bank", "federalbank.co.in", display name
// "Federal Bank" all contain "federal"). A rare non-bank sender that happens
// to match costs one body fetch and is rejected downstream by the
// normalizers — an accepted tradeoff over missing real bank mail.
//
// The exhaustive allowlist lives in the sender_rules table (seeded by
// migration, grown by the daily AI sender classification job). Code only
// contributes the EMAIL_SENDER_ALLOWLIST_EXTRA env escape hatch.

export interface SenderGateConfig {
	enabled: boolean;
	entries: readonly string[];
}

function normalizeText(value: string | undefined): string | null {
	if (value === undefined) {
		return null;
	}
	const normalized = value.trim();
	return normalized.length > 0 ? normalized : null;
}

export function resolveSenderGateConfig(env: Env): SenderGateConfig {
	const envRecord = env as unknown as Record<string, unknown>;
	const gateRaw =
		typeof envRecord.EMAIL_SENDER_GATE === 'string'
			? envRecord.EMAIL_SENDER_GATE.trim().toLowerCase()
			: '';
	const enabled = !(gateRaw === 'off' || gateRaw === 'false' || gateRaw === '0');

	const extraRaw =
		typeof envRecord.EMAIL_SENDER_ALLOWLIST_EXTRA === 'string'
			? envRecord.EMAIL_SENDER_ALLOWLIST_EXTRA
			: '';
	const extraEntries = extraRaw
		.split(',')
		.map(entry => entry.trim().toLowerCase())
		.filter(entry => entry.length > 0);

	return {
		enabled,
		entries: extraEntries,
	};
}

// Loads dynamic ALLOW patterns from sender_rules (written by the daily AI
// sender classification job and manual additions). Callers merge these with
// the built-in entries.
export async function loadSenderRuleEntries(sql: SqlClient): Promise<string[]> {
	const rows = await sql<Array<{ pattern: unknown }>>`
		select sr.pattern
		from public.sender_rules as sr
		where sr.decision = 'ALLOW'
	`;

	const entries: string[] = [];
	for (const row of rows) {
		if (typeof row.pattern !== 'string') {
			continue;
		}
		const normalized = row.pattern.trim().toLowerCase();
		if (normalized.length > 0) {
			entries.push(normalized);
		}
	}
	return entries;
}

// Parses "Display Name <alerts@hdfcbank.net>" or a bare address to a
// normalized lowercase email address. Returns null when nothing parseable.
export function parseFromAddress(header: string | null | undefined): string | null {
	const normalized = normalizeText(header ?? undefined);
	if (!normalized) {
		return null;
	}

	const angled = normalized.match(/<([^<>\s]+@[^<>\s]+)>/);
	const candidate = angled?.[1] ?? normalized;
	const match = candidate.match(/[A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/);
	return match ? match[0].toLowerCase() : null;
}

export function isAllowedSender(
	fromHeader: string | null,
	fromAddress: string | null,
	entries: readonly string[],
): boolean {
	const header = (fromHeader ?? fromAddress ?? '').toLowerCase();
	if (header.length === 0) {
		return false;
	}

	const address = fromAddress ? fromAddress.toLowerCase() : null;
	const atIndex = address ? address.lastIndexOf('@') : -1;
	const domain = address && atIndex > 0 ? address.slice(atIndex + 1) : null;

	for (const entry of entries) {
		if (entry.includes('@')) {
			if (address === entry) {
				return true;
			}
			continue;
		}
		if (entry.includes('.')) {
			if (domain && (domain === entry || domain.endsWith(`.${entry}`))) {
				return true;
			}
			continue;
		}
		if (header.includes(entry)) {
			return true;
		}
	}
	return false;
}
