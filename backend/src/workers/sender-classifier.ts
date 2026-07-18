import type { SenderBackfillJobPayload, SenderClassificationJobPayload, UUID } from '../../../shared/types';
import { SENDER_BACKFILL_MAX_IDS } from '../../../shared/types';
import { resolveAiProviderConfig } from '../lib/ai';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import { toRequiredString } from '../lib/db/serialization';
import { isAllowedSender, resolveSenderGateConfig } from '../lib/sender-gate';
import { TransientMessageError } from './queue.errors';

const AI_CHAT_TIMEOUT_MS = 30_000;
const MAX_SENDERS_PER_RUN = 200;
const GATED_SENDER_LOOKBACK_DAYS = 14;
// AI DENY rules get a second look every 30 days so one bad classification
// can't silently gate a real bank forever. SEED/USER rules are never
// re-asked. Capped so re-asks can't crowd out never-classified senders.
const DENY_REASK_DAYS = 30;
const MAX_REASK_PER_RUN = 50;
// Subjects (which we store even for gated mail — bodies are never
// downloaded) give the model context beyond the bare address. Digits are
// masked before anything leaves.
const MAX_SUBJECTS_PER_SENDER = 3;
const MAX_SUBJECT_LENGTH = 120;

const VALID_SENDER_TYPES = new Set(['BANK', 'FINTECH', 'WALLET', 'OTHER']);

interface GatedSenderRowRaw {
	from_address: unknown;
}

interface SenderSubjectRowRaw {
	from_address: unknown;
	subject: unknown;
}

interface SenderRuleRowRaw {
	pattern: unknown;
	decision: unknown;
}

interface BackfillCandidateRowRaw {
	id: unknown;
	user_id: unknown;
}

interface ClassifiedSender {
	address: string;
	financial: boolean;
	sender_type: 'BANK' | 'FINTECH' | 'WALLET' | 'OTHER';
}

export interface SenderClassificationResult {
	candidate_count: number;
	reask_candidate_count: number;
	classified_count: number;
	allow_rule_count: number;
	deny_rule_count: number;
	backfill_raw_email_count: number;
	backfill_job_count: number;
	skipped_reason: string | null;
}

function createEmptyResult(): SenderClassificationResult {
	return {
		candidate_count: 0,
		reask_candidate_count: 0,
		classified_count: 0,
		allow_rule_count: 0,
		deny_rule_count: 0,
		backfill_raw_email_count: 0,
		backfill_job_count: 0,
		skipped_reason: null,
	};
}

export interface SenderCandidate {
	address: string;
	subjects: string[];
}

// Digits are stripped before a subject leaves for the model — amounts,
// OTPs, and reference numbers in subject lines stay local.
export function maskSubjectForClassifier(subject: string): string {
	return subject.replace(/\d/g, '#').replace(/\s+/g, ' ').trim().slice(0, MAX_SUBJECT_LENGTH);
}

function buildClassificationPrompt(
	candidates: SenderCandidate[],
): Array<{ role: string; content: string }> {
	return [
		{
			role: 'system',
			content: [
				'You classify email senders for an Indian personal finance app.',
				'Each input has the sender "address" and a sample of recent "subjects"',
				'from that sender (digits masked as #). Decide if the sender is a',
				'financial institution that sends TRANSACTION alerts: a bank, credit',
				'card issuer, fintech/neobank, wallet, or payment app operating in India.',
				'Subjects like "A/c ##### debited" or "Payment of Rs.### received" are',
				'strong transaction signals; pure marketing subjects are not.',
				'Marketing-only senders, newsletters, e-commerce stores, and personal',
				'mailboxes are NOT financial. Free-mail addresses (gmail.com, yahoo.com,',
				'outlook.com, etc.) are NOT financial regardless of the local part.',
				'',
				'Return strict JSON only, no markdown, exactly:',
				'{ "senders": [ { "address": "<input address>", "financial": true|false, "type": "BANK"|"FINTECH"|"WALLET"|"OTHER" } ] }',
				'Include every input address exactly once, unchanged.',
			].join('\n'),
		},
		{
			role: 'user',
			content: JSON.stringify({ senders: candidates }),
		},
	];
}

async function callSenderClassificationModel(
	env: Env,
	apiKey: string,
	candidates: SenderCandidate[],
): Promise<ClassifiedSender[] | null> {
	const controller = new AbortController();
	const providerConfig = resolveAiProviderConfig(env);
	const timeoutId = setTimeout(() => controller.abort(), AI_CHAT_TIMEOUT_MS);

	let response: Response;
	try {
		response = await fetch(providerConfig.chatUrl, {
			method: 'POST',
			headers: {
				Authorization: `Bearer ${apiKey}`,
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({
				model: providerConfig.dailyModel,
				temperature: 0,
				messages: buildClassificationPrompt(candidates),
				response_format: { type: 'json_object' },
			}),
			signal: controller.signal,
		});
	} catch (error) {
		clearTimeout(timeoutId);
		if (error instanceof Error && error.name === 'AbortError') {
			throw new TransientMessageError('Sender classification request timed out');
		}
		throw new TransientMessageError(
			`Sender classification network error: ${error instanceof Error ? error.message : 'Unknown'}`,
		);
	}
	clearTimeout(timeoutId);

	if (response.status === 429 || response.status === 408 || response.status >= 500) {
		throw new TransientMessageError(`Sender classification transient HTTP ${response.status}`);
	}
	if (!response.ok) {
		console.error('SENDER_CLASSIFIER_HTTP_ERROR', { status: response.status });
		return null;
	}

	let content: string | undefined;
	try {
		const json = (await response.json()) as {
			choices?: Array<{ message?: { content?: string } }>;
		};
		content = json.choices?.[0]?.message?.content;
	} catch {
		content = undefined;
	}
	if (!content) {
		console.error('SENDER_CLASSIFIER_EMPTY_RESPONSE');
		return null;
	}

	const cleaned = content
		.replace(/^```(?:json)?\s*/i, '')
		.replace(/\s*```\s*$/, '')
		.trim();

	let parsed: { senders?: unknown };
	try {
		parsed = JSON.parse(cleaned) as { senders?: unknown };
	} catch {
		console.error('SENDER_CLASSIFIER_MALFORMED_JSON');
		return null;
	}
	if (!Array.isArray(parsed.senders)) {
		console.error('SENDER_CLASSIFIER_MISSING_SENDERS');
		return null;
	}

	// Only accept entries whose address is one of the inputs — the model must
	// not be able to inject arbitrary allowlist patterns.
	const candidateSet = new Set(candidates.map(candidate => candidate.address));
	const seen = new Set<string>();
	const classified: ClassifiedSender[] = [];
	for (const entry of parsed.senders) {
		if (typeof entry !== 'object' || entry === null) continue;
		const record = entry as Record<string, unknown>;
		const address = typeof record.address === 'string' ? record.address.trim().toLowerCase() : null;
		if (!address || !candidateSet.has(address) || seen.has(address)) continue;
		if (typeof record.financial !== 'boolean') continue;

		const rawType = typeof record.type === 'string' ? record.type.toUpperCase() : 'OTHER';
		const senderType = VALID_SENDER_TYPES.has(rawType)
			? (rawType as ClassifiedSender['sender_type'])
			: 'OTHER';

		seen.add(address);
		classified.push({ address, financial: record.financial, sender_type: senderType });
	}
	return classified;
}

async function loadUnruledGatedSenders(sql: SqlClient, env: Env): Promise<string[]> {
	const gatedRows = await sql<GatedSenderRowRaw[]>`
		select distinct re.from_address
		from public.raw_emails as re
		where re.status = 'IGNORED'
			and re.clean_text = ''
			and re.from_address is not null
			and re.created_at > now() - (${GATED_SENDER_LOOKBACK_DAYS} || ' days')::interval
	`;

	const ruleRows = await sql<SenderRuleRowRaw[]>`
		select sr.pattern, sr.decision
		from public.sender_rules as sr
	`;
	const ruledPatterns = new Set<string>();
	const allowEntries: string[] = [];
	for (const row of ruleRows) {
		if (typeof row.pattern !== 'string') continue;
		const pattern = row.pattern.trim().toLowerCase();
		if (pattern.length === 0) continue;
		ruledPatterns.add(pattern);
		if (row.decision === 'ALLOW') {
			allowEntries.push(pattern);
		}
	}

	const gate = resolveSenderGateConfig(env);
	const entries = [...gate.entries, ...allowEntries];

	const candidates: string[] = [];
	for (const row of gatedRows) {
		if (typeof row.from_address !== 'string') continue;
		const address = row.from_address.trim().toLowerCase();
		if (address.length === 0) continue;
		// Skip already-ruled addresses and anything the gate now allows.
		if (ruledPatterns.has(address)) continue;
		if (isAllowedSender(address, address, entries)) continue;
		candidates.push(address);
		if (candidates.length >= MAX_SENDERS_PER_RUN) break;
	}
	return candidates;
}

// AI-created DENY rules older than the re-ask window get reconsidered with
// fresh subject context. Only source 'AI': seed and user decisions are final.
async function loadDenyReaskAddresses(sql: SqlClient): Promise<string[]> {
	const rows = await sql<Array<{ pattern: unknown }>>`
		select sr.pattern
		from public.sender_rules as sr
		where sr.source = 'AI'
			and sr.decision = 'DENY'
			and sr.updated_at < now() - (${DENY_REASK_DAYS} || ' days')::interval
		order by sr.updated_at asc
		limit ${MAX_REASK_PER_RUN}
	`;

	const addresses: string[] = [];
	for (const row of rows) {
		if (typeof row.pattern !== 'string') continue;
		const address = row.pattern.trim().toLowerCase();
		if (address.length > 0) addresses.push(address);
	}
	return addresses;
}

// Fetches up to MAX_SUBJECTS_PER_SENDER recent distinct subjects per
// address, digit-masked. Bodies are never touched — subjects are the only
// content stored for gated mail.
async function loadMaskedSubjectsBySender(
	sql: SqlClient,
	addresses: string[],
): Promise<Map<string, string[]>> {
	const bySender = new Map<string, string[]>();
	if (addresses.length === 0) {
		return bySender;
	}

	const rows = await sql<SenderSubjectRowRaw[]>`
		select re.from_address, re.subject
		from public.raw_emails as re
		where re.from_address = any(${addresses}::text[])
			and re.subject is not null
		order by re.internal_date desc
		limit ${addresses.length * 20}
	`;

	for (const row of rows) {
		if (typeof row.from_address !== 'string' || typeof row.subject !== 'string') continue;
		const address = row.from_address.trim().toLowerCase();
		const masked = maskSubjectForClassifier(row.subject);
		if (masked.length === 0) continue;
		const list = bySender.get(address) ?? [];
		if (list.length >= MAX_SUBJECTS_PER_SENDER || list.includes(masked)) continue;
		list.push(masked);
		bySender.set(address, list);
	}
	return bySender;
}

async function persistSenderRules(
	sql: SqlClient,
	classified: ClassifiedSender[],
): Promise<{ allowCount: number; denyCount: number; allowedAddresses: string[] }> {
	let allowCount = 0;
	let denyCount = 0;
	const allowedAddresses: string[] = [];

	for (const sender of classified) {
		const decision = sender.financial ? 'ALLOW' : 'DENY';
		// AI rules are always exact addresses (never tokens/domains) so a
		// misclassification can't open the gate wider than one sender. A
		// conflict updates only AI rows (that's the DENY re-ask flipping or
		// re-confirming; updated_at restarts its 30-day clock) — SEED/USER
		// rules are never overwritten.
		const upserted = await sql<{ pattern: string }[]>`
			insert into public.sender_rules (pattern, decision, sender_type, source)
			values (${sender.address}, ${decision}, ${sender.sender_type}, 'AI')
			on conflict (pattern) do update
			set decision = excluded.decision,
				sender_type = excluded.sender_type,
				updated_at = now()
			where sender_rules.source = 'AI'
			returning pattern
		`;
		if (upserted.length === 0) {
			continue;
		}
		if (sender.financial) {
			allowCount += 1;
			allowedAddresses.push(sender.address);
		} else {
			denyCount += 1;
		}
	}

	return { allowCount, denyCount, allowedAddresses };
}

async function enqueueBackfillJobs(
	sql: SqlClient,
	queue: Queue,
	allowedAddresses: string[],
): Promise<{ rawEmailCount: number; jobCount: number }> {
	if (allowedAddresses.length === 0) {
		return { rawEmailCount: 0, jobCount: 0 };
	}

	const rows = await sql<BackfillCandidateRowRaw[]>`
		select re.id, re.user_id
		from public.raw_emails as re
		where re.from_address = any(${allowedAddresses}::text[])
			and re.status = 'IGNORED'
			and re.clean_text = ''
	`;

	const idsByUser = new Map<UUID, UUID[]>();
	for (const row of rows) {
		const userId = toRequiredString(row.user_id, 'raw_emails.user_id');
		const rawEmailId = toRequiredString(row.id, 'raw_emails.id');
		const list = idsByUser.get(userId) ?? [];
		list.push(rawEmailId);
		idsByUser.set(userId, list);
	}

	let jobCount = 0;
	for (const [userId, rawEmailIds] of idsByUser) {
		for (let index = 0; index < rawEmailIds.length; index += SENDER_BACKFILL_MAX_IDS) {
			const payload: SenderBackfillJobPayload = {
				job_type: 'SENDER_BACKFILL',
				user_id: userId,
				raw_email_ids: rawEmailIds.slice(index, index + SENDER_BACKFILL_MAX_IDS),
			};
			await queue.send(payload, { contentType: 'json' });
			jobCount += 1;
		}
	}

	return { rawEmailCount: rows.length, jobCount };
}

export async function runSenderClassificationJob(
	_job: SenderClassificationJobPayload,
	env: Env,
): Promise<SenderClassificationResult> {
	const result = createEmptyResult();

	const providerConfig = resolveAiProviderConfig(env);
	const apiKey = providerConfig.apiKey;
	if (!apiKey) {
		result.skipped_reason = `${providerConfig.apiKeyEnvVar} not set`;
		return result;
	}

	const config = getAppConfig(env);
	const sql = getSqlClient(config);

	const newAddresses = await loadUnruledGatedSenders(sql, env);
	result.candidate_count = newAddresses.length;

	const reaskAddresses = await loadDenyReaskAddresses(sql);
	result.reask_candidate_count = reaskAddresses.length;

	const allAddresses = [...new Set([...newAddresses, ...reaskAddresses])];
	if (allAddresses.length === 0) {
		return result;
	}

	const subjectsBySender = await loadMaskedSubjectsBySender(sql, allAddresses);
	const candidates: SenderCandidate[] = allAddresses.map(address => ({
		address,
		subjects: subjectsBySender.get(address) ?? [],
	}));

	const classified = await callSenderClassificationModel(env, apiKey, candidates);
	if (classified === null) {
		result.skipped_reason = 'invalid model response';
		return result;
	}
	result.classified_count = classified.length;

	const persisted = await persistSenderRules(sql, classified);
	result.allow_rule_count = persisted.allowCount;
	result.deny_rule_count = persisted.denyCount;

	const backfill = await enqueueBackfillJobs(
		sql,
		env.EMAIL_SYNC_QUEUE,
		persisted.allowedAddresses,
	);
	result.backfill_raw_email_count = backfill.rawEmailCount;
	result.backfill_job_count = backfill.jobCount;

	return result;
}
