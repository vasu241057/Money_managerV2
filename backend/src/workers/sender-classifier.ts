import type { SenderBackfillJobPayload, SenderClassificationJobPayload, UUID } from '../../../shared/types';
import { SENDER_BACKFILL_MAX_IDS } from '../../../shared/types';
import { resolveOpenCodeGoApiKey, resolveOpenCodeGoModel } from '../lib/ai';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import { toRequiredString } from '../lib/db/serialization';
import { isAllowedSender, resolveSenderGateConfig } from '../lib/sender-gate';
import { TransientMessageError } from './queue.errors';

const OPENCODE_GO_CHAT_URL = 'https://opencode.ai/zen/go/v1/chat/completions';
const OPENCODE_GO_TIMEOUT_MS = 30_000;
const MAX_SENDERS_PER_RUN = 200;
const GATED_SENDER_LOOKBACK_DAYS = 14;

const VALID_SENDER_TYPES = new Set(['BANK', 'FINTECH', 'WALLET', 'OTHER']);

interface GatedSenderRowRaw {
	from_address: unknown;
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
		classified_count: 0,
		allow_rule_count: 0,
		deny_rule_count: 0,
		backfill_raw_email_count: 0,
		backfill_job_count: 0,
		skipped_reason: null,
	};
}

function buildClassificationPrompt(addresses: string[]): Array<{ role: string; content: string }> {
	return [
		{
			role: 'system',
			content: [
				'You classify email sender addresses for an Indian personal finance app.',
				'For each address, decide if it belongs to a financial institution that',
				'sends TRANSACTION alerts: a bank, credit card issuer, fintech/neobank,',
				'wallet, or payment app operating in India.',
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
			content: JSON.stringify({ addresses }),
		},
	];
}

async function callSenderClassificationModel(
	env: Env,
	apiKey: string,
	addresses: string[],
): Promise<ClassifiedSender[] | null> {
	const controller = new AbortController();
	const timeoutId = setTimeout(() => controller.abort(), OPENCODE_GO_TIMEOUT_MS);

	let response: Response;
	try {
		response = await fetch(OPENCODE_GO_CHAT_URL, {
			method: 'POST',
			headers: {
				Authorization: `Bearer ${apiKey}`,
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({
				model: resolveOpenCodeGoModel(env),
				temperature: 0,
				messages: buildClassificationPrompt(addresses),
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
	const candidateSet = new Set(addresses);
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
		// misclassification can't open the gate wider than one sender.
		const inserted = await sql<{ pattern: string }[]>`
			insert into public.sender_rules (pattern, decision, sender_type, source)
			values (${sender.address}, ${decision}, ${sender.sender_type}, 'AI')
			on conflict (pattern) do nothing
			returning pattern
		`;
		if (inserted.length === 0) {
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

	const apiKey = resolveOpenCodeGoApiKey(env);
	if (!apiKey) {
		result.skipped_reason = 'OPENCODE_GO_API_KEY not set';
		return result;
	}

	const config = getAppConfig(env);
	const sql = getSqlClient(config);

	const candidates = await loadUnruledGatedSenders(sql, env);
	result.candidate_count = candidates.length;
	if (candidates.length === 0) {
		return result;
	}

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
