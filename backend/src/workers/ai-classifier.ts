import type { TransactionStatus, UUID } from '../../../shared/types';
import { resolveAiAutoVerifyMinConfidence, resolveOpenRouterApiKey, resolveOpenRouterModel } from '../lib/ai';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import {
	toIsoDateTime,
	toNullableString,
	toRequiredString,
	toSafeInteger,
} from '../lib/db/serialization';
import { TransientMessageError } from './queue.errors';
import type { AiClassificationJob } from './queue.messages';

const OPENROUTER_CHAT_COMPLETIONS_URL = 'https://openrouter.ai/api/v1/chat/completions';
const OPENROUTER_TIMEOUT_MS = 15_000;
const MAX_RAW_EMAIL_TEXT_LENGTH = 1_400;
const STRICT_NUMERIC_STRING_REGEX = /^[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$/;

type AiDecisionAction = 'VERIFY' | 'KEEP_REVIEW';

interface AiCandidateRowRaw {
	transaction_id: unknown;
	user_id: unknown;
	transaction_status: unknown;
	transaction_type: unknown;
	transaction_amount_in_paise: unknown;
	transaction_txn_date: unknown;
	transaction_category_id: unknown;
	transaction_merchant_id: unknown;
	transaction_user_note: unknown;
	financial_event_status: unknown;
	financial_event_payment_method: unknown;
	financial_event_counterparty_raw: unknown;
	financial_event_search_key: unknown;
	raw_email_clean_text: unknown;
}

interface AiCandidateRow {
	transaction_id: UUID;
	user_id: UUID;
	transaction_status: TransactionStatus;
	transaction_type: string;
	transaction_amount_in_paise: number;
	transaction_txn_date: string;
	transaction_category_id: UUID | null;
	transaction_merchant_id: UUID | null;
	transaction_user_note: string | null;
	financial_event_status: string;
	financial_event_payment_method: string;
	financial_event_counterparty_raw: string | null;
	financial_event_search_key: string | null;
	raw_email_clean_text: string | null;
}

interface AiDecision {
	action: AiDecisionAction;
	confidence_score: number;
	reason: string | null;
}

interface AiUpdateRowRaw {
	transaction_id: unknown;
	transaction_status: unknown;
}

type AiCallResult =
	| { kind: 'decision'; decision: AiDecision }
	| { kind: 'invalid_response'; details: string }
	| { kind: 'non_retriable_http'; status: number; details: string };

export type AiClassificationOutcome =
	| 'UPDATED_VERIFIED'
	| 'UPDATED_NEEDS_REVIEW'
	| 'SKIPPED_MISSING_TRANSACTION'
	| 'SKIPPED_ALREADY_REVIEWED'
	| 'SKIPPED_INACTIVE_FINANCIAL_EVENT'
	| 'SKIPPED_AI_DISABLED'
	| 'SKIPPED_NON_RETRIABLE_HTTP'
	| 'SKIPPED_INVALID_AI_RESPONSE'
	| 'SKIPPED_STALE_UPDATE';

export interface AiClassificationResult {
	transaction_id: UUID;
	outcome: AiClassificationOutcome;
	transaction_status: TransactionStatus | null;
	confidence_score: number | null;
	action: AiDecisionAction | null;
	reason: string | null;
}

function asObject(value: unknown): Record<string, unknown> | null {
	if (typeof value !== 'object' || value === null || Array.isArray(value)) {
		return null;
	}

	return value as Record<string, unknown>;
}

function isValidConfidenceScore(value: number): boolean {
	return value >= 0 && value <= 1;
}

function parseConfidenceScore(value: unknown): number | null {
	if (typeof value === 'number' && Number.isFinite(value)) {
		return isValidConfidenceScore(value) ? value : null;
	}

	if (typeof value === 'string') {
		const normalized = value.trim();
		if (!STRICT_NUMERIC_STRING_REGEX.test(normalized)) {
			return null;
		}

		const parsed = Number(normalized);
		if (Number.isFinite(parsed)) {
			return isValidConfidenceScore(parsed) ? parsed : null;
		}
	}

	return null;
}

function normalizeReason(value: unknown): string | null {
	if (typeof value !== 'string') {
		return null;
	}

	const normalized = value.trim().slice(0, 240);
	return normalized.length > 0 ? normalized : null;
}

function parseDecisionAction(value: unknown): AiDecisionAction | null {
	if (typeof value !== 'string') {
		return null;
	}

	const normalized = value.trim().toUpperCase();
	if (normalized === 'VERIFY' || normalized === 'KEEP_REVIEW') {
		return normalized;
	}

	return null;
}

function parseAiDecisionPayload(payload: unknown): AiDecision | null {
	const record = asObject(payload);
	if (!record) {
		return null;
	}

	const action = parseDecisionAction(record.action);
	if (action === null) {
		return null;
	}

	const confidenceScore = parseConfidenceScore(record.confidence_score);
	if (confidenceScore === null) {
		return null;
	}

	return {
		action,
		confidence_score: confidenceScore,
		reason: normalizeReason(record.reason),
	};
}

function extractJsonObject(text: string): string | null {
	const normalized = text.trim();
	if (normalized.length === 0) {
		return null;
	}

	if (normalized.startsWith('{') && normalized.endsWith('}')) {
		return normalized;
	}

	const match = normalized.match(/\{[\s\S]*\}/);
	return match ? match[0] : null;
}

function extractModelTextContent(content: unknown): string | null {
	if (typeof content === 'string') {
		return content;
	}

	if (!Array.isArray(content)) {
		return null;
	}

	const parts: string[] = [];
	for (const item of content) {
		const record = asObject(item);
		const text = record?.text;
		if (typeof text === 'string') {
			parts.push(text);
		}
	}

	if (parts.length === 0) {
		return null;
	}

	return parts.join('\n');
}

function mapCandidateRow(row: AiCandidateRowRaw): AiCandidateRow {
	return {
		transaction_id: toRequiredString(row.transaction_id, 'transactions.id'),
		user_id: toRequiredString(row.user_id, 'transactions.user_id'),
		transaction_status: toRequiredString(
			row.transaction_status,
			'transactions.status',
		) as TransactionStatus,
		transaction_type: toRequiredString(row.transaction_type, 'transactions.type'),
		transaction_amount_in_paise: toSafeInteger(
			row.transaction_amount_in_paise,
			'transactions.amount_in_paise',
		),
		transaction_txn_date: toIsoDateTime(row.transaction_txn_date, 'transactions.txn_date'),
		transaction_category_id: toNullableString(row.transaction_category_id, 'transactions.category_id'),
		transaction_merchant_id: toNullableString(row.transaction_merchant_id, 'transactions.merchant_id'),
		transaction_user_note: toNullableString(row.transaction_user_note, 'transactions.user_note'),
		financial_event_status: toRequiredString(row.financial_event_status, 'financial_events.status'),
		financial_event_payment_method: toRequiredString(
			row.financial_event_payment_method,
			'financial_events.payment_method',
		),
		financial_event_counterparty_raw: toNullableString(
			row.financial_event_counterparty_raw,
			'financial_events.counterparty_raw',
		),
		financial_event_search_key: toNullableString(
			row.financial_event_search_key,
			'financial_events.search_key',
		),
		raw_email_clean_text: toNullableString(row.raw_email_clean_text, 'raw_emails.clean_text'),
	};
}

async function loadCandidate(sql: SqlClient, transactionId: UUID): Promise<AiCandidateRow | null> {
	const rows = await sql<AiCandidateRowRaw[]>`
		select
			t.id as transaction_id,
			t.user_id,
			t.status as transaction_status,
			t.type as transaction_type,
			t.amount_in_paise as transaction_amount_in_paise,
			t.txn_date as transaction_txn_date,
			t.category_id as transaction_category_id,
			t.merchant_id as transaction_merchant_id,
			t.user_note as transaction_user_note,
			fe.status as financial_event_status,
			fe.payment_method as financial_event_payment_method,
			fe.counterparty_raw as financial_event_counterparty_raw,
			fe.search_key as financial_event_search_key,
			re.clean_text as raw_email_clean_text
		from public.transactions as t
		join public.financial_events as fe
			on fe.id = t.financial_event_id
			and fe.user_id = t.user_id
		left join public.raw_emails as re
			on re.id = fe.raw_email_id
			and re.user_id = t.user_id
		where t.id = ${transactionId}
		limit 1
	`;

	if (rows.length === 0) {
		return null;
	}

	return mapCandidateRow(rows[0]);
}

function buildAiPromptContext(row: AiCandidateRow): string {
	const rawEmailText = row.raw_email_clean_text
		? row.raw_email_clean_text.slice(0, MAX_RAW_EMAIL_TEXT_LENGTH)
		: null;

	return JSON.stringify(
		{
			transaction_type: row.transaction_type,
			amount_in_paise: row.transaction_amount_in_paise,
			txn_date: row.transaction_txn_date,
			payment_method: row.financial_event_payment_method,
			search_key: row.financial_event_search_key,
			counterparty_raw: row.financial_event_counterparty_raw,
			user_note: row.transaction_user_note,
			has_category: row.transaction_category_id !== null,
			has_merchant: row.transaction_merchant_id !== null,
			raw_email_excerpt: rawEmailText,
		},
		null,
		2,
	);
}

async function callOpenRouterForDecision(params: {
	apiKey: string;
	model: string;
	candidate: AiCandidateRow;
}): Promise<AiCallResult> {
	const { apiKey, model, candidate } = params;
	const controller = new AbortController();
	const timeoutId = setTimeout(() => controller.abort(), OPENROUTER_TIMEOUT_MS);

	const requestBody = {
		model,
		temperature: 0,
		messages: [
			{
				role: 'system',
				content:
					'You classify Indian banking/UPI transactions for review triage. Return strict JSON only.',
			},
			{
				role: 'user',
				content: [
					'Decide if this transaction can be auto-verified.',
					'Rules:',
					'- VERIFY only for clear financial transactions with low ambiguity.',
					'- KEEP_REVIEW for unclear merchant, unclear category, promo/reward/OTP style content, or uncertainty.',
					'Return JSON exactly with keys: action, confidence_score, reason.',
					'action must be VERIFY or KEEP_REVIEW.',
					'confidence_score must be a number between 0 and 1.',
					'reason should be short.',
					'Transaction context:',
					buildAiPromptContext(candidate),
				].join('\n'),
			},
		],
	};

	let response: Response;
	try {
		response = await fetch(OPENROUTER_CHAT_COMPLETIONS_URL, {
			method: 'POST',
			headers: {
				Authorization: `Bearer ${apiKey}`,
				'Content-Type': 'application/json',
			},
			body: JSON.stringify(requestBody),
			signal: controller.signal,
		});
	} catch (error) {
		clearTimeout(timeoutId);
		if (error instanceof Error && error.name === 'AbortError') {
			throw new TransientMessageError('OpenRouter request timed out');
		}
		throw new TransientMessageError(
			`OpenRouter request failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
		);
	}
	clearTimeout(timeoutId);

	if (!response.ok) {
		const details = await response.text().catch(() => '');
		if (response.status === 408 || response.status === 409 || response.status === 429 || response.status >= 500) {
			throw new TransientMessageError(
				`OpenRouter transient HTTP ${response.status}: ${details.slice(0, 240)}`,
			);
		}

		return {
			kind: 'non_retriable_http',
			status: response.status,
			details: details.slice(0, 240),
		};
	}

	let responseBody: unknown;
	try {
		responseBody = await response.json();
	} catch {
		return {
			kind: 'invalid_response',
			details: 'OpenRouter response was not valid JSON',
		};
	}

	const root = asObject(responseBody);
	const choices = Array.isArray(root?.choices) ? root.choices : [];
	const firstChoice = asObject(choices[0]);
	const message = asObject(firstChoice?.message);
	const rawContent = extractModelTextContent(message?.content);
	if (!rawContent) {
		return {
			kind: 'invalid_response',
			details: 'OpenRouter response did not contain message content',
		};
	}

	const maybeJson = extractJsonObject(rawContent);
	if (!maybeJson) {
		return {
			kind: 'invalid_response',
			details: 'Model output did not contain JSON object',
		};
	}

	let parsedDecisionPayload: unknown;
	try {
		parsedDecisionPayload = JSON.parse(maybeJson);
	} catch {
		return {
			kind: 'invalid_response',
			details: 'Model JSON payload could not be parsed',
		};
	}

	const decision = parseAiDecisionPayload(parsedDecisionPayload);
	if (!decision) {
		return {
			kind: 'invalid_response',
			details: 'Model payload missing required action/confidence',
		};
	}

	return { kind: 'decision', decision };
}

function createResult(
	job: AiClassificationJob,
	outcome: AiClassificationOutcome,
	fields?: Partial<Pick<AiClassificationResult, 'transaction_status' | 'confidence_score' | 'action' | 'reason'>>,
): AiClassificationResult {
	return {
		transaction_id: job.transaction_id,
		outcome,
		transaction_status: fields?.transaction_status ?? null,
		confidence_score: fields?.confidence_score ?? null,
		action: fields?.action ?? null,
		reason: fields?.reason ?? null,
	};
}

async function persistAiDecision(params: {
	sql: SqlClient;
	candidate: AiCandidateRow;
	job: AiClassificationJob;
	decision: AiDecision;
	autoVerifyMinConfidence: number;
}): Promise<AiClassificationResult> {
	const { sql, candidate, job, decision, autoVerifyMinConfidence } = params;

	const shouldAutoVerify =
		decision.action === 'VERIFY' &&
		decision.confidence_score >= autoVerifyMinConfidence &&
		candidate.transaction_category_id !== null;

	const rows = await sql<AiUpdateRowRaw[]>`
		update public.transactions as t
		set
			status = case
				when ${shouldAutoVerify} then 'VERIFIED'
				else 'NEEDS_REVIEW'
			end,
			classification_source = 'AI',
			ai_confidence_score = ${decision.confidence_score}
		where t.id = ${job.transaction_id}
			and t.user_id = ${candidate.user_id}
			and t.status = 'NEEDS_REVIEW'
		returning
			t.id as transaction_id,
			t.status as transaction_status
	`;

	if (rows.length === 0) {
		return createResult(job, 'SKIPPED_STALE_UPDATE', {
			confidence_score: decision.confidence_score,
			action: decision.action,
			reason: decision.reason,
		});
	}

	const transactionStatus = toRequiredString(
		rows[0].transaction_status,
		'transactions.status',
	) as TransactionStatus;

	return createResult(job, shouldAutoVerify ? 'UPDATED_VERIFIED' : 'UPDATED_NEEDS_REVIEW', {
		transaction_status: transactionStatus,
		confidence_score: decision.confidence_score,
		action: decision.action,
		reason: decision.reason,
	});
}

export async function runAiClassificationJob(
	job: AiClassificationJob,
	env: Env,
): Promise<AiClassificationResult> {
	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const candidate = await loadCandidate(sql, job.transaction_id);

	if (!candidate) {
		return createResult(job, 'SKIPPED_MISSING_TRANSACTION');
	}

	if (candidate.transaction_status !== 'NEEDS_REVIEW') {
		return createResult(job, 'SKIPPED_ALREADY_REVIEWED', {
			transaction_status: candidate.transaction_status,
		});
	}

	if (candidate.financial_event_status !== 'ACTIVE') {
		return createResult(job, 'SKIPPED_INACTIVE_FINANCIAL_EVENT');
	}

	const apiKey = resolveOpenRouterApiKey(env);
	if (!apiKey) {
		return createResult(job, 'SKIPPED_AI_DISABLED');
	}

	const model = resolveOpenRouterModel(env);
	const aiCallResult = await callOpenRouterForDecision({
		apiKey,
		model,
		candidate,
	});

	if (aiCallResult.kind === 'non_retriable_http') {
		return createResult(job, 'SKIPPED_NON_RETRIABLE_HTTP', {
			reason: `OpenRouter HTTP ${aiCallResult.status}`,
		});
	}

	if (aiCallResult.kind === 'invalid_response') {
		return createResult(job, 'SKIPPED_INVALID_AI_RESPONSE', {
			reason: aiCallResult.details,
		});
	}

	return persistAiDecision({
		sql,
		candidate,
		job,
		decision: aiCallResult.decision,
		autoVerifyMinConfidence: resolveAiAutoVerifyMinConfidence(env),
	});
}
