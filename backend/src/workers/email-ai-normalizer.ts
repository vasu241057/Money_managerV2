import type {
	AiNormalizationResponse,
	AiNormalizedTransactionItem,
	NormalizeRawEmailsJobPayload,
	PaymentMethod,
	RawEmailStatus,
	TransactionStatus,
	TransactionType,
	UUID,
} from '../../../shared/types';
import { NORMALIZE_RAW_EMAILS_MAX_IDS } from '../../../shared/types';
import {
	resolveNormalizerTierConfig,
	resolveOpenCodeGoApiKey,
	resolveAiQueueDelaySeconds,
} from '../lib/ai';
import { getAppConfig } from '../lib/config';
import type { SqlClient } from '../lib/db/client';
import { getSqlClient } from '../lib/db/client';
import { toIsoDateTime, toRequiredString } from '../lib/db/serialization';
import { PoisonMessageError, TransientMessageError } from './queue.errors';
import { buildAiClassificationJob } from './queue.messages';

const OPENCODE_GO_CHAT_URL = 'https://opencode.ai/zen/go/v1/chat/completions';

const VALID_PAYMENT_METHODS: readonly PaymentMethod[] = [
	'upi',
	'credit_card',
	'debit_card',
	'netbanking',
	'cash',
	'unknown',
];

const VALID_DIRECTIONS = ['debit', 'credit'] as const;

interface UserNormalizationContext {
	categories: Array<{ id: string; name: string; type: string; sub_categories: Array<{ id: string; name: string }> }>;
	accounts: Array<{ id: string; name: string; type: string; last4: string | null }>;
	misc_category_id: string | null;
}

function buildUserContextPrompt(ctx: UserNormalizationContext): string {
	const catList = ctx.categories.map(c => {
		const subList = c.sub_categories.map(s => `      { "id": "${s.id}", "name": "${s.name}" }`).join(',\n');
		return `    {
      "id": "${c.id}",
      "name": "${c.name}",
      "type": "${c.type}"${subList.length > 0 ? ',\n      "sub_categories": [\n' + subList + '\n      ]' : ''}
    }`;
	}).join(',\n');

	return [
		'',
		'USER CONTEXT — your category_id and sub_category_id MUST come from this list:',
		'',
		'Available Categories:',
		'[',
		catList,
		']',
		'',
		ctx.misc_category_id
			? `MISCELLANEOUS category ID: "${ctx.misc_category_id}" — use this when no other category fits.`
			: 'If no category fits, leave category_id as null.',
		'',
		'RULES for category matching:',
		'- Pick the best matching category from the list above based on the transaction content.',
		'- If a sub-category fits, set both category_id (parent) and sub_category_id.',
		'- If unsure, use the MISCELLANEOUS category ID. Do NOT invent category IDs.',
	].join('\n');
}

function buildSystemPrompt(ctx: UserNormalizationContext): string {
	return [
		'You are a financial transaction extractor for Indian banking and UPI SMS/email alerts.',
		'Extract ALL monetary transactions from the email text.',
		'',
		'Required JSON output format (strict — no other keys):',
		'{',
		'  "transactions": [',
		'    {',
		'      "direction": "debit" | "credit",',
		'      "amount_in_paise": number,',
		'      "txn_timestamp": "ISO-8601 string (YYYY-MM-DDTHH:mm:ss.sssZ)",',
		'      "payment_method": "upi" | "credit_card" | "debit_card" | "netbanking" | "cash" | "unknown",',
		'      "counterparty_raw": "string or null",',
		'      "instrument_last4": "string or null",',
		'      "category_guess": "string or null",',
		'      "category_id": "string or null",',
		'      "sub_category_id": "string or null",',
		'      "confidence": number',
		'    }',
		'  ]',
		'}',
		'',
		'CRITICAL RULES:',
		'- amount_in_paise: Convert rupees to paise. Rs.100.50 → 10050. NO decimals, NO commas.',
		'- txn_timestamp: Use the explicit date/time from email. Format: YYYY-MM-DDTHH:mm:ss.sssZ.',
		'- direction: "debit" = money going OUT. "credit" = money coming IN.',
		'- counterparty_raw: Extract merchant/person/bank name. null if unclear.',
		'- instrument_last4: Last 4 digits of card or account number seen in email. null if not present.',
		'- payment_method: "upi" if VPA/UPI, "credit_card"/"debit_card" if card, "netbanking" if bank transfer, "cash" if ATM, "unknown" otherwise.',
		'- category_id: Pick the best matching category ID from the user context. If none fits, use the MISCELLANEOUS category ID.',
		'- sub_category_id: Pick the best matching sub-category ID from the user context. null if none fits.',
		'- confidence: 0.95-1.0 when clear. 0.7-0.9 with inference. 0.4-0.7 when guessing. NEVER below 0.4.',
		'- If the email has NO financial transactions, return {"transactions": []}.',
		'- ALWAYS return valid JSON. No markdown, no code fences.',
		buildUserContextPrompt(ctx),
	].join('\n');
}

const KILL_SWITCH_ENV_KEYS = [
	'NORMALIZATION_KILL_SWITCH',
	'EMAIL_NORMALIZATION_KILL_SWITCH',
] as const;

interface RawEmailRecord {
	id: UUID;
	user_id: UUID;
	internal_date_iso: string;
	clean_text: string;
	status: RawEmailStatus;
}

interface RawEmailFetchRowRaw {
	id: unknown;
	user_id: unknown;
	internal_date: unknown;
	clean_text: unknown;
	status: unknown;
}

interface FinancialEventInsertRowRaw {
	id: unknown;
}

interface TransactionInsertRowRaw {
	id: unknown;
	status: unknown;
}

export interface NormalizeRawEmailsResult {
	requested_raw_email_count: number;
	loaded_raw_email_count: number;
	kill_switch_enabled: boolean;
	processed_raw_email_count: number;
	ignored_raw_email_count: number;
	unrecognized_raw_email_count: number;
	failed_raw_email_count: number;
	skipped_raw_email_count: number;
	extracted_fact_count: number;
	reconciled_fact_count: number;
	persisted_financial_event_count: number;
	created_transaction_count: number;
	needs_review_transaction_count: number;
	ai_enqueued_count: number;
	failed_raw_email_ids: UUID[];
	unrecognized_raw_email_ids: UUID[];
}

type ModelErrorKind = 'rate_limited' | 'transient' | 'permanent' | 'parse_error' | 'timeout';

interface ModelError {
	kind: ModelErrorKind;
	status?: number;
	message?: string;
	retryAfterMs?: number;
}

interface TierState {
	activeIndex: number;
	failureCountPerTier: Map<number, number>;
	cooldownUntil: Map<number, number>;
}

function normalizeBooleanEnvFlag(value: unknown): boolean {
	if (typeof value !== 'string') return false;
	switch (value.trim().toLowerCase()) {
		case '1':
		case 'true':
		case 'yes':
		case 'on':
			return true;
		default:
			return false;
	}
}

function isKillSwitchEnabled(env: Env): boolean {
	const envRecord = env as unknown as Record<string, unknown>;
	for (const key of KILL_SWITCH_ENV_KEYS) {
		if (normalizeBooleanEnvFlag(envRecord[key])) return true;
	}
	return false;
}

function directionToType(direction: 'debit' | 'credit'): TransactionType {
	return direction === 'credit' ? 'income' : 'expense';
}

function buildSearchKey(item: AiNormalizedTransactionItem): string | null {
	const parts: string[] = [];
	if (item.counterparty_raw) {
		parts.push(item.counterparty_raw.toLowerCase().replace(/[^a-z0-9 ]/g, '').trim());
	}
	if (item.payment_method && item.payment_method !== 'unknown') {
		parts.push(item.payment_method);
	}
	return parts.length > 0 ? parts.join(' | ') : null;
}

function dedupeUuidPreserveOrder(ids: UUID[]): UUID[] {
	const seen = new Set<string>();
	const deduped: UUID[] = [];
	for (const id of ids) {
		if (seen.has(id)) continue;
		seen.add(id);
		deduped.push(id);
	}
	return deduped;
}

function mapRawEmailRow(row: RawEmailFetchRowRaw): RawEmailRecord {
	return {
		id: toRequiredString(row.id, 'raw_emails.id') as UUID,
		user_id: toRequiredString(row.user_id, 'raw_emails.user_id') as UUID,
		internal_date_iso: toIsoDateTime(row.internal_date, 'raw_emails.internal_date'),
		clean_text: toRequiredString(row.clean_text, 'raw_emails.clean_text'),
		status: toRequiredString(row.status, 'raw_emails.status') as RawEmailStatus,
	};
}

async function loadRawEmailsByIds(
	sql: SqlClient,
	rawEmailIds: UUID[],
): Promise<RawEmailRecord[]> {
	const rows = await sql<RawEmailFetchRowRaw[]>`
		select re.id, re.user_id, re.internal_date, re.clean_text, re.status
		from public.raw_emails as re
		where re.id = any(${rawEmailIds}::uuid[])
		order by re.internal_date asc
	`;
	return rows.map(mapRawEmailRow);
}

interface CategoryRowRaw { id: unknown; name: unknown; type: unknown; parent_id: unknown; }

interface AccountRowRaw { id: unknown; name: unknown; type: unknown; instrument_last4: unknown; }

async function fetchUserNormalizationContext(
	sql: SqlClient,
	userId: UUID,
): Promise<UserNormalizationContext> {
	const catRows = await sql<CategoryRowRaw[]>`
		select c.id, c.name, c.type, c.parent_id
		from public.categories as c
		where c.user_id = ${userId} and c.is_system = false
		order by c.parent_id nulls first, c.name asc
	`;

	const parents = catRows.filter(r => !r.parent_id);
	const children = catRows.filter(r => r.parent_id);

	const categories = parents.map(p => ({
		id: toRequiredString(p.id, 'categories.id'),
		name: toRequiredString(p.name, 'categories.name'),
		type: toRequiredString(p.type, 'categories.type'),
		sub_categories: children
			.filter(c => toRequiredString(c.parent_id, 'categories.parent_id') === toRequiredString(p.id, 'categories.id'))
			.map(c => ({
				id: toRequiredString(c.id, 'categories.id'),
				name: toRequiredString(c.name, 'categories.name'),
			})),
	}));

	const miscCat = categories.find(c => c.name === 'Miscellaneous' && c.type === 'expense');

	const acctRows = await sql<AccountRowRaw[]>`
		select a.id, a.name, a.type, a.instrument_last4
		from public.accounts as a
		where a.user_id = ${userId}
		order by a.name asc
	`;

	const accounts = acctRows.map(a => ({
		id: toRequiredString(a.id, 'accounts.id'),
		name: toRequiredString(a.name, 'accounts.name'),
		type: toRequiredString(a.type, 'accounts.type'),
		last4: typeof a.instrument_last4 === 'string' ? a.instrument_last4 || null : null,
	}));

	return {
		categories,
		accounts,
		misc_category_id: miscCat?.id ?? null,
	};
}

function createEmptyResult(requestedCount: number): NormalizeRawEmailsResult {
	return {
		requested_raw_email_count: requestedCount,
		loaded_raw_email_count: 0,
		kill_switch_enabled: false,
		processed_raw_email_count: 0,
		ignored_raw_email_count: 0,
		unrecognized_raw_email_count: 0,
		failed_raw_email_count: 0,
		skipped_raw_email_count: 0,
		extracted_fact_count: 0,
		reconciled_fact_count: 0,
		persisted_financial_event_count: 0,
		created_transaction_count: 0,
		needs_review_transaction_count: 0,
		ai_enqueued_count: 0,
		failed_raw_email_ids: [],
		unrecognized_raw_email_ids: [],
	};
}

function classifyError(error: unknown): ModelError {
	if (error instanceof PoisonMessageError) {
		return { kind: 'permanent', message: error.message };
	}

	if (error instanceof TransientMessageError) {
		return { kind: 'transient', message: error.message };
	}

	if (error instanceof Error) {
		if (error.name === 'AbortError' || error.message.includes('timed out')) {
			return { kind: 'timeout', message: error.message };
		}

		if (error.message.includes('rate') || error.message.includes('429')) {
			return { kind: 'rate_limited', message: error.message };
		}

		if (error.message.includes('parse') || error.message.includes('JSON')) {
			return { kind: 'parse_error', message: error.message };
		}
	}

	const msg = error instanceof Error ? error.message : 'Unknown error';
	return { kind: 'transient', message: msg };
}

async function callModel(
	model: string,
	emailText: string,
	apiKey: string,
	timeoutMs: number,
	temperature: number,
	maxTokens: number,
	noTraining: boolean,
	systemPrompt: string,
): Promise<AiNormalizationResponse> {
	const controller = new AbortController();
	const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

	const callStartMs = Date.now();
	let response: Response;
	try {
		const headers: Record<string, string> = {
			Authorization: `Bearer ${apiKey}`,
			'Content-Type': 'application/json',
		};

		const body: Record<string, unknown> = {
			model,
			temperature,
			messages: [
				{ role: 'system', content: systemPrompt },
				{
					role: 'user',
					content: `Analyze this email and extract all financial transactions:\n---\n${emailText}\n---`,
				},
			],
			response_format: { type: 'json_object' },
		};

		if (maxTokens > 0) {
			body.max_tokens = maxTokens;
		}

		response = await fetch(OPENCODE_GO_CHAT_URL, {
			method: 'POST',
			headers,
			body: JSON.stringify(body),
			signal: controller.signal,
		});
	} catch (error) {
		clearTimeout(timeoutId);
		if (error instanceof Error && error.name === 'AbortError') {
			throw new TransientMessageError('AI normalizer request timed out');
		}
		throw new TransientMessageError(
			`AI normalizer network error: ${error instanceof Error ? error.message : 'Unknown'}`,
		);
	}
	clearTimeout(timeoutId);

	if (response.status === 429) {
		const retryAfter = response.headers.get('Retry-After');
		const retryAfterMs = retryAfter ? Number(retryAfter) * 1000 : undefined;
		throw new TransientMessageError(
			`AI normalizer rate limited (429)${retryAfter ? `, retry after ${retryAfter}s` : ''}`,
		);
	}

	if (response.status >= 500 || response.status === 408) {
		throw new TransientMessageError(
			`AI normalizer server error (${response.status})`,
		);
	}

	if (!response.ok) {
		throw new PoisonMessageError(
			`AI normalizer HTTP ${response.status}: model possibly unavailable`,
		);
	}

	const json = (await response.json()) as {
		choices?: Array<{ message?: { content?: string } }>;
	};

	const content = json.choices?.[0]?.message?.content;
	if (!content) {
		console.error('AI_NORMALIZER_EMPTY_RESPONSE', {
			status: response.status,
			model,
			responseKeys: Object.keys(json),
			choicesLength: json.choices?.length,
			hasMessage: Boolean(json.choices?.[0]?.message),
			messageKeys: json.choices?.[0]?.message ? Object.keys(json.choices[0].message!) : [],
			rawFirstChoice: JSON.stringify(json.choices?.[0]).slice(0, 500),
		});
		throw new TransientMessageError('AI normalizer returned empty response');
	}

	const cleaned = content
		.replace(/^```(?:json)?\s*/i, '')
		.replace(/\s*```\s*$/, '')
		.trim();

	try {
		const parsed = JSON.parse(cleaned) as AiNormalizationResponse;
		const callElapsedMs = Date.now() - callStartMs;
		const usage = (json as Record<string, unknown>).usage as Record<string, number> | undefined;

		console.info('AI_NORMALIZER_CALL_COMPLETED', {
			model,
			elapsedMs: callElapsedMs,
			txnCount: Array.isArray(parsed.transactions) ? parsed.transactions.length : 0,
			promptTokens: usage?.prompt_tokens,
			completionTokens: usage?.completion_tokens,
			totalTokens: usage?.total_tokens,
			rawResponse: JSON.stringify(parsed).slice(0, 1000),
		});

		if (!parsed.transactions || !Array.isArray(parsed.transactions)) {
			return { transactions: [] };
		}
		return parsed;
	} catch {
		throw new TransientMessageError('AI normalizer returned malformed JSON');
	}
}

async function callWithTieredFallback(
	env: Env,
	emailText: string,
	apiKey: string,
	systemPrompt: string,
): Promise<AiNormalizationResponse> {
	const config = resolveNormalizerTierConfig(env);
	const state: TierState = {
		activeIndex: 0,
		failureCountPerTier: new Map(),
		cooldownUntil: new Map(),
	};

	let totalAttempts = 0;

	while (totalAttempts < config.maxRetries) {
		let tierIdx = state.activeIndex;
		while (tierIdx < config.models.length) {
			const cooldown = state.cooldownUntil.get(tierIdx) ?? 0;
			if (Date.now() < cooldown) {
				tierIdx++;
			} else {
				break;
			}
		}

		if (tierIdx >= config.models.length) {
			throw new PoisonMessageError('All AI model tiers exhausted');
		}

		const model = config.models[tierIdx];
		state.activeIndex = tierIdx;

		try {
			return await callModel(
				model,
				emailText,
				apiKey,
				config.timeoutMs,
				config.temperature,
				config.maxTokens,
				config.noTraining,
				systemPrompt,
			);
		} catch (error) {
			const classified = classifyError(error);
			totalAttempts++;

			const tierFails = (state.failureCountPerTier.get(tierIdx) ?? 0) + 1;
			state.failureCountPerTier.set(tierIdx, tierFails);

			console.warn('AI_NORMALIZER_TIER_FAILURE', {
				tierIndex: tierIdx,
				model,
				attempt: totalAttempts,
				tierAttempts: tierFails,
				errorKind: classified.kind,
				message: classified.message ?? '',
			});

			if (classified.kind === 'rate_limited' || classified.kind === 'permanent') {
				state.cooldownUntil.set(tierIdx, Date.now() + config.cooldownMs);
				state.activeIndex = tierIdx + 1;
				continue;
			}

			if (tierFails >= config.rotateFailures) {
				state.cooldownUntil.set(tierIdx, Date.now() + config.cooldownMs);
				state.activeIndex = tierIdx + 1;
			}
		}
	}

	throw new PoisonMessageError(`All ${config.maxRetries} normalizer attempts exhausted`);
}

function validateAndCleanResponse(
	response: AiNormalizationResponse,
	minConfidence: number,
): AiNormalizedTransactionItem[] {
	const txnList = Array.isArray(response.transactions) ? response.transactions : [];
	if (txnList.length === 0) return [];

	const valid: AiNormalizedTransactionItem[] = [];

	for (const item of txnList) {
		if (!item || typeof item !== 'object') continue;

		const direction = item.direction as string;
		if (!VALID_DIRECTIONS.includes(direction as 'debit' | 'credit')) continue;

		const amountInPaise = Number(item.amount_in_paise);
		if (!Number.isSafeInteger(amountInPaise) || amountInPaise <= 0) continue;
		if (amountInPaise > 1_000_000_00) continue;

		const confidence = Number(item.confidence);
		if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) continue;

		if (confidence < minConfidence) continue;

		const txnTimestamp =
			typeof item.txn_timestamp === 'string' && item.txn_timestamp.length > 0
				? item.txn_timestamp
				: new Date().toISOString();

		const paymentMethod: PaymentMethod = VALID_PAYMENT_METHODS.includes(
			item.payment_method as PaymentMethod,
		)
			? (item.payment_method as PaymentMethod)
			: 'unknown';

		const counterpartyRaw =
			typeof item.counterparty_raw === 'string'
				? item.counterparty_raw.trim().slice(0, 500) || null
				: null;

		const instrumentLast4 =
			typeof item.instrument_last4 === 'string'
				? item.instrument_last4.trim().replace(/\D/g, '').slice(-4) || null
				: null;

		const categoryGuess =
			typeof item.category_guess === 'string'
				? item.category_guess.trim().toLowerCase() || null
				: null;

		const categoryId =
			typeof (item as unknown as Record<string, unknown>).category_id === 'string'
				? (item as unknown as Record<string, unknown>).category_id as string || null
				: null;

		const subCategoryId =
			typeof (item as unknown as Record<string, unknown>).sub_category_id === 'string'
				? (item as unknown as Record<string, unknown>).sub_category_id as string || null
				: null;

		valid.push({
			direction: direction as 'debit' | 'credit',
			amount_in_paise: amountInPaise,
			txn_timestamp: txnTimestamp,
			payment_method: paymentMethod,
			counterparty_raw: counterpartyRaw,
			instrument_last4: instrumentLast4,
			category_guess: categoryGuess,
			category_id: categoryId,
			sub_category_id: subCategoryId,
			account_id: null,
			confidence,
		});
	}

	return valid;
}

async function persistAiTransactions(
	sql: SqlClient,
	rawEmail: RawEmailRecord,
	items: AiNormalizedTransactionItem[],
	autoVerifyConfidence: number,
): Promise<{ transactionCount: number; needsReviewIds: UUID[] }> {
	return sql.begin(async (tx) => {
		const txSql = tx as unknown as SqlClient;

		const lockRows = await txSql<RawEmailFetchRowRaw[]>`
			select re.id, re.user_id, re.internal_date, re.clean_text, re.status
			from public.raw_emails as re
			where re.id = ${rawEmail.id}
			limit 1
			for update
		`;
		const lockedRow = lockRows[0] ? mapRawEmailRow(lockRows[0]) : null;
		if (!lockedRow || lockedRow.status !== 'PENDING_EXTRACTION') {
			return { transactionCount: 0, needsReviewIds: [] };
		}

		let persistedFinancialEvents = 0;
		let persistedTransactions = 0;
		const needsReviewIds: UUID[] = [];

		for (let idx = 0; idx < items.length; idx++) {
			const item = items[idx];

			const feRows = await txSql<FinancialEventInsertRowRaw[]>`
				insert into public.financial_events (
					user_id, raw_email_id, extraction_index,
					direction, amount_in_paise, currency,
					txn_timestamp, payment_method, instrument_id,
					counterparty_raw, search_key, status
				)
				values (
					${lockedRow.user_id}, ${lockedRow.id}, ${idx},
					${item.direction}, ${item.amount_in_paise}, 'INR',
					${item.txn_timestamp}, ${item.payment_method},
					${item.instrument_last4},
					${item.counterparty_raw},
					${buildSearchKey(item)},
					'ACTIVE'
				)
				on conflict (raw_email_id, extraction_index) do nothing
				returning id
			`;

			let feId: string | null = null;
			if (feRows.length > 0 && feRows[0]?.id) {
				feId = toRequiredString(feRows[0].id, 'financial_events.id');
				persistedFinancialEvents++;
			}

			if (!feId) continue;

			const status: TransactionStatus =
				item.confidence >= autoVerifyConfidence ? 'VERIFIED' : 'NEEDS_REVIEW';

			const resolvedCategoryId = item.sub_category_id || item.category_id || null;
			const resolvedAccountId = item.account_id || null;

			const txnRows = await txSql<TransactionInsertRowRaw[]>`
				insert into public.transactions (
					user_id, financial_event_id, account_id,
					category_id, merchant_id, credit_card_id,
					amount_in_paise, type, txn_date, user_note,
					status, classification_source, ai_confidence_score
				)
				values (
					${lockedRow.user_id}, ${feId}, ${resolvedAccountId},
					${resolvedCategoryId}, null, null,
					${item.amount_in_paise},
					${directionToType(item.direction)},
					${item.txn_timestamp}, null,
					${status}, 'AI', ${item.confidence}
				)
				on conflict (financial_event_id) do nothing
				returning id, status
			`;

			const insertedTxnId = txnRows[0]?.id
				? toRequiredString(txnRows[0].id, 'transactions.id')
				: null;
			if (insertedTxnId) {
				persistedTransactions++;
				const insertedStatus = toRequiredString(
					txnRows[0]?.status,
					'transactions.status',
				) as TransactionStatus;
				if (insertedStatus === 'NEEDS_REVIEW') {
					needsReviewIds.push(insertedTxnId as UUID);
				}
			}
		}

		await txSql`
			update public.raw_emails as re
			set status = 'PROCESSED'
			where re.id = ${lockedRow.id}
				and re.user_id = ${lockedRow.user_id}
				and re.status = 'PENDING_EXTRACTION'
		`;

		return {
			transactionCount: persistedTransactions,
			needsReviewIds,
		};
	});
}

async function markRawEmailFailed(
	sql: SqlClient,
	rawEmail: RawEmailRecord,
	error: unknown,
): Promise<void> {
	const reason =
		error instanceof Error
			? error.message.slice(0, 500)
			: 'Unknown normalizer error';
	try {
		await sql`
			update public.raw_emails as re
			set status = 'FAILED'
			where re.id = ${rawEmail.id}
				and re.user_id = ${rawEmail.user_id}
				and re.status = 'PENDING_EXTRACTION'
		`;
	} catch (updateError) {
		console.error('Failed to mark raw email as FAILED', {
			rawEmailId: rawEmail.id,
			userId: rawEmail.user_id,
			originalError: reason,
			updateError:
				updateError instanceof Error ? updateError.message : 'Unknown',
		});
	}
}

async function markRawEmailIgnored(
	sql: SqlClient,
	rawEmail: RawEmailRecord,
): Promise<void> {
	await sql`
		update public.raw_emails as re
		set status = 'IGNORED'
		where re.id = ${rawEmail.id}
			and re.user_id = ${rawEmail.user_id}
			and re.status = 'PENDING_EXTRACTION'
	`;
}

async function enqueueAiClassificationJobs(
	queue: Queue,
	transactionIds: UUID[],
	delaySeconds: number,
): Promise<number> {
	const deduped = dedupeUuidPreserveOrder(transactionIds);
	let enqueued = 0;

	for (const id of deduped) {
		try {
			await queue.send(buildAiClassificationJob(id), {
				contentType: 'json',
				delaySeconds,
			});
			enqueued++;
		} catch (error) {
			console.warn('Failed to enqueue AI_CLASSIFICATION from AI normalizer', {
				transactionId: id,
				error: error instanceof Error ? error.message : 'Unknown',
			});
		}
	}

	return enqueued;
}

export async function runNormalizeRawEmailsJob(
	job: NormalizeRawEmailsJobPayload,
	env: Env,
): Promise<NormalizeRawEmailsResult> {
	if (job.raw_email_ids.length > NORMALIZE_RAW_EMAILS_MAX_IDS) {
		throw new PoisonMessageError(
			`NORMALIZE_RAW_EMAILS raw_email_ids exceeds max ${NORMALIZE_RAW_EMAILS_MAX_IDS}`,
		);
	}

	const requestedIds = dedupeUuidPreserveOrder(job.raw_email_ids);
	if (requestedIds.length > NORMALIZE_RAW_EMAILS_MAX_IDS) {
		throw new PoisonMessageError(
			`NORMALIZE_RAW_EMAILS raw_email_ids exceeds max ${NORMALIZE_RAW_EMAILS_MAX_IDS}`,
		);
	}

	const result = createEmptyResult(job.raw_email_ids.length);

	if (isKillSwitchEnabled(env)) {
		result.kill_switch_enabled = true;
		result.skipped_raw_email_count = job.raw_email_ids.length;
		return result;
	}

	const apiKey = resolveOpenCodeGoApiKey(env);
	if (!apiKey) {
		result.skipped_raw_email_count = job.raw_email_ids.length;
		console.error('AI normalizer: OPENCODE_GO_API_KEY is not set');
		return result;
	}

	const config = getAppConfig(env);
	const sql = getSqlClient(config);
	const normConfig = resolveNormalizerTierConfig(env);

	const rawEmailRows = await loadRawEmailsByIds(sql, requestedIds);
	result.loaded_raw_email_count = rawEmailRows.length;

	const rawEmailById = new Map<UUID, RawEmailRecord>();
	for (const row of rawEmailRows) {
		rawEmailById.set(row.id, row);
	}

	const allNeedsReviewIds: UUID[] = [];

	const userContextCache = new Map<UUID, UserNormalizationContext>();

	for (const rawEmailId of requestedIds) {
		const rawEmail = rawEmailById.get(rawEmailId);
		if (!rawEmail || rawEmail.status !== 'PENDING_EXTRACTION') {
			result.skipped_raw_email_count++;
			continue;
		}

		let ctx = userContextCache.get(rawEmail.user_id);
		if (!ctx) {
			ctx = await fetchUserNormalizationContext(sql, rawEmail.user_id);
			userContextCache.set(rawEmail.user_id, ctx);
		}

		const systemPrompt = buildSystemPrompt(ctx);
		const emailStartMs = Date.now();

		console.info('AI_NORMALIZER_EMAIL_INPUT', {
			rawEmailId: rawEmail.id,
			textLength: rawEmail.clean_text.length,
			textPreview: rawEmail.clean_text.slice(0, 400),
		});

		try {
			const aiResponse = await callWithTieredFallback(
				env,
				rawEmail.clean_text,
				apiKey,
				systemPrompt,
			);

			const validated = validateAndCleanResponse(aiResponse, normConfig.minConfidence);

			result.extracted_fact_count +=
				Array.isArray(aiResponse.transactions) ? aiResponse.transactions.length : 0;
			result.reconciled_fact_count += validated.length;

			if (validated.length === 0) {
				await markRawEmailIgnored(sql, rawEmail);
				result.ignored_raw_email_count++;
				console.info('AI_NORMALIZER_EMAIL_IGNORED', {
					rawEmailId: rawEmail.id,
					totalElapsedMs: Date.now() - emailStartMs,
					reason: 'no transactions extracted',
				});
				continue;
			}

			for (const item of validated) {
				if (item.instrument_last4) {
					const matched = ctx.accounts.find(
						a => a.last4 && (a.last4 === item.instrument_last4),
					);
					if (matched) {
						item.account_id = matched.id;
					}
				}

				if (!item.account_id && item.payment_method === 'upi') {
					const emailLower = rawEmail.clean_text.toLowerCase();
					const counterpartyLower = (item.counterparty_raw || '').toLowerCase();
					const searchText = counterpartyLower + ' ' + emailLower;

					const matched = ctx.accounts.find(a => {
						const nameLower = a.name.toLowerCase();
						const keywords = nameLower.split(' ').filter(w => w.length > 1);
						return a.type === 'bank' && keywords.some(kw => searchText.includes(kw));
					});
					if (matched) {
						item.account_id = matched.id;
					}
				}
			}

			const persistence = await persistAiTransactions(
				sql,
				rawEmail,
				validated,
				normConfig.autoVerifyConfidence,
			);

			result.persisted_financial_event_count += persistence.transactionCount;
			result.created_transaction_count += persistence.transactionCount;
			result.needs_review_transaction_count += persistence.needsReviewIds.length;
			allNeedsReviewIds.push(...persistence.needsReviewIds);
			result.processed_raw_email_count++;

			console.info('AI_NORMALIZER_EMAIL_PROCESSED', {
				rawEmailId: rawEmail.id,
				totalElapsedMs: Date.now() - emailStartMs,
				txnCount: persistence.transactionCount,
				needsReview: persistence.needsReviewIds.length,
			});
		} catch (error) {
			if (error instanceof TransientMessageError) {
				throw error;
			}

			await markRawEmailFailed(sql, rawEmail, error);
			result.failed_raw_email_count++;
			result.failed_raw_email_ids.push(rawEmail.id);
		}
	}

	result.ai_enqueued_count = await enqueueAiClassificationJobs(
		env.AI_CLASSIFICATION_QUEUE,
		allNeedsReviewIds,
		resolveAiQueueDelaySeconds(env),
	);

	return result;
}
