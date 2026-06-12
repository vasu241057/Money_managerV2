import type { NormalizeRawEmailsJobPayload, UUID } from '../../../shared/types';
import { NORMALIZE_RAW_EMAILS_MAX_IDS } from '../../../shared/types';
import { resolveNormalizationEngine } from '../lib/ai';
import { PoisonMessageError } from './queue.errors';
import { runNormalizeRawEmailsJob as runAiNormalizer } from './email-ai-normalizer';
import { runNormalizeRawEmailsJob as runDeterministicNormalizer } from './email-deterministic-normalizer';

export type { NormalizeRawEmailsResult } from './email-ai-normalizer';

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

export async function runNormalizeRawEmailsJob(
	job: NormalizeRawEmailsJobPayload,
	env: Env,
): ReturnType<typeof runAiNormalizer> {
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

	const engine = resolveNormalizationEngine(env);

	switch (engine) {
		case 'ai':
			return runAiNormalizer(job, env);

		case 'hybrid': {
			const detResult = await runDeterministicNormalizer(job, env);

			const retryIds = [
				...detResult.failed_raw_email_ids,
				...detResult.unrecognized_raw_email_ids,
			];
			const dedupedRetryIds = dedupeUuidPreserveOrder(retryIds);

			if (dedupedRetryIds.length === 0) return detResult;

			const retryJob: NormalizeRawEmailsJobPayload = {
				job_type: 'NORMALIZE_RAW_EMAILS',
				raw_email_ids: dedupedRetryIds,
			};
			const aiResult = await runAiNormalizer(retryJob, env);

			return {
				requested_raw_email_count: detResult.requested_raw_email_count,
				loaded_raw_email_count: detResult.loaded_raw_email_count,
				kill_switch_enabled: detResult.kill_switch_enabled,
				processed_raw_email_count:
					detResult.processed_raw_email_count + aiResult.processed_raw_email_count,
				ignored_raw_email_count:
					detResult.ignored_raw_email_count + aiResult.ignored_raw_email_count,
				unrecognized_raw_email_count: aiResult.unrecognized_raw_email_count,
				failed_raw_email_count: aiResult.failed_raw_email_count,
				skipped_raw_email_count: detResult.skipped_raw_email_count,
				extracted_fact_count:
					detResult.extracted_fact_count + aiResult.extracted_fact_count,
				reconciled_fact_count:
					detResult.reconciled_fact_count + aiResult.reconciled_fact_count,
				persisted_financial_event_count:
					detResult.persisted_financial_event_count +
					aiResult.persisted_financial_event_count,
				created_transaction_count:
					detResult.created_transaction_count + aiResult.created_transaction_count,
				needs_review_transaction_count:
					detResult.needs_review_transaction_count +
					aiResult.needs_review_transaction_count,
				ai_enqueued_count: detResult.ai_enqueued_count + aiResult.ai_enqueued_count,
				failed_raw_email_ids: aiResult.failed_raw_email_ids,
				unrecognized_raw_email_ids: aiResult.unrecognized_raw_email_ids,
			};
		}

		default:
			return runDeterministicNormalizer(job, env);
	}
}
