import { env as runtimeEnv } from 'cloudflare:workers';
import type { Request } from 'express';
import { Router } from 'express';

import { resolveAiWebhookSecret } from '../lib/ai';
import { asyncHandler } from '../lib/http/async';
import { serviceUnavailable } from '../lib/http/errors';
import {
	assertAiRequiresWebhookSecret,
	getAiRequiresWebhookSecretHeaderName,
} from '../services/ai-webhook.service';
import {
	runEmailSyncProbe,
	runRetroactiveSenderGate,
	runSenderMetadataBackfill,
	runStatementHarvest,
} from '../workers/email-sync-fetcher';
import {
	applyMerchantMappings,
	previewMerchantMappings,
	type ApplyMappingProposal,
} from '../workers/merchant-mapper';

const DEFAULT_PROBE_LIMIT = 10;
const MAX_PROBE_LIMIT = 100;
const DEFAULT_BACKFILL_LIMIT = 200;
const MAX_BACKFILL_LIMIT = 1000;
const DEFAULT_MAP_PREVIEW_LIMIT = 200;
const MAX_MAP_PREVIEW_LIMIT = 500;

function parseLimit(raw: unknown, fallback: number, max: number): number {
	const parsed = parseInt(String(raw ?? ''), 10);
	return Number.isFinite(parsed) && parsed > 0 ? Math.min(parsed, max) : fallback;
}

// Dev/ops tooling. Same secret gate as /internal/requeue — these are
// mutation endpoints on a public Worker URL and must never be open.
export function createInternalOpsRouter(): Router {
	const router = Router();

	const requireSecret = (req: Request): void => {
		const expectedSecret = resolveAiWebhookSecret(runtimeEnv);
		if (!expectedSecret) {
			throw serviceUnavailable(
				'AI_WEBHOOK_NOT_CONFIGURED',
				'AI_REQUIRES_WEBHOOK_SECRET is required for internal ops endpoints',
			);
		}
		const secretHeader = req.header(getAiRequiresWebhookSecretHeaderName());
		assertAiRequiresWebhookSecret(secretHeader, expectedSecret);
	};

	// One-time header repair for pre-July-2026 rows (from_address is null):
	// re-fetches metadata only, so the drained backlog can teach templates.
	router.post(
		'/backfill/sender-metadata',
		asyncHandler(async (req, res) => {
			requireSecret(req);
			const limit = parseLimit(req.query.limit, DEFAULT_BACKFILL_LIMIT, MAX_BACKFILL_LIMIT);
			const result = await runSenderMetadataBackfill(limit, runtimeEnv);
			res.json(result);
		}),
	);

	// Retro-applies the sender gate to pre-gate PENDING rows (run the
	// metadata backfill first). ?dry_run=true previews counts without
	// mutating; the real run flips denied rows to IGNORED + purges bodies.
	router.post(
		'/gate/pending',
		asyncHandler(async (req, res) => {
			requireSecret(req);
			const limit = parseLimit(req.query.limit, 2000, 10_000);
			const dryRun = req.query.dry_run === 'true';
			const result = await runRetroactiveSenderGate(limit, dryRun, runtimeEnv);
			res.json(result);
		}),
	);

	// One-time statement harvest: re-flips historical statement-looking
	// emails (IGNORED/PROCESSED, matched by subject/sender wording) to
	// PENDING and re-runs the normalizer, which now captures them as
	// card_statements. ?dry_run=true previews counts per sender.
	router.post(
		'/statements/harvest',
		asyncHandler(async (req, res) => {
			requireSecret(req);
			const limit = parseLimit(req.query.limit, 500, 2000);
			const dryRun = req.query.dry_run === 'true';
			// ?days= restricts the backfill window (e.g. 30 = current cycle only).
			const parsedDays = parseInt(String(req.query.days ?? ''), 10);
			const windowDays =
				Number.isFinite(parsedDays) && parsedDays > 0 ? Math.min(parsedDays, 3650) : null;
			const result = await runStatementHarvest(limit, dryRun, runtimeEnv, windowDays);
			res.json(result);
		}),
	);

	// Dry-runs merchant mapping: real Tavily searches and a real model call,
	// but nothing is persisted, so the proposals can be judged before the
	// mapper is trusted to write them. Defaults ignore the 14-day lookback and
	// the retry cooldown so the whole stuck backlog is visible.
	router.post(
		'/merchants/map-preview',
		asyncHandler(async (req, res) => {
			requireSecret(req);
			const body = (req.body ?? {}) as Record<string, unknown>;
			const searchKeys = Array.isArray(body.search_keys)
				? body.search_keys.filter((key): key is string => typeof key === 'string')
				: undefined;
			const result = await previewMerchantMappings(runtimeEnv, {
				ignoreLookback: body.ignore_lookback !== false,
				ignoreCooldown: body.ignore_cooldown !== false,
				...(searchKeys === undefined || searchKeys.length === 0 ? {} : { searchKeys }),
				limit: parseLimit(req.query.limit, DEFAULT_MAP_PREVIEW_LIMIT, MAX_MAP_PREVIEW_LIMIT),
			});
			res.json(result);
		}),
	);

	// Persists proposals already reviewed by a human — the preview's own output,
	// posted back verbatim. Costs no search and no model call, and every write
	// goes through the same path the daily job uses.
	router.post(
		'/merchants/map-apply',
		asyncHandler(async (req, res) => {
			requireSecret(req);
			const body = (req.body ?? {}) as Record<string, unknown>;
			const raw = Array.isArray(body.proposals) ? body.proposals : [];
			const proposals = raw.filter(
				(entry): entry is ApplyMappingProposal =>
					typeof entry === 'object' && entry !== null,
			);
			const result = await applyMerchantMappings(runtimeEnv, proposals);
			res.json(result);
		}),
	);

	// Fetches only the newest N messages through the normal gate flow,
	// returning per-message metadata + verdict. Never moves the watermark.
	router.post(
		'/sync/probe',
		asyncHandler(async (req, res) => {
			requireSecret(req);
			const limit = parseLimit(req.query.limit, DEFAULT_PROBE_LIMIT, MAX_PROBE_LIMIT);
			const result = await runEmailSyncProbe(limit, runtimeEnv);
			res.json(result);
		}),
	);

	return router;
}
