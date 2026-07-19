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

const DEFAULT_PROBE_LIMIT = 10;
const MAX_PROBE_LIMIT = 100;
const DEFAULT_BACKFILL_LIMIT = 200;
const MAX_BACKFILL_LIMIT = 1000;

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
