import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { resolveAiWebhookSecret } from '../lib/ai';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { serviceUnavailable } from '../lib/http/errors';
import {
	assertAiRequiresWebhookSecret,
	getAiRequiresWebhookSecretHeaderName,
} from '../services/ai-webhook.service';

const DEFAULT_REQUEUE_LIMIT = 1000;
const MAX_REQUEUE_LIMIT = 10_000;

export function createInternalRequeueRouter(): Router {
	const router = Router();

	router.post(
		'/pending',
		asyncHandler(async (req, res) => {
			// Same secret gate as /internal/ai — this is a mutation endpoint on
			// a public Worker URL and must never be open.
			const expectedSecret = resolveAiWebhookSecret(runtimeEnv);
			if (!expectedSecret) {
				throw serviceUnavailable(
					'AI_WEBHOOK_NOT_CONFIGURED',
					'AI_REQUIRES_WEBHOOK_SECRET is required for internal requeue',
				);
			}
			const secretHeader = req.header(getAiRequiresWebhookSecretHeaderName());
			assertAiRequiresWebhookSecret(secretHeader, expectedSecret);

			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const parsedLimit = parseInt(String(req.query.limit ?? ''), 10);
			const limit =
				Number.isFinite(parsedLimit) && parsedLimit > 0
					? Math.min(parsedLimit, MAX_REQUEUE_LIMIT)
					: DEFAULT_REQUEUE_LIMIT;

			const rows = await sql<{ id: string }[]>`
				select re.id
				from public.raw_emails as re
				where re.status = 'PENDING_EXTRACTION'
				order by re.created_at asc
				limit ${limit}
			`;

			const ids = rows.map((r) => r.id);
			if (ids.length === 0) {
				res.json({ enqueued: 0, batches: 0, message: 'No pending emails found' });
				return;
			}

			const BATCH_SIZE = 250;
			let enqueued = 0;
			let batches = 0;

			for (let i = 0; i < ids.length; i += BATCH_SIZE) {
				const batch = ids.slice(i, i + BATCH_SIZE);
				await runtimeEnv.EMAIL_SYNC_QUEUE.send(
					{ job_type: 'NORMALIZE_RAW_EMAILS', raw_email_ids: batch },
					{ contentType: 'json' },
				);
				enqueued += batch.length;
				batches++;
			}

			res.json({ enqueued, batches, message: `Re-enqueued ${enqueued} emails in ${batches} batches` });
		}),
	);

	return router;
}
