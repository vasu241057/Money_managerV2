import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';

export function createInternalRequeueRouter(): Router {
	const router = Router();

	router.post(
		'/pending',
		asyncHandler(async (req, res) => {
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const limit = parseInt(String(req.query.limit ?? ''), 10) || 0;

			const rows = await sql<{ id: string }[]>`
				select re.id
				from public.raw_emails as re
				where re.status = 'PENDING_EXTRACTION'
				order by re.created_at asc
			`;

			const ids = limit > 0 ? rows.slice(0, limit).map((r) => r.id) : rows.map((r) => r.id);
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
