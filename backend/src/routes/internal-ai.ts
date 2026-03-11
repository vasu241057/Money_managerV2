import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { resolveAiWebhookSecret } from '../lib/ai';
import { asyncHandler } from '../lib/http/async';
import { serviceUnavailable } from '../lib/http/errors';
import {
	assertAiRequiresWebhookSecret,
	enqueueAiRequiresWebhookJob,
	getAiRequiresWebhookSecretHeaderName,
	parseAiRequiresWebhookRequest,
} from '../services/ai-webhook.service';

export function createInternalAiRouter(): Router {
	const router = Router();

	router.post(
		'/requires',
		asyncHandler(async (req, res) => {
			const expectedSecret = resolveAiWebhookSecret(runtimeEnv);
			if (!expectedSecret) {
				throw serviceUnavailable(
					'AI_WEBHOOK_NOT_CONFIGURED',
					'AI_REQUIRES_WEBHOOK_SECRET is required for internal AI webhook ingestion',
				);
			}

			const secretHeader = req.header(getAiRequiresWebhookSecretHeaderName());
			assertAiRequiresWebhookSecret(secretHeader, expectedSecret);

			const payload = parseAiRequiresWebhookRequest(req.body);
			const queued = await enqueueAiRequiresWebhookJob(runtimeEnv, payload);
			res.status(202).json({ data: queued });
		}),
	);

	return router;
}
