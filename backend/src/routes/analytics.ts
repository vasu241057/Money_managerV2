import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { getAnalyticsSummary } from '../services/analytics.service';

export function createAnalyticsRouter(): Router {
	const router = Router();

	router.get(
		'/summary',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const summary = await getAnalyticsSummary(sql, userId);
			res.status(200).json({ data: summary });
		}),
	);

	return router;
}
