import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { listGlobalMerchants, parseListGlobalMerchantsQuery } from '../services/merchants.service';

export function createMerchantsRouter(): Router {
	const router = Router();

	router.get(
		'/',
		asyncHandler(async (req, res) => {
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const query = parseListGlobalMerchantsQuery(req.query as Record<string, unknown>);
			const response = await listGlobalMerchants(sql, query);
			res.status(200).json({ data: response });
		}),
	);

	return router;
}
