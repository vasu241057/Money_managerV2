import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { parseUuid } from '../lib/http/validation';
import {
	listGlobalMerchants,
	listUserMerchants,
	parseListGlobalMerchantsQuery,
	parseListUserMerchantsQuery,
	parseSetUserMerchantCategoryRequest,
	setUserMerchantCategory,
} from '../services/merchants.service';

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

	// Registered before '/:merchantId/...' so the literal segment always wins.
	router.get(
		'/mine',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const limit = parseListUserMerchantsQuery(req.query as Record<string, unknown>);
			const response = await listUserMerchants(sql, userId, limit);
			res.status(200).json({ data: response });
		}),
	);

	router.put(
		'/:merchantId/category',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const merchantId = parseUuid(req.params.merchantId, 'merchantId');
			const input = parseSetUserMerchantCategoryRequest(req.body);
			const backfilledCount = await setUserMerchantCategory(sql, userId, merchantId, input);
			res.status(200).json({ data: { backfilled_count: backfilledCount } });
		}),
	);

	return router;
}
