import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import {
	completeGoogleOAuthCallback,
	disconnectGoogleOAuthConnection,
	getGoogleOAuthConnectionStatus,
	parseGoogleOAuthCallbackRequest,
	startGoogleOAuth,
} from '../services/oauth.service';

export function createOAuthRouter(): Router {
	const router = Router();

	router.post(
		'/google/start',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const response = await startGoogleOAuth(userId, runtimeEnv);

			res.status(200).json({ data: response });
		}),
	);

	router.post(
		'/google/callback',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const payload = parseGoogleOAuthCallbackRequest(req.body);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const response = await completeGoogleOAuthCallback(
				sql,
				userId,
				payload,
				runtimeEnv,
			);
			res.status(200).json({ data: response });
		}),
	);

	router.get(
		'/google/connection',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const response = await getGoogleOAuthConnectionStatus(sql, userId);
			res.status(200).json({ data: response });
		}),
	);

	router.delete(
		'/google/connection',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			await disconnectGoogleOAuthConnection(sql, userId);
			res.status(204).send();
		}),
	);

	return router;
}
