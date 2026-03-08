import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAppConfig } from '../lib/config';
import { getHealthResponse } from '../services/health.service';

export function createHealthRouter(): Router {
	const router = Router();

	router.get('/', async (_req, res, next) => {
		try {
			const config = getAppConfig(runtimeEnv);
			const { statusCode, payload } = await getHealthResponse(config);
			res.status(statusCode).json(payload);
		} catch (error) {
			next(error);
		}
	});

	return router;
}
