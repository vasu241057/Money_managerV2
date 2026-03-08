import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAppConfig } from '../lib/config';
import { getVersionPayload } from '../services/version.service';

export function createVersionRouter(): Router {
	const router = Router();

	router.get('/', (_req, res) => {
		const config = getAppConfig(runtimeEnv);
		res.status(200).json(getVersionPayload(config));
	});

	return router;
}
