import { Router } from 'express';

import { createHealthRouter } from './health';
import { createVersionRouter } from './version';

export function createRoutes(): Router {
	const router = Router();

	router.use('/health', createHealthRouter());
	router.use('/version', createVersionRouter());

	return router;
}
