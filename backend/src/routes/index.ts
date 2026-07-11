import { Router } from 'express';

import { requireAuthenticatedUser } from '../lib/auth';
import { createAccountsRouter } from './accounts';
import { createCategoriesRouter } from './categories';
import { createHealthRouter } from './health';
import { createInternalRequeueRouter } from './internal-requeue';
import { createMerchantsRouter } from './merchants';
import { createOAuthRouter } from './oauth';
import { createTransactionsRouter } from './transactions';
import { createVersionRouter } from './version';

export function createRoutes(): Router {
	const router = Router();

	router.use('/health', createHealthRouter());
	router.use('/version', createVersionRouter());
	router.use('/internal/requeue', createInternalRequeueRouter());
	router.use('/accounts', requireAuthenticatedUser, createAccountsRouter());
	router.use('/categories', requireAuthenticatedUser, createCategoriesRouter());
	router.use('/merchants', requireAuthenticatedUser, createMerchantsRouter());
	router.use('/transactions', requireAuthenticatedUser, createTransactionsRouter());
	router.use('/oauth', requireAuthenticatedUser, createOAuthRouter());

	return router;
}
