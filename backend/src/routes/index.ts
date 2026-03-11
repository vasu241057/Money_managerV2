import { Router } from 'express';

import { requireAuthenticatedUser } from '../lib/auth';
import { createAccountsRouter } from './accounts';
import { createCategoriesRouter } from './categories';
import { createHealthRouter } from './health';
import { createInternalAiRouter } from './internal-ai';
import { createMerchantsRouter } from './merchants';
import { createOAuthRouter } from './oauth';
import { createTransactionsRouter } from './transactions';
import { createVersionRouter } from './version';

export function createRoutes(): Router {
	const router = Router();

	router.use('/health', createHealthRouter());
	router.use('/version', createVersionRouter());
	router.use('/internal/ai', createInternalAiRouter());
	router.use('/accounts', requireAuthenticatedUser, createAccountsRouter());
	router.use('/categories', requireAuthenticatedUser, createCategoriesRouter());
	router.use('/merchants', requireAuthenticatedUser, createMerchantsRouter());
	router.use('/transactions', requireAuthenticatedUser, createTransactionsRouter());
	router.use('/oauth', requireAuthenticatedUser, createOAuthRouter());

	return router;
}
