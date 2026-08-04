import { Router } from 'express';

import { requireAuthenticatedUser } from '../lib/auth';
import { createAccountsRouter } from './accounts';
import { createAiUsageRouter } from './ai-usage';
import { createAnalyticsRouter } from './analytics';
import { createBudgetRouter } from './budget';
import { createCardStatementsRouter } from './card-statements';
import { createCategoriesRouter } from './categories';
import { createCreditCardsRouter } from './credit-cards';
import { createHealthRouter } from './health';
import { createInternalOpsRouter } from './internal-ops';
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
	router.use('/internal', createInternalOpsRouter());
	router.use('/accounts', requireAuthenticatedUser, createAccountsRouter());
	router.use('/ai-usage', requireAuthenticatedUser, createAiUsageRouter());
	router.use('/analytics', requireAuthenticatedUser, createAnalyticsRouter());
	router.use('/budget', requireAuthenticatedUser, createBudgetRouter());
	router.use('/categories', requireAuthenticatedUser, createCategoriesRouter());
	router.use('/credit-cards', requireAuthenticatedUser, createCreditCardsRouter());
	router.use('/card-statements', requireAuthenticatedUser, createCardStatementsRouter());
	router.use('/merchants', requireAuthenticatedUser, createMerchantsRouter());
	router.use('/transactions', requireAuthenticatedUser, createTransactionsRouter());
	router.use('/oauth', requireAuthenticatedUser, createOAuthRouter());

	return router;
}
