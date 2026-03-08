import { Router } from 'express';

import { requireAuthenticatedUser } from '../lib/auth';
import { createAccountsRouter } from './accounts';
import { createCategoriesRouter } from './categories';
import { createHealthRouter } from './health';
import { createTransactionsRouter } from './transactions';
import { createVersionRouter } from './version';

export function createRoutes(): Router {
	const router = Router();

	router.use('/health', createHealthRouter());
	router.use('/version', createVersionRouter());
	router.use('/accounts', requireAuthenticatedUser, createAccountsRouter());
	router.use('/categories', requireAuthenticatedUser, createCategoriesRouter());
	router.use('/transactions', requireAuthenticatedUser, createTransactionsRouter());

	return router;
}
