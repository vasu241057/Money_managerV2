import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { deleteAccount, parseDeleteAccountRequest } from '../services/account.service';

// The caller's own account only: the id comes from the verified token and
// the body carries nothing but the typed confirmation.
export function createAccountRouter(): Router {
	const router = Router();

	router.delete(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			parseDeleteAccountRequest(req.body);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const outcome = await deleteAccount(sql, userId, runtimeEnv);
			// Ids, counts and flags only; never content.
			console.info('ACCOUNT_DELETED', outcome);
			res.status(204).send();
		}),
	);

	return router;
}
