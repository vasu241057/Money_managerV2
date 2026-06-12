import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { parseUuid } from '../lib/http/validation';
import {
	createAccount,
	deleteAccount,
	listAccounts,
	parseCreateAccountRequest,
	parseUpdateAccountRequest,
	updateAccount,
} from '../services/accounts.service';

export function createAccountsRouter(): Router {
	const router = Router();

	router.get(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const accounts = await listAccounts(sql, userId);
			res.status(200).json({ data: accounts });
		}),
	);

	router.post(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const payload = parseCreateAccountRequest(req.body);

			const account = await createAccount(sql, userId, payload);
			res.status(201).json({ data: account });
		}),
	);

	router.patch(
		'/:accountId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const accountId = parseUuid(req.params.accountId, 'accountId');
			const payload = parseUpdateAccountRequest(req.body);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const account = await updateAccount(sql, userId, accountId, payload);
			res.status(200).json({ data: account });
		}),
	);

	router.delete(
		'/:accountId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const accountId = parseUuid(req.params.accountId, 'accountId');
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			await deleteAccount(sql, userId, accountId);
			res.status(204).send();
		}),
	);

	return router;
}
