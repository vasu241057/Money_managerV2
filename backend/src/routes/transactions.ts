import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { parseUuid } from '../lib/http/validation';
import {
	createManualTransaction,
	deleteManualTransaction,
	listTransactions,
	parseCreateManualTransactionRequest,
	parseListTransactionsQuery,
	parseUpdateTransactionRequest,
	updateTransaction,
} from '../services/transactions.service';

export function createTransactionsRouter(): Router {
	const router = Router();

	router.get(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const query = parseListTransactionsQuery(req.query as Record<string, unknown>);

			const response = await listTransactions(sql, userId, query);
			res.status(200).json(response);
		}),
	);

	router.post(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const payload = parseCreateManualTransactionRequest(req.body);

			const transaction = await createManualTransaction(sql, userId, payload);
			res.status(201).json({ data: transaction });
		}),
	);

	router.patch(
		'/:transactionId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const transactionId = parseUuid(req.params.transactionId, 'transactionId');
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const payload = parseUpdateTransactionRequest(req.body);

			const transaction = await updateTransaction(sql, userId, transactionId, payload);
			res.status(200).json({ data: transaction });
		}),
	);

	router.delete(
		'/:transactionId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const transactionId = parseUuid(req.params.transactionId, 'transactionId');
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			await deleteManualTransaction(sql, userId, transactionId);
			res.status(204).send();
		}),
	);

	return router;
}
