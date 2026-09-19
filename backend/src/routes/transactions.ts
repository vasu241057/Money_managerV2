import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { parseUuid } from '../lib/http/validation';
import { withIdempotency } from '../services/idempotency.service';
import {
	createManualTransaction,
	deleteTransaction,
	listTransactions,
	parseCreateManualTransactionRequest,
	parseListTransactionsQuery,
	parseReviewTransactionRequest,
	parseUpdateTransactionRequest,
	reviewTransaction,
	restoreTransaction,
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

			// Validated before the key is claimed, so a malformed body still
			// fails fast without touching the ledger.
			const payload = parseCreateManualTransactionRequest(req.body);

			// The only creating POST in the API, and the one a lost response
			// duplicates. `Idempotency-Key` is opt-in: without the header this
			// behaves exactly as before (Task 7).
			await withIdempotency(sql, userId, req, res, async () => {
				const transaction = await createManualTransaction(sql, userId, payload);
				return { status: 201, body: { data: transaction } };
			});
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

	router.post(
		'/:transactionId/review',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const transactionId = parseUuid(req.params.transactionId, 'transactionId');
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const payload = parseReviewTransactionRequest(req.body);

			const reviewResult = await reviewTransaction(sql, userId, transactionId, payload, {
				merchantConfirmationThreshold: config.merchantConfirmationThreshold,
			});
			res.status(200).json({ data: reviewResult });
		}),
	);

	router.delete(
		'/:transactionId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const transactionId = parseUuid(req.params.transactionId, 'transactionId');
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			await deleteTransaction(sql, userId, transactionId);
			res.status(204).send();
		}),
	);

	router.post(
		'/:transactionId/restore',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const transactionId = parseUuid(req.params.transactionId, 'transactionId');
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			await restoreTransaction(sql, userId, transactionId);
			res.status(204).send();
		}),
	);

	return router;
}
