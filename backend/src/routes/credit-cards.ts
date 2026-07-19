import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { parseUuid } from '../lib/http/validation';
import {
	createUserCreditCard,
	deleteUserCreditCard,
	listUserCreditCards,
	parseCreateUserCreditCardRequest,
	parseUpdateUserCreditCardRequest,
	updateUserCreditCard,
} from '../services/credit-cards.service';

export function createCreditCardsRouter(): Router {
	const router = Router();

	router.get(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const cards = await listUserCreditCards(sql, userId);
			res.status(200).json({ data: cards });
		}),
	);

	router.post(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const payload = parseCreateUserCreditCardRequest(req.body);

			const { card, relinked_transaction_count } = await createUserCreditCard(
				sql,
				userId,
				payload,
				runtimeEnv,
			);
			res.status(201).json({ data: card, relinked_transaction_count });
		}),
	);

	router.patch(
		'/:creditCardId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const creditCardId = parseUuid(req.params.creditCardId, 'creditCardId');
			const payload = parseUpdateUserCreditCardRequest(req.body);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const { card, relinked_transaction_count } = await updateUserCreditCard(
				sql,
				userId,
				creditCardId,
				payload,
				runtimeEnv,
			);
			res.status(200).json({ data: card, relinked_transaction_count });
		}),
	);

	router.delete(
		'/:creditCardId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const creditCardId = parseUuid(req.params.creditCardId, 'creditCardId');
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			await deleteUserCreditCard(sql, userId, creditCardId);
			res.status(204).send();
		}),
	);

	return router;
}
