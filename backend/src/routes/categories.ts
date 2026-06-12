import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { badRequest } from '../lib/http/errors';
import { parseUuid } from '../lib/http/validation';
import {
	createCategory,
	deleteCategory,
	listCategories,
	parseCreateCategoryRequest,
	parseUpdateCategoryRequest,
	updateCategory,
} from '../services/categories.service';

export function createCategoriesRouter(): Router {
	const router = Router();

	router.get(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const type = req.query.type;
			let categoryType: 'income' | 'expense' | 'transfer' | undefined;
			if (type !== undefined) {
				if (
					typeof type !== 'string' ||
					(type !== 'income' && type !== 'expense' && type !== 'transfer')
				) {
					throw badRequest(
						'INVALID_QUERY',
						'type must be one of: income, expense, transfer',
					);
				}
				categoryType = type;
			}

			const categories = await listCategories(sql, userId, categoryType);
			res.status(200).json({ data: categories });
		}),
	);

	router.post(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const payload = parseCreateCategoryRequest(req.body);

			const category = await createCategory(sql, userId, payload);
			res.status(201).json({ data: category });
		}),
	);

	router.patch(
		'/:categoryId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const categoryId = parseUuid(req.params.categoryId, 'categoryId');
			const payload = parseUpdateCategoryRequest(req.body);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const category = await updateCategory(sql, userId, categoryId, payload);
			res.status(200).json({ data: category });
		}),
	);

	router.delete(
		'/:categoryId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const categoryId = parseUuid(req.params.categoryId, 'categoryId');
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			await deleteCategory(sql, userId, categoryId);
			res.status(204).send();
		}),
	);

	return router;
}
