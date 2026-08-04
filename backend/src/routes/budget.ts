import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { parseUuid } from '../lib/http/validation';
import {
	currentBudgetMonth,
	createTransactionRelationship,
	confirmRecurringPattern,
	deleteCategoryBudgetRule,
	deleteTransactionRelationship,
	dismissBudgetSuggestion,
	getBudgetOverview,
	listRelationshipTargets,
	parseBudgetMonth,
	parseCreateTransactionRelationshipRequest,
	parseConfirmRecurringPatternRequest,
	parseDismissBudgetSuggestionRequest,
	parseSetCategoryBudgetRuleRequest,
	parseUpsertMonthlyBudgetRequest,
	parseUpsertBudgetPlanRequest,
	parseUpdateRecurringSeriesRequest,
	setCategoryBudgetRule,
	upsertMonthlyBudget,
	upsertBudgetPlan,
	updateRecurringSeries,
} from '../services/budget.service';

export function createBudgetRouter(): Router {
	const router = Router();

	router.get(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const month = req.query.month === undefined
				? currentBudgetMonth()
				: parseBudgetMonth(req.query.month);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			res.status(200).json({ data: await getBudgetOverview(sql, userId, month) });
		}),
	);

	router.put(
		'/months/:month/plan',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const month = parseBudgetMonth(req.params.month);
			const input = parseUpsertBudgetPlanRequest(req.body);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			await upsertBudgetPlan(sql, userId, month, input);
			res.status(200).json({ data: await getBudgetOverview(sql, userId, month) });
		}),
	);

	router.put(
		'/months/:month',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const month = parseBudgetMonth(req.params.month);
			const input = parseUpsertMonthlyBudgetRequest(req.body);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			await upsertMonthlyBudget(sql, userId, month, input);
			res.status(200).json({ data: await getBudgetOverview(sql, userId, month) });
		}),
	);

	router.put(
		'/category-rules/:categoryId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const categoryId = parseUuid(req.params.categoryId, 'categoryId');
			const input = parseSetCategoryBudgetRuleRequest(req.body);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			await setCategoryBudgetRule(sql, userId, categoryId, input);
			res.status(204).send();
		}),
	);

	router.delete(
		'/category-rules/:categoryId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const categoryId = parseUuid(req.params.categoryId, 'categoryId');
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			await deleteCategoryBudgetRule(sql, userId, categoryId);
			res.status(204).send();
		}),
	);

	router.post(
		'/relationships',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const input = parseCreateTransactionRelationshipRequest(req.body);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			await createTransactionRelationship(sql, userId, input);
			res.status(204).send();
		}),
	);

	router.get(
		'/relationship-targets/:sourceTransactionId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const sourceTransactionId = parseUuid(req.params.sourceTransactionId, 'sourceTransactionId');
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			res.status(200).json({ data: await listRelationshipTargets(sql, userId, sourceTransactionId) });
		}),
	);

	router.delete(
		'/relationships/:relationshipId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const relationshipId = parseUuid(req.params.relationshipId, 'relationshipId');
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			await deleteTransactionRelationship(sql, userId, relationshipId);
			res.status(204).send();
		}),
	);

	router.post(
		'/suggestions/dismiss',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const input = parseDismissBudgetSuggestionRequest(req.body);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			await dismissBudgetSuggestion(sql, userId, input);
			res.status(204).send();
		}),
	);

	router.post(
		'/recurring-series',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const input = parseConfirmRecurringPatternRequest(req.body);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			await confirmRecurringPattern(sql, userId, input);
			res.status(204).send();
		}),
	);

	router.patch(
		'/recurring-series/:seriesId',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const seriesId = parseUuid(req.params.seriesId, 'seriesId');
			const input = parseUpdateRecurringSeriesRequest(req.body);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			await updateRecurringSeries(sql, userId, seriesId, input);
			res.status(204).send();
		}),
	);

	return router;
}
