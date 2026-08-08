import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { badRequest, notFound } from '../lib/http/errors';
import {
	fetchTavilyKeysUsage,
	getAiUsageDashboard,
	getAiUsageSummary,
	isAiUsageAdmin,
	isValidAiUsageMonth,
} from '../services/ai-usage.service';

function requireAiUsageAdmin(userId: string): void {
	if (!isAiUsageAdmin(runtimeEnv, userId)) {
		// Deliberately indistinguishable from an absent route for non-admins.
		throw notFound('NOT_FOUND', 'Route not found');
	}
}

export function createAiUsageRouter(): Router {
	const router = Router();

	router.get(
		'/summary',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			requireAiUsageAdmin(userId);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			const summary = await getAiUsageSummary(sql, runtimeEnv, userId);
			res.status(200).json({ data: summary });
		}),
	);

	// Separate from the dashboard on purpose: this reaches out to Tavily once
	// per configured key, so it must not add latency to a screen the admin may
	// open only to read spend. The merchant mapping tab requests it on open.
	router.get(
		'/tavily-keys',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			requireAiUsageAdmin(userId);
			const keys = await fetchTavilyKeysUsage(runtimeEnv);
			res.status(200).json({ data: { keys } });
		}),
	);

	router.get(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			requireAiUsageAdmin(userId);

			const month =
				typeof req.query.month === 'string' ? req.query.month : undefined;
			if (month !== undefined && !isValidAiUsageMonth(month)) {
				throw badRequest('INVALID_MONTH', 'month must use YYYY-MM');
			}

			const rawPage = typeof req.query.page === 'string' ? req.query.page : '1';
			const page = Number(rawPage);
			if (!Number.isSafeInteger(page) || page < 1 || page > 10_000) {
				throw badRequest('INVALID_PAGE', 'page must be a positive integer');
			}

			const sql = getSqlClient(getAppConfig(runtimeEnv));
			const dashboard = await getAiUsageDashboard(sql, userId, month, page, runtimeEnv);
			res.status(200).json({ data: dashboard });
		}),
	);

	return router;
}
