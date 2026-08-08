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
import {
	SCHEDULE_TICK_MINUTES,
	isScheduledJobName,
	isTickAlignedTime,
	listJobSchedules,
	updateJobSchedule,
} from '../services/job-schedules.service';

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
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			const keys = await fetchTavilyKeysUsage(sql, runtimeEnv);
			res.status(200).json({ data: { keys } });
		}),
	);

	// The nightly schedule the dispatcher actually reads. Kept out of the
	// dashboard payload so toggling one job does not force a full reload of
	// spend, processing and mapping history.
	router.get(
		'/job-schedules',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			requireAiUsageAdmin(userId);
			const sql = getSqlClient(getAppConfig(runtimeEnv));
			const schedules = await listJobSchedules(sql, runtimeEnv);
			res.status(200).json({ data: { schedules } });
		}),
	);

	router.put(
		'/job-schedules/:job',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			requireAiUsageAdmin(userId);

			const job = req.params.job;
			if (!isScheduledJobName(job)) {
				throw notFound('NOT_FOUND', 'Unknown scheduled job');
			}

			const body = (req.body ?? {}) as Record<string, unknown>;
			if (body.enabled !== undefined && typeof body.enabled !== 'boolean') {
				throw badRequest('INVALID_ENABLED', 'enabled must be a boolean');
			}
			if (body.run_at_local !== undefined && !isTickAlignedTime(body.run_at_local)) {
				throw badRequest(
					'INVALID_RUN_AT',
					`run_at_local must be HH:MM on a ${SCHEDULE_TICK_MINUTES}-minute boundary`,
				);
			}
			if (body.enabled === undefined && body.run_at_local === undefined) {
				throw badRequest('EMPTY_UPDATE', 'enabled or run_at_local is required');
			}

			const sql = getSqlClient(getAppConfig(runtimeEnv));
			const schedule = await updateJobSchedule(sql, runtimeEnv, job, {
				...(body.enabled === undefined ? {} : { enabled: body.enabled }),
				...(body.run_at_local === undefined
					? {}
					: { run_at_local: body.run_at_local as string }),
			});
			if (schedule === null) {
				throw notFound('NOT_FOUND', 'Unknown scheduled job');
			}
			res.status(200).json({ data: { schedule } });
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
