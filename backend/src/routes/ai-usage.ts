import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import { badRequest, notFound } from '../lib/http/errors';
import { parseUuid } from '../lib/http/validation';
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
import {
	isValidQuotaDay,
	listUserQuotaRollup,
	parseUserLimitsUpdate,
	resolveDefaultQuotaCaps,
	upsertUserLimits,
} from '../services/user-quotas.service';

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

	// Per-user counters against the fair-use caps (Task 6), today in IST by
	// default. Carries each user's email: the operator has to know whose cap
	// to change. Admin-only like everything on this router.
	router.get(
		'/users',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			requireAiUsageAdmin(userId);

			const day = typeof req.query.day === 'string' ? req.query.day : null;
			if (day !== null && !isValidQuotaDay(day)) {
				throw badRequest('INVALID_DAY', 'day must use YYYY-MM-DD');
			}

			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const defaults = resolveDefaultQuotaCaps(config);
			const rollup = await listUserQuotaRollup(sql, day, defaults);
			res.status(200).json({ data: { day: rollup.day, defaults, users: rollup.users } });
		}),
	);

	// Per-user cap overrides; null clears one so the env default applies.
	router.put(
		'/users/:userId/limits',
		asyncHandler(async (req, res) => {
			const adminUserId = getAuthenticatedUserId(req);
			requireAiUsageAdmin(adminUserId);

			let targetUserId: string;
			try {
				targetUserId = parseUuid(req.params.userId, 'userId');
			} catch {
				throw notFound('NOT_FOUND', 'Unknown user');
			}
			const update = parseUserLimitsUpdate(req.body);

			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const exists = await upsertUserLimits(sql, targetUserId, update);
			if (!exists) {
				throw notFound('NOT_FOUND', 'Unknown user');
			}
			const defaults = resolveDefaultQuotaCaps(config);
			const rollup = await listUserQuotaRollup(sql, null, defaults, 1, targetUserId);
			const user = rollup.users[0];
			if (!user) {
				throw notFound('NOT_FOUND', 'Unknown user');
			}
			res.status(200).json({ data: { user } });
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
