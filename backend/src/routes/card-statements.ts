import { env as runtimeEnv } from 'cloudflare:workers';
import { Router } from 'express';

import { getAuthenticatedUserId } from '../lib/auth';
import { getAppConfig } from '../lib/config';
import { getSqlClient } from '../lib/db/client';
import { asyncHandler } from '../lib/http/async';
import {
	listCardStatements,
	parseUpsertCardStatementRequest,
	upsertUserCardStatement,
} from '../services/card-statements.service';

export function createCardStatementsRouter(): Router {
	const router = Router();

	router.get(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);

			const statements = await listCardStatements(sql, userId);
			res.status(200).json({ data: statements });
		}),
	);

	// Manual bill entry / correction (source USER — email harvests never
	// overwrite these).
	router.post(
		'/',
		asyncHandler(async (req, res) => {
			const userId = getAuthenticatedUserId(req);
			const config = getAppConfig(runtimeEnv);
			const sql = getSqlClient(config);
			const payload = parseUpsertCardStatementRequest(req.body);

			const statement = await upsertUserCardStatement(sql, userId, payload);
			res.status(200).json({ data: statement });
		}),
	);

	return router;
}
