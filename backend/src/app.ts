import express from 'express';
import { env as runtimeEnv } from 'cloudflare:workers';

import { getAppConfig } from './lib/config';
import { runWithSqlClientLifecycle } from './lib/db/client';
import { toHttpError, toHttpPayload } from './lib/http/errors';
import { createJsonBodyMiddleware } from './lib/http/json-body';
import { createRoutes } from './routes';

export function createApp() {
	const app = express();

	app.disable('x-powered-by');
	app.use(createJsonBodyMiddleware());
	app.use((req, res, next) => {
		let requestCompleted = false;
		let nextCalled = false;
		let resolveCompletion: (() => void) | null = null;
		const completion = new Promise<void>((resolve) => {
			resolveCompletion = resolve;
		});

		const finalize = () => {
			if (requestCompleted) {
				return;
			}

			requestCompleted = true;
			res.off('finish', finalize);
			res.off('close', finalize);
			resolveCompletion?.();
		};

		res.on('finish', finalize);
		res.on('close', finalize);

		try {
			const config = getAppConfig(runtimeEnv);
			void runWithSqlClientLifecycle(config, async () => {
				nextCalled = true;
				next();
				await completion;
			}).catch((error) => {
				if (!nextCalled && !requestCompleted) {
					next(error);
					return;
				}

				console.error('Failed while closing request-scoped SQL client', error);
			});
		} catch (error) {
			next(error);
		}
	});

	app.use(createRoutes());

	app.use((_req, res) => {
		res.status(404).json({
			error: 'NOT_FOUND',
			message: 'Route not found',
		});
	});

	const errorHandler: import('express').ErrorRequestHandler = (error, _req, res, _next) => {
		const httpError = toHttpError(error);
		if (httpError.statusCode >= 500) {
			console.error('Unhandled request error', error);
		} else {
			console.warn('Handled request error', {
				statusCode: httpError.statusCode,
				errorCode: httpError.errorCode,
				message: httpError.message,
			});
		}
		res.status(httpError.statusCode).json(toHttpPayload(httpError));
	};

	app.use(errorHandler);

	return app;
}
