import express from 'express';
import { env as runtimeEnv } from 'cloudflare:workers';

import { getAppConfig } from './lib/config';
import { runWithSqlClientLifecycle } from './lib/db/client';
import { reportErrorInBackground } from './lib/error-reporting';
import { toHttpError, toHttpPayload } from './lib/http/errors';
import { createJsonBodyMiddleware } from './lib/http/json-body';
import { createIpRateLimitMiddleware } from './lib/http/rate-limit';
import { createRoutes } from './routes';

export function createApp() {
	const app = express();

	app.disable('x-powered-by');
	// Before the body is read: an over-limit caller should not get to stream
	// a megabyte of JSON at us first.
	app.use(createIpRateLimitMiddleware(runtimeEnv));
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

	const errorHandler: import('express').ErrorRequestHandler = (error, req, res, _next) => {
		const httpError = toHttpError(error);
		if (httpError.statusCode >= 500) {
			console.error('Unhandled request error', error);
			// No ExecutionContext on this path, so the POST races the
			// response and may be dropped; the console line above is the
			// record of truth. Path only — never the query string, which is
			// the one part of a URL that carries caller-supplied values.
			reportErrorInBackground(runtimeEnv, {
				event: 'UNHANDLED_REQUEST_ERROR',
				message: error instanceof Error ? error.message : 'unknown',
				context: {
					method: req.method,
					path: req.path,
					statusCode: httpError.statusCode,
					errorCode: httpError.errorCode,
				},
			});
		} else {
			console.warn('Handled request error', {
				statusCode: httpError.statusCode,
				errorCode: httpError.errorCode,
				message: httpError.message,
			});
		}
		// Some errors are only actionable with a header: a 429 without
		// Retry-After tells the caller nothing about when to come back.
		for (const [name, value] of Object.entries(httpError.headers ?? {})) {
			res.setHeader(name, value);
		}
		res.status(httpError.statusCode).json(toHttpPayload(httpError));
	};

	app.use(errorHandler);

	return app;
}
