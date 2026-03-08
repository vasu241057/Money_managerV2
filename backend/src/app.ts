import express from 'express';

import { toHttpError, toHttpPayload } from './lib/http/errors';
import { createRoutes } from './routes';

export function createApp() {
	const app = express();

	app.disable('x-powered-by');
	app.use(express.json({ limit: '1mb' }));

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
