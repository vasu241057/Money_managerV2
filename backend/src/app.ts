import express from 'express';

import { createRoutes } from './routes';

export function createApp() {
	const app = express();

	app.disable('x-powered-by');
	app.use(express.json({ limit: '1mb' }));

	app.use(createRoutes());

	app.use((_req, res) => {
		res.status(404).json({ error: 'NOT_FOUND' });
	});

	const errorHandler: import('express').ErrorRequestHandler = (error, _req, res, _next) => {
		console.error('Unhandled request error', error);
		res.status(500).json({ error: 'INTERNAL_SERVER_ERROR' });
	};

	app.use(errorHandler);

	return app;
}
