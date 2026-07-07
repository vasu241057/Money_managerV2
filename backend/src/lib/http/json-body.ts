import type { NextFunction, Request, Response } from 'express';

import { badRequest, payloadTooLarge } from './errors';

const DEFAULT_LIMIT_BYTES = 1024 * 1024;

interface JsonBodyOptions {
	limitBytes?: number;
}

function isJsonContentType(req: Request): boolean {
	const contentType = req.headers['content-type'];
	if (typeof contentType !== 'string') {
		return false;
	}
	return contentType.split(';')[0]?.trim().toLowerCase() === 'application/json';
}

// Streams the request body with a hard byte limit and maps failures to the
// app's HttpError contract (INVALID_JSON / PAYLOAD_TOO_LARGE) instead of
// express.json()'s entity errors, which the error handler can't classify.
export function createJsonBodyMiddleware(options: JsonBodyOptions = {}) {
	const limitBytes = options.limitBytes ?? DEFAULT_LIMIT_BYTES;

	return (req: Request, _res: Response, next: NextFunction): void => {
		if (!isJsonContentType(req)) {
			next();
			return;
		}

		const chunks: Buffer[] = [];
		let totalBytes = 0;
		let settled = false;

		const finish = (error?: unknown): void => {
			if (settled) {
				return;
			}
			settled = true;
			req.removeListener('data', onData);
			req.removeListener('end', onEnd);
			req.removeListener('error', onError);
			next(error);
		};

		const onData = (chunk: Buffer): void => {
			totalBytes += chunk.length;
			if (totalBytes > limitBytes) {
				req.destroy();
				finish(
					payloadTooLarge(
						'PAYLOAD_TOO_LARGE',
						`Request body exceeds limit of ${limitBytes} bytes`,
					),
				);
				return;
			}
			chunks.push(chunk);
		};

		const onEnd = (): void => {
			const raw = Buffer.concat(chunks).toString('utf8').trim();
			if (raw.length === 0) {
				req.body = {};
				finish();
				return;
			}

			try {
				req.body = JSON.parse(raw);
				finish();
			} catch {
				finish(badRequest('INVALID_JSON', 'Request body is not valid JSON'));
			}
		};

		const onError = (error: Error): void => {
			finish(badRequest('INVALID_JSON', `Failed to read request body: ${error.message}`));
		};

		req.on('data', onData);
		req.on('end', onEnd);
		req.on('error', onError);
	};
}
