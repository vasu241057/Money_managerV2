import { json as expressJson } from 'express';

export function createJsonBodyMiddleware() {
	return expressJson();
}
