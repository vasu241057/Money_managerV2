const API_PREFIX = '/api';

/**
 * The deployed SPA calls same-origin endpoints under /api. Express keeps its
 * existing root-level routes, so remove only that hosting prefix before the
 * request enters the Node compatibility handler.
 */
export function toBackendRequest<T extends Request>(request: T): T {
	const url = new URL(request.url);
	if (url.pathname !== API_PREFIX && !url.pathname.startsWith(`${API_PREFIX}/`)) {
		return request;
	}

	url.pathname = url.pathname.slice(API_PREFIX.length) || '/';
	return new Request(url, request) as T;
}
