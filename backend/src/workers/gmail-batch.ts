const GMAIL_BATCH_ENDPOINT = 'https://gmail.googleapis.com/batch/gmail/v1';

export const GMAIL_BATCH_MAX_CALLS = 50;

interface FetchLike {
	(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

export interface GmailBatchSubrequest {
	id: string;
	path: string;
}

export interface GmailBatchPartResponse {
	id: string;
	status: number;
	body: unknown;
}

export interface GmailBatchResponse {
	status: number;
	body: unknown;
	parts: GmailBatchPartResponse[];
}

export class GmailBatchProtocolError extends Error {
	constructor(message: string) {
		super(message);
		this.name = 'GmailBatchProtocolError';
	}
}

function parseJsonOrText(value: string): unknown {
	const normalized = value.trim();
	if (normalized.length === 0) {
		return null;
	}

	try {
		return JSON.parse(normalized) as unknown;
	} catch {
		return normalized;
	}
}

function getMultipartBoundary(contentType: string | null): string | null {
	if (!contentType) {
		return null;
	}

	const match = contentType.match(/boundary=(?:"([^"]+)"|([^;\s]+))/i);
	return match?.[1] ?? match?.[2] ?? null;
}

function splitHeadersAndBody(value: string): {
	headers: string;
	body: string;
} | null {
	const match = /\r?\n\r?\n/.exec(value);
	if (!match || match.index === undefined) {
		return null;
	}

	return {
		headers: value.slice(0, match.index),
		body: value.slice(match.index + match[0].length),
	};
}

function getHeaderValue(headers: string, targetName: string): string | null {
	const target = targetName.toLowerCase();
	for (const line of headers.split(/\r?\n/)) {
		const separatorIndex = line.indexOf(':');
		if (separatorIndex < 0) {
			continue;
		}
		const name = line.slice(0, separatorIndex).trim().toLowerCase();
		if (name !== target) {
			continue;
		}
		const value = line.slice(separatorIndex + 1).trim();
		return value.length > 0 ? value : null;
	}
	return null;
}

function normalizeResponseContentId(value: string | null): string | null {
	if (!value) {
		return null;
	}

	const unwrapped = value.replace(/^<|>$/g, '');
	const normalized = unwrapped.startsWith('response-')
		? unwrapped.slice('response-'.length)
		: unwrapped;
	return normalized.length > 0 ? normalized : null;
}

function parseMultipartResponse(
	bodyText: string,
	boundary: string,
	requests: GmailBatchSubrequest[],
): GmailBatchPartResponse[] {
	const requestIdByContentId = new Map(
		requests.map((request, index) => [`gmail-${index}`, request.id]),
	);
	const parsedParts: GmailBatchPartResponse[] = [];
	const sections = bodyText.split(`--${boundary}`);

	for (const sectionRaw of sections) {
		let section = sectionRaw.replace(/^\r?\n/, '');
		if (section.trim().length === 0 || section.trim() === '--') {
			continue;
		}
		section = section.replace(/\r?\n--\s*$/, '');

		const outer = splitHeadersAndBody(section);
		if (!outer) {
			throw new GmailBatchProtocolError('Gmail batch response part is missing outer headers');
		}

		const nested = splitHeadersAndBody(outer.body);
		if (!nested) {
			throw new GmailBatchProtocolError('Gmail batch response part is missing HTTP headers');
		}

		const nestedHeaderLines = nested.headers.split(/\r?\n/);
		const statusLine = nestedHeaderLines[0] ?? '';
		const statusMatch = statusLine.match(/^HTTP\/\d(?:\.\d)?\s+(\d{3})(?:\s|$)/i);
		if (!statusMatch) {
			throw new GmailBatchProtocolError('Gmail batch response part has an invalid status line');
		}
		const status = Number(statusMatch[1]);

		const contentId = normalizeResponseContentId(
			getHeaderValue(outer.headers, 'content-id'),
		);
		const fallbackRequest = requests[parsedParts.length];
		const requestId = contentId
			? requestIdByContentId.get(contentId)
			: fallbackRequest?.id;
		if (!requestId) {
			throw new GmailBatchProtocolError('Gmail batch response part cannot be matched to a request');
		}

		parsedParts.push({
			id: requestId,
			status,
			body: parseJsonOrText(nested.body),
		});
	}

	if (parsedParts.length !== requests.length) {
		throw new GmailBatchProtocolError(
			`Gmail batch response returned ${parsedParts.length} parts for ${requests.length} requests`,
		);
	}

	const seenIds = new Set<string>();
	for (const part of parsedParts) {
		if (seenIds.has(part.id)) {
			throw new GmailBatchProtocolError(`Gmail batch response repeated request id ${part.id}`);
		}
		seenIds.add(part.id);
	}

	return parsedParts;
}

function buildMultipartRequestBody(
	requests: GmailBatchSubrequest[],
	boundary: string,
): string {
	const lines: string[] = [];
	for (let index = 0; index < requests.length; index += 1) {
		const request = requests[index];
		lines.push(
			`--${boundary}`,
			'Content-Type: application/http',
			`Content-ID: <gmail-${index}>`,
			'',
			`GET ${request.path} HTTP/1.1`,
			'Accept: application/json',
			'',
		);
	}
	lines.push(`--${boundary}--`, '');
	return lines.join('\r\n');
}

export async function executeGmailBatch(
	accessToken: string,
	requests: GmailBatchSubrequest[],
	fetcher: FetchLike,
): Promise<GmailBatchResponse> {
	if (requests.length === 0) {
		return { status: 200, body: null, parts: [] };
	}
	if (requests.length > GMAIL_BATCH_MAX_CALLS) {
		throw new GmailBatchProtocolError(
			`Gmail batch cannot exceed ${GMAIL_BATCH_MAX_CALLS} calls`,
		);
	}

	const requestIds = new Set<string>();
	for (const request of requests) {
		if (request.id.length === 0 || requestIds.has(request.id)) {
			throw new GmailBatchProtocolError('Gmail batch request ids must be non-empty and unique');
		}
		if (!request.path.startsWith('/gmail/v1/')) {
			throw new GmailBatchProtocolError('Gmail batch request path must target Gmail v1');
		}
		requestIds.add(request.id);
	}

	const boundary = `gmail_batch_${crypto.randomUUID().replace(/-/g, '')}`;
	const response = await fetcher(GMAIL_BATCH_ENDPOINT, {
		method: 'POST',
		headers: {
			authorization: `Bearer ${accessToken}`,
			accept: 'multipart/mixed, application/json',
			'content-type': `multipart/mixed; boundary=${boundary}`,
		},
		body: buildMultipartRequestBody(requests, boundary),
	});

	const responseText = await response.text();
	if (!response.ok) {
		return {
			status: response.status,
			body: parseJsonOrText(responseText),
			parts: [],
		};
	}

	const responseBoundary = getMultipartBoundary(response.headers.get('content-type'));
	if (!responseBoundary) {
		throw new GmailBatchProtocolError('Gmail batch response is missing its multipart boundary');
	}

	return {
		status: response.status,
		body: null,
		parts: parseMultipartResponse(responseText, responseBoundary, requests),
	};
}
