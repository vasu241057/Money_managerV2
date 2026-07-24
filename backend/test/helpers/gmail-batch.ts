type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

const GMAIL_BATCH_ENDPOINT = 'https://gmail.googleapis.com/batch/gmail/v1';

function getBoundary(contentType: string): string {
	const match = contentType.match(/boundary=(?:"([^"]+)"|([^;\s]+))/i);
	const boundary = match?.[1] ?? match?.[2];
	if (!boundary) {
		throw new Error('Test Gmail batch request is missing boundary');
	}
	return boundary;
}

function getHeader(headers: string, name: string): string | null {
	const target = name.toLowerCase();
	for (const line of headers.split(/\r?\n/)) {
		const separator = line.indexOf(':');
		if (separator < 0) {
			continue;
		}
		if (line.slice(0, separator).trim().toLowerCase() === target) {
			return line.slice(separator + 1).trim();
		}
	}
	return null;
}

export function withGmailBatchSupport(fetcher: FetchLike): FetchLike {
	return async (input, init) => {
		const url = typeof input === 'string' ? input : input.toString();
		if (url !== GMAIL_BATCH_ENDPOINT) {
			return fetcher(input, init);
		}

		const contentType = new Headers(init?.headers).get('content-type') ?? '';
		const requestBoundary = getBoundary(contentType);
		const requestBody = typeof init?.body === 'string' ? init.body : '';
		const requestSections = requestBody
			.split(`--${requestBoundary}`)
			.map(section => section.trim())
			.filter(section => section.length > 0 && section !== '--');

		const responseBoundary = 'test_gmail_batch_response';
		const responseParts: string[] = [];
		for (const section of requestSections) {
			const outerSplit = section.split(/\r?\n\r?\n/, 2);
			const outerHeaders = outerSplit[0] ?? '';
			const nestedRequest = outerSplit[1] ?? '';
			const requestLine = nestedRequest.split(/\r?\n/, 1)[0] ?? '';
			const requestLineMatch = requestLine.match(/^GET\s+(\S+)\s+HTTP\/1\.1$/);
			if (!requestLineMatch) {
				throw new Error(`Invalid test Gmail batch request line: ${requestLine}`);
			}
			const contentId = getHeader(outerHeaders, 'content-id') ?? '<gmail-unknown>';
			const response = await fetcher(
				`https://gmail.googleapis.com${requestLineMatch[1]}`,
				{
					method: 'GET',
					headers: {
						authorization:
							new Headers(init?.headers).get('authorization') ?? '',
						accept: 'application/json',
					},
				},
			);
			const body = await response.text();
			const nestedContentType = response.headers.get('content-type');
			responseParts.push(
				`--${responseBoundary}`,
				'Content-Type: application/http',
				`Content-ID: <response-${contentId.replace(/^<|>$/g, '')}>`,
				'',
				`HTTP/1.1 ${response.status} ${response.statusText || 'Status'}`,
				...(nestedContentType ? [`Content-Type: ${nestedContentType}`] : []),
				'',
				body,
			);
		}
		responseParts.push(`--${responseBoundary}--`, '');

		return new Response(responseParts.join('\r\n'), {
			status: 200,
			headers: {
				'content-type': `multipart/mixed; boundary=${responseBoundary}`,
			},
		});
	};
}
