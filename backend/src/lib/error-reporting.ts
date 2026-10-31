// Error reporting hook (Task 8). Cloudflare's observability panel keeps the
// logs, but nothing pushes: a queue draining into the DLQ at 03:00 or a cron
// that stopped claiming jobs is invisible until someone opens a dashboard.
// This posts one JSON object per noteworthy failure to a webhook the operator
// configures — ntfy, Discord, Slack, a Worker of their own, anything that
// accepts a POST.
//
// Rules this module holds itself to:
//   * Off by default. No ERROR_REPORTING_WEBHOOK_URL, no calls, no cost.
//   * Never throws and never rejects. Reporting a failure must not become a
//     second failure; every caller treats it as fire-and-forget.
//   * Never recurses. A delivery problem is a console.warn, never a report.
//   * Identifiers, not content. Callers pass ids and counts; values are
//     truncated here so an error string that quotes a row can't turn into a
//     bulk export to a third-party endpoint.
//   * Budgeted. An error storm sends at most N per minute per isolate, so a
//     bad deploy pages once instead of ten thousand times.

const DEFAULT_TIMEOUT_MS = 3000;
const DEFAULT_MAX_REPORTS_PER_MINUTE = 10;
const BUDGET_WINDOW_MS = 60_000;
const MAX_MESSAGE_CHARS = 500;
const MAX_CONTEXT_VALUE_CHARS = 200;
const MAX_CONTEXT_KEYS = 20;

export type ErrorReportSeverity = 'error' | 'warning';

export interface ErrorReport {
	/** Stable code, matching the console line it accompanies (QUEUE_ALERT_THRESHOLD_BREACH). */
	event: string;
	severity?: ErrorReportSeverity;
	message?: string | null;
	/** Identifiers and counts. Truncated before sending; never email content. */
	context?: Record<string, unknown>;
}

interface ErrorReportingBindings {
	ERROR_REPORTING_WEBHOOK_URL?: string;
	ERROR_REPORTING_WEBHOOK_TOKEN?: string;
	ERROR_REPORTING_MAX_PER_MINUTE?: string;
	ERROR_REPORTING_TIMEOUT_MS?: string;
	APP_NAME?: string;
	APP_VERSION?: string;
	NODE_ENV?: string;
}

// Per-isolate state. Isolates are short-lived and there may be many, so the
// budget is a best-effort cap on any one of them, not a global rate limit —
// the same shape as the per-colo rate limiter in http/rate-limit.ts.
let windowStartedAt = 0;
let reportsInWindow = 0;
let suppressedInWindow = 0;
let invalidUrlLogged = false;

/** Test seam: clears the per-isolate budget and the one-shot URL warning. */
export function resetErrorReportingState(): void {
	windowStartedAt = 0;
	reportsInWindow = 0;
	suppressedInWindow = 0;
	invalidUrlLogged = false;
}

function parsePositiveNumber(value: string | undefined, fallback: number): number {
	if (typeof value !== 'string') {
		return fallback;
	}

	const parsed = Number(value.trim());
	return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function truncate(value: string, maxChars: number): string {
	return value.length <= maxChars ? value : `${value.slice(0, maxChars)}…`;
}

function safeStringify(value: unknown): string {
	try {
		return JSON.stringify(value) ?? String(value);
	} catch {
		return '[unserializable]';
	}
}

/**
 * Accepts an https endpoint, plus plain http on loopback so `wrangler dev`
 * can point at a local listener. Anything else is refused rather than sent:
 * an error report over plaintext to an arbitrary host is a leak.
 */
function resolveWebhookUrl(raw: string): string | null {
	let parsed: URL;
	try {
		parsed = new URL(raw);
	} catch {
		return null;
	}

	if (parsed.protocol === 'https:') {
		return parsed.toString();
	}

	const isLoopback = parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1';
	return parsed.protocol === 'http:' && isLoopback ? parsed.toString() : null;
}

function withinBudget(limit: number): boolean {
	const now = Date.now();
	if (now - windowStartedAt >= BUDGET_WINDOW_MS) {
		if (suppressedInWindow > 0) {
			console.warn('ERROR_REPORT_SUPPRESSED', {
				suppressedCount: suppressedInWindow,
				windowSeconds: BUDGET_WINDOW_MS / 1000,
			});
		}
		windowStartedAt = now;
		reportsInWindow = 0;
		suppressedInWindow = 0;
	}

	if (reportsInWindow >= limit) {
		suppressedInWindow += 1;
		return false;
	}

	reportsInWindow += 1;
	return true;
}

function sanitizeContext(
	context: Record<string, unknown> | undefined,
): Record<string, string | number | boolean | null> {
	const sanitized: Record<string, string | number | boolean | null> = {};
	if (!context) {
		return sanitized;
	}

	let keyCount = 0;
	for (const [key, value] of Object.entries(context)) {
		if (keyCount >= MAX_CONTEXT_KEYS) {
			break;
		}
		keyCount += 1;

		if (value === null || value === undefined) {
			sanitized[key] = null;
		} else if (typeof value === 'number' || typeof value === 'boolean') {
			sanitized[key] = value;
		} else if (typeof value === 'string') {
			sanitized[key] = truncate(value, MAX_CONTEXT_VALUE_CHARS);
		} else {
			sanitized[key] = truncate(safeStringify(value), MAX_CONTEXT_VALUE_CHARS);
		}
	}

	return sanitized;
}

export function buildErrorReportPayload(
	env: ErrorReportingBindings,
	report: ErrorReport,
): Record<string, unknown> {
	return {
		source: 'money-manager-backend',
		service: env.APP_NAME ?? 'money-manager-backend',
		version: env.APP_VERSION ?? null,
		environment: env.NODE_ENV ?? 'production',
		severity: report.severity ?? 'error',
		event: report.event,
		message: report.message ? truncate(report.message, MAX_MESSAGE_CHARS) : null,
		context: sanitizeContext(report.context),
		occurred_at: new Date().toISOString(),
	};
}

/**
 * Delivers one report. Resolves true only when the sink accepted it; every
 * other outcome (disabled, bad URL, over budget, network error, non-2xx)
 * resolves false after a console line. Never rejects.
 */
export async function reportError(
	env: ErrorReportingBindings,
	report: ErrorReport,
): Promise<boolean> {
	const configured = env.ERROR_REPORTING_WEBHOOK_URL?.trim();
	if (!configured) {
		return false;
	}

	const url = resolveWebhookUrl(configured);
	if (!url) {
		// Once per isolate: a misconfigured URL would otherwise log per error.
		if (!invalidUrlLogged) {
			invalidUrlLogged = true;
			console.warn('ERROR_REPORTING_URL_INVALID', {
				reason: 'must be https, or http on localhost',
			});
		}
		return false;
	}

	const limit = parsePositiveNumber(
		env.ERROR_REPORTING_MAX_PER_MINUTE,
		DEFAULT_MAX_REPORTS_PER_MINUTE,
	);
	if (!withinBudget(limit)) {
		return false;
	}

	const timeoutMs = parsePositiveNumber(env.ERROR_REPORTING_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), timeoutMs);

	try {
		const token = env.ERROR_REPORTING_WEBHOOK_TOKEN?.trim();
		const response = await fetch(url, {
			method: 'POST',
			headers: {
				'content-type': 'application/json',
				...(token ? { authorization: `Bearer ${token}` } : {}),
			},
			body: JSON.stringify(buildErrorReportPayload(env, report)),
			signal: controller.signal,
		});

		if (!response.ok) {
			// console only: reporting a failed report would recurse.
			console.warn('ERROR_REPORT_DELIVERY_FAILED', {
				event: report.event,
				status: response.status,
			});
			return false;
		}

		return true;
	} catch (error) {
		console.warn('ERROR_REPORT_DELIVERY_FAILED', {
			event: report.event,
			message: error instanceof Error ? error.message : 'unknown',
		});
		return false;
	} finally {
		clearTimeout(timer);
	}
}

/**
 * Fire-and-forget wrapper. Pass the ExecutionContext where one exists (queue,
 * scheduled) so the runtime keeps the isolate alive for the POST; on the HTTP
 * path there is no context to pass — express owns that lifecycle — so the
 * report races the response and may be dropped. Best-effort by design: the
 * console line is always written first, by the caller.
 */
export function reportErrorInBackground(
	env: ErrorReportingBindings,
	report: ErrorReport,
	ctx?: Pick<ExecutionContext, 'waitUntil'>,
): void {
	const delivery = reportError(env, report).catch(() => false);
	if (ctx?.waitUntil) {
		ctx.waitUntil(delivery);
		return;
	}

	void delivery;
}
