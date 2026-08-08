import { useMemo, useRef, useState, type KeyboardEvent } from 'react';
import {
	Activity,
	AlertCircle,
	ArrowRight,
	Bot,
	CheckCircle2,
	ChevronDown,
	ChevronLeft,
	ChevronRight,
	ChevronUp,
	CircleDollarSign,
	Clock3,
	Cpu,
	Database,
	HelpCircle,
	Inbox,
	KeyRound,
	Layers3,
	Mail,
	RefreshCw,
	RotateCcw,
	Search,
	ShieldCheck,
	Tags,
	XCircle,
	type LucideIcon,
} from 'lucide-react';
import type {
	AiMerchantMappingSummary,
	AiTavilyKeyCredits,
	AiProcessingRun,
	AiProcessingSummary,
	AiUsageEvent,
	AiUsageModelBreakdown,
	AiUsageOperation,
	AiUsageProvider,
	MerchantType,
} from '../../../../shared/types';
import {
	currentMonthIST,
	useAiUsageDashboard,
	useAiUsageSummary,
	useTavilyKeys,
} from '../../hooks/useAiUsage';
import { SkeletonBar } from '../ui/Skeleton';

interface AiUsageDashboardProps {
	onBack: () => void;
}

const DASHBOARD_TABS = [
	{ id: 'spend', label: 'Spend' },
	{ id: 'processing', label: 'Processing' },
	{ id: 'mapping', label: 'Merchant mapping' },
] as const;

interface OperationMeta {
	label: string;
	detail: string;
	icon: LucideIcon;
}

const OPERATION_META: Record<AiUsageOperation, OperationMeta> = {
	EMAIL_NORMALIZATION: {
		label: 'Email extraction',
		detail: 'Read a transaction email',
		icon: Mail,
	},
	SENDER_CLASSIFICATION: {
		label: 'Sender check',
		detail: 'Classified financial senders',
		icon: ShieldCheck,
	},
	MERCHANT_MAPPING: {
		label: 'Merchant mapping',
		detail: 'Resolved merchant identities',
		icon: Tags,
	},
};

function formatUsd(value: number | null): string {
	if (value === null) return '—';
	if (value > 0 && value < 0.01) return `$${value.toFixed(6)}`;
	return `$${value.toFixed(2)}`;
}

function formatCount(value: number): string {
	return new Intl.NumberFormat('en-IN').format(value);
}

function formatPercent(value: number): string {
	return `${Math.round(Math.min(100, Math.max(0, value)))}%`;
}

function formatLatency(value: number): string {
	if (value < 1_000) return `${value} ms`;
	return `${(value / 1_000).toFixed(value < 10_000 ? 1 : 0)} s`;
}

function formatMonth(month: string): string {
	const date = new Date(`${month}-01T00:00:00Z`);
	return new Intl.DateTimeFormat('en-IN', { month: 'long', year: 'numeric' }).format(date);
}

function formatShortMonth(month: string): string {
	const date = new Date(`${month}-01T00:00:00Z`);
	return new Intl.DateTimeFormat('en-IN', { month: 'short' }).format(date);
}

function formatEventTime(value: string): string {
	return new Intl.DateTimeFormat('en-IN', {
		timeZone: 'Asia/Kolkata',
		day: '2-digit',
		month: 'short',
		hour: '2-digit',
		minute: '2-digit',
	}).format(new Date(value));
}

function formatDate(value: string): string {
	return new Intl.DateTimeFormat('en-IN', {
		timeZone: 'Asia/Kolkata',
		day: 'numeric',
		month: 'short',
		year: 'numeric',
	}).format(new Date(value));
}

function formatRefreshTime(value: string | null | undefined): string {
	if (!value) return 'Local ledger';
	return `Updated ${new Intl.DateTimeFormat('en-IN', {
		timeZone: 'Asia/Kolkata',
		hour: '2-digit',
		minute: '2-digit',
	}).format(new Date(value))}`;
}

function shortModelName(model: string): string {
	return model.includes('/') ? model.slice(model.indexOf('/') + 1) : model;
}

function providerLabel(provider: AiUsageProvider): string {
	return provider === 'openrouter' ? 'OpenRouter' : 'OpenCode';
}

function humanizeErrorCode(value: string): string {
	return value.toLowerCase().replaceAll('_', ' ');
}

function EventReceipt({ event, isLast }: { event: AiUsageEvent; isLast: boolean }) {
	const [expanded, setExpanded] = useState(false);
	const success = event.status === 'SUCCESS';
	const operation = OPERATION_META[event.operation];
	const OperationIcon = operation.icon;
	const isFree = event.model.endsWith(':free');

	return (
		<div style={!isLast ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}>
			<button
				type="button"
				onClick={() => setExpanded((value) => !value)}
				aria-expanded={expanded}
				className="ai-usage-focus w-full px-4 py-4 text-left flex items-start gap-3.5 transition-colors"
			>
				<div
					className="w-9 h-9 rounded-[10px] flex items-center justify-center shrink-0"
					style={{
						background: success ? 'var(--ledger-nav-active)' : 'var(--ledger-amber-bg)',
						color: success ? 'var(--ledger-accent)' : 'var(--ledger-negative)',
					}}
				>
					<OperationIcon size={16} strokeWidth={2} />
				</div>

				<div className="min-w-0 flex-1">
					<div className="flex items-start justify-between gap-3">
						<div className="min-w-0">
							<div className="flex items-center gap-1.5 min-w-0">
								<p className="text-sm font-bold truncate" style={{ color: 'var(--ledger-fg)' }}>
									{operation.label}
								</p>
								<span
									className="text-[8px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded-full shrink-0"
									style={{
										background: success ? 'var(--ledger-nav-active)' : 'var(--ledger-amber-bg)',
										color: success ? 'var(--ledger-positive)' : 'var(--ledger-negative)',
									}}
								>
									{success ? 'Settled' : 'Failed'}
								</span>
							</div>
							<p className="text-[11px] truncate mt-1" style={{ color: 'var(--ledger-muted)' }}>
								{shortModelName(event.model)} · {formatEventTime(event.created_at)}
							</p>
						</div>
						<div className="text-right shrink-0">
							<p className="text-sm font-bold tnum" style={{ color: 'var(--ledger-fg)' }}>
								{isFree && event.cost_usd === 0 ? 'Free' : formatUsd(event.cost_usd)}
							</p>
							<p className="text-[10px] tnum mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
								{formatCount(event.total_tokens)} tokens
							</p>
						</div>
					</div>
				</div>
				{expanded ? (
					<ChevronUp size={14} className="mt-1 shrink-0" style={{ color: 'var(--ledger-muted)' }} />
				) : (
					<ChevronDown size={14} className="mt-1 shrink-0" style={{ color: 'var(--ledger-muted)' }} />
				)}
			</button>

			{expanded && (
				<div
					className="mx-4 mb-4 rounded-[12px] overflow-hidden"
					style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)' }}
				>
					<div className="px-3.5 py-2.5 flex items-center justify-between gap-3" style={{ borderBottom: '1px solid var(--ledger-rule)' }}>
						<p className="text-[10px]" style={{ color: 'var(--ledger-muted)' }}>{operation.detail}</p>
						<p className="text-[10px] tnum shrink-0" style={{ color: 'var(--ledger-muted)' }}>
							{formatLatency(event.latency_ms)}
						</p>
					</div>
					<div className="grid grid-cols-2">
						{[
							{ label: 'Input tokens', value: event.prompt_tokens },
							{ label: 'Output tokens', value: event.completion_tokens },
							{ label: 'Reasoning tokens', value: event.reasoning_tokens },
							{ label: 'Cached tokens', value: event.cached_tokens },
							{ label: 'Total tokens', value: event.total_tokens },
							{ label: 'Items in call', value: event.item_count },
						].map(({ label, value }, index) => (
							<div
								key={label}
								className="px-3.5 py-3"
								style={{
									borderRight: index % 2 === 0 ? '1px solid var(--ledger-rule)' : undefined,
									borderBottom: index < 4 ? '1px solid var(--ledger-rule)' : undefined,
								}}
							>
								<p className="text-[9px] font-bold uppercase tracking-wider" style={{ color: 'var(--ledger-muted)' }}>
									{label}
								</p>
								<p className="text-sm font-semibold tnum mt-0.5" style={{ color: 'var(--ledger-fg)' }}>
									{formatCount(Number(value))}
								</p>
							</div>
						))}
					</div>
					<div className="px-3.5 py-3" style={{ borderTop: '1px solid var(--ledger-rule)' }}>
						<div className="flex flex-wrap gap-x-2 gap-y-1 text-[10px]" style={{ color: 'var(--ledger-muted)' }}>
							<span>{providerLabel(event.provider)}</span>
							<span>·</span>
							<span className="break-all">{event.model}</span>
							{event.http_status !== null && <><span>·</span><span>HTTP {event.http_status}</span></>}
							{event.error_code && <><span>·</span><span>{humanizeErrorCode(event.error_code)}</span></>}
						</div>
						{event.generation_id && (
							<p className="text-[9px] tnum mt-2 break-all" style={{ color: 'var(--ledger-muted)', opacity: 0.75 }}>
								Receipt {event.generation_id}
							</p>
						)}
					</div>
				</div>
			)}
		</div>
	);
}

function ProcessingRunReceipt({ run, isLast }: { run: AiProcessingRun; isLast: boolean }) {
	const [expanded, setExpanded] = useState(false);
	const recovered = run.status === 'SUCCESS' && run.attempt_count > 1;
	const failed = run.status === 'FAILED';
	const running = run.status === 'RUNNING';
	const label = running ? 'In progress' : failed ? 'Failed' : recovered ? 'Recovered' : 'First try';
	const color = failed
		? 'var(--ledger-negative)'
		: running || recovered
			? 'var(--ledger-amber)'
			: 'var(--ledger-positive)';
	const background = failed || running || recovered ? 'var(--ledger-amber-bg)' : 'var(--ledger-nav-active)';
	const totalCost = run.attempts.reduce((sum, attempt) => sum + (attempt.cost_usd ?? 0), 0);
	const hasKnownCost = run.attempts.some((attempt) => attempt.cost_usd !== null);
	const totalLatency = run.attempts.reduce((sum, attempt) => sum + attempt.latency_ms, 0);

	return (
		<div style={!isLast ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}>
			<button
				type="button"
				onClick={() => setExpanded((value) => !value)}
				aria-expanded={expanded}
				className="ai-usage-focus w-full px-4 py-4 text-left flex items-start gap-3.5 transition-colors"
			>
				<div className="w-9 h-9 rounded-[10px] flex items-center justify-center shrink-0" style={{ background, color }}>
					{failed ? <XCircle size={16} /> : running ? <Clock3 size={16} /> : recovered ? <RotateCcw size={16} /> : <CheckCircle2 size={16} />}
				</div>
				<div className="min-w-0 flex-1">
					<div className="flex items-start justify-between gap-3">
						<div className="min-w-0">
							<div className="flex items-center gap-1.5">
								<p className="text-sm font-bold">{run.item_count === 1 ? '1 email' : `${formatCount(run.item_count)} emails`}</p>
								<span className="text-[8px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded-full" style={{ background, color }}>
									{label}
								</span>
							</div>
							<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>
								{formatEventTime(run.started_at)} · {formatCount(run.attempt_count)} {run.attempt_count === 1 ? 'attempt' : 'attempts'}
							</p>
						</div>
						<div className="text-right shrink-0">
							<p className="text-xs font-bold tnum">{hasKnownCost ? (totalCost === 0 ? 'Free' : formatUsd(totalCost)) : '—'}</p>
							<p className="text-[10px] tnum mt-0.5" style={{ color: 'var(--ledger-muted)' }}>{formatLatency(totalLatency)}</p>
						</div>
					</div>
				</div>
				{expanded ? <ChevronUp size={14} className="mt-1 shrink-0" /> : <ChevronDown size={14} className="mt-1 shrink-0" />}
			</button>

			{expanded && (
				<div className="mx-4 mb-4 rounded-[12px] overflow-hidden" style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)' }}>
					{run.attempts.map((attempt, index) => {
						const succeeded = attempt.status === 'SUCCESS';
						return (
							<div key={attempt.id} className="px-3.5 py-3 flex items-start gap-3" style={index < run.attempts.length - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}>
								<div className="w-5 h-5 rounded-full flex items-center justify-center shrink-0 mt-0.5" style={{ background: succeeded ? 'var(--ledger-nav-active)' : 'var(--ledger-amber-bg)', color: succeeded ? 'var(--ledger-positive)' : 'var(--ledger-negative)' }}>
									{succeeded ? <CheckCircle2 size={12} /> : <XCircle size={12} />}
								</div>
								<div className="min-w-0 flex-1">
									<p className="text-[11px] font-bold">Attempt {attempt.attempt_number} · {shortModelName(attempt.model)}</p>
									<p className="text-[9px] mt-1" style={{ color: 'var(--ledger-muted)' }}>
										{succeeded ? 'Succeeded' : attempt.http_status ? `Failed · HTTP ${attempt.http_status}` : `Failed${attempt.error_code ? ` · ${humanizeErrorCode(attempt.error_code)}` : ''}`}
									</p>
								</div>
								<div className="text-right shrink-0 text-[9px] tnum" style={{ color: 'var(--ledger-muted)' }}>
									<p>{attempt.cost_usd === 0 ? 'Free' : formatUsd(attempt.cost_usd)}</p>
									<p className="mt-1">{formatLatency(attempt.latency_ms)}</p>
								</div>
							</div>
						);
					})}
					{run.attempts.length === 0 && (
						<p className="px-3.5 py-3 text-[10px]" style={{ color: 'var(--ledger-muted)' }}>Waiting for the first provider response.</p>
					)}
				</div>
			)}
		</div>
	);
}

interface ProcessingPanelProps {
	processing: AiProcessingSummary | undefined;
	selectedMonth: string;
	olderMonth: string | null;
	newerMonth: string | null;
	onSelectMonth: (month: string) => void;
	error: string | null;
	onRetry: () => void;
}

function ProcessingPanel({
	processing,
	selectedMonth,
	olderMonth,
	newerMonth,
	onSelectMonth,
	error,
	onRetry,
}: ProcessingPanelProps) {
	const data = processing ?? {
		emails_handled: 0,
		template_hits: 0,
		ai_routed_emails: 0,
		email_final_failures: 0,
		transactions_created: 0,
		logical_calls: 0,
		provider_attempts: 0,
		first_attempt_successes: 0,
		recovered_calls: 0,
		final_failures: 0,
		in_progress_calls: 0,
		recent_runs: [],
	};
	const completedCalls = data.first_attempt_successes + data.recovered_calls + data.final_failures;
	const averageAttempts = completedCalls > 0 ? data.provider_attempts / completedCalls : 0;
	const successRate = completedCalls > 0
		? ((data.first_attempt_successes + data.recovered_calls) / completedCalls) * 100
		: 0;
	const routeTotal = data.template_hits + data.ai_routed_emails;
	const templateRate = routeTotal > 0 ? (data.template_hits / routeTotal) * 100 : 0;

	return (
		<>
			<section className="ai-usage-card rounded-[16px] overflow-hidden mb-6" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
				<div className="px-4 py-3 flex items-center justify-between gap-3" style={{ borderBottom: '1px solid var(--ledger-rule)' }}>
					<button type="button" onClick={() => olderMonth && onSelectMonth(olderMonth)} disabled={!olderMonth} className="ai-usage-focus w-8 h-8 rounded-full flex items-center justify-center disabled:opacity-25" aria-label="Older month">
						<ChevronLeft size={17} />
					</button>
					<div className="text-center">
						<p className="text-[9px] font-bold uppercase tracking-[0.18em]" style={{ color: 'var(--ledger-muted)' }}>Processing report</p>
						<p className="text-sm font-bold mt-0.5">{formatMonth(selectedMonth)}</p>
					</div>
					<button type="button" onClick={() => newerMonth && onSelectMonth(newerMonth)} disabled={!newerMonth} className="ai-usage-focus w-8 h-8 rounded-full flex items-center justify-center disabled:opacity-25" aria-label="Newer month">
						<ChevronRight size={17} />
					</button>
				</div>

				<div className="p-5">
					<div className="flex items-end justify-between gap-4 mb-5">
						<div>
							<p className="text-[9px] font-bold uppercase tracking-wider" style={{ color: 'var(--ledger-muted)' }}>Emails handled</p>
							<p className="text-[34px] leading-none font-bold tracking-[-0.04em] tnum mt-1.5" style={{ color: 'var(--ledger-accent)' }}>{formatCount(data.emails_handled)}</p>
						</div>
						<div className="text-right">
							<p className="text-sm font-bold tnum" style={{ color: successRate >= 95 ? 'var(--ledger-positive)' : 'var(--ledger-amber)' }}>{formatPercent(successRate)}</p>
							<p className="text-[9px] font-bold uppercase tracking-wider mt-0.5" style={{ color: 'var(--ledger-muted)' }}>AI call success</p>
						</div>
					</div>

					<div className="grid grid-cols-[1fr_auto_1fr_auto_1fr] items-stretch rounded-[12px] overflow-hidden" style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)' }}>
						<div className="px-2 py-3.5 text-center min-w-0">
							<Inbox size={15} className="mx-auto mb-1.5" style={{ color: 'var(--ledger-accent)' }} />
							<p className="text-base font-bold tnum">{formatCount(data.emails_handled)}</p>
							<p className="text-[8px] font-bold uppercase tracking-wider mt-1" style={{ color: 'var(--ledger-muted)' }}>Handled</p>
						</div>
						<ArrowRight size={14} className="self-center" style={{ color: 'var(--ledger-muted)' }} />
						<div className="px-2 py-3.5 text-center min-w-0">
							<Layers3 size={15} className="mx-auto mb-1.5" style={{ color: 'var(--ledger-accent)' }} />
							<p className="text-[11px] font-bold tnum">{formatCount(data.template_hits)} template</p>
							<p className="text-[9px] tnum mt-1" style={{ color: 'var(--ledger-muted)' }}>{formatCount(data.ai_routed_emails)} AI · {formatPercent(templateRate)} saved</p>
						</div>
						<ArrowRight size={14} className="self-center" style={{ color: 'var(--ledger-muted)' }} />
						<div className="px-2 py-3.5 text-center min-w-0">
							<CheckCircle2 size={15} className="mx-auto mb-1.5" style={{ color: 'var(--ledger-positive)' }} />
							<p className="text-base font-bold tnum">{formatCount(data.transactions_created)}</p>
							<p className="text-[8px] font-bold uppercase tracking-wider mt-1" style={{ color: 'var(--ledger-muted)' }}>Transactions</p>
						</div>
					</div>
					{data.email_final_failures > 0 && (
						<p className="text-[10px] mt-3 flex items-center gap-1.5" style={{ color: 'var(--ledger-negative)' }}><AlertCircle size={12} /> {formatCount(data.email_final_failures)} {data.email_final_failures === 1 ? 'email' : 'emails'} could not be processed</p>
					)}
				</div>
			</section>

			<section className="mb-6">
				<div className="flex items-end justify-between gap-3 mb-3 px-1">
					<div>
						<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>Retry health</p>
						<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>One call may include several provider attempts</p>
					</div>
					<Bot size={16} style={{ color: 'var(--ledger-accent)' }} />
				</div>
				<div className="grid grid-cols-2 rounded-[14px] overflow-hidden ai-usage-card" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
					{[
						['AI calls', formatCount(data.logical_calls)],
						['Provider tries', formatCount(data.provider_attempts)],
						['First try', formatCount(data.first_attempt_successes)],
						['Recovered', formatCount(data.recovered_calls)],
						['Final failures', formatCount(data.final_failures)],
						['Avg tries / call', averageAttempts.toFixed(completedCalls > 0 ? 1 : 0)],
					].map(([label, value], index) => (
						<div key={label} className="px-4 py-3.5" style={{ borderRight: index % 2 === 0 ? '1px solid var(--ledger-rule)' : undefined, borderBottom: index < 4 ? '1px solid var(--ledger-rule)' : undefined }}>
							<p className="text-base font-bold tnum">{value}</p>
							<p className="text-[8px] font-bold uppercase tracking-wider mt-1" style={{ color: 'var(--ledger-muted)' }}>{label}</p>
						</div>
					))}
				</div>
				{data.in_progress_calls > 0 && <p className="text-[10px] mt-2 px-1" style={{ color: 'var(--ledger-amber)' }}>{formatCount(data.in_progress_calls)} currently in progress</p>}
			</section>

			<section>
				<div className="flex items-end justify-between gap-3 mb-3 px-1">
					<div>
						<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>Recent AI calls</p>
						<p className="text-[11px] mt-1 flex items-center gap-1.5" style={{ color: 'var(--ledger-muted)' }}><Database size={11} /> Metadata only · no email content</p>
					</div>
					<span className="text-[10px] tnum" style={{ color: 'var(--ledger-muted)' }}>{formatCount(data.logical_calls)} total</span>
				</div>
				{error ? (
					<div className="rounded-[14px] px-5 py-6 text-center" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-negative)' }}>
						<AlertCircle size={22} className="mx-auto mb-2" style={{ color: 'var(--ledger-negative)' }} />
						<p className="text-sm font-bold">Processing history could not be loaded</p>
						<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>{error}</p>
						<button type="button" onClick={onRetry} className="ai-usage-focus mt-4 px-4 py-2 rounded-[8px] text-xs font-bold" style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }}>Try again</button>
					</div>
				) : (
					<div className="ai-usage-card rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
						{data.recent_runs.length > 0 ? data.recent_runs.map((run, index) => (
							<ProcessingRunReceipt key={run.id} run={run} isLast={index === data.recent_runs.length - 1} />
						)) : (
							<div className="px-5 py-8 text-center">
								<Clock3 size={22} className="mx-auto mb-2" style={{ color: 'var(--ledger-muted)' }} />
								<p className="text-sm font-bold">No processing history this month</p>
								<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>Tracking starts with the first email processed after this update.</p>
							</div>
						)}
					</div>
				)}
			</section>
		</>
	);
}

const MERCHANT_TYPE_LABELS: Record<MerchantType, string> = {
	MERCHANT: 'Merchant',
	TRANSFER_INSTITUTION: 'Bank / transfer',
	AGGREGATOR: 'Aggregator',
	P2P: 'Person',
};

function MerchantMappingDayCard({ day }: { day: AiMerchantMappingSummary['days'][number] }) {
	const [expanded, setExpanded] = useState(false);
	// Searches and AI calls without mappings are a deliberate preview run
	// (the preview really pays for Tavily and tokens but persists nothing),
	// so they must read as activity, not as a broken run.
	const ranWithoutMappings = day.tavily_searches > 0 && day.merchants_mapped === 0;

	return (
		<div className="ai-usage-card rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
			<button
				type="button"
				onClick={() => setExpanded((value) => !value)}
				aria-expanded={expanded}
				className="ai-usage-focus w-full px-4 py-4 text-left flex items-start gap-3.5 transition-colors"
			>
				<div className="w-9 h-9 rounded-[10px] flex items-center justify-center shrink-0" style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}>
					<Tags size={16} strokeWidth={2} />
				</div>
				<div className="min-w-0 flex-1">
					<div className="flex items-start justify-between gap-3">
						<div className="min-w-0">
							<p className="text-sm font-bold" style={{ color: 'var(--ledger-fg)' }}>{formatDate(`${day.day}T00:00:00Z`)}</p>
							<p className="text-[10px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
								{day.counters_available
									? `${day.merchants_mapped} mapped · ${day.merchants_skipped_unsure} unsure`
									: `${day.mappings.length} mapping${day.mappings.length === 1 ? '' : 's'} on file`}
							</p>
						</div>
						<div className="text-right shrink-0">
							<p className="text-xs font-bold tnum" style={{ color: 'var(--ledger-fg)' }}>
								{day.counters_available ? `${day.tavily_searches} searches` : 'Counts not recorded'}
							</p>
							{day.counters_available && (
								<p className="text-[10px] tnum mt-0.5" style={{ color: 'var(--ledger-muted)' }}>{day.merchant_ai_calls} AI calls</p>
							)}
						</div>
					</div>
					{!day.counters_available && (
						<p className="text-[10px] mt-2 flex items-center gap-1.5" style={{ color: 'var(--ledger-muted)' }}>
							<HelpCircle size={11} /> Mappings survive, but this day's counters were never written — treat the totals as unknown, not zero.
						</p>
					)}
					{day.counters_available && ranWithoutMappings && (
						<p className="text-[10px] mt-2 flex items-center gap-1.5" style={{ color: 'var(--ledger-amber)' }}>
							<HelpCircle size={11} /> Searches without mappings — a preview run, or nothing was confidently identified.
						</p>
					)}
				</div>
				{expanded ? (
					<ChevronUp size={14} className="mt-1 shrink-0" style={{ color: 'var(--ledger-muted)' }} />
				) : (
					<ChevronDown size={14} className="mt-1 shrink-0" style={{ color: 'var(--ledger-muted)' }} />
				)}
			</button>

			{expanded && (
				<div className="mx-4 mb-4 rounded-[12px] overflow-hidden" style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)' }}>
					{day.mappings.length > 0 ? (
						day.mappings.map((mapping, index) => {
							const isMerchant = mapping.merchant_type === 'MERCHANT';
							return (
								<div key={mapping.search_key} className="px-3.5 py-3" style={index < day.mappings.length - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}>
									<div className="flex items-start justify-between gap-3">
										<div className="min-w-0 flex-1">
											{/* The payee as the bank wrote it reads far better than the
											    normalized key; the key stays visible underneath because it
											    is what the identity graph actually matches on. */}
											<p className="text-[11px] break-all" style={{ color: 'var(--ledger-fg)' }}>
												{mapping.raw_payee ?? mapping.search_key}
											</p>
											{mapping.raw_payee !== null && (
												<p className="text-[9px] font-mono break-all mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
													{mapping.search_key}
												</p>
											)}
											<div className="flex items-center gap-1.5 mt-1 min-w-0">
												<p className="text-sm font-bold truncate" style={{ color: 'var(--ledger-fg)' }}>{mapping.canonical_name}</p>
												<span className="text-[8px] font-bold uppercase tracking-wider rounded-full px-1.5 py-0.5 shrink-0" style={{ background: isMerchant ? 'var(--ledger-nav-active)' : 'var(--ledger-amber-bg)', color: isMerchant ? 'var(--ledger-accent)' : 'var(--ledger-amber)' }}>
													{MERCHANT_TYPE_LABELS[mapping.merchant_type]}
												</span>
												{!mapping.confirmed && (
													<span className="text-[8px] font-bold uppercase tracking-wider rounded-full px-1.5 py-0.5 shrink-0" style={{ background: 'var(--ledger-amber-bg)', color: 'var(--ledger-amber)' }}>
														Unconfirmed
													</span>
												)}
											</div>
										</div>
										<div className="text-right shrink-0">
											<p className="text-xs font-semibold tnum" style={{ color: mapping.default_category ? 'var(--ledger-fg)' : 'var(--ledger-muted)' }}>
												{mapping.default_category ?? 'No default category'}
											</p>
											{!mapping.default_category && (
												<p className="text-[9px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
													{isMerchant ? 'None assigned' : 'Correct — never set for aggregators & people'}
												</p>
											)}
										</div>
									</div>
								</div>
							);
						})
					) : (
						<div className="px-3.5 py-3">
							<p className="text-[10px]" style={{ color: 'var(--ledger-muted)' }}>
								No mappings were created this day — the run's answers were unsure or nothing was a fit.
							</p>
						</div>
					)}
				</div>
			)}
		</div>
	);
}

function MerchantMappingPanel({
	summary,
	keys,
	keysLoading,
	error,
	onRetry,
}: {
	summary: AiMerchantMappingSummary | undefined;
	keys: AiTavilyKeyCredits[];
	keysLoading: boolean;
	error: string | null;
	onRetry: () => void;
}) {
	const enabled = summary?.mapper_enabled ?? false;
	const days = summary?.days ?? [];
	const anyKeyUnavailable = keys.some((key) => key.unavailable);

	return (
		<>
			<section
				className="ai-usage-card rounded-[16px] overflow-hidden mb-5"
				style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
			>
				<div className="px-4 py-3.5 flex items-center justify-between gap-3">
					<div className="flex items-center gap-2.5">
						<ShieldCheck size={15} style={{ color: enabled ? 'var(--ledger-positive)' : 'var(--ledger-amber)' }} />
						<div>
							<p className="text-sm font-bold" style={{ color: 'var(--ledger-fg)' }}>
								{enabled ? 'Nightly mapping is running' : 'Nightly mapping is not running'}
							</p>
							<p className="text-[10px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
								{enabled
									? 'The daily job maps unmapped payee wordings with Tavily searches and one AI pass.'
									: 'MERCHANT_MAPPING_ENABLED is unset, so nothing is being mapped. The view below is history plus live credit state.'}
							</p>
						</div>
					</div>
					<span
						className="flex items-center gap-1.5 text-[9px] font-bold uppercase tracking-wider px-2 py-1 rounded-full shrink-0"
						style={{ background: enabled ? 'var(--ledger-nav-active)' : 'var(--ledger-amber-bg)', color: enabled ? 'var(--ledger-positive)' : 'var(--ledger-amber)' }}
					>
						<span className="w-1.5 h-1.5 rounded-full" style={{ background: enabled ? 'var(--ledger-positive)' : 'var(--ledger-amber)' }} />
						{enabled ? 'Enabled' : 'Disabled'}
					</span>
				</div>
			</section>

			<section className="mb-6">
				<div className="flex items-end justify-between gap-3 mb-3 px-1">
					<div>
						<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>Tavily credits</p>
						<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>Separate budget from the AI spend — one basic search costs 1 credit</p>
					</div>
					<Search size={16} style={{ color: 'var(--ledger-accent)' }} />
				</div>

				{keysLoading ? (
					<div className="rounded-[14px] px-5 py-6 text-center" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
						<p className="text-[11px]" style={{ color: 'var(--ledger-muted)' }}>Checking key balances with Tavily…</p>
					</div>
				) : keys.length === 0 ? (
					<div className="rounded-[14px] px-5 py-6 text-center" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
						<KeyRound size={22} className="mx-auto mb-2" style={{ color: 'var(--ledger-muted)' }} />
						<p className="text-sm font-bold">No Tavily key configured</p>
						<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>Set TAVILY_API_KEY for the mapper to search.</p>
					</div>
				) : (
					<div className="space-y-3">
						{keys.map((key) => {
							const hasNumbers = key.remaining_credits !== null;
							const usedPercent =
								key.used_credits !== null && key.limit_credits !== null && key.limit_credits > 0
									? (key.used_credits / key.limit_credits) * 100
									: key.plan_used_credits !== null && key.plan_limit_credits !== null && key.plan_limit_credits > 0
										? (key.plan_used_credits / key.plan_limit_credits) * 100
										: null;
							return (
								<div key={key.label} className="ai-usage-card rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
									<div className="px-4 py-3 flex items-center justify-between gap-3" style={{ borderBottom: '1px solid var(--ledger-rule)' }}>
										<div className="flex items-center gap-2.5">
											<span className="w-2 h-2 rounded-full" style={{ background: key.unavailable ? 'var(--ledger-amber)' : key.remaining_credits !== null && key.remaining_credits <= 100 ? 'var(--ledger-negative)' : 'var(--ledger-accent)' }} />
											<span className="text-sm font-bold">{key.label}</span>
											{key.is_live && (
												<span className="text-[8px] font-bold uppercase tracking-wider rounded-full px-1.5 py-0.5" style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}>
													Live
												</span>
											)}
										</div>
										<span className="text-[11px] tnum" style={{ color: 'var(--ledger-muted)' }}>
											{key.plan_name ? `${key.plan_name} plan` : ''}
										</span>
									</div>

									<div className="px-4 py-3.5">
										{key.unavailable ? (
											<div className="flex items-center gap-2">
												<AlertCircle size={14} style={{ color: 'var(--ledger-amber)' }} />
												<p className="text-xs font-bold" style={{ color: 'var(--ledger-amber)' }}>Credit balance unavailable</p>
												<p className="text-[10px]" style={{ color: 'var(--ledger-muted)' }}>The usage endpoint did not answer for this key.</p>
											</div>
										) : (
											<>
												<div className="flex items-end justify-between gap-3">
													<div>
														<p className="text-[24px] leading-none font-bold tracking-[-0.03em] tnum" style={{ color: 'var(--ledger-fg)' }}>
															{hasNumbers ? `${formatCount(key.remaining_credits ?? 0)} credits` : '—'}
														</p>
														<p className="text-[9px] font-bold uppercase tracking-wider mt-1" style={{ color: 'var(--ledger-muted)' }}>remaining this cycle</p>
													</div>
													<div className="text-right">
														<p className="text-xs font-semibold tnum" style={{ color: 'var(--ledger-muted)' }}>
															{key.used_credits !== null ? `${formatCount(key.used_credits)} used` : ''}
														</p>
														<p className="text-[10px] tnum mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
															{key.limit_credits !== null ? `of ${formatCount(key.limit_credits)} (key)` : key.plan_limit_credits !== null ? `of ${formatCount(key.plan_limit_credits)} (plan)` : ''}
														</p>
													</div>
												</div>
												{usedPercent !== null && (
													<div className="h-1.5 rounded-full mt-3 overflow-hidden" style={{ background: 'var(--ledger-rule)' }}>
														<div className="h-full rounded-full" style={{ width: `${Math.min(100, Math.max(0, usedPercent))}%`, background: usedPercent >= 90 ? 'var(--ledger-negative)' : 'var(--ledger-accent)' }} />
													</div>
												)}
												{key.fetched_at && (
													<p className="text-[9px] mt-2" style={{ color: 'var(--ledger-muted)' }}>
														{formatRefreshTime(key.fetched_at)}
													</p>
												)}
											</>
										)}
									</div>
								</div>
							);
						})}
						{anyKeyUnavailable && (
							<p className="text-[10px] px-1 flex items-center gap-1.5" style={{ color: 'var(--ledger-amber)' }}>
								<AlertCircle size={12} /> Some keys could not report — an exhausted or revoked key only becomes visible here, so treat the unavailable ones as suspect before a run.
							</p>
						)}
					</div>
				)}
			</section>

			<section>
				<div className="flex items-end justify-between gap-3 mb-3 px-1">
					<div>
						<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>Mapping log</p>
						<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>Days with recorded activity · open a day to read its mappings</p>
					</div>
					<Tags size={16} style={{ color: 'var(--ledger-accent)' }} />
				</div>

				{error ? (
					<div className="rounded-[14px] px-5 py-6 text-center" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-negative)' }}>
						<AlertCircle size={22} className="mx-auto mb-2" style={{ color: 'var(--ledger-negative)' }} />
						<p className="text-sm font-bold">Mapping history could not be loaded</p>
						<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>{error}</p>
						<button type="button" onClick={onRetry} className="ai-usage-focus mt-4 px-4 py-2 rounded-[8px] text-xs font-bold" style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }}>Try again</button>
					</div>
				) : days.length > 0 ? (
					<div className="space-y-3">
						{days.map((day) => (
							<MerchantMappingDayCard key={day.day} day={day} />
						))}
						<p className="text-[10px] px-1" style={{ color: 'var(--ledger-muted)' }}>
							Days with nothing recorded are not listed — a quiet day and an unmapped one are indistinguishable, by design.
							{summary?.truncated && ' Older days exist beyond this window.'}
						</p>
					</div>
				) : (
					<div className="rounded-[14px] px-5 py-8 text-center" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
						<Clock3 size={22} className="mx-auto mb-2" style={{ color: 'var(--ledger-muted)' }} />
						<p className="text-sm font-bold">No merchant mapping activity recorded</p>
						<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>A run records Tavily searches, AI calls, and every payee mapping here.</p>
					</div>
				)}
			</section>
		</>
	);
}

function AiUsageSkeleton() {
	return (
		<div aria-label="Loading AI usage" className="space-y-5">
			<div className="rounded-[14px] p-5" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
				<div className="flex justify-between mb-7">
					<SkeletonBar className="h-2.5 w-28 rounded-full" />
					<SkeletonBar className="h-5 w-16 rounded-full" delayMs={100} />
				</div>
				<SkeletonBar className="h-10 w-40 rounded-[8px]" delayMs={120} />
				<SkeletonBar className="h-2.5 w-52 rounded-full mt-3" delayMs={200} />
				<SkeletonBar className="h-3 w-full rounded-full mt-7" delayMs={260} />
				<div className="grid grid-cols-3 gap-4 mt-6">
					{[0, 1, 2].map((item) => <SkeletonBar key={item} className="h-11 rounded-[8px]" delayMs={item * 100} />)}
				</div>
			</div>
			<div className="rounded-[14px] p-5" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
				<SkeletonBar className="h-3 w-36 rounded-full mb-5" />
				<div className="grid grid-cols-3 gap-3">
					{[0, 1, 2, 3, 4, 5].map((item) => <SkeletonBar key={item} className="h-14 rounded-[8px]" delayMs={item * 70} />)}
				</div>
			</div>
			<div>
				<SkeletonBar className="h-2.5 w-32 rounded-full mb-3" />
				<div className="rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
					{[0, 1, 2].map((item) => (
						<div key={item} className="p-4 flex gap-3" style={item < 2 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}>
							<SkeletonBar className="w-9 h-9 rounded-[10px]" delayMs={item * 80} />
							<div className="flex-1 space-y-2">
								<SkeletonBar className="h-3 w-32 rounded-full" delayMs={item * 80 + 50} />
								<SkeletonBar className="h-2 w-48 rounded-full" delayMs={item * 80 + 100} />
							</div>
						</div>
					))}
				</div>
			</div>
		</div>
	);
}

export function AiUsageDashboard({ onBack }: AiUsageDashboardProps) {
	const [activeTabIndex, setActiveTabIndex] = useState(0);
	const activeTab = DASHBOARD_TABS[activeTabIndex].id;
	const [selectedMonth, setSelectedMonth] = useState(currentMonthIST);
	const [page, setPage] = useState(1);
	const swipeStart = useRef<{ x: number; y: number } | null>(null);
	const {
		summary,
		isLoading: summaryLoading,
		isRefreshing: summaryRefreshing,
		error: summaryError,
		refetch: refetchSummary,
	} = useAiUsageSummary();
	const {
		dashboard,
		isLoading,
		isRefreshing,
		error,
		refetch: refetchDashboard,
	} = useAiUsageDashboard(selectedMonth, page);
	// Only fetched while the mapping tab is open: it calls Tavily once per key.
	const {
		keys: tavilyKeys,
		isLoading: tavilyKeysLoading,
		error: tavilyKeysError,
	} = useTavilyKeys(activeTab === 'mapping');

	const providerGroups = useMemo(() => {
		const grouped = new Map<AiUsageProvider, AiUsageModelBreakdown[]>();
		for (const model of dashboard?.models ?? []) {
			const list = grouped.get(model.provider) ?? [];
			list.push(model);
			grouped.set(model.provider, list);
		}
		return [...grouped.entries()];
	}, [dashboard?.models]);

	const selectedSummary = dashboard?.months.find((item) => item.month === selectedMonth) ?? null;
	const monthOptions = Array.from(
		new Set([currentMonthIST(), selectedMonth, ...(dashboard?.months.map((item) => item.month) ?? [])]),
	).sort((a, b) => b.localeCompare(a));
	const selectedMonthIndex = monthOptions.indexOf(selectedMonth);
	const olderMonth = monthOptions[selectedMonthIndex + 1] ?? null;
	const newerMonth = selectedMonthIndex > 0 ? monthOptions[selectedMonthIndex - 1] ?? null : null;
	const historyMonths = monthOptions.slice(0, 6).reverse().map((month) => ({
		month,
		data: dashboard?.months.find((item) => item.month === month) ?? null,
	}));
	const historyMaxCost = Math.max(...historyMonths.map((item) => item.data?.cost_usd ?? 0), 0.000001);
	const totalPages = dashboard
		? Math.max(1, Math.ceil(dashboard.total_event_count / dashboard.page_size))
		: 1;

	const account = summary?.account ?? null;
	const creditCeiling = account?.purchased_credits_usd ?? account?.limit_usd ?? null;
	const creditRemaining = account?.purchased_credits_usd !== null && account?.purchased_credits_usd !== undefined
		? account.credits_remaining_usd
		: account?.remaining_usd ?? null;
	const hasRunway = creditCeiling !== null && creditCeiling > 0 && creditRemaining !== null;
	const runwaySpent = hasRunway ? Math.max(0, creditCeiling - creditRemaining) : 0;
	const runwayUsedPercent = hasRunway ? (runwaySpent / creditCeiling) * 100 : 0;
	const runwayAvailablePercent = hasRunway ? (creditRemaining / creditCeiling) * 100 : 0;
	const monthCalls = selectedSummary?.request_count ?? 0;
	const monthSuccessRate = monthCalls > 0
		? ((selectedSummary?.success_count ?? 0) / monthCalls) * 100
		: 0;
	const averageCost = monthCalls > 0 ? (selectedSummary?.cost_usd ?? 0) / monthCalls : 0;
	const initialLoading = (summaryLoading && !summary) || (isLoading && !dashboard);
	const refreshing = summaryRefreshing || isRefreshing;

	const selectMonth = (month: string) => {
		setSelectedMonth(month);
		setPage(1);
	};

	const refreshAll = () => {
		void Promise.all([refetchSummary(), refetchDashboard()]);
	};

	const finishSwipe = (x: number, y: number) => {
		if (!swipeStart.current) return;
		const deltaX = x - swipeStart.current.x;
		const deltaY = y - swipeStart.current.y;
		swipeStart.current = null;
		if (Math.abs(deltaX) < 60 || Math.abs(deltaX) <= Math.abs(deltaY) * 1.2) return;
		setActiveTabIndex((index) =>
			Math.min(DASHBOARD_TABS.length - 1, Math.max(0, index + (deltaX < 0 ? 1 : -1))),
		);
	};

	const moveFocusToTab = (current: HTMLElement, direction: number) => {
		const buttons = Array.from(
			current.querySelectorAll<HTMLButtonElement>('[role="tab"]'),
		);
		if (buttons.length === 0) return;
		const next = Math.min(buttons.length - 1, Math.max(0, activeTabIndex + direction));
		setActiveTabIndex(next);
		buttons[next]?.focus();
	};

	const onTablistKeyDown = (event: KeyboardEvent<HTMLElement>) => {
		if (event.key === 'ArrowRight') {
			event.preventDefault();
			moveFocusToTab(event.currentTarget, 1);
		} else if (event.key === 'ArrowLeft') {
			event.preventDefault();
			moveFocusToTab(event.currentTarget, -1);
		}
	};

	return (
		<div className="flex-1 flex flex-col min-h-0 relative" style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}>
			<header
				className="h-[56px] shrink-0 flex items-center justify-between gap-3 px-4 z-10"
				style={{ borderBottom: '1px solid var(--ledger-rule)', background: 'var(--ledger-bg)' }}
			>
				<div className="flex items-center gap-3 min-w-0">
					<button
						type="button"
						onClick={onBack}
						aria-label="Back to settings"
						className="ai-usage-focus w-9 h-9 rounded-full flex items-center justify-center active:scale-95 transition-transform"
						style={{ color: 'var(--ledger-fg)' }}
					>
						<ChevronLeft size={20} />
					</button>
					<div className="min-w-0">
						<h1 className="font-bold text-xl tracking-tight">AI operations</h1>
						<p className="text-[9px] uppercase font-bold tracking-[0.18em]" style={{ color: 'var(--ledger-muted)' }}>
							Private cost & processing
						</p>
					</div>
				</div>
				<button
					type="button"
					onClick={refreshAll}
					disabled={refreshing}
					aria-label="Refresh AI operations"
					className="ai-usage-focus w-9 h-9 rounded-full flex items-center justify-center disabled:opacity-50"
					style={{ color: 'var(--ledger-muted)', border: '1px solid var(--ledger-rule)' }}
				>
					<RefreshCw size={15} className={refreshing ? 'animate-spin' : ''} />
				</button>
			</header>

			<nav className="shrink-0 px-4 sm:px-5" style={{ background: 'var(--ledger-bg)', borderBottom: '1px solid var(--ledger-rule)' }} aria-label="AI operations views">
				<div className="w-full max-w-[720px] mx-auto grid grid-cols-3" role="tablist" onKeyDown={onTablistKeyDown}>
					{DASHBOARD_TABS.map((tab, index) => (
						<button
							type="button"
							key={tab.id}
							role="tab"
							id={`ai-usage-tab-${tab.id}`}
							aria-selected={activeTab === tab.id}
							aria-controls={`ai-usage-panel-${tab.id}`}
							tabIndex={activeTab === tab.id ? 0 : -1}
							onClick={() => setActiveTabIndex(index)}
							className="ai-usage-focus relative h-11 text-xs font-bold"
							style={{ color: activeTab === tab.id ? 'var(--ledger-fg)' : 'var(--ledger-muted)' }}
						>
							{tab.label}
							<span
								className="absolute left-3 right-3 bottom-0 h-0.5 rounded-full transition-opacity"
								style={{ background: 'var(--ledger-accent)', opacity: activeTab === tab.id ? 1 : 0 }}
							/>
						</button>
					))}
				</div>
			</nav>

			<main
				className="flex-1 overflow-y-auto px-4 sm:px-5 pt-5 pb-24 min-h-0"
				onTouchStart={(event) => {
					const touch = event.changedTouches[0];
					if (touch) swipeStart.current = { x: touch.clientX, y: touch.clientY };
				}}
				onTouchEnd={(event) => {
					const touch = event.changedTouches[0];
					if (touch) finishSwipe(touch.clientX, touch.clientY);
				}}
			>
				<div className="w-full max-w-[720px] mx-auto">
					{initialLoading ? (
						<AiUsageSkeleton />
					) : (
						<div role="tabpanel" id={`ai-usage-panel-${activeTab}`} aria-labelledby={`ai-usage-tab-${activeTab}`}>
							{activeTab === 'mapping' ? (
								<MerchantMappingPanel
									summary={dashboard?.merchant_mapping}
									keys={tavilyKeys}
									keysLoading={tavilyKeysLoading}
									error={error ?? tavilyKeysError}
									onRetry={() => { void refetchDashboard(); }}
								/>
							) : activeTab === 'processing' ? (
								<ProcessingPanel
									processing={dashboard?.processing}
									selectedMonth={selectedMonth}
									olderMonth={olderMonth}
									newerMonth={newerMonth}
									onSelectMonth={selectMonth}
									error={error}
									onRetry={() => { void refetchDashboard(); }}
								/>
							) : (
								<>
									<section
										className="ai-usage-card rounded-[16px] overflow-hidden mb-5"
										style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
									>
										<div className="p-5">
											<div className="flex items-center justify-between gap-3">
												<div className="flex items-center gap-2">
													<CircleDollarSign size={14} style={{ color: 'var(--ledger-accent)' }} />
													<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
														{hasRunway ? 'Credit runway' : 'AI spend position'}
													</p>
												</div>
												<span
													className="flex items-center gap-1.5 text-[9px] font-bold uppercase tracking-wider px-2 py-1 rounded-full"
													style={{ background: 'var(--ledger-nav-active)', color: 'var(--ledger-accent)' }}
												>
													<span className="w-1.5 h-1.5 rounded-full" style={{ background: account ? 'var(--ledger-positive)' : 'var(--ledger-muted)' }} />
													{account ? 'OpenRouter live' : 'Local ledger'}
												</span>
											</div>

											<div className="mt-5">
												<p className="text-[38px] leading-none font-bold tracking-[-0.045em] tnum" style={{ color: 'var(--ledger-fg)' }}>
													{hasRunway
														? formatUsd(creditRemaining)
														: formatUsd(account?.current_month_cost_usd ?? summary?.tracked_current_month_cost_usd ?? 0)}
												</p>
												<p className="text-xs mt-2" style={{ color: 'var(--ledger-muted)' }}>
													{hasRunway
														? `${formatPercent(runwayAvailablePercent)} of purchased credits still available`
														: account
															? 'Spent by the configured key this month'
															: 'Tracked from app calls this month'}
												</p>
											</div>

											{hasRunway && (
												<div className="mt-6">
													<div className="ai-runway-rail h-3 rounded-full overflow-hidden">
														<div
															className="ai-runway-fill h-full rounded-full"
															style={{ width: `${Math.min(100, Math.max(0, runwayUsedPercent))}%` }}
														/>
													</div>
													<div className="flex items-center justify-between mt-2 text-[10px] tnum" style={{ color: 'var(--ledger-muted)' }}>
														<span>{formatUsd(runwaySpent)} used</span>
														<span>{formatUsd(creditCeiling)} purchased</span>
													</div>
												</div>
											)}
										</div>

										<div className="grid grid-cols-3" style={{ borderTop: '1px solid var(--ledger-rule)', background: 'var(--ledger-bg)' }}>
											{[
												['This month', formatUsd(account?.current_month_cost_usd ?? summary?.tracked_current_month_cost_usd ?? 0)],
												['Tracked calls', formatCount(summary?.tracked_current_month_request_count ?? 0)],
												['All-time use', formatUsd(account?.all_time_cost_usd ?? summary?.tracked_all_time_cost_usd ?? 0)],
											].map(([label, value], index) => (
												<div key={label} className="px-3 py-3.5 min-w-0" style={index < 2 ? { borderRight: '1px solid var(--ledger-rule)' } : undefined}>
													<p className="text-sm font-bold tnum truncate" style={{ color: 'var(--ledger-fg)' }}>{value}</p>
													<p className="text-[8px] font-bold uppercase tracking-wider mt-1 truncate" style={{ color: 'var(--ledger-muted)' }}>{label}</p>
												</div>
											))}
										</div>

										<div className="px-4 py-2.5 flex flex-wrap items-center justify-between gap-x-3 gap-y-1" style={{ borderTop: '1px solid var(--ledger-rule)' }}>
											<p className="text-[9px]" style={{ color: 'var(--ledger-muted)' }}>
												{formatRefreshTime(account?.fetched_at)}
											</p>
											{account && !hasRunway && (
												<p className="text-[9px]" style={{ color: 'var(--ledger-amber)' }}>
													Credit balance unavailable
												</p>
											)}
											{summary?.tracked_since && (
												<p className="text-[9px]" style={{ color: 'var(--ledger-muted)' }}>
													Receipts since {formatDate(summary.tracked_since)}
												</p>
											)}
										</div>
									</section>

									{summaryError && (
										<div className="rounded-[12px] px-4 py-3.5 mb-5 flex items-start gap-3" style={{ background: 'var(--ledger-amber-bg)', color: 'var(--ledger-amber)' }}>
											<AlertCircle size={16} className="shrink-0 mt-0.5" />
											<div className="min-w-0">
												<p className="text-xs font-bold">Account balance is temporarily unavailable</p>
												<p className="text-[10px] mt-1 opacity-80">Tracked call receipts are still shown below.</p>
											</div>
										</div>
									)}

									<section
										className="ai-usage-card rounded-[16px] overflow-hidden mb-6"
										style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
									>
										<div className="px-4 py-3 flex items-center justify-between gap-3" style={{ borderBottom: '1px solid var(--ledger-rule)' }}>
											<button
												type="button"
												onClick={() => olderMonth && selectMonth(olderMonth)}
												disabled={!olderMonth}
												className="ai-usage-focus w-8 h-8 rounded-full flex items-center justify-center disabled:opacity-25"
												aria-label="Older month"
											>
												<ChevronLeft size={17} />
											</button>
											<div className="text-center">
												<p className="text-[9px] font-bold uppercase tracking-[0.18em]" style={{ color: 'var(--ledger-muted)' }}>Monthly statement</p>
												<p className="text-sm font-bold mt-0.5">{formatMonth(selectedMonth)}</p>
											</div>
											<button
												type="button"
												onClick={() => newerMonth && selectMonth(newerMonth)}
												disabled={!newerMonth}
												className="ai-usage-focus w-8 h-8 rounded-full flex items-center justify-center disabled:opacity-25"
												aria-label="Newer month"
											>
												<ChevronRight size={17} />
											</button>
										</div>

										<div className="p-5">
											<div className="flex items-end justify-between gap-4">
												<div>
													<p className="text-[9px] font-bold uppercase tracking-wider" style={{ color: 'var(--ledger-muted)' }}>Tracked spend</p>
													<p className="text-[30px] leading-none font-bold tracking-[-0.04em] tnum mt-1.5" style={{ color: 'var(--ledger-accent)' }}>
														{formatUsd(selectedSummary?.cost_usd ?? 0)}
													</p>
												</div>
												<div className="text-right">
													<p className="text-sm font-bold tnum" style={{ color: monthSuccessRate >= 95 ? 'var(--ledger-positive)' : 'var(--ledger-amber)' }}>
														{formatPercent(monthSuccessRate)}
													</p>
													<p className="text-[9px] font-bold uppercase tracking-wider mt-0.5" style={{ color: 'var(--ledger-muted)' }}>Success rate</p>
												</div>
											</div>

											<div className="grid grid-cols-3 mt-5 rounded-[12px] overflow-hidden" style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)' }}>
												{[
													['Calls', formatCount(monthCalls)],
													['Avg / call', formatUsd(averageCost)],
													['Avg latency', formatLatency(selectedSummary?.average_latency_ms ?? 0)],
												].map(([label, value], index) => (
													<div key={label} className="px-3 py-3" style={index < 2 ? { borderRight: '1px solid var(--ledger-rule)' } : undefined}>
														<p className="text-xs font-bold tnum truncate">{value}</p>
														<p className="text-[8px] font-bold uppercase tracking-wider mt-1 truncate" style={{ color: 'var(--ledger-muted)' }}>{label}</p>
													</div>
												))}
											</div>

											<div className="flex items-center gap-4 mt-3 text-[10px] tnum" style={{ color: 'var(--ledger-muted)' }}>
												<span>{formatCount(selectedSummary?.total_tokens ?? 0)} tokens</span>
												<span>{formatCount(selectedSummary?.item_count ?? 0)} items processed</span>
											</div>
										</div>

										{historyMonths.length > 0 && (
											<div className="px-4 pt-3 pb-4" style={{ borderTop: '1px solid var(--ledger-rule)' }}>
												<div className="flex items-center justify-between mb-3">
													<p className="text-[9px] font-bold uppercase tracking-[0.16em]" style={{ color: 'var(--ledger-muted)' }}>Spend pulse</p>
													<p className="text-[9px]" style={{ color: 'var(--ledger-muted)' }}>Recent tracked months</p>
												</div>
												<div className="grid gap-1" style={{ gridTemplateColumns: `repeat(${historyMonths.length}, minmax(0, 1fr))` }}>
													{historyMonths.map(({ month, data }) => {
														const selected = month === selectedMonth;
														const cost = data?.cost_usd ?? 0;
														const height = cost > 0 ? Math.max(7, (cost / historyMaxCost) * 52) : 3;
														return (
															<button
																type="button"
																key={month}
																onClick={() => selectMonth(month)}
																className="ai-usage-focus min-w-0 rounded-[8px] px-1 pt-1.5 pb-1"
																style={selected ? { background: 'var(--ledger-nav-active)' } : undefined}
																aria-label={`${formatMonth(month)}: ${formatUsd(cost)}`}
															>
																<div className="h-[56px] flex items-end justify-center">
																	<div
																		className="w-2.5 rounded-full transition-[height] duration-300"
																		style={{ height, background: selected ? 'var(--ledger-accent)' : 'var(--ledger-rule)' }}
																	/>
																</div>
																<p className="text-[9px] font-bold mt-1" style={{ color: selected ? 'var(--ledger-accent)' : 'var(--ledger-muted)' }}>{formatShortMonth(month)}</p>
																<p className="text-[8px] tnum truncate mt-0.5" style={{ color: 'var(--ledger-muted)' }}>{formatUsd(cost)}</p>
															</button>
														);
													})}
												</div>
											</div>
										)}
									</section>

									<section className="mb-6">
										<div className="flex items-end justify-between gap-3 mb-3 px-1">
											<div>
												<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>Where it went</p>
												<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>Provider and model statement</p>
											</div>
											<Cpu size={16} style={{ color: 'var(--ledger-accent)' }} />
										</div>

										{providerGroups.length === 0 ? (
											<div className="rounded-[14px] px-5 py-7 text-center" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
												<Database size={22} className="mx-auto mb-2" style={{ color: 'var(--ledger-muted)' }} />
												<p className="text-sm font-bold">No model activity yet</p>
												<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>The first AI call in this month will appear here.</p>
											</div>
										) : (
											<div className="space-y-3">
												{providerGroups.map(([provider, models]) => {
													const providerCost = models.reduce((sum, model) => sum + model.cost_usd, 0);
													const providerCalls = models.reduce((sum, model) => sum + model.request_count, 0);
													const providerTokens = models.reduce((sum, model) => sum + model.total_tokens, 0);
													return (
														<div key={provider} className="ai-usage-card rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
															<div className="px-4 py-3 flex items-center justify-between gap-3" style={{ borderBottom: '1px solid var(--ledger-rule)' }}>
																<div className="flex items-center gap-2.5">
																	<span className="w-2 h-2 rounded-full" style={{ background: 'var(--ledger-accent)' }} />
																	<span className="text-sm font-bold">{providerLabel(provider)}</span>
																</div>
																<span className="text-[11px] tnum" style={{ color: 'var(--ledger-muted)' }}>
																	{providerCalls} calls · {formatUsd(providerCost)}
																</span>
															</div>

															{models.map((model, index) => {
																const isFree = model.model.endsWith(':free');
																const modelSuccessRate = model.request_count > 0 ? (model.success_count / model.request_count) * 100 : 0;
																const tokenShare = providerTokens > 0 ? (model.total_tokens / providerTokens) * 100 : 0;
																return (
																	<div key={model.model} className="px-4 py-3.5" style={index < models.length - 1 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}>
																		<div className="flex items-start justify-between gap-3">
																			<div className="min-w-0 flex-1">
																				<div className="flex items-center gap-1.5 min-w-0">
																					<p className="text-xs font-bold break-all">{shortModelName(model.model)}</p>
																					<span className="text-[8px] font-bold uppercase tracking-wider rounded-full px-1.5 py-0.5 shrink-0" style={{ background: isFree ? 'var(--ledger-nav-active)' : 'var(--ledger-amber-bg)', color: isFree ? 'var(--ledger-accent)' : 'var(--ledger-amber)' }}>
																						{isFree ? 'Free' : 'Paid'}
																					</span>
																				</div>
																				<p className="text-[10px] mt-1" style={{ color: 'var(--ledger-muted)' }}>
																					{model.request_count} calls · {formatPercent(modelSuccessRate)} settled · {formatLatency(model.average_latency_ms)} avg
																				</p>
																				<p className="text-[10px] mt-0.5 tnum" style={{ color: 'var(--ledger-muted)' }}>
																					{formatCount(model.total_tokens)} tokens · {formatCount(model.item_count)} items
																				</p>
																			</div>
																			<div className="text-right shrink-0">
																				<p className="text-xs font-bold tnum">{isFree && model.cost_usd === 0 ? 'Free' : formatUsd(model.cost_usd)}</p>
																				{model.request_count > model.success_count && (
																					<p className="text-[9px] mt-1" style={{ color: 'var(--ledger-negative)' }}>{model.request_count - model.success_count} failed</p>
																				)}
																			</div>
																		</div>
																		<div className="h-1 rounded-full mt-3 overflow-hidden" style={{ background: 'var(--ledger-rule)' }}>
																			<div className="h-full rounded-full" style={{ width: `${tokenShare}%`, background: 'var(--ledger-accent)', opacity: 0.75 }} />
																		</div>
																	</div>
																);
															})}
														</div>
													);
												})}
											</div>
										)}
									</section>

									<section>
										<div className="flex items-end justify-between gap-3 mb-3 px-1">
											<div>
												<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>Call receipts</p>
												<p className="text-[11px] mt-1 flex items-center gap-1.5" style={{ color: 'var(--ledger-muted)' }}>
													<Database size={11} /> Metadata only · content never stored
												</p>
											</div>
											<div className="flex items-center gap-1.5 text-[10px] tnum" style={{ color: 'var(--ledger-muted)' }}>
												<Activity size={13} />
												{formatCount(dashboard?.total_event_count ?? 0)} calls
											</div>
										</div>

										{error ? (
											<div className="rounded-[14px] px-5 py-6 text-center" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-negative)' }}>
												<AlertCircle size={22} className="mx-auto mb-2" style={{ color: 'var(--ledger-negative)' }} />
												<p className="text-sm font-bold">Call receipts could not be loaded</p>
												<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>{error}</p>
												<button type="button" onClick={() => { void refetchDashboard(); }} className="ai-usage-focus mt-4 px-4 py-2 rounded-[8px] text-xs font-bold" style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }}>
													Try again
												</button>
											</div>
										) : (
											<div className="ai-usage-card rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
												{(dashboard?.events.length ?? 0) > 0 ? (
													dashboard?.events.map((event, index, events) => (
														<EventReceipt key={event.id} event={event} isLast={index === events.length - 1} />
													))
												) : (
													<div className="px-5 py-8 text-center">
														<Clock3 size={22} className="mx-auto mb-2" style={{ color: 'var(--ledger-muted)' }} />
														<p className="text-sm font-bold">No call receipts this month</p>
														<p className="text-[11px] mt-1" style={{ color: 'var(--ledger-muted)' }}>New AI attempts will be recorded automatically.</p>
													</div>
												)}
											</div>
										)}

										{dashboard && dashboard.total_event_count > dashboard.page_size && (
											<div className="flex items-center justify-between mt-3">
												<button
													type="button"
													onClick={() => setPage((value) => Math.max(1, value - 1))}
													disabled={page === 1}
													className="ai-usage-focus h-9 px-3 rounded-[8px] text-xs font-bold flex items-center gap-1 disabled:opacity-35"
													style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
												>
													<ChevronLeft size={14} /> Previous
												</button>
												<span className="text-[11px] tnum" style={{ color: 'var(--ledger-muted)' }}>Page {page} of {totalPages}</span>
												<button
													type="button"
													onClick={() => setPage((value) => Math.min(totalPages, value + 1))}
													disabled={page >= totalPages}
													className="ai-usage-focus h-9 px-3 rounded-[8px] text-xs font-bold flex items-center gap-1 disabled:opacity-35"
													style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}
												>
													Next <ChevronRight size={14} />
												</button>
											</div>
										)}
									</section>
								</>
							)}
						</div>
					)}
				</div>
			</main>
		</div>
	);
}
