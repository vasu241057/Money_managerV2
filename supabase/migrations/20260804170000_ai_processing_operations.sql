-- User-scoped email processing and retry correlation for the private AI
-- operations dashboard. Content is never copied into these records.

create table if not exists public.ai_call_runs (
  id uuid primary key,
  user_id uuid not null references public.users (id) on delete cascade,
  operation text not null check (
    operation in ('EMAIL_NORMALIZATION', 'SENDER_CLASSIFICATION', 'MERCHANT_MAPPING')
  ),
  item_count integer not null check (item_count > 0),
  status text not null default 'RUNNING'
    check (status in ('RUNNING', 'SUCCESS', 'FAILED')),
  attempt_count integer not null default 0 check (attempt_count >= 0),
  started_at timestamptz not null,
  completed_at timestamptz
);

alter table public.ai_usage_events
  add column if not exists user_id uuid references public.users (id) on delete cascade,
  add column if not exists call_id uuid references public.ai_call_runs (id) on delete cascade,
  add column if not exists attempt_number integer check (attempt_number > 0);

alter table public.raw_emails
  add column if not exists processing_route text
    check (processing_route in ('TEMPLATE', 'AI')),
  add column if not exists ai_call_id uuid
    references public.ai_call_runs (id) on delete set null,
  add column if not exists processing_completed_at timestamptz;

create index if not exists ai_call_runs_user_started_idx
  on public.ai_call_runs (user_id, started_at desc);

create unique index if not exists ai_usage_events_call_attempt_idx
  on public.ai_usage_events (call_id, attempt_number)
  where call_id is not null and attempt_number is not null;

create index if not exists ai_usage_events_user_created_idx
  on public.ai_usage_events (user_id, created_at desc, id desc);

create index if not exists raw_emails_user_processing_idx
  on public.raw_emails (user_id, processing_completed_at desc)
  where processing_route is not null;

alter table public.ai_call_runs enable row level security;
revoke all on table public.ai_call_runs from anon, authenticated;
