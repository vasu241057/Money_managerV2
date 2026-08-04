-- Per-attempt AI cost and token ledger for the admin usage dashboard.
-- Prompt/response content is deliberately excluded: this table stores only
-- operational metadata returned by the provider.

create table if not exists public.ai_usage_events (
  id uuid primary key default gen_random_uuid(),
  provider text not null check (provider in ('opencode', 'openrouter')),
  model text not null,
  operation text not null check (
    operation in ('EMAIL_NORMALIZATION', 'SENDER_CLASSIFICATION', 'MERCHANT_MAPPING')
  ),
  status text not null check (status in ('SUCCESS', 'ERROR')),
  http_status integer check (http_status between 100 and 599),
  item_count integer not null default 1 check (item_count > 0),
  prompt_tokens bigint not null default 0 check (prompt_tokens >= 0),
  completion_tokens bigint not null default 0 check (completion_tokens >= 0),
  reasoning_tokens bigint not null default 0 check (reasoning_tokens >= 0),
  cached_tokens bigint not null default 0 check (cached_tokens >= 0),
  total_tokens bigint not null default 0 check (total_tokens >= 0),
  cost_usd numeric(20, 12) check (cost_usd >= 0),
  latency_ms integer not null check (latency_ms >= 0),
  error_code text,
  generation_id text,
  created_at timestamptz not null default now()
);

create index if not exists ai_usage_events_created_at_idx
  on public.ai_usage_events (created_at desc, id desc);

create index if not exists ai_usage_events_provider_model_month_idx
  on public.ai_usage_events (provider, model, created_at desc);

-- This ledger is served only by the backend's authenticated admin route.
-- It must never be readable through Supabase's public data API.
alter table public.ai_usage_events enable row level security;
revoke all on table public.ai_usage_events from anon, authenticated;
