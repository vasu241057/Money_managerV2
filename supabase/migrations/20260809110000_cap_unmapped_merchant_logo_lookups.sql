-- Keep the manual logo-research queue bounded. A merchant that has been
-- researched twice without a trustworthy logo must not consume more lookup
-- effort until someone deliberately resets this record.

create table if not exists public.merchant_logo_lookup_attempts (
  merchant_id uuid primary key
    references public.global_merchants(id) on delete cascade,
  attempt_count smallint not null default 1
    check (attempt_count between 1 and 2),
  last_attempt_at timestamptz not null default now(),
  last_outcome text not null default 'NO_CONFIDENT_LOGO'
    check (last_outcome in ('NO_CONFIDENT_LOGO', 'LOGO_ATTACHED'))
);

comment on table public.merchant_logo_lookup_attempts is
  'Bounded manual research attempts for merchant logos; at most two unresolved scans per merchant.';
