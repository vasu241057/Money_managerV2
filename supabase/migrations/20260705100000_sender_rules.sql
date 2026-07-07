-- Dynamic sender allowlist rules, layered on top of the built-in list in
-- backend/src/lib/sender-gate.ts. Populated by the daily AI sender
-- classification job (source AI) and manual additions (source USER).
--
-- pattern follows the sender-gate entry shapes:
--   contains '@' -> exact address, contains '.' -> domain suffix,
--   otherwise    -> brand token substring of the whole From header.
-- AI-created rules always use exact addresses to avoid overbroad matches.
create table if not exists public.sender_rules (
  id uuid primary key default gen_random_uuid(),
  pattern text not null unique,
  decision text not null check (decision in ('ALLOW', 'DENY')),
  sender_type text check (sender_type in ('BANK', 'FINTECH', 'WALLET', 'OTHER')),
  source text not null default 'AI' check (source in ('SEED', 'AI', 'USER')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_sender_rules_decision on public.sender_rules (decision);
