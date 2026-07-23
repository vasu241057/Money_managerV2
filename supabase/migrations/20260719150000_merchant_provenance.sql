-- Merchant provenance + probation: AI-created mappings must earn trust.
--
-- source: who created the merchant (SEED migration, MANUAL human, AI daily
-- mapper). confirmed: unconfirmed merchants still name transactions and
-- suggest categories, but their alias hits no longer auto-verify — the
-- first human review that keeps the suggested merchant flips confirmed on
-- for everyone. dissent_count: reviews that contradicted the mapping; any
-- dissent also demotes confirmed (threshold 1 while single-user — revisit
-- for multi-tenant voting, FUTURE_FIXES).
--
-- Existing rows predate the mapper and are in live use: SEED + confirmed.

alter table public.global_merchants
  add column if not exists source text not null default 'AI'
    check (source in ('SEED', 'MANUAL', 'AI')),
  add column if not exists confirmed boolean not null default false,
  add column if not exists dissent_count integer not null default 0;

update public.global_merchants
set source = 'SEED', confirmed = true
where source = 'AI' and confirmed = false;
