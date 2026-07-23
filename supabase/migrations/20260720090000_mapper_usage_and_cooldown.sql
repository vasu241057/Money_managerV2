-- Mapper observability + retry cooldown.
--
-- usage_counters: per-day operation counts (tavily_searches,
-- merchant_ai_calls, merchants_mapped, merchants_skipped_unsure) so credit
-- burn and mapping volume are queryable without provider dashboards.
--
-- merchant_mapping_attempts: keys the mapper has already asked the model
-- about. Keys the model was unsure of (no alias created) rest here for the
-- cooldown window instead of being re-asked every day; a mapped key gains
-- an alias and never re-enters candidates anyway.

create table if not exists public.usage_counters (
  day date not null,
  counter text not null,
  count integer not null default 0,
  primary key (day, counter)
);

create table if not exists public.merchant_mapping_attempts (
  search_key text primary key,
  attempt_count integer not null default 1,
  last_attempt_at timestamptz not null default now()
);
