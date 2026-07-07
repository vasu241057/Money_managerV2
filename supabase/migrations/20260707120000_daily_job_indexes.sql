-- Indexes for the daily-cron hot paths added in the July 2026 overhaul.

-- Sender classifier: scan gated metadata-only rows by recency.
create index if not exists idx_raw_emails_gated_senders
  on public.raw_emails (created_at, from_address)
  where status = 'IGNORED' and clean_text = '';

-- Sender backfill: find a newly allowed sender's gated rows.
create index if not exists idx_raw_emails_from_address
  on public.raw_emails (from_address)
  where from_address is not null;

-- Merchant mapper: anti-join financial_events.search_key against aliases.
create index if not exists idx_financial_events_search_key
  on public.financial_events (search_key, created_at)
  where search_key is not null;
