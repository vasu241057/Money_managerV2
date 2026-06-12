-- Alert query: emails stuck in extraction queue for >30 minutes.
-- Use directly in dashboards/alerts, or read from mv_stale_pending_extraction.

select
  user_id,
  count(*)::bigint as stale_count,
  min(created_at) as oldest_created_at,
  max(created_at) as newest_created_at
from public.raw_emails
where status = 'PENDING_EXTRACTION'
  and created_at < now() - interval '30 minutes'
group by user_id
order by stale_count desc, oldest_created_at asc;
