-- Multi-tenant rollout, Task 6: quotas and fairness.
-- Plan of record: docs/MULTITENANT_TASKS.md (Task 6; A.1 §5, A.2 §8–9, A.3 §3).
--
-- Four additions, all additive and idempotent:
--   1. user_usage_daily counts what nothing else records per user and IST day:
--      Gmail messages listed and bodies fetched. AI calls are NOT counted here
--      — ai_usage_events already ledgers them per user (A.2 §9). The row also
--      carries the once-per-hour stamp for the self-healing re-enqueue of
--      stale PENDING_EXTRACTION rows (A.2 §8).
--   2. user_limits holds per-user overrides of the two daily caps; a null cap
--      means "the env default applies" (USER_DAILY_EMAIL_FETCH_CAP /
--      USER_DAILY_AI_CALL_CAP, 500 / 200 — decision D7).
--   3. oauth_connections keeps the Gmail page token (plus the query window it
--      belongs to) when the fetch cap stops a sync mid-listing, so the next
--      job resumes exactly there instead of re-listing from the cursor and
--      re-spending Gmail quota (A.3 §3).
--   4. A partial index for the per-user stale-PENDING scan the fetcher runs.
--
-- Nothing here changes behaviour on its own: the caps default far above one
-- person's volume, no row is inserted, and old code never reads the columns.
--
-- Applied by scripts/db-migrate.mjs as postgres.

-- ---------------------------------------------------------------------------
-- 1. user_usage_daily
-- ---------------------------------------------------------------------------

create table if not exists public.user_usage_daily (
  user_id uuid not null references public.users (id) on delete cascade,
  -- The IST calendar day, like every other daily job.
  day date not null,
  messages_listed integer not null default 0 check (messages_listed >= 0),
  bodies_fetched integer not null default 0 check (bodies_fetched >= 0),
  -- When the fetcher last re-enqueued this user's stale PENDING rows; the
  -- claim on it is what bounds the self-heal to once an hour.
  pending_requeue_at timestamptz,
  updated_at timestamptz not null default now(),
  primary key (user_id, day)
);

comment on table public.user_usage_daily is
  'Per-user, per-IST-day Gmail counters for the fair-use caps (Task 6). AI calls come from ai_usage_events; the email fetch cap independently bounds messages_listed and bodies_fetched.';

comment on column public.user_usage_daily.pending_requeue_at is
  'Last time the EMAIL_SYNC_USER job re-enqueued this user''s stale PENDING_EXTRACTION rows (at most hourly).';

select public.grant_app_backend('public.user_usage_daily');

-- ---------------------------------------------------------------------------
-- 2. user_limits
-- ---------------------------------------------------------------------------

create table if not exists public.user_limits (
  user_id uuid primary key references public.users (id) on delete cascade,
  daily_email_fetch_cap integer check (daily_email_fetch_cap > 0),
  daily_ai_call_cap integer check (daily_ai_call_cap > 0),
  updated_at timestamptz not null default now()
);

comment on table public.user_limits is
  'Per-user overrides of the daily caps (Task 6). Null = the env default (USER_DAILY_EMAIL_FETCH_CAP / USER_DAILY_AI_CALL_CAP). Written from the admin dashboard.';

select public.grant_app_backend('public.user_limits');

-- ---------------------------------------------------------------------------
-- 3. oauth_connections: the page token a quota stop must not lose
-- ---------------------------------------------------------------------------

alter table public.oauth_connections
  add column if not exists email_sync_continuation_token text,
  add column if not exists email_sync_continuation_after_seconds bigint,
  add column if not exists email_sync_continuation_max_seen bigint;

comment on column public.oauth_connections.email_sync_continuation_token is
  'Gmail nextPageToken saved when the daily fetch cap stopped a sync mid-listing; the next EMAIL_SYNC_USER job resumes here.';
comment on column public.oauth_connections.email_sync_continuation_after_seconds is
  'The after: query window the saved page token belongs to (a token is only valid for the query that produced it).';
comment on column public.oauth_connections.email_sync_continuation_max_seen is
  'Highest internal date seen so far in the interrupted chain, carried so the cursor lands where it would have.';

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_oauth_connections_email_sync_continuation_pair'
      and conrelid = 'public.oauth_connections'::regclass
  ) then
    alter table public.oauth_connections
      add constraint chk_oauth_connections_email_sync_continuation_pair
      check (
        (
          email_sync_continuation_token is null
          and email_sync_continuation_after_seconds is null
          and email_sync_continuation_max_seen is null
        )
        or (
          email_sync_continuation_token is not null
          and email_sync_continuation_after_seconds is not null
          and email_sync_continuation_after_seconds >= 0
          and coalesce(email_sync_continuation_max_seen, 0) >= 0
        )
      );
  end if;
end
$$;

-- ---------------------------------------------------------------------------
-- 4. raw_emails: the per-user stale-PENDING scan
-- ---------------------------------------------------------------------------

-- Small (only rows still awaiting extraction) and exactly the shape the
-- self-heal query walks: this user's pending rows, oldest first.
create index if not exists idx_raw_emails_pending_by_user
  on public.raw_emails (user_id, created_at)
  where status = 'PENDING_EXTRACTION';

-- ---------------------------------------------------------------------------
-- 5. Backstops
-- ---------------------------------------------------------------------------

select public.assert_tenancy_backstops();
