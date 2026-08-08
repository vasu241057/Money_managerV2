-- Nightly job scheduling, moved out of deploy config and into the database.
--
-- Cloudflare cron triggers are fixed at deploy time, so a schedule the admin
-- picks from the UI cannot live there. Instead the every-10-minute dispatcher
-- claims due jobs from this table. The claiming UPDATE is the lock: two
-- overlapping ticks (or the 10-minute cron and the nightly cron firing in the
-- same second) can never enqueue the same job twice for one local day.
--
-- Times are stored as local wall-clock plus an IANA zone and compared with
-- `now() at time zone timezone`, so Postgres owns the offset/DST arithmetic
-- instead of hand-rolled UTC maths — the exact place where "I picked 10:00 and
-- it ran at 04:30" bugs come from.

create table if not exists public.job_schedules (
  job text primary key,
  enabled boolean not null default false,
  run_at_local time not null,
  timezone text not null default 'Asia/Kolkata',
  -- Local date this job was last claimed: the once-per-day guard.
  last_enqueued_on date,
  last_enqueued_at timestamptz,
  updated_at timestamptz not null default now(),
  constraint job_schedules_known_job
    check (job in ('merchant_mapping', 'sender_classification')),
  -- The dispatcher only ticks every 10 minutes, so a time it cannot honour
  -- must not be storable at all: 10:07 would silently become 10:10.
  constraint job_schedules_tick_aligned check (
    extract(second from run_at_local) = 0
    and (extract(minute from run_at_local))::int % 10 = 0
  )
);

-- Seeded to reproduce today's behaviour exactly: sender classification has
-- always run at 21:30 UTC (03:00 IST), merchant mapping has never run because
-- its flag was never set. last_enqueued_on is stamped when today's slot has
-- already passed, so applying this migration can never trigger a surprise
-- immediate run at, say, 14:00.
insert into public.job_schedules (job, enabled, run_at_local, timezone, last_enqueued_on)
values
  (
    'sender_classification',
    true,
    '03:00',
    'Asia/Kolkata',
    case
      when (now() at time zone 'Asia/Kolkata')::time >= '03:00'::time
        then (now() at time zone 'Asia/Kolkata')::date
      else null
    end
  ),
  (
    'merchant_mapping',
    false,
    '03:00',
    'Asia/Kolkata',
    case
      when (now() at time zone 'Asia/Kolkata')::time >= '03:00'::time
        then (now() at time zone 'Asia/Kolkata')::date
      else null
    end
  )
on conflict (job) do nothing;
