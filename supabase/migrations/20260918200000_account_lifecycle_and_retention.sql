-- Multi-tenant rollout, Task 5: account lifecycle and body retention.
-- Plan of record: docs/MULTITENANT_TASKS.md (Task 5; A.1 §9, A.2 §10, A.4 §2).
--
-- Three additions, all additive and idempotent:
--   1. raw_emails.body_purged_at marks rows whose stored body was blanked by
--      the nightly retention job (PROCESSED / IGNORED / UNRECOGNIZED rows
--      older than RAW_EMAIL_BODY_RETENTION_DAYS; FAILED rows keep their body
--      so they stay reprocessable). Every query that recognises a gated row
--      by `status = 'IGNORED' and clean_text = ''` also requires
--      body_purged_at is null, so a purged row can never masquerade as a
--      metadata-only gated row and trigger a re-download (A.4 §2).
--   2. job_schedules accepts the purge job, seeded disabled at 03:30 IST so
--      nothing changes for the existing tenant until it is switched on.
--   3. user_deletion_log is the PII-free tombstone DELETE /account writes
--      after the users row (and everything that cascades from it) is gone.
--      It keeps the opaque provider subject only while the identity-provider
--      deletion is still pending, so the nightly retry can finish the job;
--      the subject is nulled the moment that deletion succeeds.
--
-- Applied by scripts/db-migrate.mjs as postgres.

-- ---------------------------------------------------------------------------
-- 1. raw_emails: purge marker and scan index
-- ---------------------------------------------------------------------------

alter table public.raw_emails
  add column if not exists body_purged_at timestamptz;

comment on column public.raw_emails.body_purged_at is
  'Set when the retention job blanked clean_text. Distinguishes a purged body from a sender-gated (never fetched) one: gated-row queries require this to be null.';

-- The purge scans by status and age; age is processing_completed_at with a
-- created_at fallback for rows that predate that column. The partial index
-- shrinks as bodies are purged, so the nightly scan stays cheap.
create index if not exists idx_raw_emails_body_purge_scan
  on public.raw_emails (status, (coalesce(processing_completed_at, created_at)))
  where clean_text <> '';

-- ---------------------------------------------------------------------------
-- 2. job_schedules: the purge job, seeded off
-- ---------------------------------------------------------------------------

alter table public.job_schedules
  drop constraint if exists job_schedules_known_job;
alter table public.job_schedules
  add constraint job_schedules_known_job
  check (job in ('merchant_mapping', 'sender_classification', 'raw_email_body_purge'));

-- last_enqueued_on is stamped when today's slot has already passed, so
-- enabling the job later today cannot fire it within ten minutes.
insert into public.job_schedules (job, enabled, run_at_local, timezone, last_enqueued_on)
values (
  'raw_email_body_purge',
  false,
  '03:30',
  'Asia/Kolkata',
  case
    when (now() at time zone 'Asia/Kolkata')::time >= '03:30'::time
      then (now() at time zone 'Asia/Kolkata')::date
    else null
  end
)
on conflict (job) do nothing;

-- ---------------------------------------------------------------------------
-- 3. user_deletion_log
-- ---------------------------------------------------------------------------

create table if not exists public.user_deletion_log (
  user_id uuid primary key,
  deleted_at timestamptz not null default now(),
  clerk_deleted boolean not null default false,
  google_revoked boolean not null default false,
  -- Provider identity kept only until the provider-side deletion succeeds
  -- (users.id cannot be reversed into the subject). Never an email or name.
  auth_provider text,
  auth_subject text,
  clerk_delete_attempts integer not null default 0,
  clerk_last_attempt_at timestamptz,
  clerk_last_error text,
  constraint user_deletion_log_subject_cleared_check
    check (not clerk_deleted or auth_subject is null)
);

comment on table public.user_deletion_log is
  'Tombstone per deleted account (no PII). clerk_deleted = false rows are retried by the nightly purge job using auth_subject, which is nulled on success.';

select public.grant_app_backend('public.user_deletion_log');

-- ---------------------------------------------------------------------------
-- 4. Backstops
-- ---------------------------------------------------------------------------

select public.assert_tenancy_backstops();
