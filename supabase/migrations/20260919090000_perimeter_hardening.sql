-- Multi-tenant rollout, Task 7: perimeter and operations hardening.
-- Plan of record: docs/MULTITENANT_TASKS.md (Task 7).
--
-- One addition: idempotency_keys, the replay ledger behind the optional
-- `Idempotency-Key` header on POST /transactions. A retry after a network
-- timeout must not create a second transaction, and the client cannot tell
-- "request lost" from "response lost" on its own.
--
-- Shape:
--   * (user_id, key) is the primary key, so one user's key never collides
--     with another's and a stolen key is useless across tenants.
--   * request_hash pins the payload a key was first used with: the same key
--     with a different body is a client bug (409), never a silent replay.
--   * response_status / response_body hold what to replay. They stay null
--     while the first request is still in flight; that claim row is what
--     makes two concurrent retries safe.
--   * created_at drives the nightly 24-hour purge (raw_email_body_purge).
--
-- Rate limiting needs no schema (Cloudflare bindings), and azp validation is
-- token-only, so this is the whole database footprint of Task 7.
--
-- Additive and idempotent; no existing row is read or written.
-- Applied by scripts/db-migrate.mjs as postgres.

-- ---------------------------------------------------------------------------
-- 1. idempotency_keys
-- ---------------------------------------------------------------------------

create table if not exists public.idempotency_keys (
  user_id uuid not null references public.users (id) on delete cascade,
  key text not null,
  request_hash text not null,
  response_status integer,
  response_body jsonb,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  primary key (user_id, key),
  constraint chk_idempotency_keys_key_length
    check (char_length(key) between 1 and 255),
  constraint chk_idempotency_keys_status_range
    check (response_status is null or response_status between 100 and 599),
  -- A row is either an in-flight claim (nothing recorded) or a finished
  -- response (status present). Nothing in between can be replayed.
  constraint chk_idempotency_keys_completion
    check (
      (completed_at is null and response_status is null and response_body is null)
      or (completed_at is not null and response_status is not null)
    )
);

comment on table public.idempotency_keys is
  'Replay ledger for the optional Idempotency-Key header (Task 7). A row is claimed before the handler runs and completed with its response; the nightly purge drops rows older than 24 hours.';

comment on column public.idempotency_keys.request_hash is
  'SHA-256 over the canonical JSON body the key was first used with; a different hash for the same key is a 409, never a replay.';

comment on column public.idempotency_keys.completed_at is
  'Null while the first request is still in flight. A claim older than the in-flight window is reclaimable (the worker died mid-request).';

-- The purge scans by age across all users, so it needs its own index; the
-- primary key is useless for that ordering.
create index if not exists idx_idempotency_keys_created_at
  on public.idempotency_keys (created_at);

select public.grant_app_backend('public.idempotency_keys');

-- ---------------------------------------------------------------------------
-- 2. Backstops
-- ---------------------------------------------------------------------------

select public.assert_tenancy_backstops();
