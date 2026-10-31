-- Multi-tenant rollout, Task 4b: per-user merchant confirmations and the
-- mapper's memory of resolved person keys.
-- Plan of record: docs/MULTITENANT_TASKS.md (Task 4; A.4 §4).
--
-- Until now one user's review flipped global_merchants.confirmed for every
-- user (R3). merchant_confirmations records one verdict per (merchant, user);
-- confirmed and dissent_count on global_merchants become derived caches,
-- recomputed by the review service as
--   confirmed = distinct CONFIRM users >= threshold and distinct DISSENT users < threshold
-- with the threshold from MERCHANT_CONFIRMATION_THRESHOLD (default 1, which
-- reproduces today's single-user behaviour exactly).
--
-- merchant_mapping_attempts gains last_resolution and person_display_name so
-- a person key one user already resolved fans out to a new user who pays the
-- same key with no model call and no cooldown (A.4 §4). The key already
-- identifies the person; the row goes when its last referencing user goes
-- (Task 5).
--
-- Additive, idempotent; applied by scripts/db-migrate.mjs as postgres.

-- ---------------------------------------------------------------------------
-- 1. merchant_confirmations
-- ---------------------------------------------------------------------------

create table if not exists public.merchant_confirmations (
  merchant_id uuid not null references public.global_merchants(id) on delete cascade,
  user_id uuid not null references public.users(id) on delete cascade,
  verdict text not null check (verdict in ('CONFIRM', 'DISSENT')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (merchant_id, user_id)
);
comment on table public.merchant_confirmations is
  'One review verdict per (merchant, user). global_merchants.confirmed / dissent_count are caches derived from these rows.';

drop trigger if exists update_merchant_confirmations_modtime on public.merchant_confirmations;
create trigger update_merchant_confirmations_modtime
before update on public.merchant_confirmations
for each row execute function public.update_updated_at_column();

-- A private payee is only ever judged by its owner (same trigger as every
-- other (user_id, merchant_id) table).
drop trigger if exists enforce_merchant_confirmations_merchant_scope on public.merchant_confirmations;
create trigger enforce_merchant_confirmations_merchant_scope
before insert or update on public.merchant_confirmations
for each row execute function public.enforce_user_merchant_scope();

select public.grant_app_backend('public.merchant_confirmations');

-- ---------------------------------------------------------------------------
-- 2. Backfill verdicts from the legacy cache columns
-- ---------------------------------------------------------------------------
-- Only AI-sourced merchants ever lived on probation. The voter is the owner
-- for a private payee, else the user whose transactions reference the
-- merchant most (the only user who could have reviewed it). A merchant no
-- user references gets no row: nothing to derive a verdict from.

insert into public.merchant_confirmations (merchant_id, user_id, verdict)
select
  gm.id,
  coalesce(
    gm.owner_user_id,
    (
      select t.user_id
      from public.transactions t
      where t.merchant_id = gm.id
      group by t.user_id
      order by count(*) desc, t.user_id
      limit 1
    )
  ) as voter,
  case when gm.confirmed then 'CONFIRM' else 'DISSENT' end as verdict
from public.global_merchants gm
where gm.source = 'AI'
  and (gm.confirmed or gm.dissent_count > 0)
  and coalesce(
    gm.owner_user_id,
    (select t.user_id from public.transactions t where t.merchant_id = gm.id group by t.user_id order by count(*) desc, t.user_id limit 1)
  ) is not null
on conflict (merchant_id, user_id) do nothing;

-- ---------------------------------------------------------------------------
-- 3. merchant_mapping_attempts: remember what a key resolved to
-- ---------------------------------------------------------------------------

alter table public.merchant_mapping_attempts
  add column if not exists last_resolution text
    check (last_resolution in ('MERCHANT', 'P2P', 'UNSURE')),
  add column if not exists person_display_name text;

comment on column public.merchant_mapping_attempts.last_resolution is
  'What the model last decided for this key. P2P with person_display_name lets the mapper give a new user paying the same key that payee without another model call.';
comment on column public.merchant_mapping_attempts.person_display_name is
  'Display name of the person this key resolved to (from the owning users'' merchants). Removed with the last user that references the key (Task 5).';

-- Keys already resolved as people (every owned P2P alias) are remembered.
insert into public.merchant_mapping_attempts (search_key, attempt_count, last_attempt_at, last_resolution, person_display_name)
select distinct on (gma.search_key)
  gma.search_key, 1, now(), 'P2P', gm.canonical_name
from public.global_merchant_aliases gma
join public.global_merchants gm on gm.id = gma.merchant_id
where gm.type = 'P2P' and gm.owner_user_id is not null
order by gma.search_key, gm.created_at, gm.id
on conflict (search_key) do update
set last_resolution = 'P2P',
    person_display_name = coalesce(public.merchant_mapping_attempts.person_display_name, excluded.person_display_name);

-- Keys behind a shared alias were resolved as brands.
update public.merchant_mapping_attempts mma
set last_resolution = 'MERCHANT'
where mma.last_resolution is null
  and exists (
    select 1 from public.global_merchant_aliases gma
    where gma.search_key = mma.search_key and gma.owner_user_id is null
  );

-- ---------------------------------------------------------------------------
-- 4. Self-check
-- ---------------------------------------------------------------------------

select public.assert_tenancy_backstops();
