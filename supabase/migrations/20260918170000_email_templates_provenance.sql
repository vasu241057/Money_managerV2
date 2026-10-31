-- Multi-tenant rollout, Task 3: template provenance and scope.
-- Plan of record: docs/MULTITENANT_TASKS.md (Task 3; A.1 §1, A.2 §1–§3/§7).
--
-- Templates were global: a skeleton learned from one user's mail applied to
-- everyone's, and a skeleton can carry user-specific text (a greeting name,
-- a FIXED payee). Every template now has a scope. PRIVATE rows belong to the
-- user whose email taught them (learned_from_user_id) and apply only to that
-- user's mail. A PRIVATE row is promoted to SHARED (owner null) only when a
-- different user's email independently induces the same skeleton with the
-- same structural slot map: that collision proves the skeleton holds nothing
-- user-specific. Uniqueness is per scope: one SHARED row per (sender,
-- skeleton) and one PRIVATE row per (sender, skeleton, owner), so every
-- learner keeps their own row until promotion (A.2 §2).
--
-- Existing rows stay PRIVATE and are attributed to the user whose raw email
-- taught them; rows whose source email is gone go to the admin user when that
-- user exists (single-tenant production), else to the sole user, else they
-- are deleted (a PRIVATE row without an owner is dead weight, A.2 §7).
-- Vasu's matching is unchanged: every template he has stays applicable to
-- his mail.
--
-- Additive, idempotent; applied by scripts/db-migrate.mjs as postgres.

-- ---------------------------------------------------------------------------
-- 1. Columns
-- ---------------------------------------------------------------------------

alter table public.email_templates
  add column if not exists learned_from_user_id uuid references public.users(id) on delete cascade,
  add column if not exists scope text not null default 'PRIVATE'
    check (scope in ('PRIVATE', 'SHARED'));

comment on column public.email_templates.scope is
  'PRIVATE: applies only to learned_from_user_id''s mail. SHARED: induced independently by two or more users, applies to everyone.';
comment on column public.email_templates.learned_from_user_id is
  'Owner of a PRIVATE template (the user whose email taught it); null for SHARED rows, whose provenance is deliberately anonymous.';

-- ---------------------------------------------------------------------------
-- 2. Backfill owners for legacy rows
-- ---------------------------------------------------------------------------

update public.email_templates as et
set learned_from_user_id = re.user_id
from public.raw_emails as re
where re.id = et.learned_from_raw_email_id
  and et.scope = 'PRIVATE'
  and et.learned_from_user_id is null;

do $$
declare
  v_legacy_owner uuid;
  v_orphans bigint;
begin
  select count(*) into v_orphans
  from public.email_templates
  where scope = 'PRIVATE' and learned_from_user_id is null;
  if v_orphans = 0 then
    return;
  end if;

  select id into v_legacy_owner
  from public.users
  where id = 'a661a921-cdb0-503b-a810-4b25e4f15ca0';
  if v_legacy_owner is null and (select count(*) from public.users) = 1 then
    select id into v_legacy_owner from public.users;
  end if;

  if v_legacy_owner is null then
    delete from public.email_templates
    where scope = 'PRIVATE' and learned_from_user_id is null;
    raise notice 'email_templates: deleted % ownerless legacy template(s)', v_orphans;
  else
    update public.email_templates
    set learned_from_user_id = v_legacy_owner
    where scope = 'PRIVATE' and learned_from_user_id is null;
    raise notice 'email_templates: attributed % ownerless legacy template(s) to %', v_orphans, v_legacy_owner;
  end if;
end;
$$;

-- ---------------------------------------------------------------------------
-- 3. Scope invariant, per-scope uniqueness, lookup index
-- ---------------------------------------------------------------------------

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.email_templates'::regclass and conname = 'email_templates_scope_owner_check'
  ) then
    alter table public.email_templates
      add constraint email_templates_scope_owner_check
      check ((scope = 'SHARED') = (learned_from_user_id is null));
  end if;
end;
$$;

alter table public.email_templates
  drop constraint if exists email_templates_sender_skeleton_unique;

create unique index if not exists email_templates_shared_skeleton_uidx
  on public.email_templates (from_address, skeleton_hash)
  where scope = 'SHARED';
create unique index if not exists email_templates_private_skeleton_uidx
  on public.email_templates (from_address, skeleton_hash, learned_from_user_id)
  where scope = 'PRIVATE';
create index if not exists email_templates_scope_lookup_idx
  on public.email_templates (from_address, status, scope, learned_from_user_id);

-- ---------------------------------------------------------------------------
-- 4. Self-check
-- ---------------------------------------------------------------------------

select public.assert_tenancy_backstops();
