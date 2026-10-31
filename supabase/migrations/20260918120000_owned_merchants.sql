-- Multi-tenant rollout, Task 4a: merchant ownership (private payees per user).
-- Plan of record: docs/MULTITENANT_TASKS.md (Task 4; A.1 §2, A.2 §4/§5/§11,
-- A.3 §5, A.4 §1).
--
-- global_merchants and global_merchant_aliases gain owner_user_id. Null means
-- a shared brand (seeded, manual or mapped); a user id means a private payee
-- (type P2P) that only its owner can see, search, rule on or link. Uniqueness
-- becomes per scope through partial unique indexes, so shared writes keep
-- inferring with `on conflict (...) where owner_user_id is null` (A.2 §5).
--
-- Existing P2P rows keep their ids. Each is assigned to the user whose rows
-- reference it; a row referenced by several users is cloned per extra user
-- (lowest user id keeps the original) and their references are repointed; a
-- row nothing references is deleted. merchant_ownership_migration_log records
-- every move so it can be reversed. The two transactions triggers that react
-- to merchant_id changes are disabled around the repoint (A.4 §1). The
-- same-user-or-shared triggers are created after the data step, so the
-- interim state can never trip them.
--
-- Additive, idempotent (only unowned P2P rows are ever touched), checked
-- before the triggers land; applied by scripts/db-migrate.mjs as postgres.
-- After this file the old seed's `on conflict (canonical_name)` and
-- `on conflict (search_key)` no longer infer: supabase/seed.sql is not
-- re-runnable, and code older than Task 4a cannot persist mapper output.

-- ---------------------------------------------------------------------------
-- 1. Ownership columns
-- ---------------------------------------------------------------------------

alter table public.global_merchants
  add column if not exists owner_user_id uuid references public.users(id) on delete cascade;

alter table public.global_merchant_aliases
  add column if not exists owner_user_id uuid references public.users(id) on delete cascade;

comment on column public.global_merchants.owner_user_id is
  'Null: shared brand visible to every user. Set: private payee (P2P) visible only to that user. Every alias of a merchant carries the same owner.';
comment on column public.global_merchant_aliases.owner_user_id is
  'Mirrors the owner of merchant_id (trigger-enforced). Lookups filter owner is null or owner = the requesting user, owned first.';

-- ---------------------------------------------------------------------------
-- 2. Alias surrogate key: search_key is unique per scope, not globally
-- ---------------------------------------------------------------------------

alter table public.global_merchant_aliases
  add column if not exists id uuid not null default gen_random_uuid();

do $$
declare
  v_key_attnum smallint;
  r record;
  v_has_pk boolean := false;
begin
  select attnum into v_key_attnum
  from pg_attribute
  where attrelid = 'public.global_merchant_aliases'::regclass
    and attname = 'search_key' and not attisdropped;

  for r in
    select conname, conkey
    from pg_constraint
    where conrelid = 'public.global_merchant_aliases'::regclass and contype = 'p'
  loop
    if r.conkey = array[v_key_attnum] then
      execute format('alter table public.global_merchant_aliases drop constraint %I', r.conname);
    else
      v_has_pk := true;
    end if;
  end loop;

  if not v_has_pk then
    alter table public.global_merchant_aliases add primary key (id);
  end if;
end;
$$;

-- ---------------------------------------------------------------------------
-- 3. Per-scope uniqueness and lookup indexes
-- ---------------------------------------------------------------------------

do $$
declare
  v_name_attnum smallint;
  r record;
begin
  select attnum into v_name_attnum
  from pg_attribute
  where attrelid = 'public.global_merchants'::regclass
    and attname = 'canonical_name' and not attisdropped;

  for r in
    select conname
    from pg_constraint
    where conrelid = 'public.global_merchants'::regclass
      and contype = 'u'
      and conkey = array[v_name_attnum]
  loop
    execute format('alter table public.global_merchants drop constraint %I', r.conname);
  end loop;
end;
$$;

create unique index if not exists global_merchants_shared_name_uidx
  on public.global_merchants (canonical_name)
  where owner_user_id is null;
create unique index if not exists global_merchants_owned_name_uidx
  on public.global_merchants (owner_user_id, canonical_name)
  where owner_user_id is not null;
create index if not exists global_merchants_owner_idx
  on public.global_merchants (owner_user_id)
  where owner_user_id is not null;

create unique index if not exists global_merchant_aliases_shared_key_uidx
  on public.global_merchant_aliases (search_key)
  where owner_user_id is null;
create unique index if not exists global_merchant_aliases_owned_key_uidx
  on public.global_merchant_aliases (search_key, owner_user_id)
  where owner_user_id is not null;
create index if not exists global_merchant_aliases_search_key_idx
  on public.global_merchant_aliases (search_key);
create index if not exists global_merchant_aliases_merchant_idx
  on public.global_merchant_aliases (merchant_id);
create index if not exists global_merchant_aliases_owner_idx
  on public.global_merchant_aliases (owner_user_id)
  where owner_user_id is not null;

-- ---------------------------------------------------------------------------
-- 4. Reversal log
-- ---------------------------------------------------------------------------

create table if not exists public.merchant_ownership_migration_log (
  id uuid primary key default gen_random_uuid(),
  merchant_id uuid not null,
  canonical_name text not null,
  action text not null check (action in ('OWNED', 'CLONED', 'DELETED')),
  previous_owner_user_id uuid,
  new_owner_user_id uuid,
  cloned_from_merchant_id uuid,
  created_at timestamptz not null default now()
);
comment on table public.merchant_ownership_migration_log is
  'What the Task 4a ownership migration did to each P2P merchant. OWNED: original row assigned to new_owner_user_id. CLONED: merchant_id is a copy of cloned_from_merchant_id for new_owner_user_id, whose references were repointed. DELETED: nothing referenced the row.';
select public.grant_app_backend('public.merchant_ownership_migration_log');

-- ---------------------------------------------------------------------------
-- 5. Data migration for existing P2P merchants
-- ---------------------------------------------------------------------------

do $$
declare
  m record;
  v_users uuid[];
  v_other uuid;
  v_clone uuid;
begin
  -- Reactive triggers stay quiet during the repoint (A.4 §1). When nothing
  -- is repointed (one tenant) these are no-ops.
  alter table public.transactions disable trigger auto_link_unambiguous_card_refund;
  alter table public.transactions disable trigger match_confirmed_recurring_series;

  for m in
    select gm.id, gm.canonical_name
    from public.global_merchants gm
    where gm.type = 'P2P' and gm.owner_user_id is null
    order by gm.created_at, gm.id
  loop
    select coalesce(array_agg(u order by u), '{}'::uuid[]) into v_users
    from (
      select t.user_id as u from public.transactions t where t.merchant_id = m.id
      union
      select r.user_id from public.user_merchant_rules r where r.merchant_id = m.id
      union
      select c.user_id from public.user_merchant_category_rules c where c.merchant_id = m.id
      union
      select s.user_id from public.recurring_series s where s.merchant_id = m.id
    ) refs;

    if coalesce(array_length(v_users, 1), 0) = 0 then
      insert into public.merchant_ownership_migration_log (merchant_id, canonical_name, action)
      values (m.id, m.canonical_name, 'DELETED');
      delete from public.global_merchants where id = m.id;
      continue;
    end if;

    update public.global_merchants
    set owner_user_id = v_users[1], updated_at = now()
    where id = m.id;
    update public.global_merchant_aliases
    set owner_user_id = v_users[1], updated_at = now()
    where merchant_id = m.id;
    insert into public.merchant_ownership_migration_log
      (merchant_id, canonical_name, action, previous_owner_user_id, new_owner_user_id)
    values (m.id, m.canonical_name, 'OWNED', null, v_users[1]);

    foreach v_other in array v_users[2:array_length(v_users, 1)] loop
      insert into public.global_merchants (
        canonical_name, default_category_id, type, is_context_required,
        source, confirmed, dissent_count, owner_user_id, created_at
      )
      select gm.canonical_name, gm.default_category_id, gm.type, gm.is_context_required,
        gm.source, gm.confirmed, gm.dissent_count, v_other, gm.created_at
      from public.global_merchants gm
      where gm.id = m.id
      returning id into v_clone;

      insert into public.global_merchant_aliases (search_key, merchant_id, owner_user_id, created_at)
      select gma.search_key, v_clone, v_other, gma.created_at
      from public.global_merchant_aliases gma
      where gma.merchant_id = m.id;

      insert into public.global_merchant_category_defaults (merchant_id, transaction_type, category_id)
      select v_clone, d.transaction_type, d.category_id
      from public.global_merchant_category_defaults d
      where d.merchant_id = m.id;

      -- recurring_series before transactions (A.4 §1).
      update public.recurring_series set merchant_id = v_clone
      where merchant_id = m.id and user_id = v_other;
      update public.transactions set merchant_id = v_clone
      where merchant_id = m.id and user_id = v_other;
      update public.user_merchant_rules set merchant_id = v_clone, updated_at = now()
      where merchant_id = m.id and user_id = v_other;
      update public.user_merchant_category_rules set merchant_id = v_clone, updated_at = now()
      where merchant_id = m.id and user_id = v_other;

      insert into public.merchant_ownership_migration_log
        (merchant_id, canonical_name, action, previous_owner_user_id, new_owner_user_id, cloned_from_merchant_id)
      values (v_clone, m.canonical_name, 'CLONED', null, v_other, m.id);
    end loop;
  end loop;

  alter table public.transactions enable trigger auto_link_unambiguous_card_refund;
  alter table public.transactions enable trigger match_confirmed_recurring_series;
end;
$$;

-- People never enter the logo research queue.
delete from public.merchant_logo_lookup_attempts
where merchant_id in (select id from public.global_merchants where type = 'P2P');

-- ---------------------------------------------------------------------------
-- 6. Post-migration invariants (the file rolls back if any fails)
-- ---------------------------------------------------------------------------

do $$
declare
  v_bad bigint;
begin
  select count(*) into v_bad
  from public.global_merchants where type = 'P2P' and owner_user_id is null;
  if v_bad > 0 then
    raise exception 'ownership migration left % P2P merchant(s) unowned', v_bad;
  end if;

  select count(*) into v_bad
  from public.global_merchant_aliases a
  join public.global_merchants m on m.id = a.merchant_id
  where a.owner_user_id is distinct from m.owner_user_id;
  if v_bad > 0 then
    raise exception 'ownership migration left % alias(es) whose owner differs from the merchant owner', v_bad;
  end if;

  select count(*) into v_bad
  from public.transactions t
  join public.global_merchants m on m.id = t.merchant_id
  where m.owner_user_id is not null and m.owner_user_id <> t.user_id;
  if v_bad > 0 then
    raise exception 'ownership migration left % transaction(s) referencing another user''s merchant', v_bad;
  end if;

  select count(*) into v_bad
  from public.user_merchant_rules r
  join public.global_merchants m on m.id = r.merchant_id
  where m.owner_user_id is not null and m.owner_user_id <> r.user_id;
  if v_bad > 0 then
    raise exception 'ownership migration left % merchant rule(s) referencing another user''s merchant', v_bad;
  end if;

  select count(*) into v_bad
  from public.user_merchant_category_rules c
  join public.global_merchants m on m.id = c.merchant_id
  where m.owner_user_id is not null and m.owner_user_id <> c.user_id;
  if v_bad > 0 then
    raise exception 'ownership migration left % merchant category rule(s) referencing another user''s merchant', v_bad;
  end if;

  select count(*) into v_bad
  from public.recurring_series s
  join public.global_merchants m on m.id = s.merchant_id
  where m.owner_user_id is not null and m.owner_user_id <> s.user_id;
  if v_bad > 0 then
    raise exception 'ownership migration left % recurring series referencing another user''s merchant', v_bad;
  end if;
end;
$$;

-- ---------------------------------------------------------------------------
-- 7. Backstops: same-user-or-shared merchant references, alias owner mirror
-- ---------------------------------------------------------------------------

-- One trigger function for every table shaped (user_id, merchant_id).
create or replace function public.enforce_user_merchant_scope()
returns trigger
language plpgsql
as $$
declare
  v_owner_user_id uuid;
begin
  if new.merchant_id is null then
    return new;
  end if;

  select gm.owner_user_id into v_owner_user_id
  from public.global_merchants gm
  where gm.id = new.merchant_id
  limit 1;

  if not found then
    raise exception using
      errcode = '23503',
      message = format('%s.merchant_id must reference an existing merchant', tg_table_name);
  end if;

  if v_owner_user_id is not null and v_owner_user_id <> new.user_id then
    raise exception using
      errcode = '23514',
      message = format('%s.merchant_id must belong to the same user or be a shared merchant', tg_table_name);
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_transactions_merchant_scope on public.transactions;
create trigger enforce_transactions_merchant_scope
before insert or update of merchant_id, user_id on public.transactions
for each row execute function public.enforce_user_merchant_scope();

drop trigger if exists enforce_user_merchant_rules_merchant_scope on public.user_merchant_rules;
create trigger enforce_user_merchant_rules_merchant_scope
before insert or update on public.user_merchant_rules
for each row execute function public.enforce_user_merchant_scope();

drop trigger if exists enforce_user_merchant_category_rules_merchant_scope on public.user_merchant_category_rules;
create trigger enforce_user_merchant_category_rules_merchant_scope
before insert or update on public.user_merchant_category_rules
for each row execute function public.enforce_user_merchant_scope();

drop trigger if exists enforce_recurring_series_merchant_scope on public.recurring_series;
create trigger enforce_recurring_series_merchant_scope
before insert or update on public.recurring_series
for each row execute function public.enforce_user_merchant_scope();

-- An alias always carries its merchant's owner.
create or replace function public.enforce_global_merchant_alias_owner_scope()
returns trigger
language plpgsql
as $$
declare
  v_owner_user_id uuid;
begin
  select gm.owner_user_id into v_owner_user_id
  from public.global_merchants gm
  where gm.id = new.merchant_id
  limit 1;

  if not found then
    raise exception using
      errcode = '23503',
      message = 'global_merchant_aliases.merchant_id must reference an existing merchant';
  end if;

  if new.owner_user_id is distinct from v_owner_user_id then
    raise exception using
      errcode = '23514',
      message = 'global_merchant_aliases.owner_user_id must match the owner of its merchant';
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_global_merchant_alias_owner_scope on public.global_merchant_aliases;
create trigger enforce_global_merchant_alias_owner_scope
before insert or update on public.global_merchant_aliases
for each row execute function public.enforce_global_merchant_alias_owner_scope();

-- Changing a merchant's owner (operator SQL, e.g. re-typing a mistyped
-- person) carries its aliases along and refuses when another user's rows
-- still reference it.
create or replace function public.propagate_global_merchant_owner()
returns trigger
language plpgsql
as $$
declare
  v_bad bigint;
begin
  if new.owner_user_id is not null then
    select count(*) into v_bad
    from (
      select t.user_id from public.transactions t where t.merchant_id = new.id
      union all
      select r.user_id from public.user_merchant_rules r where r.merchant_id = new.id
      union all
      select c.user_id from public.user_merchant_category_rules c where c.merchant_id = new.id
      union all
      select s.user_id from public.recurring_series s where s.merchant_id = new.id
    ) refs
    where refs.user_id <> new.owner_user_id;
    if v_bad > 0 then
      raise exception using
        errcode = '23514',
        message = format('global_merchants.owner_user_id cannot change while %s row(s) of other users reference the merchant', v_bad);
    end if;
  end if;

  update public.global_merchant_aliases
  set owner_user_id = new.owner_user_id, updated_at = now()
  where merchant_id = new.id
    and owner_user_id is distinct from new.owner_user_id;

  return new;
end;
$$;

drop trigger if exists propagate_global_merchant_owner on public.global_merchants;
create trigger propagate_global_merchant_owner
after update of owner_user_id on public.global_merchants
for each row
when (old.owner_user_id is distinct from new.owner_user_id)
execute function public.propagate_global_merchant_owner();

-- ---------------------------------------------------------------------------
-- 8. Self-check
-- ---------------------------------------------------------------------------

select public.assert_tenancy_backstops();
