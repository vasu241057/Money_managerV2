-- Multi-tenant rollout, Task 2: identity and database hardening.
-- Plan of record: docs/MULTITENANT_TASKS.md (Task 2, A.1 §3/§4/§6, A.2 §6).
--
-- Everything here is additive and idempotent. Each enforcement step is
-- preceded by a pre-check that raises on unexpected data instead of changing
-- it, so a re-run or a surprise in production aborts the transaction and
-- leaves the schema exactly as it was. Applied by scripts/db-migrate.mjs as
-- the `postgres` role (the owner of every object in `public`).
--
--   1. users: identity-provider columns (auth_provider, auth_subject) so the
--      Clerk user id is recoverable for account deletion; email is no longer
--      unique (two identities may share an address; the id is the tenant key).
--   2. card_statements.credit_card_id becomes a same-user composite FK.
--   3. Same-user-or-system category scope triggers on the three tables that
--      still relied on service code alone.
--   4. Role app_backend (the identity the Worker will connect as), blanket
--      row-level-security policies so the app sees every row, revocation of
--      every grant the Supabase Data API roles (anon, authenticated) hold on
--      public, and default privileges so future objects are born hardened.
--   5. public.grant_app_backend(regclass) for every later migration's tables
--      and public.assert_tenancy_backstops(), which the runner calls after
--      every migration and which this file calls at its end.
--
-- Role switch and Data API disablement are operator steps (runbook in the
-- plan doc); until then the app keeps connecting as postgres, which owns the
-- tables and is therefore untouched by RLS.

-- ---------------------------------------------------------------------------
-- 1. users: identity columns, email uniqueness, frozen-namespace comment
-- ---------------------------------------------------------------------------

alter table public.users
  add column if not exists auth_provider text not null default 'clerk',
  add column if not exists auth_subject text;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.users'::regclass and conname = 'users_auth_provider_check'
  ) then
    alter table public.users
      add constraint users_auth_provider_check check (auth_provider in ('clerk'));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.users'::regclass and conname = 'users_auth_subject_nonempty_check'
  ) then
    alter table public.users
      add constraint users_auth_subject_nonempty_check
      check (auth_subject is null or length(btrim(auth_subject)) > 0);
  end if;
end;
$$;

create unique index if not exists users_auth_identity_uidx
  on public.users (auth_provider, auth_subject)
  where auth_subject is not null;

-- Drop every unique constraint that covers exactly (email); keep a plain
-- index for lookups. Two Clerk identities may legitimately carry the same
-- address; the tenant key is users.id, never the email.
do $$
declare
  v_email_attnum smallint;
  r record;
begin
  select attnum into v_email_attnum
  from pg_attribute
  where attrelid = 'public.users'::regclass and attname = 'email' and not attisdropped;

  for r in
    select conname
    from pg_constraint
    where conrelid = 'public.users'::regclass
      and contype = 'u'
      and conkey = array[v_email_attnum]
  loop
    execute format('alter table public.users drop constraint %I', r.conname);
  end loop;
end;
$$;

create index if not exists idx_users_email on public.users (email);

comment on table public.users is
  'One row per tenant. id is derived deterministically from the identity provider subject (see column comment) and is the tenant key on every user-owned table.';
comment on column public.users.id is
  'FROZEN: UUIDv5-style SHA-1 of the namespace 2d0ec934-68a4-4184-adf1-46d5f9d5cc0f concatenated with the Clerk `sub` (backend/src/lib/auth.ts deriveUserUuidFromClerkSubject). Never change the namespace or the derivation; every user-owned row keys on this value.';
comment on column public.users.auth_provider is
  'Identity provider that issued auth_subject. Only ''clerk'' today.';
comment on column public.users.auth_subject is
  'Identity provider subject (Clerk `sub`). Written by the auth middleware on every request, so it fills in on the user''s next visit. Required to delete the provider-side user during account deletion.';

-- ---------------------------------------------------------------------------
-- 2. card_statements: same-user composite FK on credit_card_id
-- ---------------------------------------------------------------------------

do $$
declare
  v_bad bigint;
begin
  select count(*) into v_bad
  from public.card_statements cs
  join public.user_credit_cards c on c.id = cs.credit_card_id
  where c.user_id <> cs.user_id;

  if v_bad > 0 then
    raise exception 'pre-check failed: % card_statements row(s) reference a credit card owned by another user; resolve before applying', v_bad;
  end if;
end;
$$;

do $$
declare
  r record;
  v_has_composite boolean := false;
begin
  for r in
    select conname, pg_get_constraintdef(oid) as condef
    from pg_constraint
    where conrelid = 'public.card_statements'::regclass
      and contype = 'f'
      and confrelid = 'public.user_credit_cards'::regclass
  loop
    if r.condef like 'FOREIGN KEY (credit_card_id, user_id) REFERENCES user_credit_cards(id, user_id)%' then
      v_has_composite := true;
    else
      execute format('alter table public.card_statements drop constraint %I', r.conname);
    end if;
  end loop;

  if not v_has_composite then
    alter table public.card_statements
      add constraint card_statements_credit_card_id_fkey
      foreign key (credit_card_id, user_id)
      references public.user_credit_cards (id, user_id)
      on delete set null (credit_card_id);
  end if;
end;
$$;

-- ---------------------------------------------------------------------------
-- 3. Category scope triggers: user_category_budget_rules, user_hidden_categories,
--    recurring_series (same pattern as transactions/user_merchant_rules)
-- ---------------------------------------------------------------------------

do $$
declare
  v_bad bigint;
begin
  select count(*) into v_bad
  from public.user_category_budget_rules r
  join public.categories c on c.id = r.category_id
  where (c.user_id is null and c.is_system is distinct from true)
     or (c.user_id is not null and c.user_id <> r.user_id);
  if v_bad > 0 then
    raise exception 'pre-check failed: % user_category_budget_rules row(s) reference a category outside the owning user''s scope', v_bad;
  end if;

  select count(*) into v_bad
  from public.user_hidden_categories h
  join public.categories c on c.id = h.category_id
  where (c.user_id is null and c.is_system is distinct from true)
     or (c.user_id is not null and c.user_id <> h.user_id);
  if v_bad > 0 then
    raise exception 'pre-check failed: % user_hidden_categories row(s) reference a category outside the owning user''s scope', v_bad;
  end if;

  select count(*) into v_bad
  from public.recurring_series s
  join public.categories c on c.id = s.category_id
  where (c.user_id is null and c.is_system is distinct from true)
     or (c.user_id is not null and c.user_id <> s.user_id);
  if v_bad > 0 then
    raise exception 'pre-check failed: % recurring_series row(s) reference a category outside the owning user''s scope', v_bad;
  end if;
end;
$$;

-- One trigger function for every table shaped (user_id, category_id); the
-- table name in the message comes from TG_TABLE_NAME.
create or replace function public.enforce_user_category_scope()
returns trigger
language plpgsql
as $$
declare
  v_category_user_id uuid;
  v_category_is_system boolean;
begin
  if new.category_id is null then
    return new;
  end if;

  select c.user_id, c.is_system
    into v_category_user_id, v_category_is_system
  from public.categories c
  where c.id = new.category_id
  limit 1;

  if not found then
    raise exception using
      errcode = '23503',
      message = format('%s.category_id must reference an existing category', tg_table_name);
  end if;

  if v_category_user_id is null then
    if v_category_is_system is distinct from true then
      raise exception using
        errcode = '23514',
        message = format('%s.category_id may only reference system categories when category user_id is null', tg_table_name);
    end if;
    return new;
  end if;

  if v_category_user_id <> new.user_id then
    raise exception using
      errcode = '23514',
      message = format('%s.category_id must belong to the same user or be a system category', tg_table_name);
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_user_category_budget_rules_category_scope on public.user_category_budget_rules;
create trigger enforce_user_category_budget_rules_category_scope
before insert or update on public.user_category_budget_rules
for each row execute function public.enforce_user_category_scope();

drop trigger if exists enforce_user_hidden_categories_category_scope on public.user_hidden_categories;
create trigger enforce_user_hidden_categories_category_scope
before insert or update on public.user_hidden_categories
for each row execute function public.enforce_user_category_scope();

drop trigger if exists enforce_recurring_series_category_scope on public.recurring_series;
create trigger enforce_recurring_series_category_scope
before insert or update on public.recurring_series
for each row execute function public.enforce_user_category_scope();

-- ---------------------------------------------------------------------------
-- 4. app_backend role, grants, RLS helpers
-- ---------------------------------------------------------------------------

-- The password is never in a migration: the operator sets it once with
-- `alter role app_backend password '...'` in the SQL editor (runbook).
do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'app_backend') then
    execute 'create role app_backend with login nosuperuser nocreatedb nocreaterole noinherit nobypassrls noreplication';
  end if;
end;
$$;

grant usage on schema public to app_backend;

-- Comma-separated, quoted list of the Supabase Data API roles that exist on
-- this server (both on Supabase; possibly neither on a plain Postgres).
create or replace function public.tenancy_api_roles()
returns text
language sql
stable
as $$
  select string_agg(quote_ident(rolname), ', ' order by rolname)
  from pg_roles
  where rolname in ('anon', 'authenticated');
$$;

-- Hardens one table: RLS on, blanket policy for app_backend, CRUD grant to
-- app_backend, no grants for the Data API roles. Every migration that creates
-- a table calls this for it; assert_tenancy_backstops() fails the migration
-- otherwise.
create or replace function public.grant_app_backend(p_table regclass)
returns void
language plpgsql
as $$
declare
  v_relkind "char";
  v_api_roles text := public.tenancy_api_roles();
begin
  select c.relkind into v_relkind from pg_class c where c.oid = p_table;
  if v_relkind is null or v_relkind not in ('r', 'p') then
    raise exception 'grant_app_backend(%): not a table', p_table;
  end if;

  execute format('alter table %s enable row level security', p_table);
  execute format('drop policy if exists app_backend_all on %s', p_table);
  execute format(
    'create policy app_backend_all on %s as permissive for all to app_backend using (true) with check (true)',
    p_table
  );
  execute format('grant select, insert, update, delete on %s to app_backend', p_table);
  if v_api_roles is not null then
    execute format('revoke all on %s from %s', p_table, v_api_roles);
  end if;
end;
$$;

-- Raises with every violation listed when any table in public lacks RLS, the
-- app_backend_all policy or app_backend's CRUD grant, or when anon /
-- authenticated can touch any relation or execute any non-extension function
-- in public. Called by the migration runner after every migration, by the
-- PGlite tenancy harness after loading, and at the end of this file.
create or replace function public.assert_tenancy_backstops()
returns void
language plpgsql
as $$
declare
  v_problems text[] := '{}';
  v_app_role_oid oid;
  v_api_role text;
  v_priv text;
  r record;
begin
  select oid into v_app_role_oid from pg_roles where rolname = 'app_backend';
  if v_app_role_oid is null then
    raise exception 'tenancy backstops violated: role app_backend does not exist';
  end if;

  if not has_schema_privilege('app_backend', 'public', 'USAGE') then
    v_problems := v_problems || 'schema public: app_backend lacks USAGE';
  end if;

  for r in
    select c.oid, c.relname, c.relrowsecurity,
      exists (
        select 1 from pg_policy p
        where p.polrelid = c.oid
          and p.polname = 'app_backend_all'
          and p.polpermissive
          and p.polcmd = '*'
          and v_app_role_oid = any (p.polroles)
      ) as has_policy
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relkind in ('r', 'p')
      and not exists (
        select 1 from pg_depend d
        where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e'
      )
    order by c.relname
  loop
    if not r.relrowsecurity then
      v_problems := v_problems || format('table %I: row level security disabled', r.relname);
    end if;
    if not r.has_policy then
      v_problems := v_problems || format('table %I: missing app_backend_all policy', r.relname);
    end if;
    foreach v_priv in array array['SELECT', 'INSERT', 'UPDATE', 'DELETE'] loop
      if not has_table_privilege('app_backend', r.oid, v_priv) then
        v_problems := v_problems || format('table %I: app_backend lacks %s', r.relname, v_priv);
      end if;
    end loop;
  end loop;

  for v_api_role in select rolname from pg_roles where rolname in ('anon', 'authenticated') order by rolname loop
    for r in
      select c.oid, c.relname, c.relkind
      from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
      where n.nspname = 'public'
        and c.relkind in ('r', 'p', 'v', 'm', 'S')
        and not exists (
          select 1 from pg_depend d
          where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e'
        )
      order by c.relname
    loop
      if r.relkind = 'S' then
        if has_sequence_privilege(v_api_role, r.oid, 'USAGE, SELECT, UPDATE') then
          v_problems := v_problems || format('sequence %I: %s still has privileges', r.relname, v_api_role);
        end if;
      elsif has_table_privilege(v_api_role, r.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') then
        v_problems := v_problems || format('relation %I: %s still has privileges', r.relname, v_api_role);
      end if;
    end loop;

    for r in
      select p.oid,
        format('%I.%I(%s)', n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)) as signature
      from pg_proc p
      join pg_namespace n on n.oid = p.pronamespace
      where n.nspname = 'public'
        and p.prokind in ('f', 'p')
        and not exists (
          select 1 from pg_depend d
          where d.classid = 'pg_proc'::regclass and d.objid = p.oid and d.deptype = 'e'
        )
      order by p.proname, p.oid
    loop
      if has_function_privilege(v_api_role, r.oid, 'EXECUTE') then
        v_problems := v_problems || format('function %s: %s can execute', r.signature, v_api_role);
      end if;
    end loop;
  end loop;

  if coalesce(array_length(v_problems, 1), 0) > 0 then
    raise exception 'tenancy backstops violated: %', array_to_string(v_problems, '; ');
  end if;
end;
$$;

-- Apply to everything that exists today.
do $$
declare
  r record;
  v_api_roles text := public.tenancy_api_roles();
begin
  for r in
    select c.oid::regclass as rel
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relkind in ('r', 'p')
      and not exists (
        select 1 from pg_depend d
        where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e'
      )
    order by c.relname
  loop
    perform public.grant_app_backend(r.rel);
  end loop;

  for r in
    select c.oid::regclass as rel, c.relkind
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relkind in ('v', 'm', 'S')
      and not exists (
        select 1 from pg_depend d
        where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e'
      )
    order by c.relname
  loop
    if r.relkind = 'S' then
      execute format('grant usage, select on sequence %s to app_backend', r.rel);
      if v_api_roles is not null then
        execute format('revoke all on sequence %s from %s', r.rel, v_api_roles);
      end if;
    else
      execute format('grant select on %s to app_backend', r.rel);
      if v_api_roles is not null then
        execute format('revoke all on %s from %s', r.rel, v_api_roles);
      end if;
    end if;
  end loop;

  -- Functions default to PUBLIC execute, which the Data API exposes as RPC.
  -- Ours become app_backend-only (postgres owns them and keeps execute).
  for r in
    select
      format('%I.%I(%s)', n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)) as signature,
      case when p.prokind = 'p' then 'procedure' else 'function' end as kind
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public'
      and p.prokind in ('f', 'p')
      and not exists (
        select 1 from pg_depend d
        where d.classid = 'pg_proc'::regclass and d.objid = p.oid and d.deptype = 'e'
      )
    order by p.proname, p.oid
  loop
    execute format('revoke execute on %s %s from public', r.kind, r.signature);
    if v_api_roles is not null then
      execute format('revoke execute on %s %s from %s', r.kind, r.signature, v_api_roles);
    end if;
    execute format('grant execute on %s %s to app_backend', r.kind, r.signature);
  end loop;
end;
$$;

-- Objects created later by the migration identity are born hardened: CRUD
-- for app_backend, nothing for the Data API roles, no PUBLIC execute.
-- (RLS and the policy still come from grant_app_backend; the assert enforces.)
-- Per-schema default privileges can only reverse per-schema grants (which is
-- what Supabase's anon/authenticated grants are), never the built-in PUBLIC
-- execute on functions, so that one revoke is global for the migration role.
do $$
declare
  v_api_roles text := public.tenancy_api_roles();
begin
  execute format('alter default privileges for role %I in schema public grant select, insert, update, delete on tables to app_backend', current_user);
  execute format('alter default privileges for role %I in schema public grant usage, select on sequences to app_backend', current_user);
  execute format('alter default privileges for role %I in schema public grant execute on functions to app_backend', current_user);
  execute format('alter default privileges for role %I revoke execute on functions from public', current_user);
  if v_api_roles is not null then
    execute format('alter default privileges for role %I in schema public revoke all on tables from %s', current_user, v_api_roles);
    execute format('alter default privileges for role %I in schema public revoke all on sequences from %s', current_user, v_api_roles);
    execute format('alter default privileges for role %I in schema public revoke all on functions from %s', current_user, v_api_roles);
  end if;
end;
$$;

-- ---------------------------------------------------------------------------
-- 5. Self-check: the file only commits if every backstop holds.
-- ---------------------------------------------------------------------------

select public.assert_tenancy_backstops();
