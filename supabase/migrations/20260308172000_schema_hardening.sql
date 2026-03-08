-- Milestone hardening patch
-- - Enforce transactions.financial_event_id NOT NULL
-- - Enforce same-user FK coupling across oauth/raw_email/financial_event/transactions
-- - Enforce category ownership/system-scope on transactions and user_merchant_rules

set search_path = public;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'transactions'
      and column_name = 'financial_event_id'
      and is_nullable = 'YES'
  ) then
    if exists (select 1 from public.transactions where financial_event_id is null) then
      raise exception using
        errcode = '23514',
        message = 'Cannot set transactions.financial_event_id NOT NULL while null rows exist';
    end if;

    alter table public.transactions
      alter column financial_event_id set not null;
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.oauth_connections'::regclass
      and contype = 'u'
      and conkey = array[
        (
          select attnum::int2
          from pg_attribute
          where attrelid = 'public.oauth_connections'::regclass
            and attname = 'id'
            and not attisdropped
        ),
        (
          select attnum::int2
          from pg_attribute
          where attrelid = 'public.oauth_connections'::regclass
            and attname = 'user_id'
            and not attisdropped
        )
      ]::int2[]
  ) then
    alter table public.oauth_connections
      add constraint uq_oauth_connections_id_user unique (id, user_id);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.raw_emails'::regclass
      and contype = 'u'
      and conkey = array[
        (
          select attnum::int2
          from pg_attribute
          where attrelid = 'public.raw_emails'::regclass
            and attname = 'id'
            and not attisdropped
        ),
        (
          select attnum::int2
          from pg_attribute
          where attrelid = 'public.raw_emails'::regclass
            and attname = 'user_id'
            and not attisdropped
        )
      ]::int2[]
  ) then
    alter table public.raw_emails
      add constraint uq_raw_emails_id_user unique (id, user_id);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.financial_events'::regclass
      and contype = 'u'
      and conkey = array[
        (
          select attnum::int2
          from pg_attribute
          where attrelid = 'public.financial_events'::regclass
            and attname = 'id'
            and not attisdropped
        ),
        (
          select attnum::int2
          from pg_attribute
          where attrelid = 'public.financial_events'::regclass
            and attname = 'user_id'
            and not attisdropped
        )
      ]::int2[]
  ) then
    alter table public.financial_events
      add constraint uq_financial_events_id_user unique (id, user_id);
  end if;
end;
$$;

do $$
begin
  if exists (
    select 1
    from public.raw_emails re
    left join public.oauth_connections oc
      on oc.id = re.oauth_connection_id
     and oc.user_id = re.user_id
    where re.oauth_connection_id is not null
      and oc.id is null
  ) then
    raise exception using
      errcode = '23514',
      message = 'Cannot enforce raw_emails/oauth_connections same-user FK: mismatched rows exist';
  end if;

  if exists (
    select 1
    from public.financial_events fe
    left join public.raw_emails re
      on re.id = fe.raw_email_id
     and re.user_id = fe.user_id
    where fe.raw_email_id is not null
      and re.id is null
  ) then
    raise exception using
      errcode = '23514',
      message = 'Cannot enforce financial_events/raw_emails same-user FK: mismatched rows exist';
  end if;

  if exists (
    select 1
    from public.transactions t
    left join public.financial_events fe
      on fe.id = t.financial_event_id
     and fe.user_id = t.user_id
    where t.financial_event_id is not null
      and fe.id is null
  ) then
    raise exception using
      errcode = '23514',
      message = 'Cannot enforce transactions/financial_events same-user FK: mismatched rows exist';
  end if;
end;
$$;

alter table public.raw_emails
  drop constraint if exists raw_emails_oauth_connection_id_fkey,
  drop constraint if exists fk_raw_emails_oauth_connection_user;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'fk_raw_emails_oauth_connection_user'
      and conrelid = 'public.raw_emails'::regclass
  ) then
    alter table public.raw_emails
      add constraint fk_raw_emails_oauth_connection_user
      foreign key (oauth_connection_id, user_id)
      references public.oauth_connections(id, user_id)
      on delete set null (oauth_connection_id);
  end if;
end;
$$;

alter table public.financial_events
  drop constraint if exists financial_events_raw_email_id_fkey,
  drop constraint if exists fk_financial_events_raw_email_user;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'fk_financial_events_raw_email_user'
      and conrelid = 'public.financial_events'::regclass
  ) then
    alter table public.financial_events
      add constraint fk_financial_events_raw_email_user
      foreign key (raw_email_id, user_id)
      references public.raw_emails(id, user_id)
      on delete cascade;
  end if;
end;
$$;

alter table public.transactions
  drop constraint if exists transactions_financial_event_id_fkey,
  drop constraint if exists fk_transactions_financial_event_user;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'fk_transactions_financial_event_user'
      and conrelid = 'public.transactions'::regclass
  ) then
    alter table public.transactions
      add constraint fk_transactions_financial_event_user
      foreign key (financial_event_id, user_id)
      references public.financial_events(id, user_id)
      on delete cascade;
  end if;
end;
$$;

create or replace function public.enforce_user_merchant_rules_category_scope()
returns trigger
language plpgsql
as $$
declare
  v_category_user_id uuid;
  v_category_is_system boolean;
begin
  if new.custom_category_id is null then
    return new;
  end if;

  select c.user_id, c.is_system
    into v_category_user_id, v_category_is_system
  from public.categories c
  where c.id = new.custom_category_id
  limit 1;

  if not found then
    raise exception using
      errcode = '23503',
      message = 'user_merchant_rules.custom_category_id must reference an existing category';
  end if;

  if v_category_user_id is null then
    if v_category_is_system is distinct from true then
      raise exception using
        errcode = '23514',
        message = 'user_merchant_rules.custom_category_id may only reference system categories when category user_id is null';
    end if;
    return new;
  end if;

  if v_category_user_id <> new.user_id then
    raise exception using
      errcode = '23514',
      message = 'user_merchant_rules.custom_category_id must belong to the same user or be a system category';
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_user_merchant_rules_category_scope on public.user_merchant_rules;
create trigger enforce_user_merchant_rules_category_scope
before insert or update on public.user_merchant_rules
for each row execute function public.enforce_user_merchant_rules_category_scope();

create or replace function public.enforce_transactions_category_scope()
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
      message = 'transactions.category_id must reference an existing category';
  end if;

  if v_category_user_id is null then
    if v_category_is_system is distinct from true then
      raise exception using
        errcode = '23514',
        message = 'transactions.category_id may only reference system categories when category user_id is null';
    end if;
    return new;
  end if;

  if v_category_user_id <> new.user_id then
    raise exception using
      errcode = '23514',
      message = 'transactions.category_id must belong to the same user or be a system category';
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_transactions_category_scope on public.transactions;
create trigger enforce_transactions_category_scope
before insert or update on public.transactions
for each row execute function public.enforce_transactions_category_scope();
