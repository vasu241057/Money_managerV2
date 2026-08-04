-- Merchant identity is direction-independent, but category defaults are not.
-- Keep the existing parent rows for identity and store one category per
-- transaction type in child tables. Legacy category columns remain during
-- the compatibility window; application reads use these typed tables.

set search_path = public;

create table if not exists public.user_merchant_rule_categories (
  user_merchant_rule_id uuid not null
    references public.user_merchant_rules(id) on delete cascade,
  transaction_type text not null
    check (transaction_type in ('income', 'expense', 'transfer')),
  category_id uuid not null references public.categories(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (user_merchant_rule_id, transaction_type)
);

drop trigger if exists update_user_merchant_rule_categories_modtime
  on public.user_merchant_rule_categories;
create trigger update_user_merchant_rule_categories_modtime
before update on public.user_merchant_rule_categories
for each row execute function public.update_updated_at_column();

create or replace function public.enforce_user_merchant_rule_category_scope()
returns trigger
language plpgsql
as $$
declare
  v_rule_user_id uuid;
  v_category_user_id uuid;
  v_category_is_system boolean;
  v_category_type text;
begin
  select umr.user_id
    into v_rule_user_id
  from public.user_merchant_rules umr
  where umr.id = new.user_merchant_rule_id;

  if not found then
    raise exception using
      errcode = '23503',
      message = 'user_merchant_rule_categories must reference an existing rule';
  end if;

  select c.user_id, c.is_system, c.type
    into v_category_user_id, v_category_is_system, v_category_type
  from public.categories c
  where c.id = new.category_id
  for share;

  if not found then
    raise exception using
      errcode = '23503',
      message = 'user_merchant_rule_categories.category_id must reference an existing category';
  end if;

  if v_category_type <> new.transaction_type then
    raise exception using
      errcode = '23514',
      message = 'user merchant rule category type must match transaction_type';
  end if;

  if v_category_user_id is null then
    if v_category_is_system is distinct from true then
      raise exception using
        errcode = '23514',
        message = 'shared rule categories must be system categories';
    end if;
  elsif v_category_user_id <> v_rule_user_id then
    raise exception using
      errcode = '23514',
      message = 'user merchant rule category must belong to the same user or be a system category';
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_user_merchant_rule_category_scope
  on public.user_merchant_rule_categories;
create trigger enforce_user_merchant_rule_category_scope
before insert or update on public.user_merchant_rule_categories
for each row execute function public.enforce_user_merchant_rule_category_scope();

create table if not exists public.global_merchant_category_defaults (
  merchant_id uuid not null
    references public.global_merchants(id) on delete cascade,
  transaction_type text not null
    check (transaction_type in ('income', 'expense', 'transfer')),
  category_id uuid not null references public.categories(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (merchant_id, transaction_type)
);

drop trigger if exists update_global_merchant_category_defaults_modtime
  on public.global_merchant_category_defaults;
create trigger update_global_merchant_category_defaults_modtime
before update on public.global_merchant_category_defaults
for each row execute function public.update_updated_at_column();

create or replace function public.enforce_global_merchant_category_default_scope()
returns trigger
language plpgsql
as $$
declare
  v_category_user_id uuid;
  v_category_is_system boolean;
  v_category_type text;
begin
  select c.user_id, c.is_system, c.type
    into v_category_user_id, v_category_is_system, v_category_type
  from public.categories c
  where c.id = new.category_id
  for share;

  if not found then
    raise exception using
      errcode = '23503',
      message = 'global_merchant_category_defaults.category_id must reference an existing category';
  end if;

  if v_category_user_id is not null or v_category_is_system is distinct from true then
    raise exception using
      errcode = '23514',
      message = 'global merchant defaults must reference system categories';
  end if;

  if v_category_type <> new.transaction_type then
    raise exception using
      errcode = '23514',
      message = 'global merchant default category type must match transaction_type';
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_global_merchant_category_default_scope
  on public.global_merchant_category_defaults;
create trigger enforce_global_merchant_category_default_scope
before insert or update on public.global_merchant_category_defaults
for each row execute function public.enforce_global_merchant_category_default_scope();

-- The legacy rows contain only one category. Its category type is the only
-- safe scope to preserve; never invent an opposite-direction default.
insert into public.user_merchant_rule_categories (
  user_merchant_rule_id,
  transaction_type,
  category_id
)
select umr.id, c.type, c.id
from public.user_merchant_rules umr
join public.categories c on c.id = umr.custom_category_id
on conflict (user_merchant_rule_id, transaction_type) do nothing;

insert into public.global_merchant_category_defaults (
  merchant_id,
  transaction_type,
  category_id
)
select gm.id, c.type, c.id
from public.global_merchants gm
join public.categories c on c.id = gm.default_category_id
where c.user_id is null and c.is_system = true
on conflict (merchant_id, transaction_type) do nothing;
