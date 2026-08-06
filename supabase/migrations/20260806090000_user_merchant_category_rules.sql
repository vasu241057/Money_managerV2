-- Merchant-level category defaults owned by the user.
--
-- public.user_merchant_rules is keyed by search_key, so it only ever covers the
-- exact counterparty string a transaction arrived with. The user thinks in
-- merchants ("Swiggy is Food Delivery"), and one merchant owns many aliases, so
-- a search-key rule cannot express that intent. This table keys the same
-- decision on the resolved merchant, which every alias of that merchant reaches.
--
-- Typed per transaction_type for the same reason as
-- public.user_merchant_rule_categories: merchant identity is
-- direction-independent, category is not (a Swiggy refund is income).

set search_path = public;

create table if not exists public.user_merchant_category_rules (
  user_id uuid not null references public.users(id) on delete cascade,
  merchant_id uuid not null references public.global_merchants(id) on delete cascade,
  transaction_type text not null
    check (transaction_type in ('income', 'expense', 'transfer')),
  category_id uuid not null references public.categories(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (user_id, merchant_id, transaction_type)
);

drop trigger if exists update_user_merchant_category_rules_modtime
  on public.user_merchant_category_rules;
create trigger update_user_merchant_category_rules_modtime
before update on public.user_merchant_category_rules
for each row execute function public.update_updated_at_column();

-- Same ownership and type guarantees the search-key rules already enforce, so
-- neither surface can persist a category the user may not use.
create or replace function public.enforce_user_merchant_category_rule_scope()
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
      message = 'user_merchant_category_rules.category_id must reference an existing category';
  end if;

  if v_category_type <> new.transaction_type then
    raise exception using
      errcode = '23514',
      message = 'user merchant category rule type must match transaction_type';
  end if;

  if v_category_user_id is null then
    if v_category_is_system is distinct from true then
      raise exception using
        errcode = '23514',
        message = 'shared merchant category rules must be system categories';
    end if;
  elsif v_category_user_id <> new.user_id then
    raise exception using
      errcode = '23514',
      message = 'user merchant category rule must belong to the same user or be a system category';
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_user_merchant_category_rule_scope
  on public.user_merchant_category_rules;
create trigger enforce_user_merchant_category_rule_scope
before insert or update on public.user_merchant_category_rules
for each row execute function public.enforce_user_merchant_category_rule_scope();

-- Retyping a category must not orphan a rule in this table either. Same guard
-- 20260804090000 already applies to the sibling rule tables.
create or replace function public.enforce_category_type_reference_consistency()
returns trigger
language plpgsql
as $$
begin
  if new.type is not distinct from old.type then
    return new;
  end if;

  if exists (
    select 1
    from public.transactions as t
    where t.category_id = new.id
      and t.type <> new.type
  ) or exists (
    select 1
    from public.user_merchant_rule_categories as umrc
    where umrc.category_id = new.id
      and umrc.transaction_type <> new.type
  ) or exists (
    select 1
    from public.user_merchant_category_rules as umcr
    where umcr.category_id = new.id
      and umcr.transaction_type <> new.type
  ) or exists (
    select 1
    from public.global_merchant_category_defaults as gmcd
    where gmcd.category_id = new.id
      and gmcd.transaction_type <> new.type
  ) then
    raise exception using
      errcode = '23514',
      message = 'categories.type cannot invalidate transaction or merchant-rule classifications';
  end if;

  return new;
end;
$$;

-- The merchants settings screen ranks by transaction volume, and the backfill
-- updates one (user, merchant, type) slice at a time.
create index if not exists idx_transactions_user_merchant_type
  on public.transactions (user_id, merchant_id, type)
  where merchant_id is not null and user_deleted_at is null;
