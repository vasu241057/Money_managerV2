-- Prevent new or edited projections from crossing the immutable money-flow
-- direction or carrying a category from another transaction type. Existing
-- historical mismatches remain untouched until the user reviews them.

create or replace function public.enforce_transaction_classification_consistency()
returns trigger
language plpgsql
as $$
declare
  v_direction text;
  v_category_type text;
begin
  if tg_op = 'UPDATE'
    and new.type is not distinct from old.type
    and new.category_id is not distinct from old.category_id
    and new.financial_event_id is not distinct from old.financial_event_id
    and new.user_id is not distinct from old.user_id then
    return new;
  end if;

  select fe.direction
    into v_direction
  from public.financial_events as fe
  where fe.id = new.financial_event_id
    and fe.user_id = new.user_id
  limit 1;

  if not found then
    raise exception using
      errcode = '23503',
      message = 'transactions.financial_event_id must reference a financial event owned by the same user';
  end if;

  if (v_direction = 'debit' and new.type not in ('expense', 'transfer'))
    or (v_direction = 'credit' and new.type not in ('income', 'transfer')) then
    raise exception using
      errcode = '23514',
      message = 'transactions.type must match financial_events.direction';
  end if;

  if new.category_id is null then
    return new;
  end if;

  select c.type
    into v_category_type
  from public.categories as c
  where c.id = new.category_id
  limit 1
  for share;

  if not found then
    raise exception using
      errcode = '23503',
      message = 'transactions.category_id must reference an existing category';
  end if;

  if v_category_type <> new.type then
    raise exception using
      errcode = '23514',
      message = 'transactions.category_id type must match transactions.type';
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_transaction_classification_consistency
  on public.transactions;
create trigger enforce_transaction_classification_consistency
before insert or update of type, category_id, financial_event_id, user_id
on public.transactions
for each row execute function public.enforce_transaction_classification_consistency();

-- Direction is the immutable sign of the source fact. Type can move only
-- between the two choices compatible with that sign.
create or replace function public.enforce_financial_event_direction_immutable()
returns trigger
language plpgsql
as $$
begin
  if new.direction is distinct from old.direction then
    raise exception using
      errcode = '23514',
      message = 'financial_events.direction is immutable';
  end if;
  return new;
end;
$$;

drop trigger if exists enforce_financial_event_direction_immutable
  on public.financial_events;
create trigger enforce_financial_event_direction_immutable
before update of direction
on public.financial_events
for each row execute function public.enforce_financial_event_direction_immutable();

-- A category may change type only when doing so cannot invalidate a stored
-- transaction or a typed merchant default. Transaction/default inserts hold
-- a SHARE lock on the category, closing the concurrent-write race.
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

drop trigger if exists enforce_category_type_reference_consistency
  on public.categories;
create trigger enforce_category_type_reference_consistency
before update of type
on public.categories
for each row execute function public.enforce_category_type_reference_consistency();
