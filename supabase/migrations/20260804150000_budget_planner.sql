-- Monthly spending-plan foundation.
-- Raw financial events remain immutable. A user "delete" only hides the
-- mutable transaction projection so it can be restored from the UI.

set search_path = public;

alter table public.transactions
  add column if not exists budget_treatment text not null default 'AUTO',
  add column if not exists user_deleted_at timestamptz;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'transactions_budget_treatment_chk'
      and conrelid = 'public.transactions'::regclass
  ) then
    alter table public.transactions
      add constraint transactions_budget_treatment_chk
      check (budget_treatment in ('AUTO', 'INCLUDE', 'EXCLUDE'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'uq_transactions_id_user'
      and conrelid = 'public.transactions'::regclass
  ) then
    alter table public.transactions
      add constraint uq_transactions_id_user unique (id, user_id);
  end if;
end;
$$;

create index if not exists transactions_active_user_date_idx
  on public.transactions (user_id, txn_date desc, id desc)
  where user_deleted_at is null;

create table if not exists public.monthly_budgets (
  user_id uuid not null references public.users(id) on delete cascade,
  month_start date not null,
  limit_in_paise bigint not null check (limit_in_paise > 0),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (user_id, month_start),
  constraint monthly_budgets_month_start_chk
    check (month_start = date_trunc('month', month_start)::date)
);

drop trigger if exists update_monthly_budgets_modtime on public.monthly_budgets;
create trigger update_monthly_budgets_modtime
before update on public.monthly_budgets
for each row execute function public.update_updated_at_column();

create table if not exists public.user_category_budget_rules (
  user_id uuid not null references public.users(id) on delete cascade,
  category_id uuid not null references public.categories(id) on delete cascade,
  treatment text not null check (treatment in ('INCLUDE', 'EXCLUDE')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (user_id, category_id)
);

drop trigger if exists update_category_budget_rules_modtime on public.user_category_budget_rules;
create trigger update_category_budget_rules_modtime
before update on public.user_category_budget_rules
for each row execute function public.update_updated_at_column();

create table if not exists public.recurring_series (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  fingerprint text not null,
  search_key text,
  merchant_id uuid references public.global_merchants(id) on delete set null,
  category_id uuid references public.categories(id) on delete set null,
  direction text not null check (direction in ('debit', 'credit')),
  cadence text not null check (cadence in ('WEEKLY', 'MONTHLY')),
  expected_day integer not null,
  amount_min_in_paise bigint not null check (amount_min_in_paise > 0),
  amount_max_in_paise bigint not null check (amount_max_in_paise >= amount_min_in_paise),
  budget_treatment text not null default 'AUTO'
    check (budget_treatment in ('AUTO', 'INCLUDE', 'EXCLUDE')),
  status text not null default 'ACTIVE'
    check (status in ('ACTIVE', 'PAUSED', 'DISMISSED')),
  last_matched_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, fingerprint),
  unique (id, user_id),
  constraint recurring_series_expected_day_chk check (
    (cadence = 'WEEKLY' and expected_day between 0 and 6)
    or (cadence = 'MONTHLY' and expected_day between 1 and 31)
  )
);

drop trigger if exists update_recurring_series_modtime on public.recurring_series;
create trigger update_recurring_series_modtime
before update on public.recurring_series
for each row execute function public.update_updated_at_column();

alter table public.transactions
  add column if not exists recurring_series_id uuid;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'fk_transactions_recurring_series_user'
      and conrelid = 'public.transactions'::regclass
  ) then
    alter table public.transactions
      add constraint fk_transactions_recurring_series_user
      foreign key (recurring_series_id, user_id)
      references public.recurring_series(id, user_id)
      on delete set null (recurring_series_id);
  end if;
end;
$$;

create index if not exists transactions_recurring_series_idx
  on public.transactions (recurring_series_id, txn_date desc)
  where recurring_series_id is not null and user_deleted_at is null;

create table if not exists public.transaction_relationships (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  source_transaction_id uuid not null,
  target_transaction_id uuid not null,
  kind text not null check (kind in ('REFUND', 'REIMBURSEMENT')),
  created_by text not null check (created_by in ('AUTO', 'USER')),
  confidence numeric(4, 3) not null check (confidence between 0 and 1),
  created_at timestamptz not null default now(),
  constraint transaction_relationships_distinct_chk
    check (source_transaction_id <> target_transaction_id),
  constraint transaction_relationships_source_once unique (source_transaction_id),
  constraint fk_transaction_relationships_source_user
    foreign key (source_transaction_id, user_id)
    references public.transactions(id, user_id)
    on delete cascade,
  constraint fk_transaction_relationships_target_user
    foreign key (target_transaction_id, user_id)
    references public.transactions(id, user_id)
    on delete cascade
);

create index if not exists transaction_relationships_target_idx
  on public.transaction_relationships (user_id, target_transaction_id, created_at);

create table if not exists public.budget_suggestion_dismissals (
  user_id uuid not null references public.users(id) on delete cascade,
  kind text not null check (kind in ('REFUND_MATCH', 'RECURRING_PATTERN')),
  fingerprint text not null,
  dismissed_at timestamptz not null default now(),
  primary key (user_id, kind, fingerprint)
);

-- Auto-link only the unambiguous card case. Partial refunds are allowed, but
-- exactly one same-card + same-merchant purchase must still be capable of
-- absorbing the credit. Ambiguous matches remain suggestions for the user.
create or replace function public.auto_link_unambiguous_card_refund()
returns trigger
language plpgsql
as $$
declare
  v_candidate_count integer;
  v_target_id uuid;
  v_target_amount bigint;
  v_remaining_amount bigint;
begin
  if tg_op = 'UPDATE' then
    delete from public.transaction_relationships
    where source_transaction_id = new.id
      and user_id = new.user_id
      and created_by = 'AUTO';
  end if;

  if new.type <> 'income'
     or new.credit_card_id is null
     or new.user_deleted_at is not null then
    return new;
  end if;

  with capable as (
    select
      target.id,
      target.amount_in_paise,
      target.txn_date
    from public.transactions as target
    join public.financial_events as source_event
      on source_event.id = new.financial_event_id
      and source_event.user_id = new.user_id
      and source_event.status = 'ACTIVE'
      and source_event.direction = 'credit'
    join public.financial_events as target_event
      on target_event.id = target.financial_event_id
      and target_event.user_id = target.user_id
      and target_event.status = 'ACTIVE'
    left join lateral (
      select coalesce(sum(linked.amount_in_paise), 0)::bigint as refunded_in_paise
      from public.transaction_relationships as existing_rel
      join public.transactions as linked
        on linked.id = existing_rel.source_transaction_id
        and linked.user_id = existing_rel.user_id
        and linked.user_deleted_at is null
      where existing_rel.user_id = target.user_id
        and existing_rel.target_transaction_id = target.id
    ) as refunded on true
    where target.user_id = new.user_id
      and target.id <> new.id
      and target.type = 'expense'
      and target.credit_card_id = new.credit_card_id
      and target.user_deleted_at is null
      and target.txn_date <= new.txn_date
      and target.txn_date >= new.txn_date - interval '120 days'
      and target.amount_in_paise - refunded.refunded_in_paise >= new.amount_in_paise
      and (
        (new.merchant_id is not null and target.merchant_id = new.merchant_id)
        or (
          source_event.search_key is not null
          and target_event.search_key = source_event.search_key
        )
      )
  )
  select count(*)::integer, (array_agg(id order by txn_date desc, id))[1], min(amount_in_paise)
    into v_candidate_count, v_target_id, v_target_amount
  from capable;

  if v_candidate_count = 1 and v_target_id is not null then
    -- Candidate discovery and insertion must be serialized per purchase;
    -- otherwise two refunds arriving together could both see the same room.
    perform target.id
    from public.transactions as target
    where target.id = v_target_id
      and target.user_id = new.user_id
      and target.user_deleted_at is null
    for update;

    if not found then
      return new;
    end if;

    select (
      target.amount_in_paise - coalesce(sum(linked.amount_in_paise), 0)
    )::bigint
      into v_remaining_amount
    from public.transactions as target
    left join public.transaction_relationships as existing_rel
      on existing_rel.user_id = target.user_id
      and existing_rel.target_transaction_id = target.id
    left join public.transactions as linked
      on linked.id = existing_rel.source_transaction_id
      and linked.user_id = existing_rel.user_id
      and linked.user_deleted_at is null
    where target.id = v_target_id
      and target.user_id = new.user_id
    group by target.amount_in_paise;

    if v_remaining_amount is null or v_remaining_amount < new.amount_in_paise then
      return new;
    end if;

    insert into public.transaction_relationships (
      user_id,
      source_transaction_id,
      target_transaction_id,
      kind,
      created_by,
      confidence
    )
    values (
      new.user_id,
      new.id,
      v_target_id,
      'REFUND',
      'AUTO',
      case when v_target_amount = new.amount_in_paise then 0.995 else 0.940 end
    )
    on conflict (source_transaction_id) do nothing;
  end if;

  return new;
end;
$$;

drop trigger if exists auto_link_unambiguous_card_refund on public.transactions;
create trigger auto_link_unambiguous_card_refund
after insert or update of type, credit_card_id, merchant_id, txn_date, user_deleted_at
on public.transactions
for each row execute function public.auto_link_unambiguous_card_refund();

create or replace function public.match_confirmed_recurring_series()
returns trigger
language plpgsql
as $$
declare
  v_series_count integer;
  v_series_id uuid;
  v_broken_count integer;
  v_broken_id uuid;
begin
  if new.type <> 'expense'
     or new.recurring_series_id is not null
     or new.user_deleted_at is not null then
    return new;
  end if;

  with matching as (
    select rs.id
    from public.recurring_series as rs
    join public.financial_events as event
      on event.id = new.financial_event_id
      and event.user_id = new.user_id
      and event.status = 'ACTIVE'
    where rs.user_id = new.user_id
      and rs.status = 'ACTIVE'
      and rs.direction = event.direction
      and rs.category_id is not distinct from new.category_id
      and new.amount_in_paise between rs.amount_min_in_paise and rs.amount_max_in_paise
      and (
        (rs.merchant_id is not null and rs.merchant_id = new.merchant_id)
        or (rs.search_key is not null and rs.search_key = event.search_key)
      )
      and (
        rs.last_matched_at is null
        or (
          new.txn_date > rs.last_matched_at
          and (
            (rs.cadence = 'WEEKLY' and new.txn_date - rs.last_matched_at between interval '4 days' and interval '10 days')
            or (rs.cadence = 'MONTHLY' and new.txn_date - rs.last_matched_at between interval '20 days' and interval '45 days')
          )
        )
      )
  )
  select count(*)::integer, (array_agg(id order by id))[1]
    into v_series_count, v_series_id
  from matching;

  if v_series_count = 1 and v_series_id is not null then
    update public.transactions
    set recurring_series_id = v_series_id
    where id = new.id and user_id = new.user_id;

    update public.recurring_series
    set last_matched_at = new.txn_date,
        updated_at = now()
    where id = v_series_id and user_id = new.user_id;
  elsif v_series_count = 0 then
    -- If the expected merchant/category arrives on schedule but its amount
    -- falls outside the confirmed range, pause one unambiguous rule. This is
    -- safer than silently applying a stale rule to a materially changed bill.
    with broken as (
      select rs.id
      from public.recurring_series as rs
      join public.financial_events as event
        on event.id = new.financial_event_id
        and event.user_id = new.user_id
        and event.status = 'ACTIVE'
      where rs.user_id = new.user_id
        and rs.status = 'ACTIVE'
        and rs.direction = event.direction
        and rs.category_id is not distinct from new.category_id
        and new.amount_in_paise not between rs.amount_min_in_paise and rs.amount_max_in_paise
        and (
          (rs.merchant_id is not null and rs.merchant_id = new.merchant_id)
          or (rs.search_key is not null and rs.search_key = event.search_key)
        )
        and new.txn_date > rs.last_matched_at
        and (
          (rs.cadence = 'WEEKLY' and new.txn_date - rs.last_matched_at between interval '4 days' and interval '10 days')
          or (rs.cadence = 'MONTHLY' and new.txn_date - rs.last_matched_at between interval '20 days' and interval '45 days')
        )
    )
    select count(*)::integer, (array_agg(id order by id))[1]
      into v_broken_count, v_broken_id
    from broken;

    if v_broken_count = 1 and v_broken_id is not null then
      update public.recurring_series
      set status = 'PAUSED', updated_at = now()
      where id = v_broken_id and user_id = new.user_id;
    end if;
  end if;

  return new;
end;
$$;

drop trigger if exists match_confirmed_recurring_series on public.transactions;
create trigger match_confirmed_recurring_series
after insert or update of merchant_id, category_id, type, txn_date, user_deleted_at
on public.transactions
for each row execute function public.match_confirmed_recurring_series();
