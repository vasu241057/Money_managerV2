-- Money Manager baseline schema (Milestone 1)
-- Created: 2026-03-08

set search_path = public;

create extension if not exists pgcrypto;
create extension if not exists pg_cron;

-- ==========================================
-- 0. Utility triggers
-- ==========================================
create or replace function public.update_updated_at_column()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- ==========================================
-- 1. Identity & core app structure
-- ==========================================
create table if not exists public.users (
  id uuid primary key default gen_random_uuid(),
  email text unique not null,
  last_app_open_date timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create table if not exists public.oauth_connections (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  provider text not null check (provider in ('google')),
  email_address text not null,
  access_token text,
  refresh_token text,
  last_sync_timestamp bigint not null default 0,
  sync_status text not null default 'ACTIVE'
    check (sync_status in ('ACTIVE', 'DORMANT', 'AUTH_REVOKED', 'ERROR_PAUSED')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_oauth_connections_id_user unique (id, user_id),
  unique (user_id, email_address)
);

drop trigger if exists update_oauth_modtime on public.oauth_connections;
create trigger update_oauth_modtime
before update on public.oauth_connections
for each row execute function public.update_updated_at_column();

create table if not exists public.accounts (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  name text not null,
  type text not null check (type in ('cash', 'bank', 'card', 'other')),
  instrument_last4 text,
  initial_balance_in_paise bigint not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (id, user_id)
);

drop trigger if exists update_accounts_modtime on public.accounts;
create trigger update_accounts_modtime
before update on public.accounts
for each row execute function public.update_updated_at_column();

create table if not exists public.categories (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references public.users(id) on delete cascade,
  parent_id uuid references public.categories(id) on delete restrict,
  name text not null,
  type text not null check (type in ('income', 'expense', 'transfer')),
  icon text,
  is_system boolean not null default false,
  created_at timestamptz not null default now(),
  constraint categories_scope_chk check (
    (is_system = true and user_id is null)
    or (is_system = false and user_id is not null)
  )
);

drop index if exists public.uq_categories_system;
create unique index uq_categories_system
  on public.categories (
    coalesce(parent_id, '00000000-0000-0000-0000-000000000000'::uuid),
    name,
    type
  )
  where user_id is null and is_system = true;

drop index if exists public.uq_categories_user;
create unique index uq_categories_user
  on public.categories (
    user_id,
    coalesce(parent_id, '00000000-0000-0000-0000-000000000000'::uuid),
    name,
    type
  )
  where user_id is not null and is_system = false;

-- ==========================================
-- 2. Normalization graph
-- ==========================================
create table if not exists public.global_merchants (
  id uuid primary key default gen_random_uuid(),
  canonical_name text unique not null,
  default_category_id uuid references public.categories(id),
  type text not null default 'MERCHANT'
    check (type in ('MERCHANT', 'TRANSFER_INSTITUTION', 'AGGREGATOR', 'P2P')),
  is_context_required boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists update_global_merchants_modtime on public.global_merchants;
create trigger update_global_merchants_modtime
before update on public.global_merchants
for each row execute function public.update_updated_at_column();

create table if not exists public.global_merchant_aliases (
  search_key text primary key,
  merchant_id uuid not null references public.global_merchants(id) on delete cascade,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists update_aliases_modtime on public.global_merchant_aliases;
create trigger update_aliases_modtime
before update on public.global_merchant_aliases
for each row execute function public.update_updated_at_column();

create table if not exists public.user_merchant_rules (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  search_key text not null,
  merchant_id uuid references public.global_merchants(id),
  custom_category_id uuid references public.categories(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, search_key)
);

drop trigger if exists update_user_merchant_rules_modtime on public.user_merchant_rules;
create trigger update_user_merchant_rules_modtime
before update on public.user_merchant_rules
for each row execute function public.update_updated_at_column();

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

-- ==========================================
-- 3. Card subcategorization model (new requirement)
-- ==========================================
create table if not exists public.user_credit_cards (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  account_id uuid not null,
  card_label text not null,
  issuer_name text,
  network text,
  first4 text not null check (first4 ~ '^[0-9]{4}$'),
  last4 text not null check (last4 ~ '^[0-9]{4}$'),
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (id, user_id),
  unique (user_id, first4, last4),
  unique (user_id, card_label),
  constraint user_credit_cards_network_chk
    check (network is null or network in ('visa', 'mastercard', 'rupay', 'amex', 'diners', 'other')),
  constraint fk_user_credit_cards_account
    foreign key (account_id, user_id)
    references public.accounts(id, user_id)
    on delete cascade
);

drop trigger if exists update_user_credit_cards_modtime on public.user_credit_cards;
create trigger update_user_credit_cards_modtime
before update on public.user_credit_cards
for each row execute function public.update_updated_at_column();

create or replace function public.enforce_credit_card_account_type()
returns trigger
language plpgsql
as $$
begin
  if not exists (
    select 1
    from public.accounts a
    where a.id = new.account_id
      and a.user_id = new.user_id
      and a.type = 'card'
  ) then
    raise exception using
      errcode = '23514',
      message = 'user_credit_cards.account_id must reference an accounts row with type = card for the same user';
  end if;

  return new;
end;
$$;

drop trigger if exists enforce_user_credit_cards_account_type on public.user_credit_cards;
create trigger enforce_user_credit_cards_account_type
before insert or update on public.user_credit_cards
for each row execute function public.enforce_credit_card_account_type();

-- ==========================================
-- 4. Ingestion pipeline & projection
-- ==========================================
create table if not exists public.raw_emails (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  oauth_connection_id uuid,
  source_id text unique not null,
  internal_date timestamptz not null,
  clean_text text not null,
  status text not null default 'PENDING_EXTRACTION'
    check (status in ('PENDING_EXTRACTION', 'PROCESSED', 'IGNORED', 'UNRECOGNIZED', 'FAILED')),
  created_at timestamptz not null default now(),
  constraint uq_raw_emails_id_user unique (id, user_id),
  constraint fk_raw_emails_oauth_connection_user
    foreign key (oauth_connection_id, user_id)
    references public.oauth_connections(id, user_id)
    on delete set null (oauth_connection_id)
);

create table if not exists public.financial_events (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  raw_email_id uuid,
  extraction_index integer not null default 0,
  direction text not null check (direction in ('debit', 'credit')),
  amount_in_paise bigint not null check (amount_in_paise > 0),
  currency text not null default 'INR',
  txn_timestamp timestamptz not null,
  payment_method text not null
    check (payment_method in ('upi', 'credit_card', 'debit_card', 'netbanking', 'cash', 'unknown')),
  instrument_id text,
  counterparty_raw text,
  search_key text,
  status text not null default 'ACTIVE' check (status in ('ACTIVE', 'REVERSED')),
  created_at timestamptz not null default now(),
  constraint uq_financial_events_id_user unique (id, user_id),
  unique (raw_email_id, extraction_index),
  constraint fk_financial_events_raw_email_user
    foreign key (raw_email_id, user_id)
    references public.raw_emails(id, user_id)
    on delete cascade
);

create table if not exists public.transactions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  financial_event_id uuid not null unique,

  account_id uuid,
  category_id uuid references public.categories(id),
  merchant_id uuid references public.global_merchants(id),
  credit_card_id uuid,

  amount_in_paise bigint not null check (amount_in_paise > 0),
  type text not null check (type in ('income', 'expense', 'transfer')),
  txn_date timestamptz not null,
  user_note text,

  status text not null default 'VERIFIED' check (status in ('VERIFIED', 'NEEDS_REVIEW')),
  classification_source text not null default 'USER'
    check (classification_source in ('USER', 'SYSTEM_DEFAULT', 'HEURISTIC', 'AI')),
  ai_confidence_score numeric(3, 2),

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint transactions_ai_confidence_score_chk
    check (ai_confidence_score is null or ai_confidence_score between 0 and 1),

  constraint fk_transactions_financial_event_user
    foreign key (financial_event_id, user_id)
    references public.financial_events(id, user_id)
    on delete cascade,
  foreign key (account_id, user_id)
    references public.accounts(id, user_id),
  foreign key (credit_card_id, user_id)
    references public.user_credit_cards(id, user_id)
    on delete set null
);

create or replace function public.enforce_transaction_credit_card_consistency()
returns trigger
language plpgsql
as $$
declare
  v_card_account_id uuid;
begin
  if new.credit_card_id is null then
    return new;
  end if;

  select ucc.account_id
    into v_card_account_id
  from public.user_credit_cards ucc
  where ucc.id = new.credit_card_id
    and ucc.user_id = new.user_id
  limit 1;

  if v_card_account_id is null then
    raise exception using
      errcode = '23503',
      message = 'transactions.credit_card_id must reference a card owned by the same user';
  end if;

  if new.account_id is null then
    new.account_id = v_card_account_id;
    return new;
  end if;

  if new.account_id <> v_card_account_id then
    raise exception using
      errcode = '23514',
      message = 'transactions.account_id must match user_credit_cards.account_id when credit_card_id is set';
  end if;

  return new;
end;
$$;

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

drop trigger if exists enforce_transactions_credit_card_consistency on public.transactions;
create trigger enforce_transactions_credit_card_consistency
before insert or update on public.transactions
for each row execute function public.enforce_transaction_credit_card_consistency();

drop trigger if exists enforce_transactions_category_scope on public.transactions;
create trigger enforce_transactions_category_scope
before insert or update on public.transactions
for each row execute function public.enforce_transactions_category_scope();

drop trigger if exists update_transactions_modtime on public.transactions;
create trigger update_transactions_modtime
before update on public.transactions
for each row execute function public.update_updated_at_column();

create or replace view public.v_credit_card_transactions as
select
  t.id as transaction_id,
  t.user_id,
  t.txn_date,
  t.amount_in_paise,
  t.type,
  t.status,
  t.account_id,
  t.category_id,
  c.name as category_name,
  c.icon as category_icon,
  t.merchant_id,
  gm.canonical_name as merchant_name,
  fe.status as financial_event_status,
  t.classification_source,
  t.user_note,
  t.ai_confidence_score,
  ucc.id as credit_card_id,
  ucc.card_label,
  ucc.issuer_name,
  ucc.network,
  ucc.first4,
  ucc.last4
from public.transactions t
join public.user_credit_cards ucc
  on ucc.id = t.credit_card_id
 and ucc.user_id = t.user_id
join public.financial_events fe
  on fe.id = t.financial_event_id
 and fe.user_id = t.user_id
 and fe.status = 'ACTIVE'
left join public.categories c
  on c.id = t.category_id
left join public.global_merchants gm
  on gm.id = t.merchant_id;

-- ==========================================
-- 5. Performance indexes
-- ==========================================
create index if not exists idx_oauth_sync
  on public.oauth_connections(sync_status, last_sync_timestamp);

create index if not exists idx_raw_email_status
  on public.raw_emails(status)
  where status = 'PENDING_EXTRACTION';

drop index if exists public.idx_financial_events_reconciler;
create index idx_financial_events_reconciler
  on public.financial_events(user_id, amount_in_paise, direction, status, txn_timestamp);

create index if not exists idx_transactions_dashboard
  on public.transactions(user_id, txn_date desc);

create index if not exists idx_transactions_needs_review
  on public.transactions(status)
  where status = 'NEEDS_REVIEW';

create index if not exists idx_user_credit_cards_lookup
  on public.user_credit_cards(user_id, last4, first4)
  where is_active = true;

create index if not exists idx_transactions_credit_card
  on public.transactions(user_id, credit_card_id, txn_date desc)
  where credit_card_id is not null;

-- ==========================================
-- 6. Alerting for stale PENDING_EXTRACTION
-- ==========================================
create materialized view if not exists public.mv_stale_pending_extraction as
select
  re.user_id,
  count(*)::bigint as stale_count,
  min(re.created_at) as oldest_created_at,
  max(re.created_at) as newest_created_at,
  now() as snapshot_at
from public.raw_emails re
where re.status = 'PENDING_EXTRACTION'
  and re.created_at < now() - interval '30 minutes'
group by re.user_id
with data;

create unique index if not exists idx_mv_stale_pending_extraction_user
  on public.mv_stale_pending_extraction(user_id);

create or replace function public.refresh_mv_stale_pending_extraction()
returns void
language sql
as $$
  refresh materialized view public.mv_stale_pending_extraction;
$$;

do $$
declare
  v_job_id bigint;
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    for v_job_id in
      select jobid
      from cron.job
      where jobname = 'refresh-stale-mv'
        and username = current_user
    loop
      perform cron.unschedule(v_job_id);
    end loop;

    perform cron.schedule(
      'refresh-stale-mv',
      '*/10 * * * *',
      'select public.refresh_mv_stale_pending_extraction();'
    );
  else
    raise notice 'pg_cron is not installed; skipping refresh-stale-mv schedule';
  end if;
end;
$$;

-- ==========================================
-- 7. Card resolver helper
-- ==========================================
create or replace function public.resolve_user_credit_card(
  p_user_id uuid,
  p_first4 text,
  p_last4 text
)
returns table (
  credit_card_id uuid,
  account_id uuid,
  card_label text
)
language sql
stable
as $$
  with candidates as (
    select
      ucc.id as credit_card_id,
      ucc.account_id,
      ucc.card_label,
      ucc.first4,
      ucc.updated_at
    from public.user_credit_cards ucc
    where ucc.user_id = p_user_id
      and ucc.is_active = true
      and ucc.last4 = p_last4
  )
  select
    c.credit_card_id,
    c.account_id,
    c.card_label
  from candidates c
  where
    (p_first4 is not null and c.first4 = p_first4)
    or (p_first4 is null and (select count(*) from candidates) = 1)
  order by c.updated_at desc
  limit 1;
$$;
