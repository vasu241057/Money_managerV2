-- Money Manager deterministic seed data (Milestone 1)
-- Idempotent by design (ON CONFLICT updates)

set search_path = public;

-- ==========================================
-- 1) System categories (global)
-- ==========================================
with top_seed(seed_id, name, type, icon) as (
  values
    ('00000000-0000-0000-0000-000000000101'::uuid, 'Income', 'income', 'ArrowDownCircle'),
    ('00000000-0000-0000-0000-000000000102'::uuid, 'Expense', 'expense', 'ArrowUpCircle'),
    ('00000000-0000-0000-0000-000000000103'::uuid, 'Transfer', 'transfer', 'Repeat')
)
insert into public.categories (id, user_id, parent_id, name, type, icon, is_system)
select
  s.seed_id,
  null,
  null,
  s.name,
  s.type,
  s.icon,
  true
from top_seed s
where not exists (
  select 1
  from public.categories c
  where c.user_id is null
    and c.is_system = true
    and c.parent_id is null
    and c.name = s.name
    and c.type = s.type
);

with top_seed(name, type, icon) as (
  values
    ('Income', 'income', 'ArrowDownCircle'),
    ('Expense', 'expense', 'ArrowUpCircle'),
    ('Transfer', 'transfer', 'Repeat')
)
update public.categories c
set
  icon = s.icon,
  is_system = true
from top_seed s
where c.user_id is null
  and c.parent_id is null
  and c.name = s.name
  and c.type = s.type;

with child_seed(seed_id, parent_name, parent_type, name, type, icon) as (
  values
    ('00000000-0000-0000-0000-000000000104'::uuid, 'Expense', 'expense', 'Food', 'expense', 'Utensils'),
    ('00000000-0000-0000-0000-000000000105'::uuid, 'Expense', 'expense', 'Transport', 'expense', 'Car'),
    ('00000000-0000-0000-0000-000000000106'::uuid, 'Expense', 'expense', 'Shopping', 'expense', 'ShoppingBag'),
    ('00000000-0000-0000-0000-000000000107'::uuid, 'Expense', 'expense', 'Bills', 'expense', 'Receipt'),
    ('00000000-0000-0000-0000-000000000108'::uuid, 'Expense', 'expense', 'Health', 'expense', 'HeartPulse'),
    ('00000000-0000-0000-0000-000000000109'::uuid, 'Expense', 'expense', 'Entertainment', 'expense', 'Film'),
    ('00000000-0000-0000-0000-000000000110'::uuid, 'Expense', 'expense', 'Credit Card Spend', 'expense', 'CreditCard'),
    ('00000000-0000-0000-0000-000000000111'::uuid, 'Income', 'income', 'Salary', 'income', 'Briefcase'),
    ('00000000-0000-0000-0000-000000000112'::uuid, 'Income', 'income', 'Refund', 'income', 'Undo2'),
    ('00000000-0000-0000-0000-000000000113'::uuid, 'Income', 'income', 'Cashback', 'income', 'Gift'),
    ('00000000-0000-0000-0000-000000000114'::uuid, 'Transfer', 'transfer', 'Card Bill Payment', 'transfer', 'CreditCard'),
    ('00000000-0000-0000-0000-000000000115'::uuid, 'Transfer', 'transfer', 'Wallet Transfer', 'transfer', 'Wallet'),
    ('00000000-0000-0000-0000-000000000116'::uuid, 'Transfer', 'transfer', 'Broker Transfer', 'transfer', 'LineChart')
)
insert into public.categories (id, user_id, parent_id, name, type, icon, is_system)
select
  s.seed_id,
  null,
  p.id,
  s.name,
  s.type,
  s.icon,
  true
from child_seed s
join public.categories p
  on p.user_id is null
 and p.is_system = true
 and p.parent_id is null
 and p.name = s.parent_name
 and p.type = s.parent_type
where not exists (
  select 1
  from public.categories c
  where c.user_id is null
    and c.is_system = true
    and c.parent_id = p.id
    and c.name = s.name
    and c.type = s.type
);

with child_seed(parent_name, parent_type, name, type, icon) as (
  values
    ('Expense', 'expense', 'Food', 'expense', 'Utensils'),
    ('Expense', 'expense', 'Transport', 'expense', 'Car'),
    ('Expense', 'expense', 'Shopping', 'expense', 'ShoppingBag'),
    ('Expense', 'expense', 'Bills', 'expense', 'Receipt'),
    ('Expense', 'expense', 'Health', 'expense', 'HeartPulse'),
    ('Expense', 'expense', 'Entertainment', 'expense', 'Film'),
    ('Expense', 'expense', 'Credit Card Spend', 'expense', 'CreditCard'),
    ('Income', 'income', 'Salary', 'income', 'Briefcase'),
    ('Income', 'income', 'Refund', 'income', 'Undo2'),
    ('Income', 'income', 'Cashback', 'income', 'Gift'),
    ('Transfer', 'transfer', 'Card Bill Payment', 'transfer', 'CreditCard'),
    ('Transfer', 'transfer', 'Wallet Transfer', 'transfer', 'Wallet'),
    ('Transfer', 'transfer', 'Broker Transfer', 'transfer', 'LineChart')
)
update public.categories c
set
  icon = s.icon,
  is_system = true
from child_seed s
join public.categories p
  on p.user_id is null
 and p.is_system = true
 and p.parent_id is null
 and p.name = s.parent_name
 and p.type = s.parent_type
where c.user_id is null
  and c.parent_id = p.id
  and c.name = s.name
  and c.type = s.type;

-- ==========================================
-- 2) Global merchants (initial identity graph)
-- ==========================================
with merchant_seed(
  canonical_name,
  default_category_seed_id,
  fallback_parent_name,
  fallback_parent_type,
  fallback_category_name,
  fallback_category_type,
  type,
  is_context_required
) as (
  values
    ('CRED', '00000000-0000-0000-0000-000000000114'::uuid, 'Transfer', 'transfer', 'Card Bill Payment', 'transfer', 'TRANSFER_INSTITUTION', false),
    ('HDFC CREDIT CARD', '00000000-0000-0000-0000-000000000114'::uuid, 'Transfer', 'transfer', 'Card Bill Payment', 'transfer', 'TRANSFER_INSTITUTION', false),
    ('ICICI CREDIT CARD', '00000000-0000-0000-0000-000000000114'::uuid, 'Transfer', 'transfer', 'Card Bill Payment', 'transfer', 'TRANSFER_INSTITUTION', false),
    ('KOTAK CREDIT CARD', '00000000-0000-0000-0000-000000000114'::uuid, 'Transfer', 'transfer', 'Card Bill Payment', 'transfer', 'TRANSFER_INSTITUTION', false),
    ('PAYTM WALLET', '00000000-0000-0000-0000-000000000115'::uuid, 'Transfer', 'transfer', 'Wallet Transfer', 'transfer', 'TRANSFER_INSTITUTION', false),
    ('ZERODHA', '00000000-0000-0000-0000-000000000116'::uuid, 'Transfer', 'transfer', 'Broker Transfer', 'transfer', 'TRANSFER_INSTITUTION', false),
    ('BHARATPE', null::uuid, null, null, null, null, 'AGGREGATOR', true),
    ('RAZORPAY', null::uuid, null, null, null, null, 'AGGREGATOR', true),
    ('PAYU', null::uuid, null, null, null, null, 'AGGREGATOR', true),
    ('CCAVENUE', null::uuid, null, null, null, null, 'AGGREGATOR', true)
)
insert into public.global_merchants (canonical_name, default_category_id, type, is_context_required)
select
  s.canonical_name,
  coalesce(seed_category.id, fallback_category.id) as default_category_id,
  s.type,
  s.is_context_required
from merchant_seed s
left join public.categories seed_category
  on seed_category.id = s.default_category_seed_id
left join public.categories fallback_parent
  on fallback_parent.user_id is null
 and fallback_parent.is_system = true
 and fallback_parent.parent_id is null
 and fallback_parent.name = s.fallback_parent_name
 and fallback_parent.type = s.fallback_parent_type
left join public.categories fallback_category
  on fallback_category.user_id is null
 and fallback_category.is_system = true
 and fallback_category.parent_id = fallback_parent.id
 and fallback_category.name = s.fallback_category_name
 and fallback_category.type = s.fallback_category_type
on conflict (canonical_name) do update
set
  default_category_id = excluded.default_category_id,
  type = excluded.type,
  is_context_required = excluded.is_context_required;

-- ==========================================
-- 3) Global merchant aliases (initial dictionary)
-- ==========================================
with alias_seed(search_key, canonical_name) as (
  values
    ('CRED', 'CRED'),
    ('CREDITCARDPAYMENT', 'CRED'),

    ('HDFCCREDITCARD', 'HDFC CREDIT CARD'),
    ('HDFCCARD', 'HDFC CREDIT CARD'),

    ('ICICICREDITCARD', 'ICICI CREDIT CARD'),
    ('ICICICARD', 'ICICI CREDIT CARD'),

    ('KOTAKCREDITCARD', 'KOTAK CREDIT CARD'),
    ('KOTAKCARD', 'KOTAK CREDIT CARD'),

    ('PAYTMWALLET', 'PAYTM WALLET'),
    ('ZERODHA', 'ZERODHA'),

    ('BHARATPE', 'BHARATPE'),
    ('RAZORPAY', 'RAZORPAY'),
    ('PAYU', 'PAYU'),
    ('CCAVENUE', 'CCAVENUE')
)
insert into public.global_merchant_aliases (search_key, merchant_id)
select
  a.search_key,
  gm.id
from alias_seed a
join public.global_merchants gm
  on gm.canonical_name = a.canonical_name
on conflict (search_key) do update
set merchant_id = excluded.merchant_id;
