-- Card statements: one row per credit-card billing cycle, harvested from
-- statement emails (source EMAIL) or entered/corrected by the user (source
-- USER — never overwritten by email harvests). credit_card_id is nullable:
-- a statement whose last4 matches no registered card parks by last4 and
-- links when the card is registered (same self-heal as transaction relink).
--
-- user_credit_cards gains billing_cycle_day (derived automatically from a
-- captured statement period; manual entry only for banks that never reveal
-- it) and pdf_password (for the statement-PDF fallback; encrypted with the
-- same AES-GCM scheme as OAuth tokens before storage — never stored plain).

alter table public.user_credit_cards
  add column if not exists billing_cycle_day smallint
    check (billing_cycle_day between 1 and 31),
  add column if not exists pdf_password text;

create table if not exists public.card_statements (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  credit_card_id uuid references public.user_credit_cards(id) on delete set null,
  card_last4 text check (card_last4 ~ '^[0-9]{4}$'),
  period_start date,
  period_end date,
  statement_date date,
  total_due_in_paise bigint check (total_due_in_paise >= 0),
  min_due_in_paise bigint check (min_due_in_paise >= 0),
  due_date date,
  source text not null default 'EMAIL' check (source in ('EMAIL', 'USER')),
  source_raw_email_id uuid references public.raw_emails(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Re-processing the same email updates its statement row instead of
-- duplicating it.
create unique index if not exists card_statements_source_email_uidx
  on public.card_statements (source_raw_email_id)
  where source_raw_email_id is not null;

-- One row per card per cycle: a re-sent statement email for the same cycle
-- (same card digits + due date) is a duplicate, not a new statement.
create unique index if not exists card_statements_cycle_uidx
  on public.card_statements (user_id, card_last4, due_date)
  where card_last4 is not null and due_date is not null;

create index if not exists card_statements_user_card_idx
  on public.card_statements (user_id, credit_card_id);
