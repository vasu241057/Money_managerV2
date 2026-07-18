-- first4 becomes optional on user_credit_cards.
--
-- The engine matches cards by last4 (+ issuer tie-break) only; first4 is
-- display + uniqueness. Bank emails rarely show the first digits, so
-- requiring them forced users to have the physical card in hand to
-- register. first4 is public BIN data (no privacy value), but there is no
-- reason to demand it.
--
-- The plain unique constraint treats NULLs as distinct, so it is replaced
-- by a coalesce-based unique index to keep duplicate registrations blocked.

alter table public.user_credit_cards
  alter column first4 drop not null;

alter table public.user_credit_cards
  drop constraint if exists user_credit_cards_user_id_first4_last4_key;

create unique index if not exists user_credit_cards_user_first4_last4_uidx
  on public.user_credit_cards (user_id, coalesce(first4, ''), last4);
