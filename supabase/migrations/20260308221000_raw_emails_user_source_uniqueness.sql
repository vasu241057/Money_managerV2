-- Scope raw email idempotency to the user mailbox.
-- This avoids cross-user collisions on provider-specific message IDs.

set search_path = public;

alter table public.raw_emails
  drop constraint if exists raw_emails_source_id_key;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'uq_raw_emails_user_source'
      and conrelid = 'public.raw_emails'::regclass
  ) then
    alter table public.raw_emails
      add constraint uq_raw_emails_user_source unique (user_id, source_id);
  end if;
end;
$$;
