alter table public.oauth_connections
  add column if not exists email_sync_lease_id uuid,
  add column if not exists email_sync_lease_expires_at timestamptz;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_oauth_connections_email_sync_lease_pair'
      and conrelid = 'public.oauth_connections'::regclass
  ) then
    alter table public.oauth_connections
      add constraint chk_oauth_connections_email_sync_lease_pair
      check (
        (email_sync_lease_id is null and email_sync_lease_expires_at is null)
        or
        (email_sync_lease_id is not null and email_sync_lease_expires_at is not null)
      );
  end if;
end
$$;

create index if not exists idx_oauth_connections_email_sync_lease
  on public.oauth_connections (email_sync_lease_expires_at)
  where email_sync_lease_expires_at is not null;

comment on column public.oauth_connections.email_sync_lease_id is
  'Stable queue-chain id currently owning this Gmail connection sync.';

comment on column public.oauth_connections.email_sync_lease_expires_at is
  'Crash-recovery expiry for the Gmail connection sync lease.';
