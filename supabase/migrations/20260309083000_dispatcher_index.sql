-- Milestone 8 dispatcher scan index
-- Supports Phase 1 pagination over Google ACTIVE/DORMANT connections grouped by user.

set search_path = public;

create index if not exists idx_oauth_dispatcher_scan
	on public.oauth_connections(provider, sync_status, user_id, last_sync_timestamp);
