-- Milestone 8 dispatcher scan index
-- Supports Phase 1 pagination over Google connections grouped by user.

set search_path = public;

create index if not exists idx_oauth_dispatcher_scan
	on public.oauth_connections(provider, user_id, sync_status, last_sync_timestamp);
