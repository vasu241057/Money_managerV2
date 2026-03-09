# Money Manager Frontend

## Milestone 5 Data Layer

The frontend now supports two data sources:

- `local` (default): legacy localStorage hooks
- `remote`: backend API + React Query cache

### Feature flag

Set in `.env` (or `.env.local`):

```bash
VITE_USE_REMOTE_DATA=true
VITE_API_BASE_URL=/api
VITE_DEV_API_PROXY_TARGET=http://127.0.0.1:8787
```

Runtime override (for emergency fallback):

- `localStorage.setItem('money-manager:data-source', 'local')`
- `localStorage.setItem('money-manager:data-source', 'remote')`

Reload after changing the override.

### Auth contract

API calls send Clerk bearer tokens:

- `Authorization: Bearer <jwt>`
- optional `x-user-email` from `window.__MONEY_MANAGER_USER_EMAIL__`

Default token lookup path:

1. `window.__MONEY_MANAGER_CLERK_JWT__` (manual/dev injection)
2. `window.Clerk.session.getToken()` (if Clerk JS is mounted)

You can override token resolution at runtime with:

```ts
import { setAuthTokenProvider } from './src/lib/auth-token';

setAuthTokenProvider(async () => {
  // return Clerk session token
  return null;
});
```

### Dev proxy

Vite proxies these backend routes to `VITE_DEV_API_PROXY_TARGET`:

- `/api/accounts`
- `/api/categories`
- `/api/transactions`
- `/api/oauth`

This avoids conflicts with existing `/api/analyze` usage.

## Milestone 6 One-time localStorage bridge

When `remote` mode is enabled and a valid Clerk token is available, the frontend now runs a one-time migration for that authenticated user:

- Reads legacy localStorage keys: `accounts`, `categories`, `transactions`
- Migrates relations in order: accounts -> categories/sub-categories -> transactions
- Converts rupees to paise before API writes
- Writes deterministic legacy transaction markers into `financial_events.instrument_id` (prefix: `legacy-local:v1:`) so retry runs can skip already-migrated rows
- Stores per-user completion marker in localStorage (`money-manager:legacy-migration:v1:<issuer>:<subject>`)

The completion marker is written only after a successful run and prevents duplicate migration on subsequent app loads for the same Clerk identity.

## Milestone 7 Gmail OAuth UX

- Header now shows Gmail sync status badge:
  - `ACTIVE`
  - `DORMANT`
  - `AUTH_REVOKED`
  - `ERROR_PAUSED`
  - fallback `DISCONNECTED` when no connection exists
- Connect flow:
  - `Connect Gmail` calls backend `POST /oauth/google/start`
  - Browser redirects to Google consent screen
  - Google callback returns to `/oauth/google/callback`
  - Frontend completes handshake via backend `POST /oauth/google/callback`
- Disconnect flow:
  - `Disconnect Gmail` calls backend `DELETE /oauth/google/connection`
  - Backend clears tokens and marks status `AUTH_REVOKED`
