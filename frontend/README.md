# Money Manager Frontend

## Milestone 5 Data Layer

The frontend now supports two data sources:

- `remote` (default): backend API + React Query cache
- `local` fallback: legacy localStorage hooks

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

This avoids conflicts with existing `/api/analyze` usage.
