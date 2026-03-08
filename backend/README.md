# Backend Worker Infra (Milestone 3)

## Wired bindings

- Cron trigger: `*/10 * * * *`
- Queue producer bindings:
  - `EMAIL_SYNC_QUEUE` -> `money-manager-email-sync`
  - `AI_CLASSIFICATION_QUEUE` -> `money-manager-ai-classification`
- Queue consumer bindings:
  - `money-manager-email-sync` (`max_batch_size=10`, DLQ `money-manager-email-sync-dlq`)
  - `money-manager-ai-classification` (`max_batch_size=10`, DLQ `money-manager-ai-classification-dlq`)

## Secrets

Set secrets per environment (do not store in `wrangler.jsonc`):

```bash
cd backend
npx wrangler secret put SUPABASE_POOLER_URL
npx wrangler secret put GOOGLE_CLIENT_ID
npx wrangler secret put GOOGLE_CLIENT_SECRET
npx wrangler secret put GOOGLE_OAUTH_REDIRECT_URI
npx wrangler secret put OPENROUTER_API_KEY
```

For local development, create `backend/.dev.vars` from `backend/.dev.vars.example`.

## Local invocation checks

```bash
cd backend
TMPDIR=/tmp WRANGLER_LOG_PATH=/tmp/wrangler-logs npx wrangler dev --test-scheduled
```

In another terminal, trigger cron:

```bash
curl "http://127.0.0.1:8787/__scheduled?cron=*/10+*+*+*+*"
```

You should see scheduled handler logs, then queue handler logs for `EMAIL_SYNC_DISPATCH`.
