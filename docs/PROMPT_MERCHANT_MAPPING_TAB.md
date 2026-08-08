# Implementation task: "Merchant mapping" tab in the AI operations admin screen

## Before you write anything

Read these first and understand how they fit together. Do not start editing until you have.

**Backend**
- `backend/src/workers/merchant-mapper.ts` — the daily mapping job and the dry-run preview. Pay attention to `runMerchantMappingJob`, `previewMerchantMappings`, `searchMerchantWebContext`, and the private `incrementUsageCounters`.
- `backend/src/services/ai-usage.service.ts` — especially `getAiUsageSummary`, `getAiUsageDashboard`, `isAiUsageAdmin`, and `fetchOpenRouterKeyUsage`. That last one is your template for reading live credit state from a provider's API.
- `backend/src/routes/ai-usage.ts` — the admin gate and route shape.
- `backend/src/workers/scheduled.worker.ts` and `backend/src/workers/queue.worker.ts` — how the daily job is triggered and why it is currently skipped.
- `supabase/migrations/20260720090000_mapper_usage_and_cooldown.sql` — the `usage_counters` and `merchant_mapping_attempts` tables.

**Frontend**
- `frontend/src/components/screens/AiUsageDashboard.tsx` — the existing two-tab screen.
- `frontend/src/hooks/useAiUsage.ts` and `frontend/src/lib/api-client.ts`.
- `shared/types.ts` — the `AiUsage*` and `AiProcessing*` types.

Also skim `frontend/src/components/screens/MerchantSettings.tsx` so the new tab's vocabulary matches what the user already sees there.

## Why this exists

The merchant mapper turns raw bank payee strings into real brands — `BUNDL TECHNOLOGIES` → Swiggy, `ETERNAL LIMITED` → Zomato. It does this by running a Tavily web search per unknown payee and feeding the snippets to a model. It costs Tavily credits and AI tokens on every run.

Right now it is a black box. It has run exactly once (19 July 2026) and has been disabled since. The owner will not turn it back on for nightly use until he can see, each day, what it did, what it cost, and whether its answers were right. This tab is that visibility.

## What to build

A third tab, **Merchant mapping**, alongside the existing Spend and Processing tabs on the admin AI operations screen. It must answer:

1. **Did it run, and what did it do?** Per day: Tavily searches performed, AI calls made, merchants mapped, keys skipped as unsure.
2. **What did it map?** The actual mappings — raw payee string → canonical name, merchant type, and the default category assigned. This is the part the owner will use to judge whether the search results are trustworthy, so make it browsable and readable, not a raw dump. Being able to open a day and see its mappings is the core interaction.
3. **What is it costing, and how much runway is left?** Tavily exposes `GET https://api.tavily.com/usage`, which returns per-key and account usage plus limits. `TAVILY_API_KEY` holds **several comma-separated keys** that the mapper rotates through when one is exhausted. Show each key's own remaining/total credits and make it obvious which key is currently the live one, so an exhausted key is visible before it silently degrades a run.

Do not use the AI spend figures for Tavily. They are separate budgets from separate providers.

## Constraints

- **Follow the existing shapes.** The tab should feel like a sibling of the Processing tab, not a new design. Extend the existing admin dashboard response rather than inventing a parallel endpoint, unless you find a concrete reason not to — if so, say why in your summary.
- **Admin only**, gated exactly the way the existing AI usage routes are gated. Non-admins must see the same not-found behaviour, not a permission error.
- **Never expose key material.** Show credit balances and a stable non-secret label per key (position, or a short masked fragment). The full key must never reach the client.
- **Live provider calls must degrade, not break.** If Tavily's usage endpoint is unreachable, rate limited, or a key has been revoked, the tab still renders everything that came from the database. Follow how the OpenRouter equivalent handles failure.
- **Do not change the mapper's behaviour.** No changes to how it selects candidates, searches, prompts, or persists. This task is read-only observability over work that already happens. The one exception is if you must record additional data going forward — see below.
- Keep `shared/types.ts` the single source of truth for the response shape, as the rest of the codebase does.

## The historical-data problem — decide this deliberately

The owner wants the 19 July run visible, not just future runs.

What already exists for that day: `usage_counters` holds the four aggregate counts, and the mappings themselves are recoverable because `global_merchant_aliases` rows carry `created_at` and their `global_merchants` rows carry `source = 'AI'`. So a per-day view of *what was mapped* can be derived from data you already have, with no new table.

What does not exist historically: which key served which search, per-key credit consumption, and the keys the model was unsure about (only their count survives).

Choose one and justify it briefly in your summary:
- Derive everything possible from existing tables, accepting that the pre-existing run shows aggregates plus its mappings but no per-key detail.
- Add a table that records future runs properly, and backfill what is derivable for the historical one.

Prefer the option that gets the owner a useful view of 19 July without inventing data that was never recorded. Do not fabricate per-key history for a run that predates key tracking — show it as unavailable.

## Edge cases that will bite you

- **The tab switcher is currently a two-way binary.** The swipe handler resolves direction to one of exactly two tabs, and the tab strip is a fixed two-column grid over a hardcoded pair. Both need to become index-based or a third tab will render but be unreachable by swipe and misaligned in the strip. Check the keyboard and ARIA behaviour on the tablist too.
- **Counters mix real runs with dry runs.** `previewMerchantMappings` is a dry run that persists no mappings but *does* increment `tavily_searches` and `merchant_ai_calls`, because those calls really happened. A day can therefore show searches and AI calls with zero merchants mapped. Make that legible rather than looking like a failure — the owner will be running previews deliberately.
- **The mapper is currently disabled.** It is gated behind `MERCHANT_MAPPING_ENABLED`, and the nightly job logs a skip when unset. The tab must make "not running" plainly visible rather than showing zeros that look like a working-but-idle system.
- **Days with no run at all** should be distinguishable from days where it ran and found nothing to do.
- **Merchant type matters.** Only `MERCHANT` rows get a default category; aggregators and person-to-person payees deliberately get none. Absent category is correct there, not missing data.
- **Timezone.** The rest of this app buckets days in Asia/Kolkata while `usage_counters` keys on the database's `current_date`. Be consistent with how the existing dashboard handles months, and note any discrepancy you cannot resolve rather than silently papering over it.
- One key detail worth surfacing: a basic Tavily search costs 1 credit and the free tier is 1,000 credits per key per month, so a backlog run of a couple hundred payees is survivable but not free.

## Definition of done

- The new tab renders for an admin, with the 19 July run visible, and is reachable by both tap and swipe.
- Tavily per-key credit state renders, and still renders sensibly when the usage endpoint fails or a key is dead.
- Mappings for a given day can be opened and read.
- `npm run build --workspace=frontend`, `npm test --workspace=backend`, `npm test --workspace=frontend` all pass. Lint has three pre-existing errors in `App.tsx` and `ReviewQueue.tsx` — do not fix them, but do not add more.
- Add backend tests in the style of the existing ones for any new service function, including the Tavily-unavailable path.
- No secrets in responses, logs, or test fixtures.

In your final summary, state which historical-data option you chose and why, anything you found that contradicts this brief, and anything you deliberately left out.
