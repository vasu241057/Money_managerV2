# Future Fixes & Deferred Work

> Decisions and known issues explicitly deferred, with why. Ordered roughly
> by when they'll matter. Last updated: August 4, 2026.

## Multi-tenant checklist (before any second user)

1. **Majority-vote category promotion** — when N distinct users' rules map
   the same `search_key` to the same category, promote it to the global
   merchant's `default_category_id` (one aggregate over `user_merchant_rules`;
   privacy-fine, only counts leave the user's scope). Meaningless single-user.
2. **P2P names in the shared global table** — the merchant mapper writes
   person names (type `P2P`) into `global_merchants`, shared across users.
   One user's contact must not become another user's autocomplete. Scope
   P2P merchants per-user or stop persisting them globally.
3. **PII scrub of template skeletons** — alpha-only personal text (account
   holder's name in bank boilerplate) bakes into `email_templates.skeleton`.
4. **Provider training opt-out** — no per-request opt-out exists on OpenCode
   Go; must be configured at account level and verified before launch.
5. **Root-assignability guard** — the engine no longer assigns the
   Income/Expense/Transfer roots, but the transactions API still accepts
   them; add validation once the new FE ships.

## Implemented decisions

**Internal-transfer confirmation (Phase 2, August 4):** `transfer` means only
movement inside the user's own financial ecosystem, never generic P2P.
`TRANSFER_INSTITUTION` identity is context, not proof. Explicit transfer
category suggestions always require review; users can switch a detected debit
between Expense/Transfer and a detected credit between Income/Transfer. The
ledger renders `↔` only from `transactions.type = 'transfer'`. New writes are
guarded in the API and database against direction/category mismatches.
Historical rows are deliberately unchanged.

## Product features agreed, not yet built

6. **Admin UI + endpoints** — emails processed, per-user stats, sender_rules
   / global_merchants browsers, queue health. Spec the read-only endpoints
   after the FE redesign settles.
7. **AI spending-analysis** — removed with the triage worker; being redesigned
   separately (Vasu).
8. **Retroactive category healing** — when a merchant gains a default
   category, old fallback-categorized NEEDS_REVIEW transactions could be
   re-categorized (the same relink pattern; skipped for now to avoid fighting
   user edits).
9. **DENY confirmation counter** — before making a DENY rule effective,
   require the same verdict N times. Judged overkill: the 30-day re-ask
   covers correction.

## Engine backlog (single-user acceptable)

10. **Confirmed-empty IGNORE counter** — emails with monetary words can never
    learn IGNORE templates (by design), so recurring "OTP for payment of
    Rs.X" formats cost an AI call every time. After N identical well-formed
    empty answers, allow the learn.
11. **Lakh/crore amounts** — "Rs 1.5 lakh" parses as ₹1.50 (silent 100,000×
    under-read). Sniff the word after the amount token; multiply locally or
    force NEEDS_REVIEW.
12. **Foreign currency** — "USD 12.99" persists as ₹12.99. Currency sniff →
    NEEDS_REVIEW + stored currency code.
13. **Subject-in-extraction** — `clean_text` is body-only; senders that put
    the amount in the subject extract nothing. Prepend subject before
    masking (templates adapt transparently).
14. **Future-dated alerts** — autopay notices ("will be debited on …")
    extract as real future transactions; date > internalDate + 2 days should
    force NEEDS_REVIEW.
15. **AM/PM prefix false-match** — the time parser can read "12:45 amount"
    as 12:45 AM (needs a trailing-boundary lookahead). Accepted as rare.

## Hygiene

16. **Purge pre-gate bodies** — superseded (Sep 18, 2026) by the nightly
    `raw_email_body_purge` job from the multi-tenant Task 5: its first run
    is this purge, with `body_purged_at` stamped so purged rows are never
    mistaken for sender-gated ones. Enable it in the admin dashboard; do not
    run the old manual `update`.
17. **Card API polish** — `is_active` on POST silently coerces non-booleans
    to false (PATCH 400s correctly); card create + relink are not atomic;
    no end-to-end test that a `credit_card` extraction persists its card link.
18. **Docs drift** — CONTEXT.md §5 says PUT for accounts/categories/
    transactions; actual routes are PATCH. (Fixed July 18 for accounts +
    transactions.)

## Frontend decisions of record (July 2026 audit)

19. **Client-side filtering/aggregation accepted at current scale** — the FE
    fetches all transactions (100/page loop) and filters, groups, and sums in
    the browser; the backend's list filters (`from`/`to`/`status`/`type`/
    `account_id`/`category_id`/`credit_card_id`) go unused. Wire them when the
    dataset grows past a few thousand rows.
20. **No initial balance, by decision** — account balances are derived purely
    from transactions (income − expense per account); the FE neither asks for
    nor displays `initial_balance_in_paise`. The column and API support remain
    (the legacy-localStorage migration still writes it); drop them only if the
    migration is retired.
21. **Transfers are balance-neutral** — a transaction has a single
    `account_id`, so a transfer cannot debit one account and credit another;
    both accounts simply don't move. Proper transfer pairs need schema support
    (from/to account) if per-account balances ever must be exact.
22. **Unattributed transactions and account math** — transactions with no
    account count in the Home hero (global income − expense) but in no
    per-account balance, so the hero can differ from the accounts screen's
    net. Accepted; revisit only if it confuses.
23. **Transfer categories unmanageable in Settings, by design** — the
    Settings category manager offers expense/income tabs only; the transfer
    taxonomy is deliberately minimal (Self Transfer / Other Transfer, both
    system rows).

## Admin dashboard (planned, after 1–2 weeks of personal use)

24. **Usage/ops dashboard** — users logged in, total transaction volume,
    emails processed (per status), AI calls per provider/model, template hit
    rate, queue health. Backend counters mostly exist (result objects +
    processing stats); needs an admin-guarded route + screen.
25. **AI provider switch in the dashboard** — the backend already switches
    whole-stack via `AI_PROVIDER` (`opencode` | `openrouter`; both keys can
    be configured simultaneously, so flipping never breaks mid-flight). The
    dashboard should front this with a toggle + per-provider health/quota
    display so a lapsed OpenCode subscription is a one-click failover to
    OpenRouter free models.
26. **Verify OpenRouter free model ids before first switch** — the `:free`
    roster churns; the defaults in `lib/ai.ts` (DeepSeek R1 / Nemotron 70B /
    Llama 3.3 70B) were valid at build time. Check openrouter.ai/models,
    confirm the picks support `response_format: json_object` and enough
    context for 4-email masked batches, and update the defaults if needed.

## Screens & analytics wishlist (July 18, from first real-data session)

27. **Month navigation on Home** — the balance hero should default to the
    current month (income − expense this month), with a way to step back
    month by month; today it is all-time.
28. **Month-wise account balances** — Accounts & Cards should offer a
    per-month view of each account's delta, not just the all-time total.
29. **Per-card drill-down screen** — tapping a card (or card account) opens
    its transactions with an infographic breakdown (spend by category,
    month trend) for that card specifically.
30. **"Not a transaction" review action** — a reject button in the review
    queue for wrongly-extracted transactions (promo mistaken for a credit):
    reverses the financial event (status REVERSED) and optionally teaches
    from the rejection. Engine rows currently have no user remedy.
31. **Detected-instruments prompt (July 19, deliberately dropped for now)** —
    passive "Detected instruments" section in Accounts & Cards: group
    unattributed transactions by payment method + instrument digits and
    offer one-tap register ("₹X across N transactions on ····1234 — add
    it?"). Design agreed with Vasu: stateless over all unattributed rows (no
    watermark), ≥2-transaction threshold to filter misextracted digits,
    card-method rows ride the register/auto-bucket flow, unknown-method rows
    must ask the type. Blockers noted: bank-side relink does not exist (an
    account-flavoured mirror of `relinkTransactionsForCreditCard` is
    required, else accepting a bank prompt does nothing for history), and
    digit-less transactions (a real minority — see Slice) can never prompt.
32. **Precomputed analytics tables — Phase 2 (July 19, design agreed,
    deferred until scale demands)** — Phase 1 (live `GET /analytics/summary`
    running GROUP BYs per request) is fine for years at current volume; when
    it isn't, add three per-user-per-month rollup tables:
    `monthly_totals` (type → sum + count), `monthly_category_totals`
    (category → sum + count, null = uncategorized), and
    `monthly_account_totals` (account → income/expense sums + count, null =
    unattributed). Maintained by ONE DB trigger on `transactions` (insert
    adds, delete subtracts, update moves between buckets) — a trigger, not
    app code, because transactions are written by the engine, manual entry,
    review edits, relinks, merchant backfills, and raw SQL repairs, and a
    trigger catches every path. Month bucketing fixed to Asia/Kolkata (must
    match `monthKeyIST` on the FE). Decide REVERSED handling when item 30
    lands (trigger subtracts on that status flip). Plus an internal rebuild
    endpoint that recomputes a user's buckets from `transactions` inside one
    transaction — the drift-cure lever. The summary endpoint then reads the
    tables instead of grouping live.
