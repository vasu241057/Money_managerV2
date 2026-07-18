# Future Fixes & Deferred Work

> Decisions and known issues explicitly deferred, with why. Ordered roughly
> by when they'll matter. Last updated: July 12, 2026.

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

16. **Purge pre-gate bodies** — `update public.raw_emails set clean_text = ''
    where status in ('IGNORED','UNRECOGNIZED') and clean_text <> '';`
    (one-time, after backfill considerations).
17. **Card API polish** — `is_active` on POST silently coerces non-booleans
    to false (PATCH 400s correctly); card create + relink are not atomic;
    no end-to-end test that a `credit_card` extraction persists its card link.
18. **Docs drift** — CONTEXT.md §5 says PUT for accounts/categories/
    transactions; actual routes are PATCH.
