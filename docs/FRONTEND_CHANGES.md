# Frontend Changes — Backend Contract Updates (July 2026)

> For the frontend redesign. Backend-side everything below is live; this doc
> lists what the new UI must add or change. Not an exhaustive API reference —
> see `docs/CONTEXT.md` §5 for the full route table.

## 1. "Set as default category?" step on review (the important one)

When the user assigns a category to a NEEDS_REVIEW transaction, ask one
extra question: **"Use this category for <merchant> from now on?"**

- Yes → send the existing review call with `apply_rule: true`
  (`POST /transactions/:id/review`). This writes a `user_merchant_rules` row;
  every future transaction of this merchant gets the category automatically
  and is auto-verified. The rule can be changed later the same way.
- No → plain review, nothing remembered.

**Aggregator guard:** if `transaction.merchant.type === 'AGGREGATOR'`
(Razorpay, PayU, BillDesk…), hide the prompt or warn — an aggregator masks
the real merchant, so a default would miscategorize every other purchase
through that gateway. `merchant.type` is already in the transaction feed.

## 2. AI badge on transactions

Every transaction row exposes `classification_source`:
- `AI` → show an "AI" badge (category/merchant came from the engine, not the
  user). `ai_confidence_score` (0–1) is also available for ordering or a
  subtle confidence hint.
- `USER` → user rule or manual review decided it; no badge.
- Review inbox = `status === 'NEEDS_REVIEW'`. VERIFIED transactions with the
  AI badge were auto-verified via a known merchant + real category.

## 3. Categories: system taxonomy now exists

`GET /categories` now returns an exhaustive three-level system taxonomy
(`is_system: true`) alongside the user's own: roots → parents (Food,
Transport, Housing, Investments, …) → subcategories (Groceries, Fuel, Rent,
Crypto, … ~85 nodes). Notes for the UI:
- The three roots (Income/Expense/Transfer, `parent_id: null`) are
  **structural** — show them as group headers, never as assignable choices.
- Parents and subcategories are both assignable; build the picker as a
  two-level list (parent → drill into subs).
- New users get no auto-created personal category anymore; the system
  taxonomy (incl. "Miscellaneous") is the default vocabulary.
- **Deleting categories:** DELETE works on every category. User-owned ones
  are really deleted; system ones are hidden for this user only (response
  `{ hidden: true }`) and their direct subcategories hide with them. Build a
  "hidden categories" screen from `GET /categories?include_hidden=true`
  (rows carry `is_hidden`) with `POST /categories/:id/restore` to bring one
  back. Miscellaneous / Other Income / Other Transfer return 400
  `CATEGORY_NOT_HIDEABLE` — don't offer delete on them.

## 4. Credit cards (new API)

`/credit-cards` CRUD — GET / POST / PATCH `/:id` / DELETE `/:id`.
Fields: `account_id` (must be a `card`-type account), `card_label`,
`issuer_name`, `network`, `first4`, `last4`, `is_active`.
POST/PATCH responses include `relinked_transaction_count` — how many past
transactions were retroactively linked to the card (worth a toast).
`GET /transactions` accepts a `credit_card_id` filter; each transaction may
carry a `credit_card` object (label + first4/last4) for display.

## 5. Removed: AI spending-analysis endpoint

The old "ask AI about my spending" feature was removed with the triage
worker. Being re-added separately — do not build against the old shape.
