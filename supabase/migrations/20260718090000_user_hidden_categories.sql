-- Per-user hiding of system categories.
--
-- System categories are global rows shared by every user, so a user
-- "deleting" one cannot remove the row (it would vanish for all users and
-- break the engine's fallbacks and merchant default categories). Instead a
-- hide row is written here: the category disappears from the user's list
-- and from their AI prompt, while the shared taxonomy stays intact.
-- User-owned categories keep real DELETE semantics.
--
-- Idempotent by design.

create table if not exists public.user_hidden_categories (
  user_id uuid not null references public.users(id) on delete cascade,
  category_id uuid not null references public.categories(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (user_id, category_id)
);
