-- Merchant artwork is intentionally independent from global_merchants.
-- The catalogue can know about a brand before the extraction pipeline does;
-- once a matching global merchant appears, the transaction feed picks up the
-- logo through merchant_logo_aliases without inserting speculative merchants.

create or replace function public.normalize_merchant_logo_key(value text)
returns text
language sql
immutable
strict
parallel safe
as $$
  select upper(regexp_replace(value, '[^A-Za-z0-9]+', '', 'g'));
$$;

create table if not exists public.merchant_logos (
  id uuid primary key default gen_random_uuid(),
  canonical_name text not null,
  normalized_name text not null unique,
  website_url text,
  source_url text not null,
  source_kind text not null
    check (source_kind in ('OFFICIAL_BRAND_ASSET', 'OFFICIAL_SITE_ICON', 'SIMPLE_ICONS')),
  source_license text,
  storage_provider text not null default 'SUPABASE'
    check (storage_provider in ('SUPABASE', 'R2', 'EXTERNAL')),
  object_path text,
  public_url text,
  content_type text
    check (
      content_type is null
      or content_type in (
        'image/svg+xml',
        'image/png',
        'image/jpeg',
        'image/webp',
        'image/avif',
        'image/x-icon',
        'image/vnd.microsoft.icon'
      )
    ),
  brand_color text
    check (brand_color is null or brand_color ~ '^#[0-9A-Fa-f]{6}$'),
  content_sha256 text
    check (content_sha256 is null or content_sha256 ~ '^[0-9a-f]{64}$'),
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint merchant_logos_name_not_blank check (btrim(canonical_name) <> ''),
  constraint merchant_logos_normalized_name_valid check (
    normalized_name = public.normalize_merchant_logo_key(canonical_name)
  ),
  constraint merchant_logos_asset_complete check (
    (object_path is null and public_url is null and content_type is null and content_sha256 is null)
    or
    (object_path is not null and public_url is not null and content_type is not null and content_sha256 is not null)
  )
);

drop trigger if exists update_merchant_logos_modtime on public.merchant_logos;
create trigger update_merchant_logos_modtime
before update on public.merchant_logos
for each row execute function public.update_updated_at_column();

create table if not exists public.merchant_logo_aliases (
  search_key text primary key,
  merchant_logo_id uuid not null references public.merchant_logos(id) on delete cascade,
  created_at timestamptz not null default now(),
  constraint merchant_logo_aliases_key_valid check (
    search_key <> ''
    and search_key = public.normalize_merchant_logo_key(search_key)
  )
);

create index if not exists idx_merchant_logo_aliases_logo_id
  on public.merchant_logo_aliases (merchant_logo_id);

comment on table public.merchant_logos is
  'Verified merchant artwork catalogue. Rows do not create global merchants.';
comment on table public.merchant_logo_aliases is
  'Normalized brand identities that attach catalogue artwork to current or future global merchants.';

-- Files are public brand artwork. Writes still require an authenticated
-- Storage API key; making a bucket public only makes reads public.
insert into storage.buckets (
  id,
  name,
  public,
  file_size_limit,
  allowed_mime_types
)
values (
  'merchant-logos',
  'merchant-logos',
  true,
  1048576,
  array[
    'image/svg+xml',
    'image/png',
    'image/jpeg',
    'image/webp',
    'image/avif',
    'image/x-icon',
    'image/vnd.microsoft.icon'
  ]
)
on conflict (id) do update
set
  public = excluded.public,
  file_size_limit = excluded.file_size_limit,
  allowed_mime_types = excluded.allowed_mime_types;
