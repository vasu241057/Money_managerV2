-- Adds verified artwork metadata and aliases for branded merchants already
-- observed in the global/user-rule transaction data. This migration does not
-- insert any global merchants.

alter table public.merchant_logos
  drop constraint if exists merchant_logos_content_type_check;

alter table public.merchant_logos
  add constraint merchant_logos_content_type_check
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
  );

update storage.buckets
set allowed_mime_types = array[
  'image/svg+xml',
  'image/png',
  'image/jpeg',
  'image/webp',
  'image/avif',
  'image/x-icon',
  'image/vnd.microsoft.icon'
]
where id = 'merchant-logos';

create temporary table merchant_logo_expansion (
  canonical_name text not null,
  normalized_name text not null,
  website_url text not null,
  source_url text not null,
  source_kind text not null,
  brand_color text,
  aliases text[] not null
);

insert into merchant_logo_expansion (
  canonical_name,
  normalized_name,
  website_url,
  source_url,
  source_kind,
  brand_color,
  aliases
)
values
  (
    'Blinkit',
    'BLINKIT',
    'https://blinkit.com/',
    'https://play.google.com/store/apps/details?id=com.grofers.customerapp',
    'OFFICIAL_BRAND_ASSET',
    '#F8CB46',
    array['Blinkit']
  ),
  (
    'Instamart',
    'INSTAMART',
    'https://www.swiggy.com/instamart',
    'https://www.swiggy.com/corporate/our-businesses/',
    'OFFICIAL_BRAND_ASSET',
    '#FF5200',
    array['Instamart', 'Swiggy Instamart']
  ),
  (
    'Nykaa',
    'NYKAA',
    'https://www.nykaa.com/',
    'https://play.google.com/store/apps/details?id=com.fsn.nykaa',
    'OFFICIAL_BRAND_ASSET',
    '#FC2779',
    array['Nykaa']
  ),
  (
    'Zepto',
    'ZEPTO',
    'https://www.zeptonow.com/',
    'https://play.google.com/store/apps/details?id=com.zeptoconsumerapp',
    'OFFICIAL_BRAND_ASSET',
    '#5B1D8E',
    array['Zepto']
  ),
  (
    'Bistro',
    'BISTRO',
    'https://bistro.blinkit.com/',
    'https://play.google.com/store/apps/details?id=com.blinkit.bistro',
    'OFFICIAL_BRAND_ASSET',
    null,
    array['Bistro', 'Bistro by Blinkit']
  ),
  (
    'Ownly',
    'OWNLY',
    'https://ownly.food/',
    'https://play.google.com/store/apps/details?id=com.ownly.customer',
    'OFFICIAL_BRAND_ASSET',
    null,
    array['Ownly', 'Ownly Food']
  ),
  (
    'Akbar Travels',
    'AKBARTRAVELS',
    'https://www.akbartravels.com/in',
    'https://play.google.com/store/apps/details?id=com.akbartravel.AkbarTravels',
    'OFFICIAL_BRAND_ASSET',
    null,
    array[
      'Akbar Travels',
      'Akbar Travel',
      'AkbarTravels',
      'Akbar Travels of India'
    ]
  ),
  (
    'MySivi',
    'MYSIVI',
    'https://mysivi.ai/',
    'https://play.google.com/store/apps/details?id=com.practice.ninja_study',
    'OFFICIAL_BRAND_ASSET',
    null,
    array['MySivi', 'My Sivi']
  ),
  (
    'The Souled Store',
    'THESOULEDSTORE',
    'https://www.thesouledstore.com/',
    'https://play.google.com/store/apps/details?id=com.thesouledstore',
    'OFFICIAL_BRAND_ASSET',
    null,
    array['The Souled Store', 'Souled Store']
  ),
  (
    'Veda Sankalpa',
    'VEDASANKALPA',
    'https://vedasankalpa.com/',
    'https://vedasankalpa.com/_next/static/media/logo.77687ed1.svg',
    'OFFICIAL_BRAND_ASSET',
    null,
    array['Veda Sankalpa', 'Veda Sankalpa Seva Trust']
  ),
  (
    'Tata CLiQ',
    'TATACLIQ',
    'https://www.tatacliq.com/',
    'https://play.google.com/store/apps/details?id=com.tul.tatacliq',
    'OFFICIAL_BRAND_ASSET',
    null,
    array['Tata CLiQ', 'Tata Cliq']
  ),
  (
    'YES Bank',
    'YESBANK',
    'https://www.yes.bank.in/',
    'https://play.google.com/store/apps/details?id=in.irisbyyes.app',
    'OFFICIAL_BRAND_ASSET',
    '#0060AA',
    array['YES Bank']
  ),
  (
    'Protean eGov Technologies',
    'PROTEANEGOVTECHNOLOGIES',
    'https://www.proteantech.in/',
    'https://www.proteantech.in/media/media-kit/',
    'OFFICIAL_SITE_ICON',
    null,
    array['Protean eGov Technologies', 'Protean eGov', 'Protean']
  ),
  (
    'SuperYou',
    'SUPERYOU',
    'https://superyou.in/',
    'https://superyou.in/',
    'OFFICIAL_SITE_ICON',
    null,
    array['SuperYou', 'Super You']
  ),
  (
    'Travelogy India',
    'TRAVELOGYINDIA',
    'https://www.travelogyindia.com/',
    'https://www.travelogyindia.com/',
    'OFFICIAL_SITE_ICON',
    null,
    array['Travelogy India', 'Travelogy']
  ),
  (
    'UnderStand UPSC',
    'UNDERSTANDUPSC',
    'https://www.understandupsc.com/',
    'https://www.understandupsc.com/',
    'OFFICIAL_SITE_ICON',
    null,
    array[
      'UnderStand UPSC',
      'Understand UPSC',
      'Usarambha Education',
      'USArambha Education Pvt Ltd'
    ]
  ),
  (
    'Lounge One',
    'LOUNGEONE',
    'https://www.loungeone.ai/',
    'https://www.loungeone.ai/',
    'OFFICIAL_SITE_ICON',
    null,
    array['Lounge One', 'LoungeOne']
  ),
  (
    'Anomaly SF',
    'ANOMALYSF',
    'https://anomalysf.com/',
    'https://anomalysf.com/',
    'OFFICIAL_SITE_ICON',
    null,
    array['Anomaly SF']
  );

insert into public.merchant_logos (
  canonical_name,
  normalized_name,
  website_url,
  source_url,
  source_kind,
  source_license,
  storage_provider,
  brand_color,
  is_active
)
select
  canonical_name,
  normalized_name,
  website_url,
  source_url,
  source_kind,
  null,
  'SUPABASE',
  brand_color,
  true
from merchant_logo_expansion
on conflict (normalized_name) do update set
  canonical_name = excluded.canonical_name,
  website_url = excluded.website_url,
  source_url = case
    when public.merchant_logos.public_url is null then excluded.source_url
    else public.merchant_logos.source_url
  end,
  source_kind = case
    when public.merchant_logos.public_url is null then excluded.source_kind
    else public.merchant_logos.source_kind
  end,
  source_license = excluded.source_license,
  storage_provider = excluded.storage_provider,
  brand_color = excluded.brand_color,
  is_active = true;

-- Apply one alias at a time. Some catalog aliases intentionally collapse to
-- the same normalized key (for example "Akbar Travels" and "AkbarTravels").
-- A row-wise upsert remains idempotent and avoids PostgreSQL's cardinality
-- violation when multiple proposed rows resolve to one conflict target.
do $$
declare
  alias_record record;
begin
  for alias_record in
    select distinct on (public.normalize_merchant_logo_key(alias_name))
      public.normalize_merchant_logo_key(alias_name) as search_key,
      logo.id as merchant_logo_id
    from merchant_logo_expansion as expansion
    join public.merchant_logos as logo
      on logo.normalized_name = expansion.normalized_name
    cross join lateral unnest(expansion.aliases) as alias_name
    where public.normalize_merchant_logo_key(alias_name) <> ''
    order by
      public.normalize_merchant_logo_key(alias_name),
      logo.id
  loop
    insert into public.merchant_logo_aliases (
      search_key,
      merchant_logo_id
    )
    values (
      alias_record.search_key,
      alias_record.merchant_logo_id
    )
    on conflict (search_key) do update set
      merchant_logo_id = excluded.merchant_logo_id;
  end loop;
end
$$;

drop table merchant_logo_expansion;
