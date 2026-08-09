import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

const BATCH_ID = 'live-unmapped-merchant-logos-2026-08';
const SIMPLE_ICONS_VERSION = '16.27.0';
const DEFAULT_ASSET_DIRECTORY = '/tmp/money-manager-merchant-logo-assets';
const DEFAULT_SIMPLE_ICONS_ROOT = '/tmp/money-manager-simple-icons/package';
const manifestPath = resolve('supabase/merchant-logo-catalog.json');
const migrationPath = resolve(
  'supabase/migrations/20260809111000_seed_live_unmapped_merchant_logos.sql',
);
const assetDirectory = resolve(
  process.env.MERCHANT_LOGO_ASSET_DIR ?? DEFAULT_ASSET_DIRECTORY,
);
const simpleIconsRoot = resolve(
  process.env.SIMPLE_ICONS_ROOT ?? DEFAULT_SIMPLE_ICONS_ROOT,
);
const validateOnly = process.argv.includes('--validate-only');

const OFFICIAL_TARGETS = [
  {
    canonicalName: 'OpenAI',
    aliases: ['OpenAI'],
    websiteUrl: 'https://openai.com/',
    sourcePageUrl: 'https://apps.apple.com/us/app/chatgpt/id6448311069',
    assetUrl: 'https://is1-ssl.mzstatic.com/image/thumb/Purple221/v4/af/5b/dc/af5bdcf7-7eb9-b981-88b3-1014de5b987a/AppIcon-0-0-1x_U007epad-0-0-0-1-0-P3-85-220.png/512x512bb.jpg',
    sourceKind: 'OFFICIAL_BRAND_ASSET',
    brandColor: '#000000',
  },
  {
    canonicalName: 'Jio-bp',
    aliases: ['Jio-bp', 'Reliance BP Mobility'],
    websiteUrl: 'https://www.jiobp.com/',
    sourcePageUrl: 'https://www.jiobp.com/',
    assetUrl: 'https://www.jiobp.com/sites/default/files/Jio-BP%20logo_3_0.png',
    sourceKind: 'OFFICIAL_BRAND_ASSET',
    brandColor: null,
  },
  {
    canonicalName: 'Hotel Highway King',
    aliases: ['Hotel Highway King'],
    websiteUrl: 'https://www.hotelhighwayking.com/',
    sourcePageUrl: 'https://www.hotelhighwayking.com/',
    assetUrl: 'https://assets.simplotel.com/simplotel/image/upload/x_216,y_187,w_1404,h_1442,r_0,c_crop,q_80,dpr_1,f_auto,fl_progressive/w_355,h_200,f_auto,c_fit/hotel-highway-king/Frame_8_ae528448',
    sourceKind: 'OFFICIAL_SITE_ICON',
    brandColor: '#008135',
  },
];

const SIMPLE_ICON_TARGET = {
  canonicalName: 'OpenRouter',
  aliases: ['OpenRouter'],
  websiteUrl: 'https://openrouter.ai/',
  sourcePageUrl: 'https://openrouter.ai/blog/announcements/brand-refresh/',
  slug: 'openrouter',
};

function normalize(value) {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function slugify(value) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

function sql(value) {
  if (value === null || value === undefined) return 'null';
  return `'${String(value).replaceAll("'", "''")}'`;
}

function detectContentType(bytes) {
  const prefix = bytes.subarray(0, 1024).toString('utf8').trimStart().toLowerCase();
  if (prefix.startsWith('<svg') || (prefix.startsWith('<?xml') && prefix.includes('<svg'))) {
    return 'image/svg+xml';
  }
  if (bytes[0] === 0x89 && bytes.subarray(1, 4).toString('ascii') === 'PNG') return 'image/png';
  if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return 'image/jpeg';
  if (bytes.subarray(0, 4).toString('ascii') === 'RIFF' && bytes.subarray(8, 12).toString('ascii') === 'WEBP') return 'image/webp';
  if (bytes[0] === 0 && bytes[1] === 0 && bytes[2] === 1 && bytes[3] === 0) return 'image/x-icon';
  if (bytes.subarray(4, 8).toString('ascii') === 'ftyp' && /(?:avif|avis)/.test(bytes.subarray(8, 32).toString('ascii'))) return 'image/avif';
  return null;
}

function extensionFor(contentType) {
  return {
    'image/svg+xml': 'svg',
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/webp': 'webp',
    'image/avif': 'avif',
    'image/x-icon': 'ico',
  }[contentType];
}

function dimensions(bytes, contentType) {
  if (contentType === 'image/png' && bytes.length >= 24) {
    return { width: bytes.readUInt32BE(16), height: bytes.readUInt32BE(20) };
  }
  if (contentType === 'image/x-icon' && bytes.length >= 8) {
    return { width: bytes[6] || 256, height: bytes[7] || 256 };
  }
  return null;
}

function validateAsset(bytes, contentType, target) {
  if (!contentType) throw new Error(`${target.canonicalName}: response was not a supported image`);
  if (bytes.length < 64) throw new Error(`${target.canonicalName}: asset was too small`);
  if (bytes.length > 1_048_576) throw new Error(`${target.canonicalName}: asset exceeded 1 MiB`);
  const size = dimensions(bytes, contentType);
  if (size && (size.width < 32 || size.height < 32)) {
    throw new Error(`${target.canonicalName}: asset is only ${size.width}x${size.height}`);
  }
}

function rowFor(target, asset, rank) {
  const hash = createHash('sha256').update(asset.bytes).digest('hex');
  const extension = extensionFor(asset.contentType);
  return {
    canonical_name: target.canonicalName,
    normalized_name: normalize(target.canonicalName),
    coverage: 'EXISTING',
    catalog_batch: BATCH_ID,
    selection_rank: rank,
    selection_reason: 'Live merchant with no logo; identity and official asset verified',
    evidence_urls: [target.sourcePageUrl],
    aliases: [...new Set([target.canonicalName, ...target.aliases])],
    website_url: target.websiteUrl,
    source_page_url: target.sourcePageUrl,
    source_url: asset.sourceUrl,
    source_kind: target.sourceKind,
    source_license: asset.sourceLicense ?? null,
    brand_color: target.brandColor,
    object_path: `${asset.pathPrefix}/${slugify(target.canonicalName)}-${hash.slice(0, 12)}.${extension}`,
    content_type: asset.contentType,
    content_sha256: hash,
    _bytes: asset.bytes,
  };
}

async function officialRow(target, rank) {
  const response = await fetch(target.assetUrl, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; MoneyManagerLogoAudit/1.0)',
      Accept: 'image/avif,image/webp,image/png,image/jpeg,image/svg+xml,*/*;q=0.5',
    },
    redirect: 'follow',
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`${target.canonicalName}: asset returned ${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  const contentType = detectContentType(bytes);
  validateAsset(bytes, contentType, target);
  return rowFor(target, {
    bytes,
    contentType,
    sourceUrl: response.url,
    pathPrefix: 'official-live-unmapped',
  }, rank);
}

function simpleIconRow(rank) {
  const icons = JSON.parse(readFileSync(join(simpleIconsRoot, 'data/simple-icons.json'), 'utf8'));
  const icon = icons.find((candidate) => candidate.slug === SIMPLE_ICON_TARGET.slug);
  if (!icon) throw new Error(`Simple Icons is missing ${SIMPLE_ICON_TARGET.slug}`);
  const sourcePath = join(simpleIconsRoot, 'icons', `${icon.slug}.svg`);
  if (!existsSync(sourcePath)) throw new Error(`Simple Icons asset is missing ${icon.slug}`);
  const title = icon.title.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
  const bytes = Buffer.from(
    readFileSync(sourcePath, 'utf8')
      .replace('<svg ', `<svg fill="#${icon.hex}" `)
      .replace('>', `><title>${title}</title>`),
  );
  return rowFor({
    ...SIMPLE_ICON_TARGET,
    sourceKind: 'SIMPLE_ICONS',
    brandColor: `#${icon.hex.toUpperCase()}`,
  }, {
    bytes,
    contentType: 'image/svg+xml',
    sourceUrl: icon.source,
    sourceLicense: icon.license?.type ?? null,
    pathPrefix: `simple-icons-${SIMPLE_ICONS_VERSION}`,
  }, rank);
}

function writeAsset(row) {
  const destination = join(assetDirectory, row.object_path);
  mkdirSync(dirname(destination), { recursive: true });
  writeFileSync(destination, row._bytes);
}

function writeMigration(rows) {
  const statements = [
    '-- Generated by scripts/resolve-live-unmapped-merchant-logos.mjs.',
    '-- Four high-confidence logo matches found in the live unmapped-merchant queue.',
    '',
  ];
  for (const row of rows) {
    statements.push(
      'insert into public.merchant_logos (',
      '  canonical_name, normalized_name, website_url, source_url, source_kind,',
      '  source_license, storage_provider, brand_color',
      ') values (',
      `  ${sql(row.canonical_name)}, ${sql(row.normalized_name)}, ${sql(row.website_url)},`,
      `  ${sql(row.source_url)}, ${sql(row.source_kind)}, ${sql(row.source_license)},`,
      `  'SUPABASE', ${sql(row.brand_color)}`,
      ')',
      'on conflict (normalized_name) do update set',
      '  canonical_name = excluded.canonical_name,',
      '  website_url = excluded.website_url,',
      '  source_url = excluded.source_url,',
      '  source_kind = excluded.source_kind,',
      '  source_license = excluded.source_license,',
      '  storage_provider = excluded.storage_provider,',
      '  brand_color = excluded.brand_color,',
      '  is_active = true;',
    );
    for (const alias of row.aliases) {
      statements.push(
        'insert into public.merchant_logo_aliases (search_key, merchant_logo_id)',
        `select ${sql(normalize(alias))}, id`,
        `from public.merchant_logos where normalized_name = ${sql(row.normalized_name)}`,
        'on conflict (search_key) do update set merchant_logo_id = excluded.merchant_logo_id;',
      );
    }
    statements.push('');
  }
  statements.push(
    '-- The live merchant is a payment descriptor of the existing Amazon Pay brand.',
    'insert into public.merchant_logo_aliases (search_key, merchant_logo_id)',
    "select 'AMAZONPAYONDELIVERY', id from public.merchant_logos where normalized_name = 'AMAZONPAY'",
    'on conflict (search_key) do update set merchant_logo_id = excluded.merchant_logo_id;',
    '',
  );
  writeFileSync(migrationPath, statements.join('\n'));
}

function validateManifest(manifest) {
  const rows = manifest.rows.filter((row) => row.catalog_batch === BATCH_ID);
  if (rows.length !== 4) throw new Error(`Expected 4 live queue rows, found ${rows.length}`);
  const allAliases = new Map();
  for (const row of manifest.rows) {
    if (row.normalized_name !== normalize(row.canonical_name)) {
      throw new Error(`Invalid normalized name for ${row.canonical_name}`);
    }
    if (!/^[a-f0-9]{64}$/.test(row.content_sha256)) {
      throw new Error(`Invalid asset hash for ${row.canonical_name}`);
    }
    for (const alias of row.aliases) {
      const key = normalize(alias);
      const owner = allAliases.get(key);
      if (owner && owner !== row.normalized_name) {
        throw new Error(`Alias ${key} is shared by ${owner} and ${row.normalized_name}`);
      }
      allAliases.set(key, row.normalized_name);
    }
  }
  if (allAliases.get('AMAZONPAYONDELIVERY') !== 'AMAZONPAY') {
    throw new Error('Amazon Pay on Delivery must remain an Amazon Pay alias');
  }
  const migration = readFileSync(migrationPath, 'utf8');
  for (const row of rows) {
    if (!migration.includes(`'${row.normalized_name}'`)) {
      throw new Error(`Migration is missing ${row.canonical_name}`);
    }
  }
}

if (validateOnly) {
  validateManifest(JSON.parse(readFileSync(manifestPath, 'utf8')));
  console.log(JSON.stringify({ batch: BATCH_ID, rows: 4, migration: migrationPath }, null, 2));
  process.exit(0);
}

const rows = [];
for (const [index, target] of OFFICIAL_TARGETS.entries()) {
  rows.push(await officialRow(target, index + 1));
}
rows.push(simpleIconRow(rows.length + 1));

const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
const byNormalizedName = new Map(manifest.rows.map((row) => [row.normalized_name, row]));
for (const row of rows) {
  byNormalizedName.set(row.normalized_name, row);
  writeAsset(row);
}
const amazonPay = byNormalizedName.get('AMAZONPAY');
if (!amazonPay) throw new Error('Amazon Pay must exist before adding its delivery alias');
amazonPay.aliases = [...new Set([...amazonPay.aliases, 'Amazon Pay on Delivery'])];
manifest.rows = [...byNormalizedName.values()].map(({ _bytes, ...row }) => row);
manifest.generated_at = new Date().toISOString();
manifest.catalog_batches ??= {};
manifest.catalog_batches[BATCH_ID] = {
  generated_at: manifest.generated_at,
  selected: rows.length,
  evidence: 'Live database audit; official asset or verified Simple Icons source only',
};
writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
writeMigration(rows);
validateManifest(JSON.parse(readFileSync(manifestPath, 'utf8')));

console.log(JSON.stringify({
  batch: BATCH_ID,
  resolved: rows.map((row) => ({
    canonical_name: row.canonical_name,
    source_url: row.source_url,
    content_sha256: row.content_sha256,
  })),
  aliased_existing_logo: 'Amazon Pay on Delivery -> Amazon Pay',
  asset_directory: assetDirectory,
  migration: migrationPath,
}, null, 2));
