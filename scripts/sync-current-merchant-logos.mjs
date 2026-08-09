import { createHash } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import postgres from 'postgres';

const TARGETS = [
  {
    canonicalName: 'Blinkit',
    aliases: ['Blinkit'],
    websiteUrl: 'https://blinkit.com/',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=com.grofers.customerapp',
    assetUrl: 'https://play-lh.googleusercontent.com/dY1ryTjOaVsnslxvNamNvSBulmDUkvW5f5FJ_HDApMfecSOIxjCafpNAv4LTG8TJVyp0XR9tAAaRI4K77Eiqfg=w240-h480',
    brandColor: '#F8CB46',
    required: true,
  },
  {
    canonicalName: 'Instamart',
    aliases: ['Instamart', 'Swiggy Instamart'],
    websiteUrl: 'https://www.swiggy.com/instamart',
    sourcePageUrl: 'https://www.swiggy.com/corporate/our-businesses/',
    assetUrl: 'https://www.swiggy.com/corporate/wp-content/uploads/2025/10/instamart-new-logo.png',
    brandColor: '#FF5200',
    required: true,
  },
  {
    canonicalName: 'Nykaa',
    aliases: ['Nykaa'],
    websiteUrl: 'https://www.nykaa.com/',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=com.fsn.nykaa',
    assetUrl: 'https://play-lh.googleusercontent.com/LphGQF2SgqzQpY8iWHgC-3_iMf59_3RmprjXPTwCxACPymuxQRyB93-pBl575S7PRXo1WUW-Mp4lqYfgQNLoYK4=w240-h480',
    brandColor: '#FC2779',
    required: true,
  },
  {
    canonicalName: 'Zepto',
    aliases: ['Zepto'],
    websiteUrl: 'https://www.zeptonow.com/',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=com.zeptoconsumerapp',
    assetUrl: 'https://play-lh.googleusercontent.com/jrtmMFv4qjtgMPQeaQzUFZ3EYBkHd_8OFYl6O1Ngt5Pey52RJAR4u8K4IoPILkJz76a7s5U3DNaY3r3xnl7t8X4=w240-h480',
    brandColor: '#5B1D8E',
    required: true,
  },
  {
    canonicalName: 'Bistro',
    aliases: ['Bistro', 'Bistro by Blinkit'],
    websiteUrl: 'https://bistro.blinkit.com/',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=com.blinkit.bistro',
    assetUrl: 'https://play-lh.googleusercontent.com/hg_1f2ZpUQo_1FMvP_uS3tIhl9f_-Py9QXo3qMuZPO9HJo4wuBrD8D4tkhROi44t4F4F3zlfVHXVkTIPMjNgew=w240-h480',
    brandColor: null,
    required: true,
  },
  {
    canonicalName: 'Ownly',
    aliases: ['Ownly', 'Ownly Food'],
    websiteUrl: 'https://ownly.food/',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=com.ownly.customer',
    assetUrl: 'https://play-lh.googleusercontent.com/NiY8pMN3pr_gtKHXrYFee7NHJyXwclagpd_U3EAHC9bJnmTBDBCoj1X3MI9JPoBpoi1KzeMrRwWMyob-HJCEmA=w240-h480',
    brandColor: null,
    required: true,
  },
  {
    canonicalName: 'Akbar Travels',
    aliases: [
      'Akbar Travels',
      'Akbar Travel',
      'AkbarTravels',
      'Akbar Travels of India',
    ],
    websiteUrl: 'https://www.akbartravels.com/in',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=com.akbartravel.AkbarTravels',
    assetUrl: 'https://play-lh.googleusercontent.com/hEKoarobDfwiVKyhHAeDiOpNCi6ZAI224QIZoa40TxqASxJ3FSbvMr3CncAZXeOqLHFzf1CN9dXa28hFPlAE3g=w240-h480',
    brandColor: null,
    required: true,
  },
  {
    canonicalName: 'MySivi',
    aliases: ['MySivi', 'My Sivi'],
    websiteUrl: 'https://mysivi.ai/',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=com.practice.ninja_study',
    assetUrl: 'https://play-lh.googleusercontent.com/ipiQK__CIMRdao056LCl8DXeobNXSSHzmOztxfnW9XGguQpshcWKSLMvnQX6ozRC0tOl4nyrtMV-FreR0cfn=w240-h480',
    brandColor: null,
  },
  {
    canonicalName: 'The Souled Store',
    aliases: ['The Souled Store', 'Souled Store'],
    websiteUrl: 'https://www.thesouledstore.com/',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=com.thesouledstore',
    assetUrl: 'https://play-lh.googleusercontent.com/d6nXlvDpWaYoZU4GaLsFUprNt6me9Q39o3Ve6xCLT_OVC5rb6YDe41mgW1enBOEHshCYi_5Op1itoaFHhbVGHw=w240-h480',
    brandColor: null,
  },
  {
    canonicalName: 'Veda Sankalpa',
    aliases: ['Veda Sankalpa', 'Veda Sankalpa Seva Trust'],
    websiteUrl: 'https://vedasankalpa.com/',
    sourcePageUrl: 'https://vedasankalpa.com/',
    assetUrl: 'https://vedasankalpa.com/_next/static/media/logo.77687ed1.svg',
    brandColor: null,
  },
  {
    canonicalName: 'Tata CLiQ',
    aliases: ['Tata CLiQ', 'Tata Cliq'],
    websiteUrl: 'https://www.tatacliq.com/',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=com.tul.tatacliq',
    assetUrl: 'https://play-lh.googleusercontent.com/pQaxHUwg1RK_N_jHBH4NwkpVhBW1ell5IMwe3YCBtwaVfJ2kiCByXPomh9knUb1fqPkxSyFLE7IEZkN4rOSDng=w240-h480',
    brandColor: null,
    coverage: 'ADDITIONAL',
  },
  {
    canonicalName: 'YES Bank',
    aliases: ['YES Bank'],
    websiteUrl: 'https://www.yes.bank.in/',
    sourcePageUrl: 'https://play.google.com/store/apps/details?id=in.irisbyyes.app',
    assetUrl: 'https://play-lh.googleusercontent.com/ejSl4WbMN1fs2wbCHGQRW9utwiPzSrXYFgZ7-Ae0cdvsjCbLOWMlIEwBwYxvhbaK0if4o_CNDNRoTEdP7jyNAQ=w240-h480',
    brandColor: '#0060AA',
  },
  {
    canonicalName: 'Protean eGov Technologies',
    aliases: ['Protean eGov Technologies', 'Protean eGov', 'Protean'],
    websiteUrl: 'https://www.proteantech.in/',
    discoveryUrl: 'https://www.proteantech.in/media/media-kit/',
    brandColor: null,
  },
  {
    canonicalName: 'SuperYou',
    aliases: ['SuperYou', 'Super You'],
    websiteUrl: 'https://superyou.in/',
    discoveryUrl: 'https://superyou.in/',
    brandColor: null,
  },
  {
    canonicalName: 'Travelogy India',
    aliases: ['Travelogy India', 'Travelogy'],
    websiteUrl: 'https://www.travelogyindia.com/',
    discoveryUrl: 'https://www.travelogyindia.com/',
    brandColor: null,
  },
  {
    canonicalName: 'UnderStand UPSC',
    aliases: [
      'UnderStand UPSC',
      'Understand UPSC',
      'Usarambha Education',
      'USArambha Education Pvt Ltd',
    ],
    websiteUrl: 'https://www.understandupsc.com/',
    discoveryUrl: 'https://www.understandupsc.com/',
    brandColor: null,
  },
  {
    canonicalName: 'Lounge One',
    aliases: ['Lounge One', 'LoungeOne'],
    websiteUrl: 'https://www.loungeone.ai/',
    discoveryUrl: 'https://www.loungeone.ai/',
    brandColor: null,
  },
  {
    canonicalName: 'Anomaly SF',
    aliases: ['Anomaly SF'],
    websiteUrl: 'https://anomalysf.com/',
    discoveryUrl: 'https://anomalysf.com/',
    brandColor: null,
  },
];

const shouldUpload = process.argv.includes('--upload');
const validateOnly = process.argv.includes('--validate-only');
const manifestPath = resolve('supabase/merchant-logo-catalog.json');
const assetDirectory = resolve(
  process.env.MERCHANT_LOGO_ASSET_DIR
    ?? '/tmp/money-manager-merchant-logo-assets',
);
const headers = {
  'User-Agent': 'Mozilla/5.0 (compatible; MoneyManagerLogoSync/1.0)',
  Accept: 'text/html,application/xhtml+xml,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8',
};

function normalize(value) {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function slugify(value) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

function decodeHtml(value) {
  return value
    .replaceAll('&amp;', '&')
    .replaceAll('&#x2F;', '/')
    .replaceAll('&#47;', '/');
}

function detectContentType(bytes) {
  const prefix = bytes
    .subarray(0, 512)
    .toString('utf8')
    .replace(/^\uFEFF/, '')
    .trimStart()
    .toLowerCase();
  if (
    prefix.startsWith('<svg')
    || (prefix.startsWith('<?xml') && prefix.includes('<svg'))
  ) {
    return 'image/svg+xml';
  }
  if (bytes[0] === 0x89 && bytes.subarray(1, 4).toString('ascii') === 'PNG') {
    return 'image/png';
  }
  if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) {
    return 'image/jpeg';
  }
  if (
    bytes.subarray(0, 4).toString('ascii') === 'RIFF'
    && bytes.subarray(8, 12).toString('ascii') === 'WEBP'
  ) {
    return 'image/webp';
  }
  if (bytes[0] === 0 && bytes[1] === 0 && bytes[2] === 1 && bytes[3] === 0) {
    return 'image/x-icon';
  }
  if (
    bytes.subarray(4, 8).toString('ascii') === 'ftyp'
    && /(?:avif|avis)/.test(bytes.subarray(8, 32).toString('ascii'))
  ) {
    return 'image/avif';
  }
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

function readAttribute(tag, name) {
  return new RegExp(`\\b${name}\\s*=\\s*["']([^"']+)["']`, 'i').exec(tag)?.[1] ?? '';
}

function discoveryCandidates(html, pageUrl) {
  const candidates = [];
  for (const tag of html.match(/<(?:link|meta|img|a)\b[^>]*>/gi) ?? []) {
    const tagName = /^<(\w+)/.exec(tag)?.[1]?.toLowerCase();
    const rel = readAttribute(tag, 'rel');
    const property = readAttribute(tag, 'property') || readAttribute(tag, 'name');
    const label = `${readAttribute(tag, 'alt')} ${readAttribute(tag, 'title')}`;
    const rawUrl = tagName === 'meta'
      ? readAttribute(tag, 'content')
      : readAttribute(
        tag,
        tagName === 'a' ? 'href' : (tagName === 'link' ? 'href' : 'src'),
      );
    if (!rawUrl || rawUrl.startsWith('data:')) continue;

    let score = 0;
    const descriptor = `${rel} ${property} ${label} ${rawUrl}`.toLowerCase();
    if (/(?:apple-touch-icon|mask-icon)/.test(rel)) score += 900;
    else if (/(?:^|\s)(?:icon|shortcut icon)(?:\s|$)/i.test(rel)) score += 800;
    if (/(?:logo|logomark|wordmark|favicon|app[-_ ]?icon)/.test(descriptor)) score += 600;
    if (/og:image|twitter:image/.test(property)) score += 100;
    if (tagName === 'img') score += 80;
    if (
      tagName === 'a'
      && /\.(?:svg|png|webp|jpe?g|avif)(?:[?#]|$)/i.test(rawUrl)
    ) {
      score += 60;
    }
    if (score < 100) continue;

    try {
      candidates.push({
        url: new URL(decodeHtml(rawUrl), pageUrl).toString(),
        score,
      });
    } catch {
      // Ignore malformed URLs from otherwise valid official pages.
    }
  }
  candidates.sort((left, right) => right.score - left.score);
  candidates.push({ url: new URL('/favicon.ico', pageUrl).toString(), score: 0 });
  return [...new Map(candidates.map((candidate) => [candidate.url, candidate])).values()];
}

async function fetchResponse(url, accept = headers.Accept) {
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const response = await fetch(url, {
        headers: { ...headers, Accept: accept },
        redirect: 'follow',
        signal: AbortSignal.timeout(20_000),
      });
      if (response.ok) return response;
      lastError = new Error(`${response.status} ${response.statusText}`);
      if (response.status < 500 && response.status !== 429) break;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError ?? new Error('request failed');
}

async function readVerifiedAsset(url) {
  const response = await fetchResponse(
    url,
    'image/avif,image/webp,image/png,image/svg+xml,image/*,*/*;q=0.5',
  );
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length === 0) throw new Error('asset was empty');
  if (bytes.length > 1_048_576) throw new Error('asset exceeded the 1 MiB limit');
  const contentType = detectContentType(bytes);
  if (!contentType) {
    throw new Error(
      'response was not a supported image (HTML and anti-bot pages are rejected)',
    );
  }
  return { bytes, contentType, sourceUrl: response.url };
}

async function downloadTarget(target) {
  let asset;
  let sourceKind = 'OFFICIAL_BRAND_ASSET';
  if (target.assetUrl) {
    asset = await readVerifiedAsset(target.assetUrl);
  } else {
    sourceKind = 'OFFICIAL_SITE_ICON';
    const page = await fetchResponse(
      target.discoveryUrl,
      'text/html,application/xhtml+xml',
    );
    const html = await page.text();
    const failures = [];
    for (const candidate of discoveryCandidates(html, page.url)) {
      try {
        asset = await readVerifiedAsset(candidate.url);
        break;
      } catch (error) {
        failures.push(error instanceof Error ? error.message : String(error));
      }
    }
    if (!asset) {
      throw new Error(
        `no verified logo/icon found (${failures.at(-1) ?? 'no candidates'})`,
      );
    }
  }

  const hash = createHash('sha256').update(asset.bytes).digest('hex');
  const extension = extensionFor(asset.contentType);
  const objectPath = `official-brand-assets/${slugify(target.canonicalName)}-${hash.slice(0, 12)}.${extension}`;
  const destination = join(assetDirectory, objectPath);
  mkdirSync(dirname(destination), { recursive: true });
  writeFileSync(destination, asset.bytes);
  return {
    canonical_name: target.canonicalName,
    normalized_name: normalize(target.canonicalName),
    coverage: target.coverage ?? 'EXISTING',
    aliases: [...new Set([target.canonicalName, ...target.aliases])],
    website_url: target.websiteUrl,
    source_page_url: target.sourcePageUrl ?? target.discoveryUrl,
    source_url: asset.sourceUrl,
    source_kind: sourceKind,
    source_license: null,
    brand_color: target.brandColor,
    object_path: objectPath,
    content_type: asset.contentType,
    content_sha256: hash,
  };
}

function updateManifest(successfulRows, failures) {
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  const rowsByName = new Map(
    manifest.rows.map((row) => [row.normalized_name, row]),
  );
  for (const row of successfulRows) {
    rowsByName.set(row.normalized_name, {
      ...rowsByName.get(row.normalized_name),
      ...row,
    });
  }
  const rows = [...rowsByName.values()];
  manifest.generated_at = new Date().toISOString();
  manifest.existing_priority_count = rows.filter(
    (row) => row.coverage === 'EXISTING',
  ).length;
  manifest.additional_count = rows.filter(
    (row) => row.coverage === 'ADDITIONAL',
  ).length;
  manifest.rows = rows;
  const successfulNames = new Set(
    successfulRows.map((row) => row.normalized_name),
  );
  manifest.official_site_failures = [
    ...(manifest.official_site_failures ?? []).filter(
      (failure) => !successfulNames.has(normalize(failure.canonical_name)),
    ),
    ...failures,
  ];
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
}

function storageContext() {
  const storageAdminKey = process.env.SUPABASE_SECRET_KEY
    ?? process.env.SUPABASE_SERVICE_ROLE_KEY;
  const poolerUrl = process.env.SUPABASE_POOLER_URL;
  if (!storageAdminKey || !poolerUrl) {
    throw new Error(
      '--upload requires SUPABASE_POOLER_URL and SUPABASE_SECRET_KEY '
      + '(or legacy SUPABASE_SERVICE_ROLE_KEY)',
    );
  }
  const pooler = new URL(poolerUrl);
  const projectRef = /^postgres\.([a-z0-9]+)$/i.exec(
    decodeURIComponent(pooler.username),
  )?.[1];
  const projectUrl = process.env.SUPABASE_PROJECT_URL
    ?? (projectRef ? `https://${projectRef}.supabase.co` : null);
  if (!projectUrl) {
    throw new Error(
      'SUPABASE_PROJECT_URL could not be inferred from the pooler URL',
    );
  }
  const storageHeaders = { apikey: storageAdminKey };
  if (!storageAdminKey.startsWith('sb_secret_')) {
    storageHeaders.Authorization = `Bearer ${storageAdminKey}`;
  }
  return { poolerUrl, projectUrl, storageHeaders };
}

async function uploadRows(rows) {
  const { poolerUrl, projectUrl, storageHeaders } = storageContext();
  const sql = postgres(poolerUrl, { max: 1, prepare: false });
  const uploaded = [];
  try {
    for (const row of rows) {
      const bytes = readFileSync(join(assetDirectory, row.object_path));
      const encodedPath = row.object_path
        .split('/')
        .map(encodeURIComponent)
        .join('/');
      const response = await fetch(
        `${projectUrl}/storage/v1/object/merchant-logos/${encodedPath}`,
        {
          method: 'POST',
          headers: {
            ...storageHeaders,
            'Content-Type': row.content_type,
            'Cache-Control': '31536000, immutable',
            'x-upsert': 'true',
          },
          body: bytes,
        },
      );
      if (!response.ok) {
        throw new Error(
          `Storage upload failed for ${row.canonical_name}: `
          + `${response.status} ${await response.text()}`,
        );
      }
      const publicUrl = `${projectUrl}/storage/v1/object/public/merchant-logos/${encodedPath}`;
      const [logo] = await sql`
        insert into public.merchant_logos (
          canonical_name,
          normalized_name,
          website_url,
          source_url,
          source_kind,
          source_license,
          storage_provider,
          object_path,
          public_url,
          content_type,
          brand_color,
          content_sha256,
          is_active
        ) values (
          ${row.canonical_name},
          ${row.normalized_name},
          ${row.website_url},
          ${row.source_url},
          ${row.source_kind},
          ${row.source_license},
          'SUPABASE',
          ${row.object_path},
          ${publicUrl},
          ${row.content_type},
          ${row.brand_color},
          ${row.content_sha256},
          true
        )
        on conflict (normalized_name) do update set
          canonical_name = excluded.canonical_name,
          website_url = excluded.website_url,
          source_url = excluded.source_url,
          source_kind = excluded.source_kind,
          source_license = excluded.source_license,
          storage_provider = excluded.storage_provider,
          object_path = excluded.object_path,
          public_url = excluded.public_url,
          content_type = excluded.content_type,
          brand_color = excluded.brand_color,
          content_sha256 = excluded.content_sha256,
          is_active = true
        returning id
      `;
      for (const alias of row.aliases) {
        const searchKey = normalize(alias);
        if (!searchKey) continue;
        await sql`
          insert into public.merchant_logo_aliases (
            search_key,
            merchant_logo_id
          ) values (
            ${searchKey},
            ${logo.id}
          )
          on conflict (search_key) do update set
            merchant_logo_id = excluded.merchant_logo_id
        `;
      }
      uploaded.push(row.canonical_name);
    }

    const searchKeys = rows.flatMap((row) => row.aliases.map(normalize));
    const matchedCurrentMerchants = await sql`
      select
        gm.canonical_name,
        ml.canonical_name as logo_name,
        ml.public_url
      from public.global_merchants as gm
      join public.merchant_logo_aliases as mla
        on mla.search_key = public.normalize_merchant_logo_key(gm.canonical_name)
      join public.merchant_logos as ml
        on ml.id = mla.merchant_logo_id
        and ml.is_active = true
        and ml.public_url is not null
      where mla.search_key in ${sql(searchKeys)}
      order by gm.canonical_name
    `;
    return { uploaded, matchedCurrentMerchants };
  } finally {
    await sql.end();
  }
}

if (validateOnly) {
  const normalizedNames = TARGETS.map((target) => normalize(target.canonicalName));
  const duplicateNames = normalizedNames.filter(
    (name, index) => normalizedNames.indexOf(name) !== index,
  );
  if (duplicateNames.length > 0) {
    throw new Error(`Duplicate merchant targets: ${[...new Set(duplicateNames)].join(', ')}`);
  }
  for (const target of TARGETS) {
    if (!target.aliases.includes(target.canonicalName)) {
      throw new Error(`${target.canonicalName} must include its canonical name as an alias`);
    }
    for (const url of [
      target.websiteUrl,
      target.sourcePageUrl,
      target.assetUrl,
      target.discoveryUrl,
    ].filter(Boolean)) {
      if (!url.startsWith('https://')) {
        throw new Error(`${target.canonicalName} contains a non-HTTPS source URL`);
      }
    }
  }
  const migration = readFileSync(
    resolve('supabase/migrations/20260724102000_expand_current_merchant_logos.sql'),
    'utf8',
  );
  const missingFromMigration = TARGETS
    .map((target) => target.canonicalName)
    .filter((name) => !migration.includes(`'${name.replaceAll("'", "''")}'`));
  if (missingFromMigration.length > 0) {
    throw new Error(
      `Merchant targets missing from migration: ${missingFromMigration.join(', ')}`,
    );
  }
  console.log(JSON.stringify({
    targets: TARGETS.length,
    requiredPriorityTargets: TARGETS.filter((target) => target.required).length,
    directAssets: TARGETS.filter((target) => target.assetUrl).length,
    officialSiteDiscovery: TARGETS.filter((target) => target.discoveryUrl).length,
  }, null, 2));
  process.exit(0);
}

if (!existsSync(manifestPath)) {
  throw new Error(`Merchant logo manifest was not found at ${manifestPath}`);
}

const successfulRows = [];
const failures = [];
let nextTargetIndex = 0;
async function worker() {
  while (nextTargetIndex < TARGETS.length) {
    const target = TARGETS[nextTargetIndex];
    nextTargetIndex += 1;
    try {
      successfulRows.push(await downloadTarget(target));
    } catch (error) {
      failures.push({
        canonical_name: target.canonicalName,
        website_url: target.websiteUrl,
        required: target.required === true,
        reason: error instanceof Error ? error.message : String(error),
      });
    }
  }
}
await Promise.all(Array.from({ length: 5 }, () => worker()));
updateManifest(successfulRows, failures);

const uploadResult = shouldUpload
  ? await uploadRows(successfulRows)
  : null;
const requiredFailures = failures.filter((failure) => failure.required);
console.log(JSON.stringify({
  attempted: TARGETS.length,
  downloaded: successfulRows.length,
  uploaded: uploadResult?.uploaded.length ?? 0,
  matchedCurrentMerchants: uploadResult?.matchedCurrentMerchants ?? [],
  failures,
  assetDirectory,
}, null, 2));
if (requiredFailures.length > 0) process.exitCode = 1;
