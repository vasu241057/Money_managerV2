import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import postgres from 'postgres';

const cliArgs = process.argv.slice(2);
const positionalArgs = [];
let batchId = null;
for (let index = 0; index < cliArgs.length; index += 1) {
  if (cliArgs[index] === '--batch') {
    batchId = cliArgs[index + 1] ?? null;
    if (!batchId) throw new Error('--batch requires a batch identifier');
    index += 1;
  } else {
    positionalArgs.push(cliArgs[index]);
  }
}

const manifestPath = resolve(positionalArgs[0] ?? 'supabase/merchant-logo-catalog.json');
const assetDirectory = resolve(
  positionalArgs[1] ?? '/tmp/money-manager-merchant-logo-assets',
);
const storageAdminKey = process.env.SUPABASE_SECRET_KEY
  ?? process.env.SUPABASE_SERVICE_ROLE_KEY;
const poolerUrl = process.env.SUPABASE_POOLER_URL;

if (!storageAdminKey) {
  throw new Error(
    'SUPABASE_SECRET_KEY (recommended) or SUPABASE_SERVICE_ROLE_KEY (legacy) '
      + 'is required for Storage uploads. Add it to backend/.dev.vars locally; never commit it.',
  );
}
if (!poolerUrl) {
  throw new Error('SUPABASE_POOLER_URL is required');
}

const pooler = new URL(poolerUrl);
const username = decodeURIComponent(pooler.username);
const projectRef = /^postgres\.([a-z0-9]+)$/i.exec(username)?.[1];
const projectUrl = process.env.SUPABASE_PROJECT_URL
  ?? (projectRef ? `https://${projectRef}.supabase.co` : null);
if (!projectUrl) {
  throw new Error('SUPABASE_PROJECT_URL is required when it cannot be inferred from the pooler URL');
}

const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
const rows = batchId
  ? manifest.rows.filter((row) => row.catalog_batch === batchId)
  : manifest.rows;
if (batchId && rows.length === 0) {
  throw new Error(`No merchant logo rows found for batch ${batchId}`);
}
const sql = postgres(poolerUrl, { max: 1, prepare: false });
let uploaded = 0;
const storageHeaders = {
  apikey: storageAdminKey,
};

// Legacy service-role keys are JWTs and need the Authorization header.
// Supabase's current sb_secret_ keys authenticate through the apikey header.
if (!storageAdminKey.startsWith('sb_secret_')) {
  storageHeaders.Authorization = `Bearer ${storageAdminKey}`;
}

try {
  for (const row of rows) {
    const bytes = readFileSync(join(assetDirectory, row.object_path));
    const contentSha256 = createHash('sha256').update(bytes).digest('hex');
    if (contentSha256 !== row.content_sha256) {
      throw new Error(
        `Asset hash mismatch for ${row.canonical_name}: expected ${row.content_sha256}, got ${contentSha256}`,
      );
    }
    const encodedPath = row.object_path.split('/').map(encodeURIComponent).join('/');
    const uploadUrl = `${projectUrl}/storage/v1/object/merchant-logos/${encodedPath}`;
    const response = await fetch(uploadUrl, {
      method: 'POST',
      headers: {
        ...storageHeaders,
        'Content-Type': row.content_type,
        'Cache-Control': '31536000, immutable',
        'x-upsert': 'true',
      },
      body: bytes,
    });
    if (!response.ok) {
      throw new Error(
        `Storage upload failed for ${row.canonical_name}: ${response.status} ${await response.text()}`,
      );
    }

    const publicUrl = `${projectUrl}/storage/v1/object/public/merchant-logos/${encodedPath}`;
    await sql`
      update public.merchant_logos
      set
        object_path = ${row.object_path},
        public_url = ${publicUrl},
        content_type = ${row.content_type},
        content_sha256 = ${row.content_sha256},
        brand_color = ${row.brand_color},
        storage_provider = 'SUPABASE',
        is_active = true
      where normalized_name = ${row.normalized_name}
    `;
    uploaded += 1;
  }
} finally {
  await sql.end();
}

console.log(JSON.stringify({
  uploaded,
  batch: batchId,
  bucket: 'merchant-logos',
  projectUrl,
}, null, 2));
