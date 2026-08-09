import postgres from 'postgres';

const recordAttempts = process.argv.includes('--record-attempts');
const poolerUrl = process.env.SUPABASE_POOLER_URL;

if (!poolerUrl) {
  throw new Error('SUPABASE_POOLER_URL is required (load backend/.dev.vars first)');
}

const sql = postgres(poolerUrl, { max: 1, prepare: false });

try {
  // Deliberately use only actual MERCHANT identities. P2P names, banks, and
  // transfer labels are not brands and should never enter a logo-research queue.
  const candidates = await sql`
    select
      gm.id,
      gm.canonical_name,
      count(tx.id)::integer as transaction_count,
      coalesce(lookup.attempt_count, 0)::integer as prior_attempt_count,
      lookup.last_attempt_at
    from public.global_merchants as gm
    left join public.merchant_logo_aliases as logo_alias
      on logo_alias.search_key = public.normalize_merchant_logo_key(gm.canonical_name)
    left join public.merchant_logos as logo
      on logo.id = logo_alias.merchant_logo_id
      and logo.is_active = true
      and logo.public_url is not null
    left join public.merchant_logo_lookup_attempts as lookup
      on lookup.merchant_id = gm.id
    left join public.transactions as tx
      on tx.merchant_id = gm.id
    where gm.type = 'MERCHANT'
      and logo.id is null
      and coalesce(lookup.attempt_count, 0) < 2
    group by gm.id, gm.canonical_name, lookup.attempt_count, lookup.last_attempt_at
    order by count(tx.id) desc, gm.canonical_name asc
  `;

  const recorded = [];
  if (recordAttempts) {
    await sql.begin(async (transaction) => {
      for (const candidate of candidates) {
        const [attempt] = await transaction`
          insert into public.merchant_logo_lookup_attempts (
            merchant_id,
            attempt_count,
            last_attempt_at,
            last_outcome
          ) values (
            ${candidate.id},
            1,
            now(),
            'NO_CONFIDENT_LOGO'
          )
          on conflict (merchant_id) do update
          set
            attempt_count = public.merchant_logo_lookup_attempts.attempt_count + 1,
            last_attempt_at = now(),
            last_outcome = 'NO_CONFIDENT_LOGO'
          where public.merchant_logo_lookup_attempts.attempt_count < 2
          returning attempt_count, last_attempt_at
        `;
        if (attempt) {
          recorded.push({
            canonical_name: candidate.canonical_name,
            attempt_count: Number(attempt.attempt_count),
            last_attempt_at: attempt.last_attempt_at,
          });
        }
      }
    });
  }

  console.log(JSON.stringify({
    mode: recordAttempts ? 'recorded_lookup_attempts' : 'read_only',
    eligible_merchants: candidates.map((candidate) => ({
      canonical_name: candidate.canonical_name,
      transaction_count: Number(candidate.transaction_count),
      prior_attempt_count: Number(candidate.prior_attempt_count),
      last_attempt_at: candidate.last_attempt_at,
    })),
    recorded_attempts: recorded,
    skipped_after_two_attempts: 'not included',
  }, null, 2));
} finally {
  await sql.end();
}
