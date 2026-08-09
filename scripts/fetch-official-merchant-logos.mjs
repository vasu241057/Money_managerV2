import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

const TARGETS = [
  ['Amazon', 'https://www.amazon.in/', ['Amazon', 'Amazon Smart Store']],
  ['Amazon Pay', 'https://www.amazon.in/amazonpay/home', ['Amazon Pay']],
  ['BHARATPE', 'https://bharatpe.com/', ['BHARATPE']],
  ['Blinkit', 'https://blinkit.com/', ['Blinkit']],
  ['CCAVENUE', 'https://www.ccavenue.com/', ['CCAVENUE']],
  ['Cleartrip', 'https://www.cleartrip.com/', ['Cleartrip']],
  ['CRED', 'https://cred.club/', ['CRED', 'Dreamplug Paytech Solutions']],
  ['Delhivery', 'https://www.delhivery.com/', ['Delhivery']],
  ['District', 'https://www.district.in/', ['District']],
  ['EaseMyTrip', 'https://www.easemytrip.com/', ['EaseMyTrip']],
  ['Flipkart', 'https://www.flipkart.com/', ['Flipkart']],
  ['Frido', 'https://myfrido.com/', ['Frido']],
  ['Goibibo', 'https://www.goibibo.com/', ['Goibibo']],
  ['Instamart', 'https://www.swiggy.com/instamart', ['Instamart']],
  ['Kiwi', 'https://gokiwi.in/', ['Kiwi']],
  ['Kotak Mahindra Bank', 'https://www.kotak.com/', ['Kotak Mahindra Bank', 'KOTAK CREDIT CARD']],
  ['Lenskart', 'https://www.lenskart.com/', ['Lenskart']],
  ['LinkedIn', 'https://www.linkedin.com/', ['LinkedIn']],
  ['Miniso', 'https://www.miniso.com/', ['Miniso']],
  ['MobiKwik', 'https://www.mobikwik.com/', ['MobiKwik']],
  ['Myntra', 'https://www.myntra.com/', ['Myntra']],
  ['Narayana Health', 'https://www.narayanahealth.org/', ['Narayana Health']],
  ['Nykaa', 'https://www.nykaa.com/', ['Nykaa']],
  ['OpenAI', 'https://openai.com/', ['OpenAI']],
  ['PAYU', 'https://payu.in/', ['PAYU']],
  ['Plum', 'https://www.plumhq.com/', ['Plum']],
  ['PVR INOX', 'https://www.pvrcinemas.com/', ['PVR INOX']],
  ['Slice', 'https://sliceit.com/', ['Slice', 'Slice Small Finance Bank']],
  ['Tata AIA Life Insurance', 'https://www.tataaia.com/', ['Tata AIA Life Insurance']],
  ['The Souled Store', 'https://www.thesouledstore.com/', ['The Souled Store']],
  ['Urban Company', 'https://www.urbancompany.com/', ['Urban Company']],
  ['YES Bank', 'https://www.yesbank.in/', ['YES Bank']],
  ['Zepto', 'https://www.zeptonow.com/', ['Zepto']],
  ['Domino’s Pizza India', 'https://www.dominos.co.in/', ['Dominos', 'Domino’s Pizza'], 'ADDITIONAL'],
  ['Pizza Hut India', 'https://www.pizzahut.co.in/', ['Pizza Hut'], 'ADDITIONAL'],
  ['Subway India', 'https://subway.in/', ['Subway'], 'ADDITIONAL'],
  ['Haldiram’s', 'https://www.haldirams.com/', ['Haldirams'], 'ADDITIONAL'],
  ['Bikanervala', 'https://bikanervala.com/', [], 'ADDITIONAL'],
  ['Wow! Momo', 'https://www.wowmomo.com/', ['Wow Momo'], 'ADDITIONAL'],
  ['Chaayos', 'https://chaayos.com/', [], 'ADDITIONAL'],
  ['Chai Point', 'https://chaipoint.com/', [], 'ADDITIONAL'],
  ['Theobroma', 'https://theobroma.in/', [], 'ADDITIONAL'],
  ['Barbeque Nation', 'https://www.barbequenation.com/', [], 'ADDITIONAL'],
  ['Baskin Robbins India', 'https://baskinrobbinsindia.com/', ['Baskin Robbins'], 'ADDITIONAL'],
  ['Naturals Ice Cream', 'https://naturalicecreams.in/', [], 'ADDITIONAL'],
  ['The Belgian Waffle Co.', 'https://thebelgianwaffle.co/', ['Belgian Waffle'], 'ADDITIONAL'],
  ['Third Wave Coffee', 'https://www.thirdwavecoffeeroasters.com/', [], 'ADDITIONAL'],
  ['Blue Tokai', 'https://bluetokaicoffee.com/', [], 'ADDITIONAL'],
  ['Cafe Coffee Day', 'https://www.cafecoffeeday.com/', ['CCD'], 'ADDITIONAL'],
  ['Barista Coffee', 'https://www.barista.co.in/', ['Barista'], 'ADDITIONAL'],
  ['Biryani By Kilo', 'https://biryanibykilo.com/', [], 'ADDITIONAL'],
  ['La Pino’z Pizza', 'https://lapinozpizza.in/', ['La Pinoz'], 'ADDITIONAL'],
  ['Faasos', 'https://www.faasos.com/', [], 'ADDITIONAL'],
  ['EatSure', 'https://www.eatsure.com/', [], 'ADDITIONAL'],
  ['BOX8', 'https://box8.in/', ['Box8'], 'ADDITIONAL'],
  ['FreshMenu', 'https://www.freshmenu.com/', [], 'ADDITIONAL'],
  ['DMart', 'https://www.dmartindia.com/', [], 'ADDITIONAL'],
  ['JioMart', 'https://www.jiomart.com/', [], 'ADDITIONAL'],
  ['Nature’s Basket', 'https://www.naturesbasket.co.in/', ['Natures Basket'], 'ADDITIONAL'],
  ['Spencer’s Retail', 'https://www.spencers.in/', ['Spencers'], 'ADDITIONAL'],
  ['More Retail', 'https://www.morestore.com/', ['More Supermarket'], 'ADDITIONAL'],
  ['Star Bazaar', 'https://www.starbazaarindia.com/', [], 'ADDITIONAL'],
  ['LuLu Hypermarket India', 'https://www.luluhypermarket.in/', ['Lulu Hypermarket'], 'ADDITIONAL'],
  ['Ratnadeep', 'https://www.ratnadeep.com/', [], 'ADDITIONAL'],
  ['MilkBasket', 'https://www.milkbasket.com/', [], 'ADDITIONAL'],
  ['Country Delight', 'https://countrydelight.in/', [], 'ADDITIONAL'],
  ['Otipy', 'https://www.otipy.com/', [], 'ADDITIONAL'],
  ['Patanjali', 'https://www.patanjaliayurved.net/', [], 'ADDITIONAL'],
  ['Mother Dairy', 'https://www.motherdairy.com/', [], 'ADDITIONAL'],
  ['Britannia', 'https://www.britannia.co.in/', [], 'ADDITIONAL'],
  ['Parle', 'https://www.parleproducts.com/', [], 'ADDITIONAL'],
  ['MTR Foods', 'https://www.mtrfoods.com/', ['MTR'], 'ADDITIONAL'],
  ['Bikaji', 'https://www.bikaji.com/', [], 'ADDITIONAL'],
  ['Meesho', 'https://www.meesho.com/', [], 'ADDITIONAL'],
  ['AJIO', 'https://www.ajio.com/', ['Ajio'], 'ADDITIONAL'],
  ['Tata CLiQ', 'https://www.tatacliq.com/', [], 'ADDITIONAL'],
  ['Snapdeal', 'https://www.snapdeal.com/', [], 'ADDITIONAL'],
  ['IndiaMART', 'https://www.indiamart.com/', [], 'ADDITIONAL'],
  ['FirstCry', 'https://www.firstcry.com/', [], 'ADDITIONAL'],
  ['Pepperfry', 'https://www.pepperfry.com/', [], 'ADDITIONAL'],
  ['Urban Ladder', 'https://www.urbanladder.com/', [], 'ADDITIONAL'],
  ['Croma', 'https://www.croma.com/', [], 'ADDITIONAL'],
  ['Reliance Digital', 'https://www.reliancedigital.in/', [], 'ADDITIONAL'],
  ['Vijay Sales', 'https://www.vijaysales.com/', [], 'ADDITIONAL'],
  ['Poorvika', 'https://www.poorvika.com/', [], 'ADDITIONAL'],
  ['Westside', 'https://www.westside.com/', [], 'ADDITIONAL'],
  ['Pantaloons', 'https://www.pantaloons.com/', [], 'ADDITIONAL'],
  ['Shoppers Stop', 'https://www.shoppersstop.com/', [], 'ADDITIONAL'],
  ['Lifestyle Stores', 'https://www.lifestylestores.com/', ['Lifestyle'], 'ADDITIONAL'],
  ['Max Fashion', 'https://www.maxfashion.in/', [], 'ADDITIONAL'],
  ['Fabindia', 'https://www.fabindia.com/', [], 'ADDITIONAL'],
  ['Biba', 'https://www.biba.in/', [], 'ADDITIONAL'],
  ['Manyavar', 'https://www.manyavar.com/', [], 'ADDITIONAL'],
  ['Bewakoof', 'https://www.bewakoof.com/', [], 'ADDITIONAL'],
  ['Snitch', 'https://www.snitch.co.in/', [], 'ADDITIONAL'],
  ['Rare Rabbit', 'https://thehouseofrare.com/', [], 'ADDITIONAL'],
  ['Purplle', 'https://www.purplle.com/', [], 'ADDITIONAL'],
  ['Tira Beauty', 'https://www.tirabeauty.com/', ['Tira'], 'ADDITIONAL'],
  ['SUGAR Cosmetics', 'https://www.sugarcosmetics.com/', ['Sugar'], 'ADDITIONAL'],
  ['Mamaearth', 'https://mamaearth.in/', [], 'ADDITIONAL'],
  ['Minimalist', 'https://beminimalist.co/', [], 'ADDITIONAL'],
  ['The Derma Co', 'https://thedermaco.com/', [], 'ADDITIONAL'],
  ['Pilgrim', 'https://discoverpilgrim.com/', [], 'ADDITIONAL'],
  ['Lakmé', 'https://www.lakmeindia.com/', ['Lakme'], 'ADDITIONAL'],
  ['Forest Essentials', 'https://www.forestessentialsindia.com/', [], 'ADDITIONAL'],
  ['Kama Ayurveda', 'https://www.kamaayurveda.in/', [], 'ADDITIONAL'],
  ['Health & Glow', 'https://www.healthandglow.com/', [], 'ADDITIONAL'],
  ['Home Centre India', 'https://www.homecentre.in/', ['Home Centre'], 'ADDITIONAL'],
  ['Wakefit', 'https://www.wakefit.co/', [], 'ADDITIONAL'],
  ['WoodenStreet', 'https://www.woodenstreet.com/', [], 'ADDITIONAL'],
  ['Godrej Interio', 'https://www.godrejinterio.com/', [], 'ADDITIONAL'],
  ['Livspace', 'https://www.livspace.com/', [], 'ADDITIONAL'],
  ['Asian Paints', 'https://www.asianpaints.com/', [], 'ADDITIONAL'],
  ['RentoMojo', 'https://www.rentomojo.com/', [], 'ADDITIONAL'],
  ['Furlenco', 'https://www.furlenco.com/', [], 'ADDITIONAL'],
  ['Chumbak', 'https://www.chumbak.com/', [], 'ADDITIONAL'],
  ['MakeMyTrip', 'https://www.makemytrip.com/', [], 'ADDITIONAL'],
  ['Yatra', 'https://www.yatra.com/', [], 'ADDITIONAL'],
  ['redBus', 'https://www.redbus.in/', ['Redbus'], 'ADDITIONAL'],
  ['IRCTC', 'https://www.irctc.co.in/', [], 'ADDITIONAL'],
  ['Ixigo', 'https://www.ixigo.com/', [], 'ADDITIONAL'],
  ['Ola Cabs', 'https://www.olacabs.com/', ['Ola'], 'ADDITIONAL'],
  ['Rapido', 'https://www.rapido.bike/', [], 'ADDITIONAL'],
  ['Namma Yatri', 'https://www.nammayatri.in/', [], 'ADDITIONAL'],
  ['BluSmart', 'https://blu-smart.com/', [], 'ADDITIONAL'],
  ['Zoomcar', 'https://www.zoomcar.com/', [], 'ADDITIONAL'],
  ['Porter', 'https://porter.in/', [], 'ADDITIONAL'],
  ['Blue Dart', 'https://www.bluedart.com/', ['BlueDart'], 'ADDITIONAL'],
  ['DTDC', 'https://www.dtdc.in/', [], 'ADDITIONAL'],
  ['Ecom Express', 'https://www.ecomexpress.in/', [], 'ADDITIONAL'],
  ['XpressBees', 'https://www.xpressbees.com/', [], 'ADDITIONAL'],
  ['Shiprocket', 'https://www.shiprocket.in/', [], 'ADDITIONAL'],
  ['Treebo', 'https://www.treebo.com/', [], 'ADDITIONAL'],
  ['FabHotels', 'https://www.fabhotels.com/', [], 'ADDITIONAL'],
  ['Taj Hotels', 'https://www.tajhotels.com/', ['Taj'], 'ADDITIONAL'],
  ['ITC Hotels', 'https://www.itchotels.com/', [], 'ADDITIONAL'],
  ['The Oberoi', 'https://www.oberoihotels.com/', ['Oberoi Hotels'], 'ADDITIONAL'],
  ['Lemon Tree Hotels', 'https://www.lemontreehotels.com/', [], 'ADDITIONAL'],
  ['Cashfree Payments', 'https://www.cashfree.com/', ['Cashfree'], 'ADDITIONAL'],
  ['Pine Labs', 'https://www.pinelabs.com/', [], 'ADDITIONAL'],
  ['BillDesk', 'https://www.billdesk.com/', [], 'ADDITIONAL'],
  ['Instamojo', 'https://www.instamojo.com/', [], 'ADDITIONAL'],
  ['Easebuzz', 'https://easebuzz.in/', [], 'ADDITIONAL'],
  ['Juspay', 'https://juspay.in/', [], 'ADDITIONAL'],
  ['BHIM', 'https://www.bhimupi.org.in/', ['BHIM UPI'], 'ADDITIONAL'],
  ['Freecharge', 'https://www.freecharge.in/', [], 'ADDITIONAL'],
  ['State Bank of India', 'https://sbi.co.in/', ['SBI'], 'ADDITIONAL'],
  ['Bank of Baroda', 'https://www.bankofbaroda.in/', ['BOB'], 'ADDITIONAL'],
  ['Punjab National Bank', 'https://www.pnbindia.in/', ['PNB'], 'ADDITIONAL'],
  ['Canara Bank', 'https://canarabank.com/', [], 'ADDITIONAL'],
  ['Union Bank of India', 'https://www.unionbankofindia.co.in/', [], 'ADDITIONAL'],
  ['Federal Bank', 'https://www.federalbank.co.in/', [], 'ADDITIONAL'],
  ['IndusInd Bank', 'https://www.indusind.com/', [], 'ADDITIONAL'],
  ['IDFC FIRST Bank', 'https://www.idfcfirstbank.com/', [], 'ADDITIONAL'],
  ['Groww', 'https://groww.in/', [], 'ADDITIONAL'],
  ['Upstox', 'https://upstox.com/', [], 'ADDITIONAL'],
  ['Angel One', 'https://www.angelone.in/', [], 'ADDITIONAL'],
  ['Dhan', 'https://dhan.co/', [], 'ADDITIONAL'],
  ['Navi', 'https://navi.com/', [], 'ADDITIONAL'],
  ['Bajaj Finserv', 'https://www.bajajfinserv.in/', [], 'ADDITIONAL'],
  ['Tata Capital', 'https://www.tatacapital.com/', [], 'ADDITIONAL'],
  ['ET Money', 'https://www.etmoney.com/', [], 'ADDITIONAL'],
  ['CoinDCX', 'https://coindcx.com/', [], 'ADDITIONAL'],
  ['Policybazaar', 'https://www.policybazaar.com/', [], 'ADDITIONAL'],
  ['ACKO', 'https://www.acko.com/', [], 'ADDITIONAL'],
  ['Digit Insurance', 'https://www.godigit.com/', ['Go Digit'], 'ADDITIONAL'],
  ['Disney+ Hotstar', 'https://www.hotstar.com/in/', ['Hotstar'], 'ADDITIONAL'],
  ['Sony LIV', 'https://www.sonyliv.com/', [], 'ADDITIONAL'],
  ['ZEE5', 'https://www.zee5.com/', [], 'ADDITIONAL'],
  ['JioSaavn', 'https://www.jiosaavn.com/', [], 'ADDITIONAL'],
  ['Gaana', 'https://gaana.com/', [], 'ADDITIONAL'],
  ['MX Player', 'https://www.mxplayer.in/', [], 'ADDITIONAL'],
  ['Sun NXT', 'https://www.sunnxt.com/', [], 'ADDITIONAL'],
  ['Hoichoi', 'https://www.hoichoi.tv/', [], 'ADDITIONAL'],
  ['Pocket FM', 'https://pocketfm.com/', [], 'ADDITIONAL'],
  ['Kuku FM', 'https://kukufm.com/', [], 'ADDITIONAL'],
  ['Dream11', 'https://www.dream11.com/', [], 'ADDITIONAL'],
  ['MPL', 'https://www.mpl.live/', [], 'ADDITIONAL'],
  ['WinZO', 'https://www.winzogames.com/', [], 'ADDITIONAL'],
  ['Cricbuzz', 'https://www.cricbuzz.com/', [], 'ADDITIONAL'],
  ['Vi', 'https://www.myvi.in/', ['Vodafone Idea'], 'ADDITIONAL'],
  ['ACT Fibernet', 'https://www.actcorp.in/', [], 'ADDITIONAL'],
  ['Tata Play', 'https://www.tataplay.com/', [], 'ADDITIONAL'],
  ['Dish TV', 'https://www.dishtv.in/', [], 'ADDITIONAL'],
  ['Apollo Hospitals', 'https://www.apollohospitals.com/', [], 'ADDITIONAL'],
  ['Apollo Pharmacy', 'https://www.apollopharmacy.in/', [], 'ADDITIONAL'],
  ['Tata 1mg', 'https://www.1mg.com/', ['1mg'], 'ADDITIONAL'],
  ['PharmEasy', 'https://pharmeasy.in/', [], 'ADDITIONAL'],
  ['Netmeds', 'https://www.netmeds.com/', [], 'ADDITIONAL'],
  ['MedPlus Mart', 'https://www.medplusmart.com/', ['MedPlus'], 'ADDITIONAL'],
  ['Practo', 'https://www.practo.com/', [], 'ADDITIONAL'],
  ['MediBuddy', 'https://www.medibuddy.in/', [], 'ADDITIONAL'],
  ['Cult.fit', 'https://www.cult.fit/', ['Cultfit'], 'ADDITIONAL'],
  ['HealthifyMe', 'https://www.healthifyme.com/', [], 'ADDITIONAL'],
  ['Manipal Hospitals', 'https://www.manipalhospitals.com/', [], 'ADDITIONAL'],
  ['Fortis Healthcare', 'https://www.fortishealthcare.com/', [], 'ADDITIONAL'],
  ['Max Healthcare', 'https://www.maxhealthcare.in/', [], 'ADDITIONAL'],
  ['Dr Lal PathLabs', 'https://www.lalpathlabs.com/', [], 'ADDITIONAL'],
  ['Thyrocare', 'https://www.thyrocare.com/', [], 'ADDITIONAL'],
  ['Metropolis Healthcare', 'https://www.metropolisindia.com/', [], 'ADDITIONAL'],
  ['Titan Eye+', 'https://www.titaneyeplus.com/', ['Titan Eyeplus'], 'ADDITIONAL'],
  ['Clove Dental', 'https://clovedental.in/', [], 'ADDITIONAL'],
  ['Vedantu', 'https://www.vedantu.com/', [], 'ADDITIONAL'],
  ['Physics Wallah', 'https://www.pw.live/', ['PW'], 'ADDITIONAL'],
  ['upGrad', 'https://www.upgrad.com/', [], 'ADDITIONAL'],
  ['Simplilearn', 'https://www.simplilearn.com/', [], 'ADDITIONAL'],
  ['Great Learning', 'https://www.mygreatlearning.com/', [], 'ADDITIONAL'],
  ['Scaler', 'https://www.scaler.com/', [], 'ADDITIONAL'],
  ['Coding Ninjas', 'https://www.codingninjas.com/', [], 'ADDITIONAL'],
  ['GeeksforGeeks', 'https://www.geeksforgeeks.org/', ['GFG'], 'ADDITIONAL'],
  ['Testbook', 'https://testbook.com/', [], 'ADDITIONAL'],
  ['Adda247', 'https://www.adda247.com/', [], 'ADDITIONAL'],
  ['Cuemath', 'https://www.cuemath.com/', [], 'ADDITIONAL'],
  ['Extramarks', 'https://www.extramarks.com/', [], 'ADDITIONAL'],
];

const manifestPath = resolve('supabase/merchant-logo-catalog.json');
const assetDirectory = resolve(
  process.argv[2] ?? '/tmp/money-manager-merchant-logo-assets',
);
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));

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

function decodeHtml(value) {
  return value
    .replaceAll('&amp;', '&')
    .replaceAll('&#x2F;', '/')
    .replaceAll('&#47;', '/');
}

function iconCandidates(html, pageUrl) {
  const links = [...html.matchAll(/<link\b[^>]*>/gi)].map((match) => match[0]);
  const candidates = [];
  for (const link of links) {
    const rel = /\brel\s*=\s*["']([^"']+)["']/i.exec(link)?.[1] ?? '';
    if (!/(?:^|\s)(?:icon|shortcut icon|apple-touch-icon)(?:\s|$)/i.test(rel)) continue;
    const href = /\bhref\s*=\s*["']([^"']+)["']/i.exec(link)?.[1];
    if (!href || href.startsWith('data:')) continue;
    const sizes = /\bsizes\s*=\s*["']([^"']+)["']/i.exec(link)?.[1] ?? '';
    const numericSize = Number.parseInt(sizes, 10) || 0;
    try {
      candidates.push({
        url: new URL(decodeHtml(href), pageUrl).toString(),
        score: /apple-touch-icon/i.test(rel) ? 10_000 + numericSize : numericSize,
      });
    } catch {
      // Ignore malformed HTML URLs.
    }
  }
  candidates.sort((left, right) => right.score - left.score);
  candidates.push({ url: new URL('/favicon.ico', pageUrl).toString(), score: -1 });
  return [...new Map(candidates.map((candidate) => [candidate.url, candidate])).values()];
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
  if (bytes[0] === 0x89 && bytes.subarray(1, 4).toString('ascii') === 'PNG') return 'image/png';
  if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return 'image/jpeg';
  if (bytes.subarray(0, 4).toString('ascii') === 'RIFF'
    && bytes.subarray(8, 12).toString('ascii') === 'WEBP') return 'image/webp';
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
  if (contentType === 'image/svg+xml') return 'svg';
  if (contentType === 'image/png') return 'png';
  if (contentType === 'image/jpeg') return 'jpg';
  if (contentType === 'image/webp') return 'webp';
  if (contentType === 'image/avif') return 'avif';
  return 'ico';
}

function writeSeedMigration(rows) {
  const existingCount = rows.filter((row) => row.coverage === 'EXISTING').length;
  const additionalCount = rows.filter((row) => row.coverage === 'ADDITIONAL').length;
  const statements = [
    '-- Generated by scripts/build-merchant-logo-catalog.mjs and scripts/fetch-official-merchant-logos.mjs.',
    `-- ${existingCount} current-brand mappings + ${additionalCount} additional catalogue brands.`,
    '',
  ];
  for (const row of rows) {
    statements.push(
      `insert into public.merchant_logos (`,
      `  canonical_name, normalized_name, website_url, source_url, source_kind,`,
      `  source_license, storage_provider, brand_color`,
      `) values (`,
      `  ${sql(row.canonical_name)}, ${sql(row.normalized_name)}, ${sql(row.website_url ?? row.source_url)},`,
      `  ${sql(row.source_url)}, ${sql(row.source_kind)}, ${sql(row.source_license)},`,
      `  'SUPABASE', ${sql(row.brand_color)}`,
      `)`,
      `on conflict (normalized_name) do update set`,
      `  canonical_name = excluded.canonical_name,`,
      `  website_url = excluded.website_url,`,
      `  source_url = excluded.source_url,`,
      `  source_kind = excluded.source_kind,`,
      `  source_license = excluded.source_license,`,
      `  storage_provider = excluded.storage_provider,`,
      `  brand_color = excluded.brand_color,`,
      `  is_active = true;`,
    );
    for (const alias of row.aliases) {
      const searchKey = normalize(alias);
      if (!searchKey) continue;
      statements.push(
        `insert into public.merchant_logo_aliases (search_key, merchant_logo_id)`,
        `select ${sql(searchKey)}, id`,
        `from public.merchant_logos where normalized_name = ${sql(row.normalized_name)}`,
        `on conflict (search_key) do update set merchant_logo_id = excluded.merchant_logo_id;`,
      );
    }
    statements.push('');
  }
  writeFileSync(
    resolve('supabase/migrations/20260724101000_seed_merchant_logo_catalog.sql'),
    `${statements.join('\n')}\n`,
  );
}

const headers = {
  'User-Agent': 'Mozilla/5.0 (compatible; MoneyManagerLogoAudit/1.0)',
  Accept: 'text/html,application/xhtml+xml,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8',
};

const officialRows = [];
const failures = [];
async function fetchTarget([canonicalName, websiteUrl, aliases, coverage = 'EXISTING']) {
  try {
    const page = await fetch(websiteUrl, { headers, redirect: 'follow', signal: AbortSignal.timeout(15_000) });
    if (!page.ok) throw new Error(`page returned ${page.status}`);
    const html = await page.text();
    let asset = null;
    for (const candidate of iconCandidates(html, page.url)) {
      try {
        const response = await fetch(candidate.url, {
          headers,
          redirect: 'follow',
          signal: AbortSignal.timeout(15_000),
        });
        if (!response.ok) continue;
        const bytes = Buffer.from(await response.arrayBuffer());
        if (bytes.length === 0 || bytes.length > 1_048_576) continue;
        // Do not trust response headers or URL extensions. Anti-bot pages can
        // return HTML while claiming to be a favicon or another image.
        const contentType = detectContentType(bytes);
        if (!contentType) continue;
        asset = { bytes, contentType, sourceUrl: response.url };
        break;
      } catch {
        // Try the next icon declared by the official page.
      }
    }
    if (!asset) throw new Error('no usable official icon was declared');

    const hash = createHash('sha256').update(asset.bytes).digest('hex');
    const objectPath = `official-sites/${slugify(canonicalName)}-${hash.slice(0, 12)}.${extensionFor(asset.contentType)}`;
    const destination = join(assetDirectory, objectPath);
    mkdirSync(dirname(destination), { recursive: true });
    writeFileSync(destination, asset.bytes);
    officialRows.push({
      canonical_name: canonicalName,
      normalized_name: normalize(canonicalName),
      coverage,
      aliases: [...new Set([canonicalName, ...aliases])],
      website_url: page.url,
      source_url: asset.sourceUrl,
      source_kind: 'OFFICIAL_SITE_ICON',
      source_license: null,
      brand_color: null,
      object_path: objectPath,
      content_type: asset.contentType,
      content_sha256: hash,
    });
  } catch (error) {
    failures.push({
      canonical_name: canonicalName,
      website_url: websiteUrl,
      reason: error instanceof Error ? error.message : String(error),
    });
  }
}

let nextTargetIndex = 0;
async function worker() {
  while (nextTargetIndex < TARGETS.length) {
    const target = TARGETS[nextTargetIndex];
    nextTargetIndex += 1;
    await fetchTarget(target);
  }
}
await Promise.all(Array.from({ length: 10 }, () => worker()));

const rowsByName = new Map(manifest.rows.map((row) => [row.normalized_name, row]));
for (const row of officialRows) {
  rowsByName.set(row.normalized_name, {
    ...rowsByName.get(row.normalized_name),
    ...row,
  });
}
const mergedRows = [...rowsByName.values()];
const existingRows = mergedRows.filter((row) => row.coverage === 'EXISTING');
const coreAdditionalRows = mergedRows
  .filter((row) => row.coverage === 'ADDITIONAL' && !row.catalog_batch)
  .sort((left, right) => {
    const leftOfficial = left.source_kind === 'OFFICIAL_SITE_ICON' ? 0 : 1;
    const rightOfficial = right.source_kind === 'OFFICIAL_SITE_ICON' ? 0 : 1;
    return leftOfficial - rightOfficial;
  })
  .slice(0, 300);
const batchRows = mergedRows
  .filter((row) => row.coverage === 'ADDITIONAL' && row.catalog_batch)
  .sort((left, right) => {
    const batchOrder = left.catalog_batch.localeCompare(right.catalog_batch);
    return batchOrder || (left.selection_rank ?? 0) - (right.selection_rank ?? 0);
  });
const additionalRows = [...coreAdditionalRows, ...batchRows];
const rows = [...existingRows, ...additionalRows];
manifest.generated_at = new Date().toISOString();
manifest.existing_priority_count = existingRows.length;
manifest.additional_count = additionalRows.length;
manifest.rows = rows;
manifest.official_site_failures = failures;
writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
writeSeedMigration([...existingRows, ...coreAdditionalRows]);

console.log(JSON.stringify({
  attempted: TARGETS.length,
  fetched: officialRows.length,
  failed: failures.length,
  failures,
  existingPriority: manifest.existing_priority_count,
  additional: manifest.additional_count,
  total: rows.length,
}, null, 2));
