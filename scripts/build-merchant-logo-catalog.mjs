import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

const SIMPLE_ICONS_VERSION = '16.27.0';
const ADDITIONAL_TARGET = 300;

// Ordered for Indian transaction relevance first, then international brands
// commonly encountered by Indian users. Missing/retired Simple Icons slugs
// are ignored; the build fails unless 300 verified additions remain.
const ADDITIONAL_CANDIDATE_SLUGS = `
airindia airtel akasaair amul axisbank bigbasket boat byjus dunzo indigo jio
mahindra oyo tata tcs unacademy unstop wazirx zoho
cashfree afterpay adyen alipay americanexpress applepay braintree cashapp
coinbase dinersclub discover googlepay klarna mastercard paypal revolut
samsungpay stripe unionpay visa wise westernunion xendit square
adidas alibabadotcom aliexpress asics asos bata bestbuy bigcommerce burberry
carrefour cartier chanel costco crocs decathlon dior ebay etsy farfetch gap
gucci handm ikea jd nike newbalance puma reebok rolex shein shopify skechers
target temu thenorthface underarmour uniqlo vans walmart wish zara zalando
burgerking cocacola costacoffee deliveroo dominos doordash foodpanda grab
justeat kfc krispykreme mcdonalds nestle pepsi pizzahut popeyes redbull
starbucks subway tacobell timhortons ubereats
aeroflot aeromexico airasia aircanada airchina airfrance airserbia
airtransat americanairlines ana britishairways bookingdotcom emirates
etihadairways finnair iberia japanairlines klm lufthansa qatarairways ryanair
singaporeairlines southwestairlines swiss thai tripadvisor tripdotcom trivago
turkishairlines unitedairlines wizzair
dhl fedex ups
audi bmw bugatti cadillac chevrolet citroen fiat ford honda hyundai jeep kia
lamborghini landrover mazda mercedes mg mitsubishi nissan peugeot porsche
renault rollsroyce skoda suzuki tesla toyota volkswagen volvo yamaha
abbott abbvie astrazeneca bayer gsk johnsonandjohnson merck pfizer roche sanofi
siemenshealthineers
1password adobe audible canva claude coursera deezer discord disneyplus
dropbox duolingo edx evernote facebook fitbit garmin github gitlab google
googledrive googlemaps googlemeet googleone googlephotos googlesheets
googlewallet grammarly headspace hulu imdb instagram jetbrains khanacademy
medium microsoft microsoft365 onedrive notion patreon pinterest plex
primevideo quora reddit salesforce slack snapchat soundcloud spotify steam
strava telegram tiktok tinder twitch udacity udemy vimeo whatsapp wikipedia
youtubemusic zoom
att motorola nokia oneplus oppo realme samsung tmobile vodafone vivo xiaomi
crunchyroll ea epicgames f1 nintendo playstation rockstargames ubisoft xbox
bosch dyson lg panasonic philips siemens sony whirlpool
asana calendly cloudflare digitalocean figma godaddy hubspot mailchimp miro
mondaydotcom oracle squarespace twilio vercel wix wordpress zapier zendesk
accor airarabia airnewzealand alltrails applmusic apptv arlo autodesk avast
avira babbel bandcamp beats beatsbydre behance bitdefender bitly blackmagicdesign
bose brave broadcom buffer calvinklein canon casio cisco coach converse
crowdstrike dacia daihatsu dailymotion dell dolby dreamstime dribbble
duckduckgo ea sports epson eventbrite fiverr fossil garmin generalmotors
giphy glassdoor gofundme goodreads gotomeeting groupon hackerrank harley
hermes huawei indeed intel intuit kaggle kaspersky lenovo line logitech
loom loreal louisvuitton macys mastercard mercedesbenz messenger meta
michelin mini moncler mongodb monster monsterenergy mozilla nespresso
netapp netlify norton nvidia okta olympics opera paramountplus patreon
payoneer peloton peugeot pinterest playstation portableapps protonmail
ralphlauren razer redhat renault samsung seagate shell siemens signal
skype sonos soundcloud spacex speedtest stackoverflow starbucks suzuki
symantec telegram tesco ticketmaster tomtom tripadvisor trustpilot
tupperware uber eats ulule unilever universalpictures viber virgin
virginatlantic vk vlcmediaplayer waze webex wechat weetransfer
xiaomi yahoo yelp zelle
acer
`.trim().split(/\s+/);

// Existing canonical merchants get first priority. Multiple database names
// may intentionally point at one exact brand mark.
const EXISTING_BRAND_ALIASES = {
  airbnb: ['Airbnb'],
  amazon: ['Amazon', 'Amazon Smart Store'],
  amazonpay: ['Amazon Pay'],
  anthropic: ['Anthropic'],
  apple: ['Apple Media Services'],
  atlassian: ['Atlassian'],
  bookmyshow: ['BookMyShow'],
  googlecloud: ['Google Cloud'],
  googleplay: ['Google Play'],
  hdfcbank: ['HDFC Bank', 'HDFC CREDIT CARD'],
  icicibank: ['ICICI Bank', 'ICICI CREDIT CARD'],
  linkedin: ['LinkedIn'],
  netflix: ['Netflix'],
  openai: ['OpenAI'],
  paytm: ['PAYTM WALLET'],
  phonepe: ['PhonePe'],
  razorpay: ['Razorpay', 'RAZORPAY'],
  relianceindustrieslimited: ['Reliance Retail'],
  swiggy: ['Swiggy'],
  uber: ['Uber'],
  youtube: ['YouTube'],
  zerodha: ['ZERODHA'],
  zomato: ['Zomato'],
};

const CURRENT_GLOBAL_NAMES = new Set(
  `
Abdul Kadir|Aditi Sharma|Aditya Basavaraj Biradar|Airbnb|Akbar Travels|Akshay S
Alcool & Cubes|Amazon|Amazon Pay|Amazon Smart Store|AMC Enterprises|Amit Kumar Agrawal
Anomaly SF|Anthropic|Apple Media Services|April Moon Retail|Atlassian|Balaji Mega Mart
Basavaraju|BHARATPE|Bistro|Blinkit|BookMyShow|CCAVENUE|CHALUMESWAMY|CHANDU N
Chowde Gowda|Cleartrip|CRED|Delhivery|Devaraja K R|Dikshit Khandelwal|District
Divya Khandelwal|Dreamplug Paytech Solutions|EaseMyTrip|Eramma|Flipkart|Frido
Gangubai|Goibibo|Google Cloud|Google Play|Goplakrishnan E|Goyal Hospital|Goyal Medicals
GURURAJ SHIVANANDAPP|Hanumamma|Harkesh Harkesh|HDFC Bank|HDFC CREDIT CARD
Husensab Kallibaddi|ICICI Bank|ICICI CREDIT CARD|Indra Kumar B S|Instamart
Juice Junction|Kantharaju BB|Kaushik H B|Khetan Diagnostic Center|Kiwi
KOTAK CREDIT CARD|Kotak Mahindra Bank|Krishnamurthy B M|Kumara|KURUBA ANITHA
Lenskart|LinkedIn|Lounge One|MAHADEVAPPA|Mahesh|Manjunath M|Manjunath R|Miniso
MobiKwik|Mukesh Kumar Mittal|Myntra|MySivi|NAGENDRA|Narayana Health|Nerlu Cafe
Netflix|Nykaa|Ola Filling Station|OpenAI|Ownly|PAYTM WALLET|PAYU|PhonePe|PINTU
Plum|Pramod Singh|PRAVEEN KUMAR M|Printq Digital Imaging|Priyanka Sharma
Protean eGov Technologies|PVR INOX|R Babu|Raj Kirana Store|Rajanna C|Ravi Kumar C S
Razorpay|RAZORPAY|Reliance Retail|ROSHAN LAL RAIGAR|S G & Sons|SACHIN KUMARA H K
Sahanaj Bano|Sameer Patel|Sandeep Singh|Sanjunath|Santhosh G|Santhosh Kumar K
SANTHOSHKUMAR S|Satyam Verma|SHAIJU THANKAPPAN|SHIVA SHANTH D|SHREYA GUPTA
SITA DEVI MINA|Slice|Slice Small Finance Bank|Sohan Lal Saini|Sri Lakshmi Enterprises
Subhash Chand Meena|Sunil Kumawat|SuperYou|Swiggy|Tata AIA Life Insurance
The Souled Store|Tomo AI|Travelogy India|Uber|Urban Company|Usarambha Education
UTKARSH SINGH|Vasu Khandelwal|Veda Sankalpa Seva Trust|VENKATESH RUDRAPPA
Wishvish Foods|YES Bank|YouTube|Zepto|ZERODHA|Zomato
`
    .trim()
    .split('|')
    .map((value) => normalize(value)),
);

const BLOCKED_SOURCE_HOSTS = [
  'wikipedia.org',
  'wikimedia.org',
  'worldvectorlogo.com',
  'seeklogo.com',
  'brandsoftheworld.com',
  'logowik.com',
  'svgrepo.com',
];

function normalize(value) {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function sql(value) {
  if (value === null || value === undefined) return 'null';
  return `'${String(value).replaceAll("'", "''")}'`;
}

function sourceIsTrusted(source) {
  try {
    const host = new URL(source).hostname.toLowerCase();
    return !BLOCKED_SOURCE_HOSTS.some((blocked) => host === blocked || host.endsWith(`.${blocked}`));
  } catch {
    return false;
  }
}

function licenseLabel(icon) {
  if (!icon.license) return null;
  return icon.license.type ?? icon.license.url ?? null;
}

function decorateSvg(svg, icon) {
  const title = icon.title.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
  return svg
    .replace('<svg ', `<svg fill="#${icon.hex}" `)
    .replace('>', `><title>${title}</title>`);
}

function buildRow(icon, coverage, aliases, assetDirectory) {
  const sourceSvg = readFileSync(join(assetDirectory, '..', 'icons', `${icon.slug}.svg`), 'utf8');
  const svg = decorateSvg(sourceSvg, icon);
  const hash = createHash('sha256').update(svg).digest('hex');
  const objectPath = `simple-icons-${SIMPLE_ICONS_VERSION}/${icon.slug}-${hash.slice(0, 12)}.svg`;
  return {
    canonical_name: icon.title,
    normalized_name: normalize(icon.title),
    coverage,
    aliases: [...new Set([icon.title, ...aliases])],
    simple_icons_slug: icon.slug,
    simple_icons_version: SIMPLE_ICONS_VERSION,
    source_url: icon.source,
    source_kind: 'SIMPLE_ICONS',
    source_license: licenseLabel(icon),
    brand_color: `#${icon.hex.toUpperCase()}`,
    object_path: objectPath,
    content_type: 'image/svg+xml',
    content_sha256: hash,
    svg,
  };
}

const simpleIconsRoot = resolve(process.argv[2] ?? '/tmp/money-manager-simple-icons/package');
const outputAssetDirectory = resolve(
  process.argv[3] ?? '/tmp/money-manager-merchant-logo-assets',
);
const metadataPath = join(simpleIconsRoot, 'data', 'simple-icons.json');
if (!existsSync(metadataPath)) {
  throw new Error(`Simple Icons metadata was not found at ${metadataPath}`);
}

const metadata = JSON.parse(readFileSync(metadataPath, 'utf8'));
const bySlug = new Map(metadata.map((icon) => [icon.slug, icon]));
mkdirSync(outputAssetDirectory, { recursive: true });

const rows = [];
for (const [slug, aliases] of Object.entries(EXISTING_BRAND_ALIASES)) {
  const icon = bySlug.get(slug);
  if (!icon || !sourceIsTrusted(icon.source)) continue;
  rows.push(buildRow(icon, 'EXISTING', aliases, join(simpleIconsRoot, 'data')));
}

const usedNames = new Set(rows.map((row) => row.normalized_name));
const missingCandidates = [];
for (const slug of [...new Set(ADDITIONAL_CANDIDATE_SLUGS)]) {
  if (rows.filter((row) => row.coverage === 'ADDITIONAL').length >= ADDITIONAL_TARGET) break;
  const icon = bySlug.get(slug);
  if (!icon) {
    missingCandidates.push(slug);
    continue;
  }
  const normalizedName = normalize(icon.title);
  if (
    usedNames.has(normalizedName)
    || CURRENT_GLOBAL_NAMES.has(normalizedName)
    || !sourceIsTrusted(icon.source)
  ) {
    continue;
  }
  const aka = icon.aliases?.aka ?? [];
  const row = buildRow(icon, 'ADDITIONAL', aka, join(simpleIconsRoot, 'data'));
  rows.push(row);
  usedNames.add(row.normalized_name);
}

const additionalCount = rows.filter((row) => row.coverage === 'ADDITIONAL').length;
if (additionalCount !== ADDITIONAL_TARGET) {
  throw new Error(
    `Expected ${ADDITIONAL_TARGET} additional merchants, found ${additionalCount}. `
      + `Missing candidate slugs: ${missingCandidates.join(', ')}`,
  );
}

for (const row of rows) {
  const destination = join(outputAssetDirectory, row.object_path);
  mkdirSync(dirname(destination), { recursive: true });
  writeFileSync(destination, row.svg);
}

const manifestRows = rows.map(({ svg: _svg, ...row }) => row);
const manifestPath = resolve('supabase/merchant-logo-catalog.json');
writeFileSync(
  manifestPath,
  `${JSON.stringify({
    generated_at: new Date().toISOString(),
    simple_icons_version: SIMPLE_ICONS_VERSION,
    existing_priority_count: rows.length - additionalCount,
    additional_count: additionalCount,
    rows: manifestRows,
  }, null, 2)}\n`,
);

const statements = [
  '-- Generated by scripts/build-merchant-logo-catalog.mjs.',
  `-- ${rows.length - additionalCount} current-brand mappings + ${additionalCount} additional catalogue brands.`,
  '',
];

for (const row of manifestRows) {
  statements.push(
    `insert into public.merchant_logos (`,
    `  canonical_name, normalized_name, website_url, source_url, source_kind,`,
    `  source_license, storage_provider, brand_color`,
    `) values (`,
    `  ${sql(row.canonical_name)}, ${sql(row.normalized_name)}, ${sql(row.source_url)},`,
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

console.log(JSON.stringify({
  rows: rows.length,
  existingPriority: rows.length - additionalCount,
  additional: additionalCount,
  assets: outputAssetDirectory,
  manifest: manifestPath,
}, null, 2));
