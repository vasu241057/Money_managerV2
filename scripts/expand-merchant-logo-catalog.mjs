import { createHash } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import sharp from 'sharp';
import {
  SECOND_BATCH_CANDIDATES,
  SECOND_BATCH_EVIDENCE,
} from './merchant-logo-batch-2-candidates.mjs';
import {
  THIRD_BATCH_CANDIDATES,
  THIRD_BATCH_EVIDENCE,
} from './merchant-logo-batch-3-candidates.mjs';

const useSecondBatch = process.argv.includes('--batch-2');
const useThirdBatch = process.argv.includes('--batch-3');
if (useSecondBatch && useThirdBatch) {
  throw new Error('Choose only one batch flag');
}
const BATCH_ID = useThirdBatch
  ? 'india-next-100-2026-08-batch-3'
  : useSecondBatch
    ? 'india-next-100-2026-08-batch-2'
    : 'india-next-100-2026-08';
const BATCH_TARGET = 100;
const SIMPLE_ICONS_VERSION = '16.27.0';
const DEFAULT_ASSET_DIRECTORY = '/tmp/money-manager-merchant-logo-assets';
const DEFAULT_SIMPLE_ICONS_ROOT = '/tmp/money-manager-simple-icons/package';

const EVIDENCE = {
  shopping: {
    label: 'Apple App Store India Shopping chart, 9 August 2026',
    url: 'https://apps.apple.com/in/iphone/charts/6024',
  },
  food: {
    label: 'Apple App Store India Food & Drink chart, 9 August 2026',
    url: 'https://apps.apple.com/in/iphone/charts/6023',
  },
  travel: {
    label: 'Apple App Store India Travel chart, 9 August 2026',
    url: 'https://apps.apple.com/in/iphone/charts/6003',
  },
  finance: {
    label: 'Apple App Store India Finance chart, 9 August 2026',
    url: 'https://apps.apple.com/in/iphone/charts/6015',
  },
  entertainment: {
    label: 'Apple App Store India Entertainment chart, 9 August 2026',
    url: 'https://apps.apple.com/in/iphone/charts/6016',
  },
  ecommerce: {
    label: 'Similarweb India Ecommerce & Shopping ranking, June 2026',
    url: 'https://www.similarweb.com/top-websites/india/e-commerce-and-shopping/',
  },
  brandz: {
    label: 'Kantar BrandZ India Top 100, 2025',
    url: 'https://www.kantar.com/campaigns/brandz/-/media/AB948C5121A54C28914E1B08C8B6EB1E.ashx',
  },
  coverage: {
    label: 'High-frequency India transaction category coverage',
    url: 'https://www.kantar.com/campaigns/brandz/india',
  },
  ...SECOND_BATCH_EVIDENCE,
  ...THIRD_BATCH_EVIDENCE,
};

function candidate(canonicalName, websiteUrl, aliases, evidenceKey, options = {}) {
  return {
    canonicalName,
    websiteUrl,
    aliases,
    evidenceKey,
    simpleIconSlug: options.simpleIconSlug ?? null,
    appStoreTerm: options.appStoreTerm ?? canonicalName,
    appStoreId: options.appStoreId ?? null,
    expectedSellerTokens: options.expectedSellerTokens ?? [],
    evidenceRank: options.evidenceRank ?? null,
    transactionRelevance: options.transactionRelevance ?? 'MEDIUM',
    preferAppStore: options.preferAppStore === true,
    disableAppStore: options.disableAppStore === true,
  };
}

// Ranked before acquisition. The pool is deliberately larger than 100:
// inaccessible, duplicate, misleading, or invalid assets are rejected and the
// next verified candidate takes their place. Current India charts lead, then
// Kantar's consumer-facing Top 100 gaps, followed by broad transaction coverage.
const FIRST_BATCH_CANDIDATES = [
  candidate('Shopsy', 'https://www.shopsy.in/', [], 'ecommerce'),
  candidate('Meesho', 'https://www.meesho.com/', [], 'shopping'),
  candidate('OLX India', 'https://www.olx.in/', ['OLX'], 'shopping'),
  candidate('Savana', 'https://www.savana.com/', [], 'shopping'),
  candidate('Cashify', 'https://www.cashify.in/', [], 'shopping'),
  candidate('SHEIN India', 'https://www.sheinindia.in/', ['SHEIN'], 'shopping'),
  candidate('FirstClub', 'https://firstclub.in/', ['First Club'], 'shopping', { appStoreId: '6744534743' }),
  candidate('Tata Neu', 'https://www.tataneu.com/', ['TataNeu'], 'shopping'),
  candidate('Zudio', 'https://www.zudio.com/', [], 'brandz', {
    appStoreTerm: 'ZUDIO FANZ',
    preferAppStore: true,
  }),
  candidate('Vishal Mega Mart', 'https://www.vishalmegamart.com/', [], 'brandz'),
  candidate('Max Fashion', 'https://www.maxfashion.in/', [], 'shopping'),
  candidate('Wakefit', 'https://www.wakefit.co/', [], 'shopping'),
  candidate('Forest Essentials', 'https://www.forestessentialsindia.com/', [], 'shopping'),
  candidate('Health & Glow', 'https://www.healthandglow.com/', ['Health and Glow'], 'shopping'),
  candidate('CaratLane', 'https://www.caratlane.com/', ['Carat Lane'], 'brandz'),
  candidate('Tanishq', 'https://www.tanishq.co.in/', [], 'brandz', { appStoreId: '1494086207' }),
  candidate('Kalyan Jewellers', 'https://www.kalyanjewellers.net/', [], 'brandz'),
  candidate('Decathlon India', 'https://www.decathlon.in/', ['Decathlon'], 'shopping'),
  candidate('Skechers India', 'https://www.skechers.in/', ['Skechers'], 'shopping'),
  candidate('Crocs India', 'https://www.crocs.in/', ['Crocs'], 'shopping'),

  candidate('Toing', 'https://www.toing.com/', [], 'food', { appStoreId: '6748918399' }),
  candidate('Swish', 'https://swish.in/', [], 'food', { appStoreId: '6504881715' }),
  candidate('EazyDiner', 'https://www.eazydiner.com/', ['Eazy Diner'], 'food', { appStoreId: '1018594093' }),
  candidate('EatClub', 'https://eatclub.in/', ['Eat Club'], 'food', { appStoreId: '1535583068' }),
  candidate('Licious', 'https://www.licious.in/', [], 'food', { appStoreId: '1052440342' }),
  candidate('magicpin', 'https://magicpin.in/', ['Magicpin'], 'food', { appStoreId: '1171775851' }),
  candidate('Dineout', 'https://www.dineout.co.in/', [], 'food'),
  candidate('Cafe Coffee Day', 'https://www.cafecoffeeday.com/', ['CCD'], 'food'),
  candidate('Faasos', 'https://www.faasos.com/', [], 'food'),
  candidate('Behrouz Biryani', 'https://www.behrouzbiryani.com/', ['Behrouz'], 'food'),
  candidate('Oven Story Pizza', 'https://www.ovenstory.in/', ['Oven Story'], 'food'),
  candidate('The Good Bowl', 'https://www.thegoodbowl.in/', ['Good Bowl'], 'food'),
  candidate('MOJO Pizza', 'https://mojopizza.in/', ['MojoPizza'], 'food'),
  candidate("Wendy's India", 'https://www.wendys.com/en-in', ['Wendys India', "Wendy's"], 'food'),
  candidate('Absolute Barbecues', 'https://www.absolutebarbecues.com/', ['ABs Absolute Barbecues'], 'food'),
  candidate('Dindigul Thalappakatti', 'https://thalappakatti.com/', ['Thalappakatti'], 'food'),
  candidate('Saravana Bhavan', 'https://saravanabhavan.com/', [], 'food'),
  candidate('Sagar Ratna', 'https://www.sagarratna.in/', [], 'food'),
  candidate('Moti Mahal Delux', 'https://motimahal.com/', ['Moti Mahal'], 'food'),
  candidate('Social', 'https://socialoffline.in/', ['Social Offline'], 'food', { disableAppStore: true }),

  candidate('IRCTC', 'https://www.irctc.co.in/', ['IRCTC Rail Connect'], 'travel'),
  candidate('RailOne', 'https://railone.indianrail.gov.in/', ['Rail One'], 'travel'),
  candidate('Agoda', 'https://www.agoda.com/', [], 'travel'),
  candidate('ConfirmTkt', 'https://www.confirmtkt.com/', ['Confirm Tkt'], 'travel'),
  candidate('BlaBlaCar', 'https://www.blablacar.in/', ['Bla Bla Car'], 'travel'),
  candidate('Adani One', 'https://www.adanione.com/', ['Adani OneApp'], 'travel'),
  candidate('AbhiBus', 'https://www.abhibus.com/', ['Abhi Bus'], 'travel', { appStoreId: '778605614' }),
  candidate('SpiceJet', 'https://www.spicejet.com/', [], 'travel'),
  candidate('Air India Express', 'https://www.airindiaexpress.com/', [], 'travel'),
  candidate('Club Mahindra', 'https://www.clubmahindra.com/', [], 'travel'),
  candidate('Thomas Cook India', 'https://www.thomascook.in/', ['Thomas Cook'], 'travel'),
  candidate('SOTC Travel', 'https://www.sotc.in/', ['SOTC'], 'travel'),
  candidate('RailYatri', 'https://www.railyatri.in/', ['Rail Yatri'], 'travel'),
  candidate('Sterling Holidays', 'https://www.sterlingholidays.com/', ['Sterling Resorts'], 'travel'),
  candidate('Skyscanner', 'https://www.skyscanner.co.in/', [], 'travel'),
  candidate('Hostelworld', 'https://www.hostelworld.com/', [], 'travel'),

  candidate('POP', 'https://popclub.co/', ['POP UPI'], 'finance', { appStoreTerm: 'POP UPI' }),
  candidate('mPokket', 'https://www.mpokket.in/', [], 'finance'),
  candidate('Bajaj Finserv', 'https://www.bajajfinserv.in/', ['Bajaj Finance'], 'finance'),
  candidate('LIC', 'https://licindia.in/', ['Life Insurance Corporation of India', 'My LIC'], 'brandz'),
  candidate('Moneyview', 'https://moneyview.in/', ['Money View'], 'finance'),
  candidate('Bachatt', 'https://www.bachatt.app/', [], 'finance'),
  candidate('True Balance', 'https://www.truebalance.io/', [], 'finance'),
  candidate('KreditBee', 'https://www.kreditbee.in/', ['Kredit Bee'], 'finance'),
  candidate('GoodScore', 'https://goodscore.app/', ['Good Score'], 'finance'),
  candidate('Kissht', 'https://www.kissht.com/', [], 'finance'),
  candidate('super.money', 'https://www.super.money/', ['Super Money'], 'finance'),
  candidate('PayZapp', 'https://v1.hdfcbank.com/payzapp/', ['Pay Zapp'], 'finance'),
  candidate('FamApp', 'https://www.famapp.in/', ['FamPay', 'Fam App'], 'finance'),
  candidate('Bank of Baroda', 'https://www.bankofbaroda.in/', ['BOB', 'bob World'], 'brandz'),
  candidate('Indian Overseas Bank', 'https://www.iob.in/', ['IOB'], 'brandz'),
  candidate('IDBI Bank', 'https://www.idbibank.in/', ['IDBI'], 'finance'),
  candidate('Bank of India', 'https://bankofindia.co.in/', ['BOI'], 'finance'),
  candidate('Indian Bank', 'https://www.indianbank.in/', [], 'finance'),
  candidate('AU Small Finance Bank', 'https://www.au.bank.in/', ['AU Bank'], 'finance'),
  candidate('RBL Bank', 'https://www.rblbank.com/', ['RBL'], 'finance'),
  candidate('Bandhan Bank', 'https://bandhanbank.com/', [], 'finance'),
  candidate('South Indian Bank', 'https://www.southindianbank.com/', ['SIB'], 'finance'),
  candidate('Karnataka Bank', 'https://karnatakabank.com/', ['KBL'], 'finance'),
  candidate('Karur Vysya Bank', 'https://www.kvb.co.in/', ['KVB'], 'finance'),
  candidate('City Union Bank', 'https://www.cityunionbank.com/', ['CUB'], 'finance'),
  candidate('HSBC India', 'https://www.hsbc.co.in/', ['HSBC'], 'finance'),
  candidate('Standard Chartered India', 'https://www.sc.com/in/', ['Standard Chartered'], 'finance'),
  candidate('DBS Bank India', 'https://www.dbs.com/in/', ['DBS', 'digibank'], 'finance'),
  candidate('Jupiter Money', 'https://jupiter.money/', ['Jupiter'], 'finance'),
  candidate('Fi Money', 'https://fi.money/', ['Fi'], 'finance', { appStoreTerm: 'Fi Money' }),
  candidate('Tide India', 'https://www.tide.co/en-IN/', ['Tide'], 'brandz', { simpleIconSlug: 'tide' }),
  candidate('Muthoot Finance', 'https://www.muthootfinance.com/', ['Muthoot'], 'finance'),

  candidate('SBI Life', 'https://www.sbilife.co.in/', ['SBI Life Insurance'], 'brandz'),
  candidate('HDFC Life', 'https://www.hdfclife.com/', ['HDFC Life Insurance'], 'brandz'),
  candidate('ICICI Prudential Life', 'https://www.iciciprulife.com/', ['ICICI Prudential'], 'brandz'),
  candidate('Bajaj Allianz', 'https://www.bajajallianz.com/', ['Bajaj Insurance'], 'brandz'),
  candidate('Maruti Suzuki', 'https://www.marutisuzuki.com/', ['Maruti'], 'brandz'),
  candidate('Bajaj Auto', 'https://www.bajajauto.com/', [], 'brandz'),
  candidate('TVS Motor', 'https://www.tvsmotor.com/', ['TVS'], 'brandz'),
  candidate('Hero MotoCorp', 'https://www.heromotocorp.com/', ['Hero'], 'brandz'),
  candidate('Royal Enfield', 'https://www.royalenfield.com/in/en/', [], 'brandz'),
  candidate('Havells', 'https://havells.com/', [], 'brandz', { simpleIconSlug: 'havells' }),
  candidate('Dabur', 'https://www.dabur.com/', [], 'brandz'),
  candidate('Kingfisher', 'https://www.kingfisherworld.com/', [], 'brandz', { disableAppStore: true }),
  candidate('Horlicks', 'https://www.horlicks.in/', [], 'brandz'),
  candidate('IndianOil', 'https://iocl.com/', ['Indian Oil', 'IOCL'], 'brandz'),
  candidate('Colgate-Palmolive', 'https://www.colgatepalmolive.co.in/', ['Colgate'], 'brandz'),
  candidate('Berger Paints', 'https://www.bergerpaints.com/', ['Berger'], 'brandz'),
  candidate('Hindustan Petroleum', 'https://www.hindustanpetroleum.com/', ['HPCL', 'HP Petrol'], 'brandz'),
  candidate('Bharat Petroleum', 'https://www.bharatpetroleum.in/', ['BPCL'], 'coverage'),
  candidate('Nayara Energy', 'https://www.nayaraenergy.com/', ['Nayara'], 'coverage'),
  candidate('Adani Total Gas', 'https://www.adanigas.com/', ['Adani Gas'], 'brandz'),
  candidate('Mahanagar Gas', 'https://www.mahanagargas.com/', ['MGL'], 'coverage'),
  candidate('Indraprastha Gas', 'https://www.iglonline.net/', ['IGL'], 'coverage'),
  candidate('Tata Power', 'https://www.tatapower.com/', [], 'coverage'),
  candidate('Adani Electricity', 'https://www.adanielectricity.com/', [], 'coverage'),
  candidate('Torrent Power', 'https://www.torrentpower.com/', [], 'coverage'),
  candidate('BESCOM', 'https://bescom.karnataka.gov.in/', [], 'coverage'),
  candidate('CESC', 'https://www.cesc.co.in/', [], 'coverage'),
  candidate('Naukri.com', 'https://www.naukri.com/', ['Naukri'], 'brandz'),
  candidate('Ekart Logistics', 'https://www.ekartlogistics.com/', ['Ekart'], 'brandz'),
  candidate('Ecom Express', 'https://www.ecomexpress.in/', [], 'coverage'),

  candidate('Spotify', 'https://www.spotify.com/in-en/', [], 'entertainment', { simpleIconSlug: 'spotify' }),
  candidate('Microsoft', 'https://www.microsoft.com/en-in/', ['Microsoft 365', 'Office 365'], 'coverage'),
  candidate('Adobe', 'https://www.adobe.com/in/', ['Adobe Creative Cloud'], 'coverage'),
  candidate('Canva', 'https://www.canva.com/en_in/', [], 'coverage'),
  candidate('Notion', 'https://www.notion.com/', [], 'coverage', { simpleIconSlug: 'notion' }),
  candidate('Amazon Prime Video', 'https://www.primevideo.com/', ['Prime Video'], 'entertainment'),
  candidate('Audible', 'https://www.audible.in/', [], 'coverage', { simpleIconSlug: 'audible' }),
  candidate('Steam', 'https://store.steampowered.com/', [], 'coverage', { simpleIconSlug: 'steam' }),
  candidate('PlayStation', 'https://www.playstation.com/en-in/', [], 'coverage', { simpleIconSlug: 'playstation' }),
  candidate('Xbox', 'https://www.xbox.com/en-IN/', [], 'coverage'),
  candidate('Nintendo', 'https://www.nintendo.com/', [], 'coverage'),
  candidate('Epic Games', 'https://store.epicgames.com/en-US/', [], 'coverage', { simpleIconSlug: 'epicgames' }),
  candidate('Udemy', 'https://www.udemy.com/', [], 'coverage', { simpleIconSlug: 'udemy' }),
  candidate('Grammarly', 'https://www.grammarly.com/', [], 'coverage', { simpleIconSlug: 'grammarly' }),
  candidate('Zoom', 'https://www.zoom.com/', ['Zoom Video'], 'coverage', { simpleIconSlug: 'zoom' }),
  candidate('GitHub', 'https://github.com/', ['GitHub Copilot'], 'coverage', { simpleIconSlug: 'github' }),
  candidate('GitLab', 'https://about.gitlab.com/', [], 'coverage', { simpleIconSlug: 'gitlab' }),
  candidate('Dropbox', 'https://www.dropbox.com/', [], 'coverage', { simpleIconSlug: 'dropbox' }),
  candidate('Google Drive', 'https://workspace.google.com/intl/en_in/products/drive/', [], 'coverage', { simpleIconSlug: 'googledrive' }),
  candidate('Google Photos', 'https://photos.google.com/', [], 'coverage', { simpleIconSlug: 'googlephotos' }),
  candidate('Tinder', 'https://tinder.com/', [], 'coverage', { simpleIconSlug: 'tinder' }),
  candidate('OnePlus', 'https://www.oneplus.in/', [], 'coverage', { simpleIconSlug: 'oneplus' }),
  candidate('OPPO', 'https://www.oppo.com/in/', [], 'coverage', { simpleIconSlug: 'oppo' }),
  candidate('vivo', 'https://www.vivo.com/in/', [], 'coverage', { simpleIconSlug: 'vivo' }),
  candidate('Xiaomi', 'https://www.mi.com/in/', ['Mi India'], 'coverage', { simpleIconSlug: 'xiaomi' }),
  candidate('Motorola', 'https://www.motorola.in/', ['Moto'], 'coverage', { simpleIconSlug: 'motorola' }),
  candidate('Samsung', 'https://www.samsung.com/in/', [], 'coverage', { simpleIconSlug: 'samsung' }),
  candidate('Dell', 'https://www.dell.com/en-in/', [], 'coverage', { simpleIconSlug: 'dell' }),
  candidate('Lenovo', 'https://www.lenovo.com/in/en/', [], 'coverage', { simpleIconSlug: 'lenovo' }),
  candidate('Sony', 'https://www.sony.co.in/', [], 'coverage', { simpleIconSlug: 'sony' }),
  candidate('Bosch', 'https://www.bosch.in/', [], 'coverage', { simpleIconSlug: 'bosch' }),

  candidate('Mother Dairy', 'https://www.motherdairy.com/', [], 'coverage'),
  candidate('More Retail', 'https://www.moreretail.in/', ['More Supermarket'], 'coverage'),
  candidate('Apollo Hospitals', 'https://www.apollohospitals.com/', [], 'coverage'),
  candidate('Fortis Healthcare', 'https://www.fortishealthcare.com/', ['Fortis'], 'coverage'),
  candidate('Thyrocare', 'https://www.thyrocare.com/', [], 'coverage'),
  candidate('CoinDCX', 'https://coindcx.com/', [], 'finance'),
  candidate('Hoichoi', 'https://www.hoichoi.tv/', [], 'entertainment'),
  candidate('Dream11', 'https://www.dream11.com/', [], 'entertainment'),
  candidate('JioHotstar', 'https://www.hotstar.com/in/', ['Jio Hotstar'], 'entertainment'),
  candidate('Airtel Xstream Play', 'https://www.airtelxstream.in/', ['Xstream Play'], 'entertainment'),
  candidate('STAGE', 'https://www.stage.in/', ['Stage OTT'], 'entertainment'),
  candidate('FanCode', 'https://www.fancode.com/', ['Fan Code'], 'entertainment'),
  candidate('Aha', 'https://www.aha.video/', ['aha OTT'], 'entertainment', { appStoreTerm: 'aha OTT' }),
  candidate('Discovery+', 'https://www.discoveryplus.in/', ['Discovery Plus'], 'entertainment'),
  candidate('Lionsgate Play', 'https://www.lionsgateplay.com/', [], 'entertainment'),
];

const CANDIDATES = useThirdBatch
  ? THIRD_BATCH_CANDIDATES
  : useSecondBatch
    ? SECOND_BATCH_CANDIDATES
    : FIRST_BATCH_CANDIDATES;

const BLOCKED_SOURCE_HOSTS = [
  'wikipedia.org',
  'wikimedia.org',
  'worldvectorlogo.com',
  'seeklogo.com',
  'brandsoftheworld.com',
  'logowik.com',
  'svgrepo.com',
];

const manifestPath = resolve('supabase/merchant-logo-catalog.json');
const reportPath = resolve(
  useThirdBatch
    ? 'supabase/merchant-logo-next-100-batch-3-report.json'
    : useSecondBatch
      ? 'supabase/merchant-logo-next-100-batch-2-report.json'
      : 'supabase/merchant-logo-next-100-report.json',
);
const seedMigrationPath = resolve(
  useThirdBatch
    ? 'supabase/migrations/20260809130000_seed_next_100_merchant_logo_catalog_batch_3.sql'
    : useSecondBatch
      ? 'supabase/migrations/20260809120000_seed_next_100_merchant_logo_catalog_batch_2.sql'
      : 'supabase/migrations/20260809100000_seed_next_100_merchant_logo_catalog.sql',
);
const contactSheetPath = resolve(
  useThirdBatch
    ? 'supabase/merchant-logo-next-100-batch-3-contact-sheet.png'
    : useSecondBatch
      ? 'supabase/merchant-logo-next-100-batch-2-contact-sheet.png'
      : 'supabase/merchant-logo-next-100-contact-sheet.png',
);
const contactSheetReportPath = useThirdBatch
  ? 'supabase/merchant-logo-next-100-batch-3-contact-sheet.png'
  : useSecondBatch
    ? 'supabase/merchant-logo-next-100-batch-2-contact-sheet.png'
    : 'supabase/merchant-logo-next-100-contact-sheet.png';
const assetBatchFolder = useThirdBatch
  ? 'next-100-batch-3'
  : useSecondBatch
    ? 'next-100-batch-2'
    : 'next-100';
const validateBatchOutputs = useSecondBatch || useThirdBatch;
const priorVerifiedReserveReportPath = resolve(
  'supabase/merchant-logo-next-100-batch-2-report.json',
);
const assetDirectory = resolve(process.env.MERCHANT_LOGO_ASSET_DIR ?? DEFAULT_ASSET_DIRECTORY);
const simpleIconsRoot = resolve(process.env.SIMPLE_ICONS_ROOT ?? DEFAULT_SIMPLE_ICONS_ROOT);
const validateOnly = process.argv.includes('--validate-only');

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

function sourceIsTrusted(source) {
  try {
    const host = new URL(source).hostname.toLowerCase();
    return !BLOCKED_SOURCE_HOSTS.some(
      (blocked) => host === blocked || host.endsWith(`.${blocked}`),
    );
  } catch {
    return false;
  }
}

function decodeHtml(value) {
  return value
    .replaceAll('&amp;', '&')
    .replaceAll('&#x2F;', '/')
    .replaceAll('&#47;', '/')
    .replaceAll('&quot;', '"');
}

function safeHttpUrl(value, pageUrl) {
  try {
    const url = new URL(decodeHtml(value), pageUrl);
    return url.protocol === 'https:' || url.protocol === 'http:' ? url.toString() : null;
  } catch {
    return null;
  }
}

function extractAssetCandidates(html, pageUrl) {
  const candidates = [];
  const push = (url, score, kind) => {
    const resolved = safeHttpUrl(url, pageUrl);
    if (resolved) candidates.push({ url: resolved, score, kind });
  };

  for (const link of [...html.matchAll(/<link\b[^>]*>/gi)].map((match) => match[0])) {
    const rel = /\brel\s*=\s*["']([^"']+)["']/i.exec(link)?.[1] ?? '';
    const href = /\bhref\s*=\s*["']([^"']+)["']/i.exec(link)?.[1];
    if (!href || href.startsWith('data:')) continue;
    const sizes = /\bsizes\s*=\s*["']([^"']+)["']/i.exec(link)?.[1] ?? '';
    const numericSize = Number.parseInt(sizes, 10) || 0;
    if (/apple-touch-icon/i.test(rel)) push(href, 30_000 + numericSize, 'apple-touch-icon');
    else if (/(?:^|\s)(?:icon|shortcut icon)(?:\s|$)/i.test(rel)) {
      push(href, 20_000 + numericSize, 'site-icon');
    }
  }

  for (const meta of [...html.matchAll(/<meta\b[^>]*>/gi)].map((match) => match[0])) {
    const property = /\b(?:property|name)\s*=\s*["']([^"']+)["']/i.exec(meta)?.[1] ?? '';
    const content = /\bcontent\s*=\s*["']([^"']+)["']/i.exec(meta)?.[1];
    if (content && /(?:og:image|twitter:image)/i.test(property)) {
      push(content, 10_000, property.toLowerCase());
    }
  }

  for (const image of [...html.matchAll(/<img\b[^>]*>/gi)].map((match) => match[0])) {
    const descriptor = [
      /\balt\s*=\s*["']([^"']*)["']/i.exec(image)?.[1],
      /\bclass\s*=\s*["']([^"']*)["']/i.exec(image)?.[1],
      /\bid\s*=\s*["']([^"']*)["']/i.exec(image)?.[1],
    ].filter(Boolean).join(' ');
    const src = /\b(?:src|data-src)\s*=\s*["']([^"']+)["']/i.exec(image)?.[1];
    if (src && /(?:logo|brand|wordmark)/i.test(descriptor)) push(src, 15_000, 'page-logo');
  }

  for (const match of html.matchAll(/["']logo["']\s*:\s*["']([^"']+)["']/gi)) {
    push(match[1], 14_000, 'structured-logo');
  }

  push('/favicon.ico', 0, 'favicon-fallback');
  candidates.sort((left, right) => right.score - left.score);
  return [...new Map(candidates.map((item) => [item.url, item])).values()].slice(0, 16);
}

function detectContentType(bytes) {
  const prefix = bytes
    .subarray(0, 1024)
    .toString('utf8')
    .replace(/^\uFEFF/, '')
    .trimStart()
    .toLowerCase();
  if (prefix.startsWith('<svg') || (prefix.startsWith('<?xml') && prefix.includes('<svg'))) {
    return 'image/svg+xml';
  }
  if (bytes[0] === 0x89 && bytes.subarray(1, 4).toString('ascii') === 'PNG') return 'image/png';
  if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return 'image/jpeg';
  if (
    bytes.subarray(0, 4).toString('ascii') === 'RIFF'
    && bytes.subarray(8, 12).toString('ascii') === 'WEBP'
  ) return 'image/webp';
  if (bytes[0] === 0 && bytes[1] === 0 && bytes[2] === 1 && bytes[3] === 0) {
    return 'image/x-icon';
  }
  if (
    bytes.subarray(4, 8).toString('ascii') === 'ftyp'
    && /(?:avif|avis)/.test(bytes.subarray(8, 32).toString('ascii'))
  ) return 'image/avif';
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

async function validateAssetQuality(bytes, contentType, sourceUrl) {
  let metadata;
  try {
    metadata = await sharp(bytes, { failOn: 'error' }).metadata();
  } catch (error) {
    throw new Error(`image decoder rejected bytes: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!metadata.width || !metadata.height) throw new Error('image dimensions are unavailable');
  if (
    contentType !== 'image/svg+xml'
    && (metadata.width < 48 || metadata.height < 48)
  ) {
    throw new Error(`asset is only ${metadata.width}x${metadata.height}`);
  }
  const sourceHost = new URL(sourceUrl).hostname.toLowerCase();
  if (
    sourceHost === 'facebook.com'
    || sourceHost.endsWith('.facebook.com')
    || sourceHost === 'fbcdn.net'
    || sourceHost.endsWith('.fbcdn.net')
    || sourceHost === 'instagram.com'
    || sourceHost.endsWith('.instagram.com')
  ) {
    throw new Error(`asset is a generic social-site icon from ${sourceHost}`);
  }
  return {
    width: metadata.width,
    height: metadata.height,
    scalable: contentType === 'image/svg+xml',
  };
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

function buildSimpleIconRow(target, icon, rank) {
  if (!sourceIsTrusted(icon.source)) {
    throw new Error(`Simple Icons source is blocked: ${icon.source}`);
  }
  const sourcePath = join(simpleIconsRoot, 'icons', `${icon.slug}.svg`);
  if (!existsSync(sourcePath)) throw new Error(`Simple Icons asset is missing: ${icon.slug}`);
  const selectedColor = target.simpleIconColor ?? `#${icon.hex}`;
  const svg = decorateSvg(readFileSync(sourcePath, 'utf8'), {
    ...icon,
    hex: selectedColor.replace(/^#/, ''),
  });
  const bytes = Buffer.from(svg);
  const hash = createHash('sha256').update(bytes).digest('hex');
  const objectPath = `simple-icons-${SIMPLE_ICONS_VERSION}/${icon.slug}-${hash.slice(0, 12)}.svg`;
  return buildRow(target, rank, {
    bytes,
    contentType: 'image/svg+xml',
    sourceUrl: icon.source,
    sourceKind: 'SIMPLE_ICONS',
    sourceLicense: licenseLabel(icon),
    websiteUrl: target.websiteUrl,
    brandColor: selectedColor.toUpperCase(),
    objectPath,
    dimensions: { width: 24, height: 24, scalable: true },
    identityVerification: {
      method: 'SIMPLE_ICONS_WITH_OFFICIAL_SOURCE',
      simple_icons_slug: icon.slug,
      simple_icons_version: SIMPLE_ICONS_VERSION,
      official_source_url: icon.source,
      color_override: target.simpleIconColor,
    },
  });
}

function buildRow(target, rank, asset) {
  const evidence = EVIDENCE[target.evidenceKey];
  return {
    canonical_name: target.canonicalName,
    normalized_name: normalize(target.canonicalName),
    coverage: 'ADDITIONAL',
    catalog_batch: BATCH_ID,
    selection_rank: rank,
    selection_reason: [
      evidence.label,
      target.evidenceRank ? `source rank #${target.evidenceRank}` : null,
      `${target.transactionRelevance.toLowerCase()} transaction relevance`,
    ].filter(Boolean).join('; '),
    evidence_rank: target.evidenceRank,
    transaction_relevance: target.transactionRelevance,
    evidence_urls: [evidence.url],
    aliases: [...new Set([target.canonicalName, ...target.aliases])],
    website_url: asset.websiteUrl,
    source_page_url: asset.sourcePageUrl ?? asset.websiteUrl,
    source_url: asset.sourceUrl,
    source_kind: asset.sourceKind,
    source_license: asset.sourceLicense,
    brand_color: asset.brandColor,
    object_path: asset.objectPath,
    content_type: asset.contentType,
    content_sha256: createHash('sha256').update(asset.bytes).digest('hex'),
    asset_dimensions: asset.dimensions,
    identity_verification: asset.identityVerification,
    _bytes: asset.bytes,
  };
}

const headers = {
  'User-Agent': 'Mozilla/5.0 (compatible; MoneyManagerLogoAudit/2.0)',
  Accept: 'text/html,application/xhtml+xml,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8',
};

async function fetchOfficialRow(target, rank) {
  const page = await fetch(target.websiteUrl, {
    headers,
    redirect: 'follow',
    signal: AbortSignal.timeout(12_000),
  });
  if (!page.ok) throw new Error(`official page returned ${page.status}`);
  const contentType = page.headers.get('content-type') ?? '';
  if (!contentType.includes('text/html')) throw new Error('official page did not return HTML');
  const html = await page.text();
  const failures = [];
  for (const candidateAsset of extractAssetCandidates(html, page.url)) {
    try {
      const response = await fetch(candidateAsset.url, {
        headers,
        redirect: 'follow',
        signal: AbortSignal.timeout(12_000),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const bytes = Buffer.from(await response.arrayBuffer());
      if (bytes.length < 64) throw new Error('asset is too small');
      if (bytes.length > 1_048_576) throw new Error('asset exceeds 1 MiB');
      const detectedType = detectContentType(bytes);
      if (!detectedType) throw new Error('asset bytes are not a supported image');
      const dimensions = await validateAssetQuality(bytes, detectedType, response.url);
      const hash = createHash('sha256').update(bytes).digest('hex');
      const objectPath = `official-sites-${assetBatchFolder}/${slugify(target.canonicalName)}-${hash.slice(0, 12)}.${extensionFor(detectedType)}`;
      return buildRow(target, rank, {
        bytes,
        contentType: detectedType,
        sourceUrl: response.url,
        sourceKind: candidateAsset.kind.includes('logo') || candidateAsset.kind.includes('image')
          ? 'OFFICIAL_BRAND_ASSET'
          : 'OFFICIAL_SITE_ICON',
        sourceLicense: null,
        websiteUrl: page.url,
        sourcePageUrl: page.url,
        brandColor: null,
        objectPath,
        dimensions,
        identityVerification: {
          method: 'OFFICIAL_DOMAIN',
          requested_domain: new URL(target.websiteUrl).hostname,
          resolved_page_domain: new URL(page.url).hostname,
          asset_domain: new URL(response.url).hostname,
        },
      });
    } catch (error) {
      failures.push(`${candidateAsset.kind}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  throw new Error(`no usable official asset (${failures.at(-1) ?? 'no candidates'})`);
}

function appMatchScore(target, result) {
  const trackName = normalize(result.trackName ?? '');
  if (!trackName) return 0;
  const names = [target.canonicalName, target.appStoreTerm, ...target.aliases]
    .map(normalize)
    .filter((name) => name.length >= 3);
  let score = 0;
  for (const name of names) {
    if (trackName === name) score = Math.max(score, 1_000);
    else if (trackName.startsWith(name) || name.startsWith(trackName)) score = Math.max(score, 700);
    else if (name.length >= 5 && trackName.includes(name)) score = Math.max(score, 500);
  }
  const officialHost = new URL(target.websiteUrl).hostname.replace(/^www\./, '').split('.')[0];
  const sellerText = normalize(`${result.sellerName ?? ''} ${result.bundleId ?? ''}`);
  if (officialHost.length >= 4 && sellerText.includes(normalize(officialHost))) score += 100;
  return score;
}

function hasStrongAppIdentity(target, result) {
  if (target.appStoreId) return String(result.trackId) === target.appStoreId;
  const sellerText = normalize(`${result.sellerName ?? ''} ${result.artistName ?? ''} ${result.bundleId ?? ''}`);
  const meaningfulNames = [
    target.canonicalName,
    target.appStoreTerm,
    ...target.aliases,
    ...(target.expectedSellerTokens ?? []),
  ]
    .flatMap((name) => name.split(/[^A-Za-z0-9]+/))
    .map(normalize)
    .filter((name) => name.length >= 5 && !['INDIA', 'MONEY', 'FINANCE', 'MOTOR'].includes(name));
  if (meaningfulNames.some((name) => sellerText.includes(name))) return true;
  if (!result.sellerUrl) return false;
  try {
    const appHost = new URL(result.sellerUrl).hostname.replace(/^www\./, '');
    const officialHost = new URL(target.websiteUrl).hostname.replace(/^www\./, '');
    return appHost === officialHost || appHost.endsWith(`.${officialHost}`) || officialHost.endsWith(`.${appHost}`);
  } catch {
    return false;
  }
}

async function fetchAppStoreRow(target, rank) {
  if (target.disableAppStore) throw new Error('App Store fallback disabled for ambiguous brand name');
  const lookupUrl = new URL(
    target.appStoreId
      ? 'https://itunes.apple.com/lookup'
      : 'https://itunes.apple.com/search',
  );
  if (target.appStoreId) {
    lookupUrl.searchParams.set('id', target.appStoreId);
  } else {
    lookupUrl.searchParams.set('term', target.appStoreTerm);
    lookupUrl.searchParams.set('entity', 'software');
    lookupUrl.searchParams.set('limit', '10');
  }
  lookupUrl.searchParams.set('country', 'in');
  let search = null;
  let searchError = null;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      const response = await fetch(lookupUrl, {
        headers,
        redirect: 'follow',
        signal: AbortSignal.timeout(12_000),
      });
      if (response.ok) {
        search = response;
        break;
      }
      searchError = new Error(`App Store lookup returned ${response.status}`);
    } catch (error) {
      searchError = error;
    }
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 500 * (attempt + 1)));
  }
  if (!search) {
    throw new Error(searchError instanceof Error ? searchError.message : 'App Store lookup failed');
  }
  const payload = await search.json();
  const matches = (Array.isArray(payload.results) ? payload.results : [])
    .map((result) => ({ result, score: appMatchScore(target, result) }))
    .filter((match) => (target.appStoreId || match.score >= 500) && hasStrongAppIdentity(target, match.result))
    .sort((left, right) => right.score - left.score);
  if (matches.length === 0) throw new Error('no confident India App Store identity match');
  const match = matches[0].result;
  const artworkUrl = match.artworkUrl512 ?? match.artworkUrl100 ?? match.artworkUrl60;
  if (!artworkUrl || !match.trackViewUrl) throw new Error('matched app has no artwork URL');
  const response = await fetch(artworkUrl, {
    headers,
    redirect: 'follow',
    signal: AbortSignal.timeout(12_000),
  });
  if (!response.ok) throw new Error(`App Store artwork returned ${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length < 64) throw new Error('App Store artwork is too small');
  if (bytes.length > 1_048_576) throw new Error('App Store artwork exceeds 1 MiB');
  const contentType = detectContentType(bytes);
  if (!contentType) throw new Error('App Store artwork is not a supported image');
  const dimensions = await validateAssetQuality(bytes, contentType, response.url);
  const hash = createHash('sha256').update(bytes).digest('hex');
  const objectPath = `official-app-store-${assetBatchFolder}/${slugify(target.canonicalName)}-${hash.slice(0, 12)}.${extensionFor(contentType)}`;
  return buildRow(target, rank, {
    bytes,
    contentType,
    sourceUrl: response.url,
    sourceKind: 'OFFICIAL_BRAND_ASSET',
    sourceLicense: null,
    websiteUrl: target.websiteUrl,
    sourcePageUrl: match.trackViewUrl,
    brandColor: null,
    objectPath,
    dimensions,
    identityVerification: {
      method: 'APPLE_APP_STORE_IDENTITY',
      country: 'IN',
      track_id: match.trackId,
      track_name: match.trackName,
      seller_name: match.sellerName ?? null,
      artist_name: match.artistName ?? null,
      bundle_id: match.bundleId ?? null,
      seller_url: match.sellerUrl ?? null,
      official_domain: new URL(target.websiteUrl).hostname,
      expected_seller_tokens: target.expectedSellerTokens ?? [],
    },
  });
}

function writeSeedMigration(rows) {
  const statements = [
    '-- Generated by scripts/expand-merchant-logo-catalog.mjs.',
    `-- ${rows.length} verified additional brands in batch ${BATCH_ID}.`,
    '',
  ];
  for (const row of rows) {
    statements.push(
      'insert into public.merchant_logos (',
      '  canonical_name, normalized_name, website_url, source_url, source_kind,',
      '  source_license, storage_provider, brand_color',
      ') values (',
      `  ${sql(row.canonical_name)}, ${sql(row.normalized_name)}, ${sql(row.website_url ?? row.source_url)},`,
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
      const searchKey = normalize(alias);
      if (!searchKey) continue;
      statements.push(
        'insert into public.merchant_logo_aliases (search_key, merchant_logo_id)',
        `select ${sql(searchKey)}, id`,
        `from public.merchant_logos where normalized_name = ${sql(row.normalized_name)}`,
        'on conflict (search_key) do update set merchant_logo_id = excluded.merchant_logo_id;',
      );
    }
    statements.push('');
  }
  writeFileSync(
    seedMigrationPath,
    `${statements.join('\n')}\n`,
  );
}

function escapeXml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

async function writeContactSheet(rows) {
  const columns = 10;
  const tileWidth = 220;
  const tileHeight = 180;
  const rowsCount = Math.ceil(rows.length / columns);
  const width = columns * tileWidth;
  const height = rowsCount * tileHeight;
  const composites = [];

  for (const [index, row] of rows.entries()) {
    const left = (index % columns) * tileWidth;
    const top = Math.floor(index / columns) * tileHeight;
    const bytes = readFileSync(join(assetDirectory, row.object_path));
    const logo = await sharp(bytes, { failOn: 'error' })
      .resize(112, 112, {
        fit: 'contain',
        withoutEnlargement: false,
        background: '#ffffff',
      })
      .flatten({ background: '#ffffff' })
      .png()
      .toBuffer();
    const displayName = row.canonical_name.length > 24
      ? `${row.canonical_name.slice(0, 23)}…`
      : row.canonical_name;
    const label = Buffer.from(`
      <svg width="${tileWidth}" height="${tileHeight}">
        <rect x="0.5" y="0.5" width="${tileWidth - 1}" height="${tileHeight - 1}" rx="10" fill="#ffffff" stroke="#dbe2ea"/>
        <text x="12" y="22" font-family="Arial, Helvetica, sans-serif" font-size="13" font-weight="700" fill="#52606d">#${row.selection_rank}</text>
        <text x="110" y="166" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="14" font-weight="600" fill="#172b4d">${escapeXml(displayName)}</text>
      </svg>
    `);
    composites.push({ input: label, left, top });
    composites.push({ input: logo, left: left + 54, top: top + 32 });
  }

  await sharp({
    create: {
      width,
      height,
      channels: 4,
      background: '#f3f6f9',
    },
  }).composite(composites).png({ compressionLevel: 9 }).toFile(contactSheetPath);
}

async function validateBatchArtifacts(manifest) {
  const batchRows = manifest.rows.filter((row) => row.catalog_batch === BATCH_ID);
  const localHashes = new Set();
  for (const row of batchRows) {
    const assetPath = join(assetDirectory, row.object_path);
    if (!existsSync(assetPath)) throw new Error(`Missing local asset for ${row.canonical_name}`);
    const bytes = readFileSync(assetPath);
    const actualHash = createHash('sha256').update(bytes).digest('hex');
    if (actualHash !== row.content_sha256) {
      throw new Error(`Local hash mismatch for ${row.canonical_name}`);
    }
    if (detectContentType(bytes) !== row.content_type) {
      throw new Error(`Actual bytes/content type mismatch for ${row.canonical_name}`);
    }
    const dimensions = await validateAssetQuality(bytes, row.content_type, row.source_url);
    if (
      row.asset_dimensions?.width !== dimensions.width
      || row.asset_dimensions?.height !== dimensions.height
      || row.asset_dimensions?.scalable !== dimensions.scalable
    ) {
      throw new Error(`Recorded dimensions are stale for ${row.canonical_name}`);
    }
    if (localHashes.has(actualHash)) throw new Error(`Duplicate local asset hash ${actualHash}`);
    localHashes.add(actualHash);
    if (!row.aliases.some((alias) => normalize(alias) === row.normalized_name)) {
      throw new Error(`Canonical alias missing for ${row.canonical_name}`);
    }
  }

  if (!existsSync(seedMigrationPath)) throw new Error(`Batch migration is missing: ${seedMigrationPath}`);
  const migrationSql = readFileSync(seedMigrationPath, 'utf8');
  const migrationRows = migrationSql.match(/insert into public\.merchant_logos \(/g)?.length ?? 0;
  if (migrationRows !== BATCH_TARGET) {
    throw new Error(`Expected ${BATCH_TARGET} migration rows, found ${migrationRows}`);
  }
  for (const row of batchRows) {
    if (!migrationSql.includes(`where normalized_name = ${sql(row.normalized_name)}`)) {
      throw new Error(`Migration is missing aliases for ${row.canonical_name}`);
    }
    for (const alias of row.aliases) {
      if (!migrationSql.includes(`select ${sql(normalize(alias))}, id`)) {
        throw new Error(`Migration is missing alias ${alias} for ${row.canonical_name}`);
      }
    }
  }

  if (!existsSync(reportPath)) throw new Error(`Batch report is missing: ${reportPath}`);
  const report = JSON.parse(readFileSync(reportPath, 'utf8'));
  if (report.batch_id !== BATCH_ID || report.selected_count !== BATCH_TARGET) {
    throw new Error('Batch report identity/count is stale');
  }
  if (report.candidate_pool < 150 || report.ranked_pool?.length !== report.candidate_pool) {
    throw new Error('Batch report does not contain the complete oversized ranked pool');
  }
  if (!existsSync(contactSheetPath)) throw new Error(`Contact sheet is missing: ${contactSheetPath}`);

  return {
    local_assets: batchRows.length,
    unique_local_hashes: localHashes.size,
    migration_rows: migrationRows,
    report_selected: report.selected_count,
    ranked_pool: report.ranked_pool.length,
    contact_sheet: contactSheetPath,
  };
}

function validateCatalog(manifest) {
  const batchRows = manifest.rows.filter((row) => row.catalog_batch === BATCH_ID);
  const batchNames = new Set(batchRows.map((row) => row.normalized_name));
  const normalizedNames = new Set();
  const aliases = new Map();
  const contentHashes = new Map();
  for (const row of manifest.rows) {
    if (row.normalized_name !== normalize(row.canonical_name)) {
      throw new Error(`Invalid normalized name for ${row.canonical_name}`);
    }
    if (normalizedNames.has(row.normalized_name)) {
      throw new Error(`Duplicate normalized name: ${row.normalized_name}`);
    }
    normalizedNames.add(row.normalized_name);
    if (!/^https:\/\//.test(row.source_url) || !/^https:\/\//.test(row.website_url ?? row.source_url)) {
      throw new Error(`Non-HTTPS source for ${row.canonical_name}`);
    }
    if (!/^[a-f0-9]{64}$/.test(row.content_sha256)) {
      throw new Error(`Invalid content hash for ${row.canonical_name}`);
    }
    const hashOwner = contentHashes.get(row.content_sha256);
    if (
      hashOwner
      && hashOwner !== row.normalized_name
      && (batchNames.has(hashOwner) || row.catalog_batch === BATCH_ID)
    ) {
      throw new Error(
        `Batch asset for ${row.canonical_name} duplicates ${hashOwner}`,
      );
    }
    contentHashes.set(row.content_sha256, row.normalized_name);
    for (const alias of row.aliases) {
      const key = normalize(alias);
      const owner = aliases.get(key);
      if (owner && owner !== row.normalized_name) {
        throw new Error(`Alias ${key} is shared by ${owner} and ${row.normalized_name}`);
      }
      aliases.set(key, row.normalized_name);
    }
  }
  if (batchRows.length !== BATCH_TARGET) {
    throw new Error(`Expected ${BATCH_TARGET} ${BATCH_ID} rows, found ${batchRows.length}`);
  }
  const orderedRanks = batchRows.map((row) => row.selection_rank);
  if (new Set(orderedRanks).size !== BATCH_TARGET) throw new Error('Batch selection ranks are not unique');
  if (manifest.additional_count !== manifest.rows.filter((row) => row.coverage === 'ADDITIONAL').length) {
    throw new Error('Manifest additional_count is stale');
  }
  return {
    total: manifest.rows.length,
    existing: manifest.rows.length - manifest.additional_count,
    additional: manifest.additional_count,
    batch: batchRows.length,
    aliases: aliases.size,
  };
}

if (!existsSync(manifestPath)) throw new Error(`Manifest not found: ${manifestPath}`);
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));

if (validateOnly) {
  const catalogValidation = validateCatalog(manifest);
  const artifactValidation = validateBatchOutputs
    ? await validateBatchArtifacts(manifest)
    : null;
  console.log(JSON.stringify({ ...catalogValidation, artifacts: artifactValidation }, null, 2));
  process.exit(0);
}

if (!existsSync(join(simpleIconsRoot, 'data', 'simple-icons.json'))) {
  throw new Error(
    `Simple Icons ${SIMPLE_ICONS_VERSION} was not found at ${simpleIconsRoot}. `
    + 'Run the documented npm pack and extract commands first.',
  );
}

const simpleIcons = JSON.parse(readFileSync(join(simpleIconsRoot, 'data', 'simple-icons.json'), 'utf8'));
const simpleIconsBySlug = new Map(simpleIcons.map((icon) => [icon.slug, icon]));
const priorBatchRows = new Map(
  manifest.rows
    .filter((row) => row.catalog_batch === BATCH_ID)
    .map((row) => [row.normalized_name, row]),
);
const priorVerifiedReserveRows = new Map();
if (useThirdBatch && existsSync(priorVerifiedReserveReportPath)) {
  const priorReport = JSON.parse(readFileSync(priorVerifiedReserveReportPath, 'utf8'));
  for (const row of priorReport.verified_not_selected ?? []) {
    priorVerifiedReserveRows.set(row.normalized_name, row);
  }
}
const baseRows = manifest.rows.filter((row) => row.catalog_batch !== BATCH_ID);
const usedNames = new Set(baseRows.map((row) => row.normalized_name));
const usedAliases = new Map();
for (const row of baseRows) {
  for (const alias of row.aliases) usedAliases.set(normalize(alias), row.normalized_name);
}

const eligible = [];
const rejected = [];
const candidateNames = new Set();
for (const [index, target] of CANDIDATES.entries()) {
  const normalizedName = normalize(target.canonicalName);
  if (target.rejectReason) {
    rejected.push({ rank: index + 1, canonical_name: target.canonicalName, reason: target.rejectReason });
    continue;
  }
  if (!normalizedName) {
    rejected.push({ rank: index + 1, canonical_name: target.canonicalName, reason: 'blank normalized name' });
    continue;
  }
  if (candidateNames.has(normalizedName)) {
    rejected.push({ rank: index + 1, canonical_name: target.canonicalName, reason: 'duplicate candidate' });
    continue;
  }
  candidateNames.add(normalizedName);
  if (usedNames.has(normalizedName)) {
    rejected.push({ rank: index + 1, canonical_name: target.canonicalName, reason: 'already in catalog' });
    continue;
  }
  const conflictingAlias = [target.canonicalName, ...target.aliases]
    .map(normalize)
    .find((key) => usedAliases.has(key));
  if (conflictingAlias) {
    rejected.push({
      rank: index + 1,
      canonical_name: target.canonicalName,
      reason: `alias already owned by ${usedAliases.get(conflictingAlias)}`,
    });
    continue;
  }
  eligible.push({ target, rank: index + 1 });
}

const attempts = [];
let nextIndex = 0;
async function worker() {
  while (nextIndex < eligible.length) {
    const entry = eligible[nextIndex];
    nextIndex += 1;
    let officialError = null;
    let appStoreError = null;
    const cached = priorBatchRows.get(normalize(entry.target.canonicalName));
    const cachedPath = cached ? join(assetDirectory, cached.object_path) : null;
    const cachedUsesDisabledAppStore = cached?.source_page_url?.includes('apps.apple.com')
      && entry.target.disableAppStore;
    const cachedMissesPinnedAppStore = entry.target.appStoreId
      && String(cached?.identity_verification?.track_id ?? '') !== entry.target.appStoreId;
    const cachedMissesPreferredAppStore = entry.target.preferAppStore
      && !cached?.source_page_url?.includes('apps.apple.com');
    const cachedMissesForcedSimpleIcon = entry.target.forceSimpleIcon
      && cached?.source_kind !== 'SIMPLE_ICONS';
    const cachedMissesSimpleColor = entry.target.simpleIconColor
      && cached?.brand_color !== entry.target.simpleIconColor.toUpperCase();
    if (
      cached
      && cachedPath
      && existsSync(cachedPath)
      && !cachedUsesDisabledAppStore
      && !cachedMissesPinnedAppStore
      && !cachedMissesPreferredAppStore
      && !cachedMissesForcedSimpleIcon
      && !cachedMissesSimpleColor
    ) {
      const bytes = readFileSync(cachedPath);
      const hash = createHash('sha256').update(bytes).digest('hex');
      if (hash === cached.content_sha256) {
        attempts.push({
          ...entry,
          row: { ...cached, selection_rank: entry.rank, _bytes: bytes },
          method: 'CACHE',
        });
        continue;
      }
    }
    const verifiedReserve = priorVerifiedReserveRows.get(
      normalize(entry.target.canonicalName),
    );
    const verifiedReservePath = verifiedReserve
      ? join(assetDirectory, verifiedReserve.object_path)
      : null;
    const verifiedReserveUsesDisabledAppStore = verifiedReserve?.source_page_url?.includes('apps.apple.com')
      && entry.target.disableAppStore;
    const verifiedReserveMissesPinnedAppStore = entry.target.appStoreId
      && String(verifiedReserve?.identity_verification?.track_id ?? '') !== entry.target.appStoreId;
    const verifiedReserveMissesPreferredAppStore = entry.target.preferAppStore
      && !verifiedReserve?.source_page_url?.includes('apps.apple.com');
    const verifiedReserveMissesForcedSimpleIcon = entry.target.forceSimpleIcon
      && verifiedReserve?.source_kind !== 'SIMPLE_ICONS';
    if (
      verifiedReserve
      && verifiedReservePath
      && existsSync(verifiedReservePath)
      && !verifiedReserveUsesDisabledAppStore
      && !verifiedReserveMissesPinnedAppStore
      && !verifiedReserveMissesPreferredAppStore
      && !verifiedReserveMissesForcedSimpleIcon
    ) {
      const bytes = readFileSync(verifiedReservePath);
      const hash = createHash('sha256').update(bytes).digest('hex');
      if (hash === verifiedReserve.content_sha256) {
        attempts.push({
          ...entry,
          row: buildRow(entry.target, entry.rank, {
            bytes,
            contentType: verifiedReserve.content_type,
            sourceUrl: verifiedReserve.source_url,
            sourceKind: verifiedReserve.source_kind,
            sourceLicense: null,
            websiteUrl: entry.target.websiteUrl,
            sourcePageUrl: verifiedReserve.source_page_url,
            brandColor: null,
            objectPath: verifiedReserve.object_path,
            dimensions: verifiedReserve.asset_dimensions,
            identityVerification: verifiedReserve.identity_verification,
          }),
          method: 'PRIOR_VERIFIED_RESERVE_CACHE',
        });
        continue;
      }
    }
    if (entry.target.forceSimpleIcon) {
      try {
        const icon = simpleIconsBySlug.get(entry.target.simpleIconSlug);
        if (!icon) throw new Error(`slug ${entry.target.simpleIconSlug} is unavailable`);
        const row = buildSimpleIconRow(entry.target, icon, entry.rank);
        attempts.push({ ...entry, row, method: 'SIMPLE_ICONS' });
      } catch (error) {
        rejected.push({
          rank: entry.rank,
          canonical_name: entry.target.canonicalName,
          reason: `forced Simple Icons asset failed: ${error instanceof Error ? error.message : String(error)}`,
        });
      }
      continue;
    }
    const preferAppStore = entry.target.preferAppStore || Boolean(entry.target.appStoreId);
    try {
      const row = preferAppStore
        ? await fetchAppStoreRow(entry.target, entry.rank)
        : await fetchOfficialRow(entry.target, entry.rank);
      attempts.push({ ...entry, row, method: row.source_kind });
      continue;
    } catch (error) {
      officialError = error;
    }

    try {
      const row = preferAppStore
        ? await fetchOfficialRow(entry.target, entry.rank)
        : await fetchAppStoreRow(entry.target, entry.rank);
      attempts.push({
        ...entry,
        row,
        method: row.source_kind,
        primary_failure: officialError instanceof Error ? officialError.message : String(officialError),
      });
      continue;
    } catch (error) {
      appStoreError = error;
    }

    {
      if (entry.target.simpleIconSlug) {
        try {
          const icon = simpleIconsBySlug.get(entry.target.simpleIconSlug);
          if (!icon) throw new Error(`slug ${entry.target.simpleIconSlug} is unavailable`);
          const row = buildSimpleIconRow(entry.target, icon, entry.rank);
          attempts.push({
            ...entry,
            row,
            method: 'SIMPLE_ICONS',
            primary_failure: officialError instanceof Error ? officialError.message : String(officialError),
            secondary_failure: appStoreError instanceof Error ? appStoreError.message : String(appStoreError),
          });
          continue;
        } catch (fallbackError) {
          rejected.push({
            rank: entry.rank,
            canonical_name: entry.target.canonicalName,
            reason: `primary: ${officialError instanceof Error ? officialError.message : String(officialError)}; App Store/official fallback: ${appStoreError instanceof Error ? appStoreError.message : String(appStoreError)}; Simple Icons: ${fallbackError instanceof Error ? fallbackError.message : String(fallbackError)}`,
          });
          continue;
        }
      }
      rejected.push({
        rank: entry.rank,
        canonical_name: entry.target.canonicalName,
        reason: `primary: ${officialError instanceof Error ? officialError.message : String(officialError)}; fallback: ${appStoreError instanceof Error ? appStoreError.message : String(appStoreError)}`,
      });
    }
  }
}

await Promise.all(Array.from({ length: 5 }, () => worker()));
attempts.sort((left, right) => left.rank - right.rank);

const selected = [];
const selectedAliases = new Map(usedAliases);
const selectedHashes = new Map(baseRows.map((row) => [row.content_sha256, row.normalized_name]));
for (const attempt of attempts) {
  if (selected.length >= BATCH_TARGET) break;
  const collision = attempt.row.aliases
    .map(normalize)
    .find((key) => selectedAliases.has(key));
  if (collision) {
    rejected.push({
      rank: attempt.rank,
      canonical_name: attempt.target.canonicalName,
      reason: `selected alias collision with ${selectedAliases.get(collision)}`,
    });
    continue;
  }
  const hashOwner = selectedHashes.get(attempt.row.content_sha256);
  if (hashOwner && hashOwner !== attempt.row.normalized_name) {
    rejected.push({
      rank: attempt.rank,
      canonical_name: attempt.target.canonicalName,
      reason: `asset bytes duplicate ${hashOwner}`,
    });
    continue;
  }
  selected.push(attempt);
  selectedHashes.set(attempt.row.content_sha256, attempt.row.normalized_name);
  for (const alias of attempt.row.aliases) {
    selectedAliases.set(normalize(alias), attempt.row.normalized_name);
  }
}

if (selected.length !== BATCH_TARGET) {
  throw new Error(
    `Only ${selected.length} verified non-overlapping merchants were available; `
    + `${BATCH_TARGET} are required. See acquisition failures in command output.`,
  );
}

mkdirSync(assetDirectory, { recursive: true });
const selectedRows = selected.map(({ row }) => {
  const destination = join(assetDirectory, row.object_path);
  mkdirSync(dirname(destination), { recursive: true });
  writeFileSync(destination, row._bytes);
  const { _bytes, ...manifestRow } = row;
  return manifestRow;
});
await writeContactSheet(selectedRows);

manifest.generated_at = new Date().toISOString();
manifest.rows = [...baseRows, ...selectedRows];
manifest.existing_priority_count = manifest.rows.filter((row) => row.coverage === 'EXISTING').length;
manifest.additional_count = manifest.rows.filter((row) => row.coverage === 'ADDITIONAL').length;
manifest.catalog_batches = {
  ...(manifest.catalog_batches ?? {}),
  [BATCH_ID]: {
    generated_at: manifest.generated_at,
    target: BATCH_TARGET,
    selected: selectedRows.length,
    candidate_pool: CANDIDATES.length,
    evidence: EVIDENCE,
  },
};
writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
writeSeedMigration(selectedRows);

rejected.sort((left, right) => left.rank - right.rank);
const selectedReport = selected.map((attempt) => ({
  rank: attempt.rank,
  canonical_name: attempt.row.canonical_name,
  normalized_name: attempt.row.normalized_name,
  aliases: attempt.row.aliases,
  selection_reason: attempt.row.selection_reason,
  evidence_urls: attempt.row.evidence_urls,
  website_url: attempt.row.website_url,
  source_page_url: attempt.row.source_page_url,
  source_url: attempt.row.source_url,
  source_kind: attempt.row.source_kind,
  object_path: attempt.row.object_path,
  content_type: attempt.row.content_type,
  content_sha256: attempt.row.content_sha256,
  asset_dimensions: attempt.row.asset_dimensions,
  identity_verification: attempt.row.identity_verification,
  acquisition_method: attempt.method,
  primary_failure: attempt.primary_failure ?? null,
  secondary_failure: attempt.secondary_failure ?? null,
}));
const selectedNames = new Set(selected.map((attempt) => attempt.row.normalized_name));
const verifiedNotSelected = attempts
  .filter((attempt) => !selectedNames.has(attempt.row.normalized_name))
  .map((attempt) => ({
    rank: attempt.rank,
    canonical_name: attempt.row.canonical_name,
    normalized_name: attempt.row.normalized_name,
    source_page_url: attempt.row.source_page_url,
    source_url: attempt.row.source_url,
    source_kind: attempt.row.source_kind,
    object_path: attempt.row.object_path,
    content_type: attempt.row.content_type,
    content_sha256: attempt.row.content_sha256,
    asset_dimensions: attempt.row.asset_dimensions,
    identity_verification: attempt.row.identity_verification,
    acquisition_method: attempt.method,
    reason: 'verified fallback candidate beyond the first 100 selected rows',
    primary_failure: attempt.primary_failure ?? null,
    secondary_failure: attempt.secondary_failure ?? null,
  }));
const report = {
  batch_id: BATCH_ID,
  generated_at: manifest.generated_at,
  candidate_pool: CANDIDATES.length,
  eligible_candidates: eligible.length,
  acquired_candidates: attempts.length,
  ranked_pool: CANDIDATES.map((target, index) => {
    const rank = index + 1;
    const normalizedName = normalize(target.canonicalName);
    const selectedAttempt = selected.find((attempt) => attempt.rank === rank);
    const acquiredAttempt = attempts.find((attempt) => attempt.rank === rank);
    const rejection = rejected.find((item) => item.rank === rank);
    return {
      rank,
      canonical_name: target.canonicalName,
      normalized_name: normalizedName,
      aliases: [target.canonicalName, ...target.aliases],
      website_url: target.websiteUrl,
      evidence_key: target.evidenceKey,
      evidence_rank: target.evidenceRank,
      evidence_url: EVIDENCE[target.evidenceKey].url,
      transaction_relevance: target.transactionRelevance,
      acquisition_preference: target.preferAppStore ? 'APPLE_APP_STORE' : 'OFFICIAL_SITE',
      outcome: selectedAttempt
        ? 'SELECTED'
        : acquiredAttempt
          ? 'VERIFIED_NOT_SELECTED'
          : 'REJECTED_OR_ALREADY_PRESENT',
      failure: rejection?.reason ?? null,
    };
  }),
  selected_count: selectedReport.length,
  selected: selectedReport,
  verified_not_selected_count: verifiedNotSelected.length,
  verified_not_selected: verifiedNotSelected,
  rejected,
  contact_sheet: contactSheetReportPath,
  visual_review: {
    status: 'GENERATED_FOR_REVIEW',
    reviewed_at: null,
    notes: null,
  },
};
writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);

const validation = validateCatalog(manifest);
const artifactValidation = validateBatchOutputs
  ? await validateBatchArtifacts(manifest)
  : null;
console.log(JSON.stringify({
  ...validation,
  artifacts: artifactValidation,
  candidatePool: CANDIDATES.length,
  eligible: eligible.length,
  acquired: attempts.length,
  rejected: rejected.length,
  assetDirectory,
  report: reportPath,
}, null, 2));
