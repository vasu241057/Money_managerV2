// Ranked candidate pool for the third India-focused 100-logo expansion.
// The first section is driven by current India App Store positions and high
// statement relevance. It is followed by verified reserves from batch two,
// recurring billers, Indian retail brands, and common paid subscriptions.

import { SECOND_BATCH_CANDIDATES } from './merchant-logo-batch-2-candidates.mjs';

export const THIRD_BATCH_EVIDENCE = {
  financeTraffic: {
    label: 'Similarweb India Finance traffic ranking, June 2026',
    url: 'https://www.similarweb.com/top-websites/india/finance/',
  },
  foodTraffic: {
    label: 'Similarweb India Food & Drink traffic ranking, June 2026',
    url: 'https://www.similarweb.com/top-websites/india/food-and-drink/',
  },
  travelTraffic: {
    label: 'Similarweb India Travel & Tourism traffic ranking, June 2026',
    url: 'https://www.similarweb.com/top-websites/india/travel-and-tourism/',
  },
  telecomIndia: {
    label: 'TRAI Indian Telecom Services Performance Indicators, January-March 2026',
    url: 'https://www.trai.gov.in/node/18167',
  },
  insurerIndia: {
    label: 'IRDAI licensed insurance entities in India',
    url: 'https://irdai.gov.in/en/list-of-licensed-insurance-entities',
  },
  recurringBills: {
    label: 'RBI Bharat Bill Payment System recurring-payment categories',
    url: 'https://www.rbi.org.in/scripts/FS_Overview.aspx?fn=9',
  },
  paidSoftware: {
    label: 'Common software, cloud, and subscription descriptors on Indian payment statements',
    url: 'https://www.kantar.com/campaigns/brandz/india',
  },
  healthcareIndia: {
    label: 'High-frequency Indian hospital, diagnostic, and healthcare payment coverage',
    url: 'https://irdai.gov.in/insurers/health-insurers',
  },
};

function candidate(canonicalName, websiteUrl, aliases, evidenceKey, options = {}) {
  return {
    canonicalName,
    websiteUrl,
    aliases,
    evidenceKey,
    evidenceRank: options.evidenceRank ?? null,
    transactionRelevance: options.transactionRelevance ?? 'MEDIUM',
    simpleIconSlug: options.simpleIconSlug ?? null,
    appStoreTerm: options.appStoreTerm ?? canonicalName,
    appStoreId: options.appStoreId ?? null,
    expectedSellerTokens: options.expectedSellerTokens ?? [],
    forceSimpleIcon: options.forceSimpleIcon === true,
    simpleIconColor: options.simpleIconColor ?? null,
    rejectReason: options.rejectReason ?? null,
    preferAppStore: options.preferAppStore === true,
    disableAppStore: options.disableAppStore === true,
  };
}

const app = (canonicalName, websiteUrl, aliases, evidenceKey, evidenceRank, options = {}) =>
  candidate(canonicalName, websiteUrl, aliases, evidenceKey, {
    ...options,
    evidenceRank,
    preferAppStore: true,
    transactionRelevance: options.transactionRelevance ?? 'HIGH',
  });

const brand = (canonicalName, websiteUrl, aliases, evidenceKey, options = {}) =>
  candidate(canonicalName, websiteUrl, aliases, evidenceKey, {
    ...options,
    transactionRelevance: options.transactionRelevance ?? 'HIGH',
  });

function carry(name) {
  const prior = SECOND_BATCH_CANDIDATES.find((item) => item.canonicalName === name);
  if (!prior) throw new Error(`Missing verified batch-two reserve candidate: ${name}`);
  return { ...prior };
}

function carryWith(name, overrides) {
  return { ...carry(name), ...overrides };
}

const VERIFIED_RESERVE_OVERRIDES = new Map([
  ['Jio', {
    appStoreId: '1074964262',
    appStoreTerm: 'MyJio',
    expectedSellerTokens: ['JIO PLATFORMS', 'RELIANCE JIO'],
    preferAppStore: true,
  }],
  ['Tata Motors', {
    appStoreId: '1129966888',
    appStoreTerm: 'Tata Motors Service Connect',
    expectedSellerTokens: ['TATA MOTORS'],
    preferAppStore: true,
  }],
  ['Jockey India', {
    appStoreTerm: 'Jockey India Shopping App',
    expectedSellerTokens: ['PAGE INDUSTRIES', 'JOCKEY'],
    preferAppStore: true,
  }],
  ['Surf Excel', {
    rejectReason: 'visual review rejected the HUL parent-company favicon returned by the official brand page',
  }],
  ['Lux', {
    rejectReason: 'visual review rejected the tiny ambiguous single-letter favicon',
  }],
  ['William Penn', {
    rejectReason: 'visual review rejected an extremely short, poorly legible wordmark crop',
  }],
  ['WedTree', {
    rejectReason: 'visual review rejected a white-on-white wordmark',
  }],
  ['Aesthesy', {
    rejectReason: 'visual review rejected a nearly invisible white-on-white asset',
  }],
]);

const VERIFIED_RESERVE_NAMES = [
  'CESC',
  'Mahanagar Gas',
  'Indraprastha Gas',
  'Nayara Energy',
  'Infosys',
  'UltraTech Cement',
  'Jio',
  'HCLTech',
  'Wipro',
  'DLF',
  'Surf Excel',
  'Tech Mahindra',
  'Lodha',
  'Coforge',
  'Ambuja Cement',
  'JK Cement',
  'MRF',
  'Jockey India',
  'Tata Motors',
  'Godrej Properties',
  'Lifebuoy',
  'Parachute',
  'Lux',
  'Oberoi Realty',
  'Indian Art Villa',
  'William Penn',
  'WedTree',
  'SapnaOnline',
  'Aesthesy',
  'Hawkins',
  'Carrier Midea India',
  'John Jacobs',
  'Girias',
  'Pearl Shaving',
  'BSNL',
  'Excitel',
  'Hathway',
  'MSEDCL',
  'BSES Rajdhani',
  'BWSSB',
];

// Deliberately larger than 100. A candidate does not enter the catalog unless
// its identity and image bytes validate, its aliases remain unambiguous, its
// hash is unique, and its final contact-sheet tile passes visual review.
export const THIRD_BATCH_CANDIDATES = [
  carry('Kuku TV'),

  app('ICICI Lombard', 'https://www.icicilombard.com/', ['ILTakeCare', 'ICICI Lombard General Insurance'], 'health', 12, {
    appStoreTerm: 'ILTakeCare Insurance App',
    expectedSellerTokens: ['ICICI', 'LOMBARD'],
  }),
  app('FreshToHome', 'https://www.freshtohome.com/', ['Fresh To Home'], 'food', 24, {
    expectedSellerTokens: ['FRESHTOHOME'],
  }),
  app('GIVA', 'https://www.giva.co/', ['GIVA Jewellery', 'GIVA Jewelry'], 'shopping', 23, {
    expectedSellerTokens: ['GIVA'],
  }),
  app('Indeed', 'https://in.indeed.com/', ['Indeed India', 'Indeed Job Search'], 'business', 7, {
    simpleIconSlug: 'indeed',
    forceSimpleIcon: true,
  }),
  app('PSPCL', 'https://www.pspcl.in/', ['Punjab State Power Corporation', 'PSPCL Consumer'], 'business', 16, {
    appStoreTerm: 'PSPCL Consumer App',
    expectedSellerTokens: ['PSPCL', 'PUNJAB STATE POWER'],
  }),
  app('TeraBox', 'https://www.terabox.com/', ['Tera Box', 'TeraBox Premium'], 'productivity', 13, {
    expectedSellerTokens: ['FLEXTECH', 'TERABOX'],
  }),
  app('Story TV', 'https://www.storymatrix.com/', ['StoryTV', 'Story TV Short Drama'], 'entertainment', 6, {
    expectedSellerTokens: ['STORYMATRIX', 'STORY'],
  }),
  app('NetShort', 'https://www.netshort.com/', ['Net Short', 'NetShort Premium'], 'entertainment', 9, {
    expectedSellerTokens: ['NETSHORT', 'NET SHORT'],
  }),
  app('Style Union', 'https://styleunion.in/', ['StyleUnion'], 'lifestyle', 22, {
    expectedSellerTokens: ['STYLE UNION', 'STYLEUNION'],
  }),
  app('Disha AI Health Coach', 'https://disha.ai/', ['Disha Health', 'Disha AI'], 'health', 3, {
    appStoreTerm: 'Disha AI Health Coach',
    expectedSellerTokens: ['DISHA'],
  }),
  app('Da Fit', 'https://www.crrepa.com/', ['DaFit'], 'health', 7, {
    expectedSellerTokens: ['CRREPA', 'MO YOUNG'],
  }),
  app('StepsApp', 'https://steps.app/', ['Steps App', 'StepsApp Pedometer'], 'health', 9, {
    expectedSellerTokens: ['STEPSAPP'],
  }),
  app('Sweatcoin', 'https://sweatco.in/', ['Sweat Coin', 'Sweatcoin Premium'], 'health', 14, {
    expectedSellerTokens: ['SWEATCO'],
  }),
  app('Fastrack', 'https://www.fastrack.in/', ['Fastrack Smart World', 'Fastrack Reflex'], 'health', 16, {
    expectedSellerTokens: ['TITAN', 'FASTRACK'],
  }),
  app('JustFit', 'https://enerjoy.life/', ['Just Fit', 'JustFit Premium'], 'health', 21, {
    expectedSellerTokens: ['ENERJOY', 'JUSTFIT'],
  }),
  app('Cure Skin', 'https://cureskin.com/', ['CureSkin', 'Cure Skin and Hair'], 'health', 23, {
    expectedSellerTokens: ['CURESKIN', 'CURE SKIN'],
  }),
  app('Driefcase', 'https://www.driefcase.com/', ['Driefcase Health Records'], 'medical', 9, {
    expectedSellerTokens: ['DRIECASE', 'DRIECASE HEALTH'],
  }),
  app('Jan Aushadhi', 'https://janaushadhi.gov.in/', ['Jan Aushadhi Sugam', 'PMBI'], 'medical', 22, {
    appStoreTerm: 'Jan Aushadhi Sugam',
    expectedSellerTokens: ['PMBI', 'PHARMACEUTICALS'],
  }),
  app('Vidal Health', 'https://www.vidalhealthtpa.com/', ['Vidal Health TPA'], 'medical', 25, {
    expectedSellerTokens: ['VIDAL'],
  }),
  app('WePlay', 'https://weplayapp.com/', ['We Play', 'WePlay Premium'], 'entertainment', 14, {
    expectedSellerTokens: ['WEPLAY', 'WEJOY'],
  }),
  app('GoodShort', 'https://www.goodshort.com/', ['Good Short', 'GoodShort Premium'], 'entertainment', 23, {
    expectedSellerTokens: ['GOODSHORT', 'NEWREADING'],
  }),

  brand('Manappuram Finance', 'https://www.manappuram.com/', ['Manappuram', 'MAFIL'], 'financeTraffic'),
  brand('IIFL Finance', 'https://www.iifl.com/', ['IIFL', 'India Infoline Finance'], 'financeTraffic'),
  brand('Poonawalla Fincorp', 'https://poonawallafincorp.com/', ['Poonawalla Finance'], 'financeTraffic'),
  brand('Shriram Finance', 'https://www.shriramfinance.in/', ['Shriram Transport Finance', 'STFC'], 'financeTraffic'),
  brand('Axis Max Life', 'https://www.axismaxlife.com/', ['Max Life Insurance', 'Axis Max Life Insurance'], 'insurerIndia'),
  brand('Tata AIG', 'https://www.tataaig.com/', ['Tata AIG General Insurance'], 'insurerIndia'),
  brand('Oriental Insurance', 'https://orientalinsurance.org.in/', ['OICL', 'The Oriental Insurance'], 'insurerIndia'),
  brand('New India Assurance', 'https://www.newindia.co.in/', ['NIACL', 'The New India Assurance'], 'insurerIndia'),
  brand('United India Insurance', 'https://uiic.co.in/', ['UIIC'], 'insurerIndia'),
  brand('National Insurance', 'https://nationalinsurance.nic.co.in/', ['NICL', 'National Insurance Company'], 'insurerIndia'),
  brand('Cholamandalam Finance', 'https://www.cholamandalam.com/', ['Chola Finance', 'Chola MS'], 'financeTraffic'),
  brand('L&T Finance', 'https://www.ltfinance.com/', ['L and T Finance', 'LTF'], 'financeTraffic'),
  brand('Aditya Birla Capital', 'https://www.adityabirlacapital.com/', ['AB Capital', 'ABCL'], 'financeTraffic'),
  brand('Hero FinCorp', 'https://www.herofincorp.com/', ['Hero Finance'], 'financeTraffic'),
  brand('Mahindra Finance', 'https://www.mahindrafinance.com/', ['Mahindra and Mahindra Financial Services', 'MMFSL'], 'financeTraffic'),

  ...VERIFIED_RESERVE_NAMES.map((name) =>
    carryWith(name, VERIFIED_RESERVE_OVERRIDES.get(name) ?? {})),

  brand('Tata Play Fiber', 'https://www.tataplayfiber.com/', ['Tata Sky Broadband'], 'telecomIndia'),
  brand('YOU Broadband', 'https://youbroadband.in/', ['YOU Broadband India', 'YouTele'], 'telecomIndia'),
  brand('Spectra', 'https://www.spectra.co/', ['Spectra Broadband', 'Spectranet'], 'telecomIndia'),
  brand('Alliance Broadband', 'https://alliancebroadband.co.in/', ['Alliance Broadband Services'], 'telecomIndia'),
  brand('DEN Networks', 'https://www.dennetworks.com/', ['DEN Broadband', 'DEN Cable'], 'telecomIndia', {
    rejectReason: 'visual review rejected a promotional download banner instead of a merchant mark',
  }),
  brand('GTPL Hathway', 'https://www.gtpl.net/', ['GTPL Broadband', 'GTPL KCBPL'], 'telecomIndia'),
  brand('RailWire', 'https://www.railwire.co.in/', ['Rail Wire', 'RailTel RailWire'], 'telecomIndia'),
  brand('Tikona', 'https://www.tikona.in/', ['Tikona Broadband', 'Tikona Infinet'], 'telecomIndia'),
  app('Adani Electricity', 'https://www.adanielectricity.com/', ['Adani Electricity Mumbai', 'AEML'], 'recurringBills', null, {
    appStoreId: '1526452496',
    expectedSellerTokens: ['ADANI ELECTRICITY MUMBAI'],
  }),
  brand('BESCOM', 'https://bescom.karnataka.gov.in/', ['Bangalore Electricity Supply Company'], 'recurringBills'),
  brand('Delhi Jal Board', 'https://delhijalboard.delhi.gov.in/', ['DJB Water Bill'], 'recurringBills', {
    rejectReason: 'visual review rejected a promotional government header image rather than a biller logo',
  }),
  brand('TANGEDCO', 'https://www.tnebltd.gov.in/', ['TNEB', 'Tamil Nadu Electricity Board'], 'recurringBills'),
  brand('KSEB', 'https://kseb.in/', ['Kerala State Electricity Board', 'KSEB Limited'], 'recurringBills'),
  brand('UPPCL', 'https://www.uppclonline.com/', ['Uttar Pradesh Power Corporation'], 'recurringBills'),
  brand('WBSEDCL', 'https://www.wbsedcl.in/', ['West Bengal State Electricity Distribution'], 'recurringBills'),
  brand('Gujarat Gas', 'https://www.gujaratgas.com/', ['Gujarat Gas Limited', 'GGL'], 'recurringBills'),
  brand('GAIL Gas', 'https://gailgas.com/', ['GAIL Gas Limited'], 'recurringBills'),
  brand('Sabarmati Gas', 'https://www.sabarmatigas.in/', ['SGL Gas'], 'recurringBills'),
  brand('Chennai Metro Water', 'https://cmwssb.tn.gov.in/', ['CMWSSB', 'Chennai Water'], 'recurringBills', {
    rejectReason: 'visual review rejected the generic Tamil Nadu government crest',
  }),
  brand('Hyderabad Metro Water', 'https://www.hyderabadwater.gov.in/', ['HMWSSB', 'Hyderabad Water Board'], 'recurringBills'),

  brand('Costa Coffee India', 'https://www.costacoffee.in/', ['Costa Coffee'], 'foodTraffic'),
  brand('Nilgiris', 'https://www.nilgiris1905.com/', ['Nilgiris Supermarket', 'Nilgiris Dairy'], 'foodTraffic'),
  brand('V-Mart', 'https://stores.vmartretail.com/', ['VMart', 'V-Mart Retail'], 'ecommerce', {
    rejectReason: 'visual review rejected a promotional storefront banner instead of a logo',
  }),
  brand('Allen Solly', 'https://allensolly.abfrl.in/', ['Allen Solly India'], 'brandz'),
  app('Van Heusen', 'https://vanheusenindia.abfrl.in/', ['Van Heusen India'], 'brandz', null, {
    appStoreId: '1521959195',
    expectedSellerTokens: ['ADITYA BIRLA FASHION', 'VAN HEUSEN'],
  }),
  brand('Peter England', 'https://peterengland.abfrl.in/', ['Peter England India'], 'brandz'),
  brand('Louis Philippe', 'https://louisphilippe.abfrl.in/', ['Louis Philippe India'], 'brandz'),
  brand('Raymond', 'https://www.myraymond.com/', ['Raymond Limited', 'The Raymond Shop'], 'brandz'),
  brand('W for Woman', 'https://wforwoman.com/', ['W India', 'W Womenswear'], 'ecommerce', { disableAppStore: true }),
  brand("Levi's India", 'https://levi.in/', ['Levis India', "Levi's"], 'ecommerce'),
  brand('Marks & Spencer India', 'https://www.marksandspencer.in/', ['Marks and Spencer', 'M&S India'], 'ecommerce'),
  brand('Jack & Jones India', 'https://www.jackjones.in/', ['Jack and Jones'], 'ecommerce'),
  brand('ONLY India', 'https://www.only.in/', ['ONLY Fashion'], 'ecommerce', { disableAppStore: true }),
  brand('Vero Moda India', 'https://www.veromoda.in/', ['Vero Moda'], 'ecommerce'),
  brand('U.S. Polo Assn.', 'https://uspoloassn.in/', ['US Polo', 'USPA India'], 'ecommerce'),
  brand('Woodland', 'https://www.woodlandworldwide.com/', ['Woodland India', 'Aero Club Woodland'], 'ecommerce'),
  brand('Campus Activewear', 'https://www.campusshoes.com/', ['Campus Shoes'], 'ecommerce'),
  brand('Red Tape', 'https://redtape.com/', ['RedTape India'], 'ecommerce'),
  brand('Mochi Shoes', 'https://www.mochishoes.com/', ['Mochi'], 'ecommerce'),
  brand('Pepe Jeans India', 'https://www.pepejeans.in/', ['Pepe Jeans'], 'ecommerce'),
  brand('Relaxo', 'https://relaxofootwear.com/', ['Relaxo Footwear', 'Sparx Relaxo'], 'brandz'),
  brand('ASICS India', 'https://www.asics.co.in/', ['ASICS'], 'ecommerce'),

  brand('Evernote', 'https://evernote.com/', ['Evernote Personal', 'Evernote Professional'], 'paidSoftware', { simpleIconSlug: 'evernote', forceSimpleIcon: true }),
  app('Slack', 'https://slack.com/intl/en-in/', ['Slack Pro', 'Slack Technologies'], 'business', null, { expectedSellerTokens: ['SLACK'] }),
  brand('Freshworks', 'https://www.freshworks.com/', ['Freshdesk', 'Freshservice'], 'paidSoftware'),
  brand('GoDaddy', 'https://www.godaddy.com/en-in/', ['Go Daddy', 'GoDaddy India'], 'paidSoftware', { simpleIconSlug: 'godaddy', forceSimpleIcon: true }),
  brand('Namecheap', 'https://www.namecheap.com/', ['Namecheap Inc'], 'paidSoftware', { simpleIconSlug: 'namecheap', forceSimpleIcon: true }),
  brand('Hostinger', 'https://www.hostinger.in/', ['Hostinger India'], 'paidSoftware', { simpleIconSlug: 'hostinger', forceSimpleIcon: true }),
  brand('Amazon Web Services', 'https://aws.amazon.com/', ['AWS', 'Amazon AWS'], 'paidSoftware'),
  brand('Microsoft Azure', 'https://azure.microsoft.com/en-in/', ['Azure', 'MS Azure'], 'paidSoftware'),
  brand('DigitalOcean', 'https://www.digitalocean.com/', ['Digital Ocean'], 'paidSoftware', { simpleIconSlug: 'digitalocean', forceSimpleIcon: true }),
  brand('Cloudflare', 'https://www.cloudflare.com/', ['Cloudflare Pro'], 'paidSoftware', { simpleIconSlug: 'cloudflare', forceSimpleIcon: true }),
  brand('Jira', 'https://www.atlassian.com/software/jira', ['Jira Software', 'Atlassian Jira'], 'paidSoftware', { simpleIconSlug: 'jira', forceSimpleIcon: true }),
  brand('Confluence', 'https://www.atlassian.com/software/confluence', ['Atlassian Confluence'], 'paidSoftware', { simpleIconSlug: 'confluence', forceSimpleIcon: true }),
  brand('Trello', 'https://trello.com/', ['Trello Premium', 'Atlassian Trello'], 'paidSoftware', { simpleIconSlug: 'trello', forceSimpleIcon: true }),
  brand('Asana', 'https://asana.com/', ['Asana Premium'], 'paidSoftware', { simpleIconSlug: 'asana', forceSimpleIcon: true }),
  brand('Monday.com', 'https://monday.com/', ['Monday', 'Monday Work Management'], 'paidSoftware'),
  brand('Todoist', 'https://todoist.com/', ['Todoist Pro', 'Doist'], 'paidSoftware', { simpleIconSlug: 'todoist', forceSimpleIcon: true }),
  brand('Calendly', 'https://calendly.com/', ['Calendly Standard'], 'paidSoftware', { simpleIconSlug: 'calendly', forceSimpleIcon: true }),
  brand('Wix', 'https://www.wix.com/', ['Wix.com', 'Wix Premium'], 'paidSoftware', { simpleIconSlug: 'wix', forceSimpleIcon: true }),
  brand('Reddit', 'https://www.reddit.com/', ['Reddit Premium'], 'paidSoftware', { simpleIconSlug: 'reddit', forceSimpleIcon: true }),
  brand('Telegram Premium', 'https://telegram.org/', ['Telegram', 'Telegram Messenger'], 'paidSoftware', { simpleIconSlug: 'telegram', forceSimpleIcon: true }),
  brand('Snapchat+', 'https://www.snapchat.com/plus', ['Snapchat Plus', 'Snap Inc'], 'paidSoftware', { simpleIconSlug: 'snapchat', forceSimpleIcon: true }),
  brand('Figma', 'https://www.figma.com/', ['Figma Professional'], 'paidSoftware', { simpleIconSlug: 'figma', forceSimpleIcon: true }),
  brand('Miro', 'https://miro.com/', ['Miro Starter', 'RealtimeBoard'], 'paidSoftware', { simpleIconSlug: 'miro', forceSimpleIcon: true }),
  brand('Linear', 'https://linear.app/', ['Linear Business'], 'paidSoftware', { simpleIconSlug: 'linear', forceSimpleIcon: true }),
  brand('TradingView', 'https://in.tradingview.com/', ['Trading View', 'TradingView Premium'], 'financeTraffic', { evidenceRank: 4, simpleIconSlug: 'tradingview', forceSimpleIcon: true }),

  brand('Aster Hospitals', 'https://www.asterhospitals.in/', ['Aster Hospital', 'Aster DM Healthcare'], 'healthcareIndia'),
  brand('KIMS Hospitals', 'https://www.kimshospitals.com/', ['KIMS Hospital', 'Krishna Institute of Medical Sciences'], 'healthcareIndia'),
  brand("Rainbow Children's Hospital", 'https://www.rainbowhospitals.in/', ['Rainbow Hospital'], 'healthcareIndia'),
  brand('Yashoda Hospitals', 'https://www.yashodahospitals.com/', ['Yashoda Hospital'], 'healthcareIndia'),
  brand('Medicover Hospitals', 'https://www.medicoverhospitals.in/', ['Medicover Hospital'], 'healthcareIndia'),
  brand('SRL Diagnostics', 'https://www.srlworld.com/', ['SRL Labs', 'SRL Limited'], 'healthcareIndia'),
  brand('Vijaya Diagnostic Centre', 'https://www.vijayadiagnostic.com/', ['Vijaya Diagnostics'], 'healthcareIndia'),
  brand("Dr Agarwal's Eye Hospital", 'https://www.dragarwal.com/', ['Dr Agarwals Eye Hospital', 'Dr Agarwal Eye Hospital'], 'healthcareIndia'),
  brand('Centre for Sight', 'https://www.centreforsight.net/', ['Center for Sight', 'CFS Eye Hospital'], 'healthcareIndia'),
];
