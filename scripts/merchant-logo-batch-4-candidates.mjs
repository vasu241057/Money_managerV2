// Ranked candidate pool for the fourth India-focused 100-logo expansion.
//
// Research starts with the complete catalog/alias audit performed on 9 August
// 2026. The ranking then fills material Indian transaction gaps: banks,
// brokers, insurers, recurring billers, durable-goods brands, hospitals, and
// paid digital services. The last section brings forward only byte-verified,
// not-selected reserves from batch three. Acquisition still performs identity,
// bytes, dimension, alias, and duplicate-hash checks before a row is selected.

import { THIRD_BATCH_CANDIDATES } from './merchant-logo-batch-3-candidates.mjs';

export const FOURTH_BATCH_EVIDENCE = {
  financeApps: {
    label: 'Apple App Store India Finance chart, accessed 9 August 2026',
    url: 'https://apps.apple.com/in/iphone/charts/6015',
  },
  securitiesIndia: {
    label: 'SEBI registered stock broker and depository participant coverage, accessed 9 August 2026',
    url: 'https://www.sebi.gov.in/intermediaries.html',
  },
  insurersIndia: {
    label: 'IRDAI licensed insurance-entity coverage, accessed 9 August 2026',
    url: 'https://irdai.gov.in/en/list-of-licensed-insurance-entities',
  },
  recurringBills: {
    label: 'RBI Bharat Bill Payment System recurring-biller categories, accessed 9 August 2026',
    url: 'https://www.rbi.org.in/scripts/FS_Overview.aspx?fn=9',
  },
  telecomIndia: {
    label: 'TRAI telecom and broadband-service coverage, accessed 9 August 2026',
    url: 'https://www.trai.gov.in/',
  },
  retailTraffic: {
    label: 'Similarweb India retail and ecommerce traffic coverage, June 2026',
    url: 'https://www.similarweb.com/top-websites/india/e-commerce-and-shopping/',
  },
  travelTraffic: {
    label: 'Similarweb India travel and tourism traffic coverage, June 2026',
    url: 'https://www.similarweb.com/top-websites/india/travel-and-tourism/',
  },
  healthcareIndia: {
    label: 'India high-frequency hospital, diagnostics, and health-service coverage',
    url: 'https://irdai.gov.in/insurers/health-insurers',
  },
  appCharts: {
    label: 'Apple App Store India category charts, accessed 9 August 2026',
    url: 'https://apps.apple.com/in/iphone/charts',
  },
  paymentRelevance: {
    label: 'India UPI merchant-transaction relevance: Government of India FY 2025–26 update',
    url: 'https://www.pib.gov.in/PressReleasePage.aspx?PRID=2257087',
  },
};

function candidate(canonicalName, websiteUrl, aliases, evidenceKey, options = {}) {
  return {
    canonicalName,
    websiteUrl,
    aliases,
    evidenceKey,
    evidenceRank: options.evidenceRank ?? null,
    transactionRelevance: options.transactionRelevance ?? 'HIGH',
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

const app = (canonicalName, websiteUrl, aliases, evidenceKey, options = {}) =>
  candidate(canonicalName, websiteUrl, aliases, evidenceKey, {
    ...options,
    preferAppStore: true,
  });

const brand = (canonicalName, websiteUrl, aliases, evidenceKey, options = {}) =>
  candidate(canonicalName, websiteUrl, aliases, evidenceKey, options);

function reserve(name) {
  const row = THIRD_BATCH_CANDIDATES.find((item) => item.canonicalName === name);
  if (!row) throw new Error(`Missing batch-three verified reserve candidate: ${name}`);
  return { ...row };
}

// This deliberately oversized pool leaves room for anti-bot pages, obsolete
// apps, ambiguous App Store identities, favicon-only sites, duplicate artwork,
// and aliases owned by an earlier catalog row. It contains 178 candidates.
export const FOURTH_BATCH_CANDIDATES = [
  // Indian financial institutions and frequently paid financial services.
  app('Central Bank of India', 'https://www.centralbankofindia.co.in/', ['Central Bank', 'Cent Mobile'], 'financeApps', { expectedSellerTokens: ['CENTRAL BANK'] }),
  app('Bank of Maharashtra', 'https://bankofmaharashtra.in/', ['BOM', 'Maha Mobile'], 'financeApps', { expectedSellerTokens: ['BANK OF MAHARASHTRA'] }),
  app('UCO Bank', 'https://www.ucobank.com/', ['UCO', 'UCO mBanking'], 'financeApps', { expectedSellerTokens: ['UCO BANK'] }),
  app('Punjab & Sind Bank', 'https://punjabandsindbank.co.in/', ['PSB Bank', 'PSB UNiC'], 'financeApps', { expectedSellerTokens: ['PUNJAB AND SIND'] }),
  app('India Post Payments Bank', 'https://www.ippbonline.com/', ['IPPB', 'Post Office Payments Bank'], 'financeApps', { expectedSellerTokens: ['INDIA POST'] }),
  app('Ujjivan Small Finance Bank', 'https://www.ujjivansfb.bank/', ['Ujjivan SFB', 'Ujjivan Bank'], 'financeApps', { expectedSellerTokens: ['UJJIVAN'] }),
  app('Equitas Small Finance Bank', 'https://www.equitas.bank.in/', ['Equitas SFB', 'Equitas Bank'], 'financeApps', { expectedSellerTokens: ['EQUITAS'] }),
  app('Jana Small Finance Bank', 'https://www.janabank.com/', ['Jana SFB', 'Jana Bank'], 'financeApps', { expectedSellerTokens: ['JANA'] }),
  app('ESAF Small Finance Bank', 'https://www.esaf.bank.in/', ['ESAF SFB', 'ESAF Bank'], 'financeApps', { expectedSellerTokens: ['ESAF'] }),
  app('Suryoday Small Finance Bank', 'https://suryoday.bank/', ['Suryoday SFB', 'Suryoday Bank'], 'financeApps', { expectedSellerTokens: ['SURYODAY'] }),
  app('Unity Small Finance Bank', 'https://unitybank.co.in/', ['Unity SFB', 'Unity Bank'], 'financeApps', { expectedSellerTokens: ['UNITY'] }),
  app('DCB Bank', 'https://www.dcbbank.com/', ['DCB', 'DCB Mobile'], 'financeApps', { expectedSellerTokens: ['DCB BANK'] }),
  app('CSB Bank', 'https://www.csb.co.in/', ['Catholic Syrian Bank', 'CSB mPassbook'], 'financeApps', { expectedSellerTokens: ['CSB BANK', 'CATHOLIC SYRIAN'] }),
  app('Tamilnad Mercantile Bank', 'https://www.tmb.in/', ['TMB', 'TMB mConnect'], 'financeApps', { expectedSellerTokens: ['TAMILNAD MERCANTILE'] }),
  app('OneCard', 'https://www.getonecard.app/', ['One Card', 'FPL Technologies'], 'financeApps', { appStoreTerm: 'OneCard Credit Card App', expectedSellerTokens: ['ONECARD', 'FPL'] }),
  app('LazyPay', 'https://www.lazypay.in/', ['Lazy Pay', 'PayU LazyPay'], 'financeApps', { expectedSellerTokens: ['LAZYPAY', 'PAYU'] }),
  app('Fibe', 'https://www.fibe.in/', ['EarlySalary', 'Fibe Loans'], 'financeApps', { expectedSellerTokens: ['FIBE', 'EARLYSALARY'] }),
  app('CASHe', 'https://www.cashe.co.in/', ['Cashe Loans'], 'financeApps', { expectedSellerTokens: ['CASHE'] }),
  app('MoneyTap', 'https://www.moneytap.com/', ['Money Tap', 'MoneyTap Credit Line'], 'financeApps', { expectedSellerTokens: ['MONEYTAP'] }),
  app('Lendingkart', 'https://www.lendingkart.com/', ['Lending Kart'], 'financeApps', { expectedSellerTokens: ['LENDINGKART'] }),
  app('Indifi', 'https://www.indifi.com/', ['Indifi Technologies'], 'financeApps', { expectedSellerTokens: ['INDIFI'] }),
  app('Paytm Money', 'https://www.paytmmoney.com/', ['PaytmMoney'], 'financeApps', { expectedSellerTokens: ['PAYTM'] }),

  // Registered market intermediaries and account-maintenance billers.
  app('CAMS', 'https://www.camsonline.com/', ['Computer Age Management Services', 'myCAMS'], 'securitiesIndia', { expectedSellerTokens: ['CAMS', 'COMPUTER AGE'] }),
  app('KFintech', 'https://www.kfintech.com/', ['KFin Technologies', 'KFinKart'], 'securitiesIndia', { expectedSellerTokens: ['KFIN'] }),
  app('CDSL', 'https://www.cdslindia.com/', ['Central Depository Services', 'CDSL Easi'], 'securitiesIndia', { expectedSellerTokens: ['CDSL'] }),
  app('NSDL', 'https://nsdl.co.in/', ['National Securities Depository', 'NSDL CAS'], 'securitiesIndia', { expectedSellerTokens: ['NSDL'] }),
  app('Sharekhan', 'https://www.sharekhan.com/', ['Share Khan'], 'securitiesIndia', { expectedSellerTokens: ['SHAREKHAN'] }),
  app('Motilal Oswal', 'https://www.motilaloswal.com/', ['Motilal Oswal Securities', 'MO Investor'], 'securitiesIndia', { expectedSellerTokens: ['MOTILAL OSWAL'] }),
  app('ICICI Direct', 'https://www.icicidirect.com/', ['ICICI Securities', 'ICICIDirect'], 'securitiesIndia', { expectedSellerTokens: ['ICICI SECURITIES', 'ICICIDIRECT'] }),
  app('HDFC Securities', 'https://www.hdfcsec.com/', ['HDFC Sec', 'HDFC Securities Limited'], 'securitiesIndia', { expectedSellerTokens: ['HDFC SECURITIES'] }),
  app('Kotak Securities', 'https://www.kotaksecurities.com/', ['Kotak Sec', 'Kotak Neo'], 'securitiesIndia', { expectedSellerTokens: ['KOTAK SECURITIES'] }),
  app('5paisa', 'https://www.5paisa.com/', ['5 Paisa', 'Five Paisa'], 'securitiesIndia', { expectedSellerTokens: ['5PAISA'] }),
  app('Edelweiss', 'https://www.edelweiss.in/', ['Edelweiss Broking', 'Nuvama Wealth'], 'securitiesIndia', { expectedSellerTokens: ['EDELWEISS'] }),
  app('Nuvama', 'https://www.nuvama.com/', ['Nuvama Wealth', 'Edelweiss Nuvama'], 'securitiesIndia', { expectedSellerTokens: ['NUVAMA'] }),
  app('Ventura Securities', 'https://www.venturasecurities.com/', ['Ventura', 'Ventura Prime'], 'securitiesIndia', { expectedSellerTokens: ['VENTURA'] }),
  app('Anand Rathi', 'https://www.anandrathi.com/', ['AnandRathi', 'Anand Rathi Wealth'], 'securitiesIndia', { expectedSellerTokens: ['ANAND RATHI'] }),
  app('Choice Broking', 'https://choiceindia.com/', ['Choice India', 'Choice FinX'], 'securitiesIndia', { expectedSellerTokens: ['CHOICE'] }),
  app('Alice Blue', 'https://aliceblueonline.com/', ['AliceBlue', 'ANT Mobi'], 'securitiesIndia', { expectedSellerTokens: ['ALICE BLUE'] }),

  // Insurance premiums are a high-value recurring payment category.
  app('HDFC ERGO', 'https://www.hdfcergo.com/', ['HDFC Ergo General Insurance', 'my:health Suraksha'], 'insurersIndia', { expectedSellerTokens: ['HDFC ERGO'] }),
  app('SBI General Insurance', 'https://www.sbigeneral.in/', ['SBI General', 'SBIG Insurance'], 'insurersIndia', { expectedSellerTokens: ['SBI GENERAL'] }),
  app('Reliance General Insurance', 'https://www.reliancegeneral.co.in/', ['Reliance General', 'RGI'], 'insurersIndia', { expectedSellerTokens: ['RELIANCE GENERAL'], rejectReason: 'visual review rejected a 48px icon from a redirected, non-Reliance General domain' }),
  app('Royal Sundaram', 'https://www.royalsundaram.in/', ['Royal Sundaram General Insurance'], 'insurersIndia', { expectedSellerTokens: ['ROYAL SUNDARAM'], rejectReason: 'visual review rejected a Google logo asset from the official site' }),
  app('Kotak Life', 'https://www.kotaklife.com/', ['Kotak Mahindra Life Insurance', 'Kotak Life Insurance'], 'insurersIndia', { expectedSellerTokens: ['KOTAK LIFE'] }),
  app('Canara HSBC Life Insurance', 'https://www.canarahsbclife.com/', ['Canara HSBC Life', 'Canara HSBC Oorja'], 'insurersIndia', { expectedSellerTokens: ['CANARA HSBC'] }),
  app('IndiaFirst Life', 'https://www.indiafirstlife.com/', ['India First Life Insurance'], 'insurersIndia', { expectedSellerTokens: ['INDIAFIRST'] }),
  app('Aviva Life Insurance', 'https://www.avivaindia.com/', ['Aviva Life', 'Aviva India'], 'insurersIndia', { expectedSellerTokens: ['AVIVA'] }),
  app('Pramerica Life Insurance', 'https://www.pramericalife.in/', ['Pramerica Life'], 'insurersIndia', { expectedSellerTokens: ['PRAMERICA'], rejectReason: 'visual review rejected a 50px favicon as too small for a catalog logo' }),
  app('Bandhan Life', 'https://www.bandhanlife.com/', ['Bandhan Life Insurance'], 'insurersIndia', { expectedSellerTokens: ['BANDHAN LIFE'] }),
  app('PNB MetLife', 'https://www.pnbmetlife.com/', ['PNB Metlife India Insurance'], 'insurersIndia', { expectedSellerTokens: ['PNB METLIFE'] }),
  app('Future Generali India Insurance', 'https://general.futuregenerali.in/', ['Future Generali', 'FGII'], 'insurersIndia', { expectedSellerTokens: ['FUTURE GENERALI'] }),
  app('Liberty General Insurance', 'https://www.libertyinsurance.in/', ['Liberty Insurance'], 'insurersIndia', { expectedSellerTokens: ['LIBERTY GENERAL'] }),
  app('IFFCO Tokio', 'https://www.iffcotokio.co.in/', ['IFFCO Tokio General Insurance'], 'insurersIndia', { expectedSellerTokens: ['IFFCO TOKIO'] }),
  app('Universal Sompo', 'https://www.universalsompo.com/', ['Universal Sompo General Insurance'], 'insurersIndia', { expectedSellerTokens: ['UNIVERSAL SOMPO'] }),
  app('Magma HDI', 'https://www.magmahdi.com/', ['Magma HDI General Insurance'], 'insurersIndia', { expectedSellerTokens: ['MAGMA HDI'] }),
  app('Raheja QBE', 'https://www.rahejaqbe.com/', ['Raheja QBE General Insurance'], 'insurersIndia', { expectedSellerTokens: ['RAHEJA QBE'], rejectReason: 'visual review rejected a Great Place to Work certification badge rather than the merchant logo' }),

  // Electricity, water, DTH, and broadband make up recurring BBPS descriptors.
  app('TSSPDCL', 'https://www.tssouthernpower.com/', ['Telangana State Southern Power Distribution Company', 'TSSPDCL Bill'], 'recurringBills', { expectedSellerTokens: ['TSSPDCL', 'TELANGANA SOUTHERN'] }),
  app('TGNPDCL', 'https://www.tgnpdcl.com/', ['Telangana Northern Power Distribution Company', 'TGNPDCL Bill'], 'recurringBills', { expectedSellerTokens: ['TGNPDCL', 'TELANGANA NORTHERN'], rejectReason: 'visual review rejected an unrelated portrait returned by the official page' }),
  app('APEPDCL', 'https://www.apeasternpower.com/', ['Andhra Pradesh Eastern Power Distribution Company', 'APEPDCL Bill'], 'recurringBills', { expectedSellerTokens: ['APEPDCL'] }),
  app('APSPDCL', 'https://www.apspdcl.in/', ['Andhra Pradesh Southern Power Distribution Company', 'APSPDCL Bill'], 'recurringBills', { expectedSellerTokens: ['APSPDCL'] }),
  app('UHBVN', 'https://uhbvn.org.in/', ['Uttar Haryana Bijli Vitran Nigam', 'UHBVN Bill'], 'recurringBills', { expectedSellerTokens: ['UHBVN'] }),
  app('DHBVN', 'https://dhbvn.org.in/', ['Dakshin Haryana Bijli Vitran Nigam', 'DHBVN Bill'], 'recurringBills', { expectedSellerTokens: ['DHBVN'] }),
  app('Delhi Jal Board', 'https://delhijalboard.delhi.gov.in/', ['DJB', 'Delhi Water Bill'], 'recurringBills', { expectedSellerTokens: ['DELHI JAL'] }),
  app('Chennai Metro Water', 'https://cmwssb.tn.gov.in/', ['CMWSSB', 'Chennai Water Bill'], 'recurringBills', { expectedSellerTokens: ['CMWSSB'] }),
  app('Sun Direct', 'https://www.sundirect.in/', ['Sun Direct DTH'], 'recurringBills', { expectedSellerTokens: ['SUN DIRECT'] }),
  app('d2h', 'https://www.d2h.com/', ['Videocon d2h', 'D2H Recharge'], 'recurringBills', { expectedSellerTokens: ['D2H', 'VIDEOCON'] }),
  app('Airtel Digital TV', 'https://www.airtel.in/dth/', ['Airtel DTH', 'Airtel TV Recharge'], 'recurringBills', { expectedSellerTokens: ['AIRTEL', 'BHARTI'] }),
  brand('SITI Broadband', 'https://www.sitinetworks.com/', ['SITI Cable', 'SITI Networks'], 'telecomIndia'),
  brand('Kerala Vision', 'https://www.keralavisionisp.com/', ['Kerala Vision Broadband', 'KCCL'], 'telecomIndia'),
  brand('DEN Broadband', 'https://www.dennetworks.com/', ['DEN Networks', 'DEN Cable'], 'telecomIndia', { rejectReason: 'visual review rejected a download-app promotional banner rather than a logo' }),
  brand('Fastway', 'https://www.fastway.in/', ['Fastway Cable', 'Fastway Broadband'], 'telecomIndia'),

  // High-frequency retail, durable goods, and automotive servicing payments.
  brand('V-Mart Retail', 'https://www.vmartretail.com/', ['VMart', 'V Mart'], 'retailTraffic', { rejectReason: 'visual review rejected an unrelated marketplace asset after the official domain redirected away' }),
  brand('Reliance Trends', 'https://reliancetrends.com/', ['Trends', 'Reliance Retail Trends'], 'retailTraffic', { rejectReason: 'visual review rejected a blank social-media image from a redirected storefront' }),
  brand('Clovia', 'https://www.clovia.com/', ['Clovia India'], 'retailTraffic'),
  brand('Zivame', 'https://www.zivame.com/', ['Zivame India'], 'retailTraffic'),
  brand('Libas', 'https://www.libas.in/', ['Libas India'], 'retailTraffic'),
  brand('Aurelia', 'https://www.aureliashop.in/', ['Aurelia India'], 'retailTraffic', { rejectReason: 'visual review rejected an unrelated Aurelia language-tutor app; the publisher did not match the official apparel domain' }),
  brand('Go Colors', 'https://gocolors.com/', ['GoColors'], 'retailTraffic'),
  brand('Soch', 'https://www.soch.com/', ['Soch India'], 'retailTraffic'),
  brand('Blissclub', 'https://blissclub.com/', ['Bliss Club'], 'retailTraffic'),
  brand('DaMENSCH', 'https://www.damensch.com/', ['Da Mensch'], 'retailTraffic'),
  brand('Bombay Shaving Company', 'https://www.bombayshavingcompany.com/', ['Bombay Shaving'], 'retailTraffic'),
  brand('Beardo', 'https://beardo.in/', ['Beardo India'], 'retailTraffic'),
  brand('Ustraa', 'https://www.ustraa.com/', ['Ustraa India'], 'retailTraffic'),
  brand('Sangeetha Mobiles', 'https://www.sangeethamobiles.com/', ['Sangeetha'], 'retailTraffic'),
  brand('Lotus Electronics', 'https://www.lotuselectronics.com/', ['Lotus'], 'retailTraffic'),
  brand('Pai International', 'https://www.paiinternational.in/', ['Pai Electronics'], 'retailTraffic'),
  brand('Ather Energy', 'https://www.atherenergy.com/', ['Ather', 'Ather Grid'], 'paymentRelevance'),
  brand('Ola Electric', 'https://www.olaelectric.com/', ['Ola Electric Mobility'], 'paymentRelevance', { rejectReason: 'visual review rejected a white-on-white wordmark that is not usable in the catalog tile' }),
  brand('Hero Electric', 'https://www.heroelectric.in/', ['Hero Electric Vehicles'], 'paymentRelevance'),
  brand('CEAT', 'https://www.ceat.com/', ['CEAT Tyres'], 'paymentRelevance'),
  brand('Apollo Tyres', 'https://www.apollotyres.com/', ['Apollo Tyre'], 'paymentRelevance'),
  brand('JK Tyre', 'https://www.jktyre.com/', ['JK Tyres'], 'paymentRelevance'),
  brand('Goodyear India', 'https://www.goodyear.co.in/', ['Goodyear'], 'paymentRelevance'),
  brand('Michelin India', 'https://www.michelin.in/', ['Michelin'], 'paymentRelevance'),

  // Consumer electronics and appliances are common higher-value statements.
  brand('realme', 'https://www.realme.com/in/', ['Realme India'], 'retailTraffic'),
  brand('Nothing', 'https://in.nothing.tech/', ['Nothing India'], 'retailTraffic', { rejectReason: 'visual review rejected a generic red-dot favicon with no identifiable Nothing branding' }),
  brand('Nokia', 'https://www.nokia.com/phones/en_in/', ['Nokia India'], 'retailTraffic', { simpleIconSlug: 'nokia' }),
  brand('ASUS India', 'https://www.asus.com/in/', ['ASUS', 'Asus India'], 'retailTraffic'),
  brand('HP India', 'https://www.hp.com/in-en/', ['HP', 'Hewlett Packard India'], 'retailTraffic'),
  brand('Acer India', 'https://www.acer.com/in-en/', ['Acer'], 'retailTraffic', { simpleIconSlug: 'acer' }),
  brand('LG India', 'https://www.lg.com/in/', ['LG'], 'retailTraffic', { simpleIconSlug: 'lg' }),
  brand('Haier India', 'https://www.haier.com/in/', ['Haier'], 'retailTraffic'),
  brand('Panasonic India', 'https://www.panasonic.com/in/', ['Panasonic'], 'retailTraffic', { simpleIconSlug: 'panasonic' }),
  brand('TCL India', 'https://www.tcl.com/in/en', ['TCL'], 'retailTraffic'),
  brand('Dyson India', 'https://www.dyson.in/', ['Dyson'], 'retailTraffic'),
  brand('Whirlpool India', 'https://www.whirlpoolindia.com/', ['Whirlpool'], 'retailTraffic'),
  brand('Voltas', 'https://www.voltas.com/', ['Voltas India'], 'retailTraffic'),
  brand('Blue Star', 'https://www.bluestarindia.com/', ['BlueStar'], 'retailTraffic'),
  brand('IFB', 'https://www.ifbappliances.com/', ['IFB Appliances'], 'retailTraffic'),
  brand('Kent RO', 'https://www.kent.co.in/', ['Kent', 'Kent RO Systems'], 'retailTraffic'),
  brand('Eureka Forbes', 'https://www.eurekaforbes.com/', ['Aquaguard', 'EurekaForbes'], 'retailTraffic'),
  brand('Crompton', 'https://www.crompton.co.in/', ['Crompton Greaves Consumer Electricals'], 'retailTraffic'),
  brand('Bajaj Electricals', 'https://www.bajajelectricals.com/', ['Bajaj Electrical'], 'retailTraffic'),
  brand('V-Guard', 'https://www.vguard.in/', ['V Guard'], 'retailTraffic'),
  brand('Usha', 'https://www.usha.com/', ['Usha International'], 'retailTraffic'),

  // Travel and dining brands add common consumer-card and UPI descriptors.
  brand('Ginger Hotels', 'https://www.gingerhotels.com/', ['Ginger'], 'travelTraffic'),
  brand('Marriott', 'https://www.marriott.com/', ['Marriott Bonvoy', 'Marriott Hotels'], 'travelTraffic', { simpleIconSlug: 'marriott', forceSimpleIcon: true }),
  brand('Hilton', 'https://www.hilton.com/en/locations/india/', ['Hilton Honors', 'Hilton Hotels'], 'travelTraffic', { simpleIconSlug: 'hilton', forceSimpleIcon: true }),
  brand('Hyatt', 'https://www.hyatt.com/', ['World of Hyatt', 'Hyatt Hotels'], 'travelTraffic'),
  brand('Radisson Hotels', 'https://www.radissonhotels.com/en-us/destination/india', ['Radisson', 'Radisson Rewards'], 'travelTraffic'),
  brand('Sarovar Hotels', 'https://www.sarovarhotels.com/', ['Sarovar'], 'travelTraffic'),
  brand('The Fern Hotels', 'https://www.fernhotels.com/', ['Fern Hotels'], 'travelTraffic'),
  brand('Alliance Air', 'https://www.allianceair.in/', ['AllianceAir'], 'travelTraffic'),
  brand('Star Air', 'https://www.starair.in/', ['StarAir'], 'travelTraffic'),
  brand('Fly91', 'https://fly91.in/', ['FLY91'], 'travelTraffic'),
  brand('Burger Singh', 'https://www.burgersinghonline.com/', ['BurgerSingh'], 'retailTraffic'),
  brand('California Burrito', 'https://www.californiaburrito.in/', ['California Burrito India'], 'retailTraffic'),
  brand('Biryani Blues', 'https://biryaniblues.com/', ['BiryaniBlues'], 'retailTraffic'),
  brand('RollsKing', 'https://rollsking.in/', ['Rolls King'], 'retailTraffic'),
  brand('Chai Sutta Bar', 'https://chaisuttabarindia.com/', ['CSB Chai Sutta Bar'], 'retailTraffic'),
  brand('Samosa Party', 'https://www.samosaparty.in/', ['SamosaParty'], 'retailTraffic'),
  brand('Cafe Delhi Heights', 'https://cafedelhiheights.com/', ['Cafe Delhi'], 'retailTraffic'),
  brand('Nando’s India', 'https://www.nandosindia.com/', ['Nandos India', 'Nando’s'], 'retailTraffic'),
  brand('Punjab Grill', 'https://www.punjabgrill.in/', ['PunjabGrill'], 'retailTraffic'),

  // Medical care, local mobility, learning, and subscriptions fill recurring gaps.
  app('Aster DM Healthcare', 'https://www.asterhospitals.in/', ['Aster Hospitals', 'Aster DM'], 'healthcareIndia', { expectedSellerTokens: ['ASTER'] }),
  app('Cloudnine', 'https://www.cloudninecare.com/', ['Cloud Nine', 'Cloudnine Hospitals'], 'healthcareIndia', { expectedSellerTokens: ['CLOUDNINE'] }),
  app('HCG Cancer Centre', 'https://www.hcgoncology.com/', ['HealthCare Global', 'HCG Hospitals'], 'healthcareIndia', { expectedSellerTokens: ['HCG', 'HEALTHCARE GLOBAL'] }),
  app('Yulu', 'https://www.yulu.bike/', ['Yulu Bikes'], 'appCharts', { expectedSellerTokens: ['YULU'] }),
  app('Bounce', 'https://bounce.in/', ['Bounce Infinity', 'Bounce Scooters'], 'appCharts', { expectedSellerTokens: ['BOUNCE'] }),
  app('Vogo', 'https://vogo.in/', ['Vogo Rentals'], 'appCharts', { expectedSellerTokens: ['VOGO'] }),
  app('Revv', 'https://www.revv.co.in/', ['Revv Self Drive'], 'appCharts', { expectedSellerTokens: ['REVV'] }),
  app('Savaari', 'https://www.savaari.com/', ['Savaari Car Rentals'], 'appCharts', { expectedSellerTokens: ['SAVAARI'] }),
  app('inDrive', 'https://indrive.com/en-in', ['in Drive'], 'appCharts', { expectedSellerTokens: ['INDRIVE'] }),
  app('Chalo', 'https://chalo.com/', ['Chalo Bus'], 'appCharts', { expectedSellerTokens: ['CHALO'] }),
  app('Tummoc', 'https://tummoc.com/', ['Tummoc App'], 'appCharts', { expectedSellerTokens: ['TUMMOC'] }),
  app('StudyIQ', 'https://www.studyiq.com/', ['Study IQ'], 'appCharts', { expectedSellerTokens: ['STUDYIQ'] }),
  brand('LeetCode', 'https://leetcode.com/', ['Leet Code', 'LeetCode Premium'], 'appCharts', { simpleIconSlug: 'leetcode', forceSimpleIcon: true }),
  brand('Skillshare', 'https://www.skillshare.com/', ['Skillshare Premium'], 'appCharts', { simpleIconSlug: 'skillshare', forceSimpleIcon: true }),
  brand('Udacity', 'https://www.udacity.com/', ['Udacity Nanodegree'], 'appCharts', { simpleIconSlug: 'udacity', forceSimpleIcon: true }),
  brand('Pluralsight', 'https://www.pluralsight.com/', ['Pluralsight Skills'], 'appCharts', { simpleIconSlug: 'pluralsight', forceSimpleIcon: true }),
  brand('MasterClass', 'https://www.masterclass.com/', ['MasterClass Subscription'], 'appCharts'),

  // Byte-verified batch-three reserves. The runner may reuse their immutable
  // local cache only after re-hashing it; otherwise it performs a fresh lookup.
  reserve('BESCOM'),
  reserve('Campus Activewear'),
  reserve('Mochi Shoes'),
  reserve('Pepe Jeans India'),
  reserve('Relaxo'),
  reserve('ASICS India'),
  reserve('Evernote'),
  reserve('Slack'),
  reserve('Freshworks'),
  reserve('GoDaddy'),
  reserve('Namecheap'),
  reserve('Hostinger'),
  reserve('Amazon Web Services'),
  reserve('Microsoft Azure'),
  reserve('DigitalOcean'),
  reserve('Cloudflare'),
  reserve('Jira'),
  reserve('Confluence'),
  reserve('Trello'),
  reserve('Asana'),
  reserve('Monday.com'),
  reserve('Todoist'),
  reserve('Calendly'),
  reserve('Wix'),
  reserve('Reddit'),
  reserve('Telegram Premium'),
  reserve('Snapchat+'),
  reserve('Figma'),
  reserve('Miro'),
  reserve('Linear'),
  reserve('TradingView'),
  reserve('KIMS Hospitals'),
  reserve("Rainbow Children's Hospital"),
  reserve('Yashoda Hospitals'),
  reserve('Medicover Hospitals'),
  reserve('Vijaya Diagnostic Centre'),
  reserve("Dr Agarwal's Eye Hospital"),
  reserve('Centre for Sight'),
];
