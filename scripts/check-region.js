const fs = require('fs');
const https = require('https');
const { HttpsProxyAgent } = require('https-proxy-agent');

const proxy = process.env.HTTPS_PROXY || process.env.https_proxy;

let agent = null;
if (proxy) {
  console.log(`using proxy: ${proxy}`);
  agent = new HttpsProxyAgent(proxy);
}

const COOKIE_FILE = '.cookies';

const WHITELIST_REGIONS = new Set([
  'albania',
  'andorra',
  'armenia',
  'austria',
  'azerbaijan',
  'belarus',
  'belgium',
  'bosnia and herzegovina',
  'bulgaria',
  'croatia',
  'cyprus',
  'czech republic',
  'czechia',
  'denmark',
  'estonia',
  'finland',
  'france',
  'georgia',
  'germany',
  'greece',
  'hungary',
  'iceland',
  'ireland',
  'italy',
  'kazakhstan',
  'kosovo',
  'latvia',
  'liechtenstein',
  'lithuania',
  'luxembourg',
  'malta',
  'moldova',
  'monaco',
  'montenegro',
  'netherlands',
  'north macedonia',
  'norway',
  'poland',
  'portugal',
  'romania',
  'russia',
  'russian federation',
  'san marino',
  'serbia',
  'slovakia',
  'slovenia',
  'spain',
  'sweden',
  'switzerland',
  'turkey',
  'ukraine',
  'united kingdom',
  'vatican city',
  'antigua and barbuda',
  'argentina',
  'bahamas',
  'barbados',
  'belize',
  'bolivia',
  'brazil',
  'canada',
  'chile',
  'colombia',
  'costa rica',
  'cuba',
  'dominica',
  'dominican republic',
  'ecuador',
  'el salvador',
  'grenada',
  'guatemala',
  'guyana',
  'haiti',
  'honduras',
  'jamaica',
  'mexico',
  'nicaragua',
  'panama',
  'paraguay',
  'peru',
  'saint kitts and nevis',
  'saint lucia',
  'saint vincent and the grenadines',
  'suriname',
  'trinidad and tobago',
  'united states',
  'united states of america',
  'uruguay',
  'venezuela',
]);

const REGION_ALIASES = {
  uk: 'united kingdom',
  usa: 'united states',
  us: 'united states',
};

function normalizeRegion(region) {
  const normalized = String(region || '')
    .toLowerCase()
    .replace(/\(.*?\)/g, '')
    .replace(/[.'’]/g, '')
    .replace(/,/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return REGION_ALIASES[normalized] || normalized;
}

function loadCookies() {
  const value = fs.readFileSync(COOKIE_FILE, 'utf8').trim();
  if (!value) {
    throw new Error('Cookie file .cookies is empty');
  }
  return value;
}

function requestUConfig(cookie) {
  return new Promise((resolve, reject) => {
    const request = https.request(
      {
        method: 'GET',
        hostname: 'exhentai.org',
        path: '/uconfig.php',
        headers: {
          Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          Referer: 'https://exhentai.org/',
          'Upgrade-Insecure-Requests': 1,
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
          cookie,
        },
        agent,
      },
      (res) => {
        let html = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          html += chunk;
        });
        res.on('end', () => {
          if (res.statusCode !== 200) {
            reject(
              new Error(
                `Unexpected status code: ${res.statusCode}. body=${html
                  .slice(0, 200)
                  .replace(/\s+/g, ' ')}`
              )
            );
            return;
          }
          resolve(html);
        });
      }
    );

    request.on('error', reject);
    request.end();
  });
}

function extractRegion(html) {
  const match =
    html.match(/from\s*<strong>\s*([^<]+)\s*<\/strong>\s*or use a VPN or proxy/i) ||
    html.match(/from\s*<strong>\s*([^<]+)\s*<\/strong>/i);
  return match ? match[1].trim() : '';
}

function isWhitelistedRegion(region) {
  const normalized = normalizeRegion(region);
  return WHITELIST_REGIONS.has(normalized);
}

async function run() {
  const cookie = loadCookies();
  const html = await requestUConfig(cookie);
  const region = extractRegion(html);

  if (!region) {
    console.error('Failed to parse region from uconfig page.');
    process.exitCode = 1;
    return;
  }

  const normal = isWhitelistedRegion(region);
  console.log(region);
  if (!normal) {
    console.error(`Current region is not in the whitelist: ${region}`);
    process.exitCode = 2;
    return;
  }
}

run().catch((err) => {
  console.error(err.stack || err);
  process.exit(1);
});
