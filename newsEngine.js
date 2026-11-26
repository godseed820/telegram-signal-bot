const FEEDS = [
  "https://www.coindesk.com/arc/outboundfeeds/rss/",
  "https://cointelegraph.com/rss",
];
const CACHE_MS = 5 * 60 * 1000;
const MAX_AGE_MS = 24 * 60 * 60 * 1000;
let cachedAt = 0;
let cachedArticles = [];

const TERMS = {
  BTC: ["bitcoin", "btc", "xbt"],
  ETH: ["ethereum", "ether", "eth"],
  SOL: ["solana", "sol"],
};
const POSITIVE = /surge|rally|soar|record high|inflow|adoption|approval|approved|bullish|breakout|accumulate|buying pressure|upgrade|partnership|growth|recovery|launch/i;
const NEGATIVE = /crash|plunge|sell.?off|outflow|hack|exploit|lawsuit|ban|banned|rejected|bearish|liquidation|breach|outage|downgrade|fraud|investigation|sanction/i;
const HIGH_IMPACT = /fed|federal reserve|interest rate|inflation|cpi|jobs report|nonfarm|nfp|sec |securities and exchange|etf|hack|exploit|outage|ban|lawsuit|war|sanction|regulation|liquidation/i;

function decodeXml(value = "") {
  return value.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/<[^>]*>/g, " ").replace(/&amp;/g, "&").replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/\s+/g, " ").trim();
}

function tag(item, name) {
  const match = item.match(new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${name}>`, "i"));
  return match ? decodeXml(match[1]) : "";
}

export function parseNewsFeed(xml, source = "Unknown", nowMs = Date.now()) {
  const blocks = [...String(xml).matchAll(/<(?:item|entry)\b[^>]*>[\s\S]*?<\/(?:item|entry)>/gi)];
  return blocks.map(([item]) => {
    const title = tag(item, "title");
    const description = tag(item, "description") || tag(item, "summary") || tag(item, "content:encoded");
    const published = tag(item, "pubDate") || tag(item, "published") || tag(item, "updated");
    const linkTag = item.match(/<link\b[^>]*href=["']([^"']+)["'][^>]*\/?\s*>/i);
    const link = tag(item, "link") || linkTag?.[1] || "";
    const publishedAt = Date.parse(published);
    if (!title || !Number.isFinite(publishedAt) || nowMs - publishedAt > MAX_AGE_MS || publishedAt > nowMs + 5 * 60 * 1000) return null;
    return { title, description, link, source, publishedAt };
  }).filter(Boolean);
}

async function fetchFeed(url) {
  const response = await fetch(url, {
    headers: { "user-agent": "SignalBot/1.0 (news aggregation)" },
    signal: AbortSignal.timeout(8000),
  });
  if (!response.ok) throw new Error(`News feed returned HTTP ${response.status}`);
  return response.text();
}

export async function getNewsArticles(nowMs = Date.now()) {
  if (nowMs - cachedAt < CACHE_MS) return cachedArticles;
  const results = await Promise.allSettled(FEEDS.map(fetchFeed));
  const successful = results.filter((result) => result.status === "fulfilled").length;
  if (successful === 0) throw new Error("All configured news feeds failed");
  const articles = [];
  results.forEach((result, index) => {
    if (result.status !== "fulfilled") return;
    articles.push(...parseNewsFeed(result.value, index === 0 ? "CoinDesk" : "Cointelegraph", nowMs));
  });
  const unique = new Map();
  for (const article of articles) {
    const key = article.title.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
    if (!unique.has(key)) unique.set(key, article);
  }
  cachedArticles = [...unique.values()].sort((a, b) => b.publishedAt - a.publishedAt).slice(0, 100);
  cachedAt = nowMs;
  return cachedArticles;
}

export function analyzeNewsForSymbol(symbol, articles, nowMs = Date.now()) {
  const coin = String(symbol).replace(/USDT$/, "").toUpperCase();
  const terms = TERMS[coin] || [coin.toLowerCase()];
  const relevant = articles.filter((article) => {
    const text = `${article.title} ${article.description}`.toLowerCase();
    return terms.some((term) => new RegExp(`(^|[^a-z0-9])${term}([^a-z0-9]|$)`, "i").test(text));
  }).slice(0, 8);
  if (!relevant.length) return {
    status: "UNAVAILABLE_OR_NO_MATCH", sentiment: "NEUTRAL", confidence: "LOW", impact: "UNKNOWN",
    headlineCount: 0, conflict: false, summary: "No recent instrument-specific headlines were found; news was not used as a trading confirmation.", headlines: [],
  };
  let score = 0;
  let impactCount = 0;
  for (const article of relevant) {
    const text = `${article.title} ${article.description}`;
    score += (POSITIVE.test(text) ? 1 : 0) - (NEGATIVE.test(text) ? 1 : 0);
    if (HIGH_IMPACT.test(text)) impactCount++;
  }
  const sentiment = score >= 2 ? "BULLISH" : score <= -2 ? "BEARISH" : "MIXED";
  const confidence = Math.abs(score) >= 3 ? "MODERATE" : "LOW";
  const freshHighImpact = relevant.filter((a) => HIGH_IMPACT.test(`${a.title} ${a.description}`) && nowMs - a.publishedAt <= 45 * 60 * 1000);
  const conflict = freshHighImpact.length >= 2 && (
    (sentiment === "BEARISH") || (sentiment === "BULLISH")
  );
  return {
    status: "OK", sentiment, confidence, impact: impactCount ? "ELEVATED" : "NORMAL",
    headlineCount: relevant.length, conflict,
    summary: `${relevant.length} recent relevant headline(s); sentiment ${sentiment.toLowerCase()}, confidence ${confidence.toLowerCase()}. Headlines are keyword-classified, not AI-verified.`,
    headlines: relevant.slice(0, 4).map(({ title, source, link, publishedAt }) => ({ title, source, link, publishedAt })),
  };
}

export async function getSymbolNews(symbol, nowMs = Date.now()) {
  try {
    const articles = await getNewsArticles(nowMs);
    return analyzeNewsForSymbol(symbol, articles, nowMs);
  } catch (error) {
    return {
      status: "FEED_ERROR", sentiment: "UNKNOWN", confidence: "NONE", impact: "UNKNOWN",
      headlineCount: 0, conflict: false, summary: `News feed unavailable (${error.message}); technical analysis continued without news confirmation.`, headlines: [],
    };
  }
}
