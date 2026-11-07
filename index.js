import { pathToFileURL } from "node:url";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import zlib from "node:zlib";

const TELEGRAM_API = "https://api.telegram.org";
const KRAKEN_CHARTS_API = "https://futures.kraken.com/api/charts/v1";
const MIN_PUBLIC_REQUEST_INTERVAL_MS = 500;
const MONITOR_FILE = resolve(process.cwd(), "trade-monitors.json");
const activeMonitors = new Map();

let persistenceQueue = Promise.resolve();
let nextPublicRequestAt = 0;

// =====================================================================
// SETTINGS YOU CAN TWEAK
// =====================================================================
// The bot trades ONLY these three markets.
const TRADE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const KRAKEN_MARKETS = Object.freeze({
  BTCUSDT: Object.freeze({ contract: "PF_XBTUSD", pair: "BTC:USD" }),
  ETHUSDT: Object.freeze({ contract: "PF_ETHUSD", pair: "ETH:USD" }),
  SOLUSDT: Object.freeze({ contract: "PF_SOLUSD", pair: "SOL:USD" }),
});
const COIN_ALIASES = Object.freeze({
  BTC: "BTCUSDT", XBT: "BTCUSDT", ETH: "ETHUSDT", SOL: "SOLUSDT",
});

const AUTO_SCAN_INTERVAL_MS = 3 * 60 * 1000;
const ALERT_COOLDOWN_MS = 2 * 60 * 60 * 1000;
// With 25x leverage, liquidation is roughly 3.5-4% away.
// Signals with a stop-loss wider than this are skipped (turned into WAIT).
const MAX_SL_PCT = 2.5;
const TAKE_PROFIT_R = 2.5; // TP = 2.5 x the risk distance
const MIN_SL_ATR = 0.6; // stop-loss is never closer than 0.6 x ATR(15m)
// ICT kill zones, in UTC hours. Change these if you want other windows.
const KILLZONES = [
  { name: "London", from: 7, to: 10 },
  { name: "New York", from: 12, to: 15 },
];

const SUBSCRIBERS_FILE = resolve(process.cwd(), "auto-subscribers.json");
const autoSubscribers = new Set();
const lastAlertAt = new Map();

// BOS detector settings and independent Telegram alert subscriptions.
const BOS_SUBSCRIBERS_FILE = resolve(process.cwd(), "bos-subscribers.json");
const bosSubscribers = new Set();
const BOS_SCAN_INTERVAL_MS = 5 * 60 * 1000;
const BOS_ALERT_COOLDOWN_MS = 60 * 60 * 1000;
const BOS_SYMBOLS = TRADE_SYMBOLS;
const TF_MS = Object.freeze({
  "1m": 60 * 1000,
  "5m": 5 * 60 * 1000,
  "15m": 15 * 60 * 1000,
  "30m": 30 * 60 * 1000,
  "1h": 60 * 60 * 1000,
  "4h": 4 * 60 * 60 * 1000,
  "1d": 24 * 60 * 60 * 1000,
});
const BOS_TIMEFRAMES = TF_MS;
// Automatic alerts skip 1m and 5m (too noisy). Manual /bos still supports all.
const BOS_AUTO_TIMEFRAMES = ["15m", "30m", "1h", "4h", "1d"];
const lastBosAlertAt = new Map();

// =====================================================================
// BASIC HELPERS
// =====================================================================
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeSymbol(input) {
  const s = String(input || "")
    .trim()
    .toUpperCase()
    .replace(/[-_/]/g, "")
    .replace(/(USDT|USD|PERP)$/, "");
  return COIN_ALIASES[s] || null;
}

async function waitForKrakenRequestSlot() {
  const now = Date.now();
  const scheduledAt = Math.max(now, nextPublicRequestAt);
  nextPublicRequestAt = scheduledAt + MIN_PUBLIC_REQUEST_INTERVAL_MS;
  const waitMs = scheduledAt - now;
  if (waitMs > 0) await sleep(waitMs);
}

async function fetchKrakenJson(url) {
  await waitForKrakenRequestSlot();
  const response = await fetch(url, { signal: AbortSignal.timeout(30000) });
  const body = await response.json();
  if (!response.ok || body?.result === "error") {
    throw new Error(`Kraken API request failed (HTTP ${response.status}).`);
  }
  return body;
}

export function parseKrakenCandles(payload, intervalMs) {
  return payload.candles
    .map((row) => ({
      startTime: Number(row.time),
      closeTime: Number(row.time) + intervalMs,
      open: Number(row.open),
      high: Number(row.high),
      low: Number(row.low),
      close: Number(row.close),
    }))
    .sort((a, b) => a.startTime - b.startTime);
}

async function fetchCandles(symbol, interval, count, intervalMs) {
  const key = normalizeSymbol(symbol);
  if (!key) throw new Error("This bot only trades BTC, ETH and SOL.");
  const url = `${KRAKEN_CHARTS_API}/trade/${encodeURIComponent(KRAKEN_MARKETS[key].contract)}/${interval}?count=${count}`;
  const rawData = await fetchKrakenJson(url);
  return parseKrakenCandles(rawData, intervalMs);
}

// Higher timeframes change slowly, so they are cached to save API calls.
const candleCache = new Map();
const CACHE_TTL_MS = { "1d": 30 * 60 * 1000, "4h": 10 * 60 * 1000, "1h": 4 * 60 * 1000 };
async function fetchCandlesCached(symbol, interval, count) {
  const ttl = CACHE_TTL_MS[interval] || 0;
  const key = `${symbol}-${interval}-${count}`;
  const hit = candleCache.get(key);
  if (ttl && hit && Date.now() - hit.at < ttl) return hit.candles;
  const candles = await fetchCandles(symbol, interval, count, TF_MS[interval]);
  if (ttl) candleCache.set(key, { at: Date.now(), candles });
  return candles;
}

function completedOnly(candles, nowMs) {
  return candles.filter((c) => c.closeTime <= nowMs);
}

function formatPrice(value) {
  if (!value) return "0.00";
  const decimals = value >= 100 ? 2 : value >= 1 ? 4 : 6;
  return value.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

// =====================================================================
// INDICATOR / STRUCTURE HELPERS
// =====================================================================
function calcAtr(candles, period = 14) {
  if (candles.length < 2) return 0;
  const trs = candles.slice(1).map((c, i) =>
    Math.max(c.high - c.low, Math.abs(c.high - candles[i].close), Math.abs(c.low - candles[i].close)),
  );
  const recent = trs.slice(-period);
  return recent.reduce((s, v) => s + v, 0) / recent.length;
}

function calcEma(values, period) {
  if (!values.length) return 0;
  const k = 2 / (period + 1);
  let e = values[0];
  for (let i = 1; i < values.length; i++) e = values[i] * k + e * (1 - k);
  return e;
}

function calcRsi(closes, period = 14) {
  if (closes.length < period + 1) return null;
  let gain = 0;
  let loss = 0;
  for (let i = 1; i <= period; i++) {
    const d = closes[i] - closes[i - 1];
    if (d >= 0) gain += d; else loss -= d;
  }
  let avgGain = gain / period;
  let avgLoss = loss / period;
  for (let i = period + 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    avgGain = (avgGain * (period - 1) + Math.max(d, 0)) / period;
    avgLoss = (avgLoss * (period - 1) + Math.max(-d, 0)) / period;
  }
  if (avgLoss === 0) return 100;
  return 100 - 100 / (1 + avgGain / avgLoss);
}

function findSwings(candles, strength = 2) {
  const highs = [];
  const lows = [];
  for (let i = strength; i < candles.length - strength; i++) {
    let isH = true;
    let isL = true;
    for (let j = i - strength; j <= i + strength; j++) {
      if (j === i) continue;
      if (candles[j].high >= candles[i].high) isH = false;
      if (candles[j].low <= candles[i].low) isL = false;
    }
    if (isH) highs.push({ index: i, price: candles[i].high });
    if (isL) lows.push({ index: i, price: candles[i].low });
  }
  return { highs, lows };
}

function swingTrend(candles, strength = 2) {
  const { highs, lows } = findSwings(candles, strength);
  if (highs.length < 2 || lows.length < 2) return "NEUTRAL";
  const [h1, h2] = highs.slice(-2);
  const [l1, l2] = lows.slice(-2);
  if (h2.price > h1.price && l2.price > l1.price) return "BULLISH";
  if (h2.price < h1.price && l2.price < l1.price) return "BEARISH";
  return "NEUTRAL";
}

function analyzeDailyTrend(completedDaily) {
  const candles = completedDaily;
  if (candles.length < 10) return "NEUTRAL";
  const previous = candles.slice(-10, -5);
  const recent = candles.slice(-5);
  const average = (items, key) => items.reduce((sum, c) => sum + c[key], 0) / items.length;
  const higherHighs = average(recent, "high") > average(previous, "high");
  const higherLows = average(recent, "low") > average(previous, "low");
  const lowerHighs = average(recent, "high") < average(previous, "high");
  const lowerLows = average(recent, "low") < average(previous, "low");
  const risingClose = average(recent, "close") > average(previous, "close");
  const fallingClose = average(recent, "close") < average(previous, "close");
  if (higherHighs && higherLows && risingClose) return "BULLISH";
  if (lowerHighs && lowerLows && fallingClose) return "BEARISH";
  return "CONSOLIDATION";
}

// TOP-DOWN BIAS: Daily + 4H must not disagree. If they conflict, no trade.
function computeBias(daily, h4) {
  const d = daily === "BULLISH" || daily === "BEARISH" ? daily : "NEUTRAL";
  if (d !== "NEUTRAL" && d === h4) return { bias: d, strength: "STRONG" };
  if (d !== "NEUTRAL" && h4 === "NEUTRAL") return { bias: d, strength: "MODERATE" };
  if (d === "NEUTRAL" && h4 !== "NEUTRAL") return { bias: h4, strength: "MODERATE" };
  return { bias: "NEUTRAL", strength: "NONE" };
}

// Supply/demand zones (base candle followed by a strong impulse), unbroken only.
function findZones(candles, tfLabel, atr) {
  const zones = [];
  for (let i = 1; i < candles.length - 1; i++) {
    const base = candles[i];
    const imp = candles[i + 1];
    if (Math.abs(imp.close - imp.open) < atr * 0.8) continue;
    let type = null;
    if (base.close < base.open && imp.close > imp.open && imp.close > base.high) type = "DEMAND";
    else if (base.close > base.open && imp.close < imp.open && imp.close < base.low) type = "SUPPLY";
    if (!type) continue;
    const high = base.high;
    const low = base.low;
    if (high - low > atr * 2.5) continue;
    const later = candles.slice(i + 2);
    const broken = type === "DEMAND" ? later.some((c) => c.close < low) : later.some((c) => c.close > high);
    if (broken) continue;
    let touches = 0;
    let inside = false;
    for (const c of later) {
      const overlap = c.low <= high && c.high >= low;
      if (overlap && !inside) touches++;
      inside = overlap;
    }
    if (touches > 2) continue;
    zones.push({ type, high, low, index: i, tf: tfLabel, touches, startTime: base.startTime });
  }
  return zones.reverse(); // newest first
}

// Horizontal support/resistance levels from clustered swing points (1H + 4H).
function buildSrLevels(c1, c4, atr1) {
  const tol = Math.max(atr1 * 0.3, 1e-9);
  const pts = [];
  const s1 = findSwings(c1, 3);
  for (const s of [...s1.highs, ...s1.lows]) pts.push({ price: s.price, w: 1 });
  const s4 = findSwings(c4, 2);
  for (const s of [...s4.highs, ...s4.lows]) pts.push({ price: s.price, w: 2 });
  pts.sort((a, b) => a.price - b.price);
  const levels = [];
  let group = [];
  const flush = () => {
    if (!group.length) return;
    const price = group.reduce((s, p) => s + p.price, 0) / group.length;
    const touches = group.reduce((s, p) => s + p.w, 0);
    if (group.length >= 2) levels.push({ price, touches, count: group.length });
    group = [];
  };
  for (const p of pts) {
    if (group.length && p.price - group[group.length - 1].price > tol) flush();
    group.push(p);
  }
  flush();
  return levels;
}

// 15m trigger candle in the trade direction.
function ltfConfirmation(c15, dir) {
  const last = c15[c15.length - 1];
  const prev = c15[c15.length - 2];
  const body = Math.abs(last.close - last.open);
  const range = Math.max(last.high - last.low, 1e-12);
  if (dir === "LONG") {
    if (last.close <= last.open) return null;
    const lowerWick = Math.min(last.open, last.close) - last.low;
    if (last.close > prev.high) return "15m bullish candle closed above the previous high";
    if (lowerWick >= body * 1.5 && lowerWick / range >= 0.4) return "15m bullish rejection wick";
    return null;
  }
  if (last.close >= last.open) return null;
  const upperWick = last.high - Math.max(last.open, last.close);
  if (last.close < prev.low) return "15m bearish candle closed below the previous low";
  if (upperWick >= body * 1.5 && upperWick / range >= 0.4) return "15m bearish rejection wick";
  return null;
}

// =====================================================================
// THE SIX STRATEGIES
// Every strategy only looks in the direction of the top-down bias (ctx.dir).
// Each returns null (no setup) or a hit object.
// =====================================================================

// 1) SMC: break of structure / CHOCH, then a retest of the order block (+FVG).
function stratSMC(ctx) {
  const { c15, dir, atr15 } = ctx;
  const n = c15.length;
  if (n < 60) return null;
  const long = dir === "LONG";
  const { highs, lows } = findSwings(c15, 2);
  const swings = long ? highs : lows;
  for (let j = n - 2; j >= n - 26 && j >= 10; j--) {
    const cj = c15[j];
    if (Math.abs(cj.close - cj.open) < atr15 * 0.6) continue;
    if (long ? cj.close <= cj.open : cj.close >= cj.open) continue;
    const prior = swings.filter((s) => s.index <= j - 3).slice(-1)[0];
    if (!prior) continue;
    const brokeNow = long
      ? cj.close > prior.price && c15[j - 1].close <= prior.price
      : cj.close < prior.price && c15[j - 1].close >= prior.price;
    if (!brokeNow) continue;
    const between = c15.slice(prior.index + 1, j);
    if (long ? between.some((c) => c.close > prior.price) : between.some((c) => c.close < prior.price)) continue;

    let ob = null;
    for (let k = j - 1; k >= Math.max(0, j - 12); k--) {
      const ck = c15[k];
      if (long ? ck.close < ck.open : ck.close > ck.open) {
        ob = { high: ck.high, low: ck.low, index: k };
        break;
      }
    }
    if (!ob || ob.high - ob.low > atr15 * 2.5) continue;
    const later = c15.slice(j + 1);
    if (long ? later.some((c) => c.close < ob.low) : later.some((c) => c.close > ob.high)) continue;

    let fvg = null;
    if (long && c15[j - 2].high < cj.low) fvg = { low: c15[j - 2].high, high: cj.low };
    if (!long && c15[j - 2].low > cj.high) fvg = { low: cj.high, high: c15[j - 2].low };

    const tol = atr15 * 0.1;
    const inOb = ctx.live >= ob.low - tol && ctx.live <= ob.high + tol;
    const inFvg = fvg && ctx.live >= fvg.low - tol && ctx.live <= fvg.high + tol;
    if (!inOb && !inFvg) continue;
    const last = c15[n - 1];
    const touched = long ? last.low <= Math.max(ob.high, fvg ? fvg.high : 0) : last.high >= Math.min(ob.low, fvg ? fvg.low : Infinity);
    if (!touched) continue;
    const confirm = ltfConfirmation(c15, dir) || (long ? (last.close > last.open ? "15m bullish candle in the zone" : null) : (last.close < last.open ? "15m bearish candle in the zone" : null));
    if (!confirm) continue;

    const priorTrend = swingTrend(c15.slice(0, j - 1), 2);
    const label = (long && priorTrend === "BEARISH") || (!long && priorTrend === "BULLISH") ? "CHOCH" : "BOS";
    return {
      id: "SMC", name: "SMC", short: "SMC", direction: dir,
      detail: `${label} on 15m (${long ? "bullish" : "bearish"} break of ${formatPrice(prior.price)}), price retesting the order block ${formatPrice(ob.low)}-${formatPrice(ob.high)}${fvg ? " with a fair value gap" : ""}; ${confirm}`,
      invalidation: long ? ob.low : ob.high,
      zones: [{ type: long ? "DEMAND" : "SUPPLY", high: ob.high, low: ob.low, startTime: c15[ob.index].startTime, label: "SMC OB" }],
      levels: [{ price: prior.price, label: label }],
    };
  }
  return null;
}

// 2) Liquidity: sweep of equal highs/lows, previous day high/low or swing points.
function stratLiquidity(ctx) {
  const { c15, c1, c4, d1, dir, atr15, atr1 } = ctx;
  const n = c15.length;
  const long = dir === "LONG";
  const pools = [];
  const pd = d1[d1.length - 1];
  if (pd) pools.push(long ? { price: pd.low, label: "PDL" } : { price: pd.high, label: "PDH" });

  const s1 = findSwings(c1.slice(-80), 2);
  const pts = long ? s1.lows : s1.highs;
  const tol = atr1 * 0.15;
  for (let a = 0; a < pts.length; a++) {
    for (let b = a + 1; b < pts.length; b++) {
      if (Math.abs(pts[a].price - pts[b].price) <= tol && pts[b].index - pts[a].index >= 3) {
        const price = long ? Math.min(pts[a].price, pts[b].price) : Math.max(pts[a].price, pts[b].price);
        pools.push({ price, label: long ? "EQL" : "EQH" });
      }
    }
  }
  if (pts.length) pools.push({ price: pts[pts.length - 1].price, label: long ? "1H LOW" : "1H HIGH" });
  const s4 = findSwings(c4, 2);
  const p4 = long ? s4.lows : s4.highs;
  if (p4.length) pools.push({ price: p4[p4.length - 1].price, label: long ? "4H LOW" : "4H HIGH" });

  for (const pool of pools) {
    for (let k = n - 1; k >= n - 4; k--) {
      const ck = c15[k];
      const swept = long ? ck.low < pool.price && ck.close > pool.price : ck.high > pool.price && ck.close < pool.price;
      if (!swept) continue;
      const depth = long ? pool.price - ck.low : ck.high - pool.price;
      if (depth > atr15 * 2) continue;
      const after = c15.slice(k + 1);
      if (long ? after.some((c) => c.close < pool.price) : after.some((c) => c.close > pool.price)) continue;
      const last = c15[n - 1];
      const mid = (last.high + last.low) / 2;
      const confirmed = k === n - 1
        ? (long ? last.close > mid : last.close < mid)
        : (long ? last.close > last.open : last.close < last.open);
      if (!confirmed) continue;
      const slice = c15.slice(k);
      const extreme = long ? Math.min(...slice.map((c) => c.low)) : Math.max(...slice.map((c) => c.high));
      const sideTxt = long ? "sell-side liquidity below" : "buy-side liquidity above";
      return {
        id: "LIQ", name: "Liquidity", short: "LIQUIDITY", direction: dir,
        detail: `Swept ${sideTxt} ${pool.label} (${formatPrice(pool.price)}) and closed back ${long ? "above" : "below"} it; ${long ? "bullish" : "bearish"} follow-through`,
        invalidation: extreme,
        zones: [],
        levels: [{ price: pool.price, label: `${pool.label} SWEPT` }],
      };
    }
  }
  return null;
}

// 3) Supply & Demand: price testing a fresh 4H/1H zone with a 15m trigger.
function stratSupplyDemand(ctx) {
  const long = ctx.dir === "LONG";
  const want = long ? "DEMAND" : "SUPPLY";
  const zones = [...ctx.zones4, ...ctx.zones1].filter((z) => z.type === want);
  const tol = ctx.atr15 * 0.1;
  const zone = zones.find((z) => ctx.live >= z.low - tol && ctx.live <= z.high + tol);
  if (!zone) return null;
  const confirm = ltfConfirmation(ctx.c15, ctx.dir);
  if (!confirm) return null;
  return {
    id: "SD", name: "Supply & Demand", short: "S/D", direction: ctx.dir,
    detail: `Price is inside a ${zone.tf} ${want.toLowerCase()} zone (${formatPrice(zone.low)}-${formatPrice(zone.high)}, tested ${zone.touches}x before); ${confirm}`,
    invalidation: long ? zone.low : zone.high,
    zones: [{ ...zone, label: `${zone.tf} ${zone.type}` }],
    levels: [],
  };
}

// 4) ICT: kill zone + premium/discount + fair value gap tap + 15m trigger.
function stratICT(ctx) {
  const hour = new Date(ctx.nowMs).getUTCHours();
  const kz = KILLZONES.find((z) => hour >= z.from && hour < z.to);
  if (!kz) return null;
  const long = ctx.dir === "LONG";
  const range = ctx.c1.slice(-48);
  const hi = Math.max(...range.map((c) => c.high));
  const lo = Math.min(...range.map((c) => c.low));
  const mid = (hi + lo) / 2;
  if (long ? ctx.live >= mid : ctx.live <= mid) return null;

  const cands = [];
  for (const [arr, tf, span] of [[ctx.c15, "15M", 48], [ctx.c1, "1H", 24]]) {
    const s = arr.slice(-span);
    for (let i = 1; i < s.length - 1; i++) {
      const a = s[i - 1];
      const b = s[i];
      const c = s[i + 1];
      let gap = null;
      if (long && a.high < c.low && b.close > b.open) gap = { low: a.high, high: c.low };
      if (!long && a.low > c.high && b.close < b.open) gap = { low: c.high, high: a.low };
      if (!gap || gap.high - gap.low < ctx.atr15 * 0.3) continue;
      const later = s.slice(i + 2);
      if (long ? later.some((x) => x.close < gap.low) : later.some((x) => x.close > gap.high)) continue;
      cands.push({ ...gap, tf, startTime: b.startTime });
    }
  }
  cands.sort((x, y) => y.startTime - x.startTime);
  const tol = ctx.atr15 * 0.1;
  const gap = cands.find((g) => ctx.live >= g.low - tol && ctx.live <= g.high + tol);
  if (!gap) return null;
  const confirm = ltfConfirmation(ctx.c15, ctx.dir);
  if (!confirm) return null;

  const size = hi - lo;
  const oteA = long ? hi - 0.79 * size : lo + 0.62 * size;
  const oteB = long ? hi - 0.62 * size : lo + 0.79 * size;
  const inOte = ctx.live >= oteA && ctx.live <= oteB;
  const last3 = ctx.c15.slice(-3);
  return {
    id: "ICT", name: "ICT", short: "ICT", direction: ctx.dir,
    detail: `${kz.name} kill zone, price in ${long ? "discount" : "premium"} of the 2-day range, tapped a ${gap.tf} ${long ? "bullish" : "bearish"} fair value gap (${formatPrice(gap.low)}-${formatPrice(gap.high)})${inOte ? ", inside the 62-79% OTE area" : ""}; ${confirm}`,
    invalidation: long ? Math.min(gap.low, ...last3.map((c) => c.low)) : Math.max(gap.high, ...last3.map((c) => c.high)),
    zones: [{ type: "FVG", high: gap.high, low: gap.low, startTime: gap.startTime, label: `${gap.tf} FVG` }],
    levels: [{ price: mid, label: "EQ 50%" }],
  };
}

// 5) Support & Resistance: bounce or break-and-retest at a clustered level.
function stratSR(ctx) {
  const long = ctx.dir === "LONG";
  const confirm = ltfConfirmation(ctx.c15, ctx.dir);
  if (!confirm) return null;
  const n = ctx.c15.length;
  const last = ctx.c15[n - 1];
  const tol = ctx.atr1 * 0.3;
  let best = null;
  for (const lv of ctx.levels) {
    const touching = long
      ? last.low <= lv.price + tol * 0.5 && last.low >= lv.price - tol && last.close > lv.price
      : last.high >= lv.price - tol * 0.5 && last.high <= lv.price + tol && last.close < lv.price;
    if (!touching) continue;
    if (!best || lv.touches > best.touches) best = lv;
  }
  if (!best) return null;
  const c1 = ctx.c1;
  const flipped = long
    ? c1.slice(-30, -3).some((c) => c.close < best.price) && c1.slice(-3).every((c) => c.close > best.price)
    : c1.slice(-30, -3).some((c) => c.close > best.price) && c1.slice(-3).every((c) => c.close < best.price);
  const last2 = ctx.c15.slice(-2);
  return {
    id: "SR", name: "Support & Resistance", short: "S/R", direction: ctx.dir,
    detail: `${flipped ? "Break and retest" : "Bounce"} at ${long ? "support" : "resistance"} ${formatPrice(best.price)} (${best.count} swing points); ${confirm}`,
    invalidation: long ? Math.min(best.price, ...last2.map((c) => c.low)) : Math.max(best.price, ...last2.map((c) => c.high)),
    zones: [],
    levels: [{ price: best.price, label: long ? "SUPPORT" : "RESISTANCE" }],
  };
}

// 6) APA (price-action structure). Works on 1H structure, 15m trigger.
//    Patterns: Invalidation (incl. New Invalidation), Structure Retest,
//    Hidden Invalidation, Hidden Structure.
// To keep the logic in one place, bullish setups are handled by mirroring
// the prices (low becomes high) and running the bearish rules.
function mirrorCandles(candles, long) {
  if (!long) return candles;
  return candles.map((c) => ({
    startTime: c.startTime, closeTime: c.closeTime,
    open: -c.open, close: -c.close, high: -c.low, low: -c.high,
  }));
}

// Alternating swing highs / lows (zigzag).
export function zigzag(candles, strength = 2) {
  const { highs, lows } = findSwings(candles, strength);
  const all = [
    ...highs.map((s) => ({ ...s, type: "H" })),
    ...lows.map((s) => ({ ...s, type: "L" })),
  ].sort((a, b) => a.index - b.index);
  const out = [];
  for (const p of all) {
    const last = out[out.length - 1];
    if (last && last.type === p.type) {
      if ((p.type === "H" && p.price > last.price) || (p.type === "L" && p.price < last.price)) out[out.length - 1] = p;
    } else {
      out.push(p);
    }
  }
  return out;
}

export function stratAPA(ctx) {
  const long = ctx.dir === "LONG";
  const confirm = ltfConfirmation(ctx.c15, ctx.dir);
  if (!confirm) return null;

  const back = (x) => (long ? -x : x);
  const mc = mirrorCandles(ctx.c1.slice(-120), long);
  const m15 = mirrorCandles(ctx.c15.slice(-10), long);
  const live = long ? -ctx.live : ctx.live;
  const n = mc.length;
  const atr = ctx.atr1;
  const tol = atr * 0.35;
  const zz = zigzag(mc, 2);
  const side = long ? "low" : "high"; // the extreme price is rejected from
  const maxHigh = (fromIdx) => Math.max(...mc.slice(fromIdx).map((c) => c.high), ...m15.map((c) => c.high));
  const minLow = (fromIdx) => Math.min(...mc.slice(fromIdx).map((c) => c.low));
  const base = { id: "APA", name: "APA (Price Action)", short: "APA", direction: ctx.dir, zones: [] };

  // --- A) INVALIDATION / NEW INVALIDATION ---------------------------------
  // Level high -> pullback low -> sweep high above the level -> break of that
  // pullback low -> retest of the level. Stop goes above the retest high
  // (the "new invalidation"); the setup is void if the sweep high is taken.
  for (let k = zz.length - 3; k >= 0; k--) {
    const [h0, l1, h2] = [zz[k], zz[k + 1], zz[k + 2]];
    if (h0.type !== "H" || l1.type !== "L" || h2.type !== "H") continue;
    if (h2.index < n - 70) break;
    const sweep = h2.price - h0.price;
    if (sweep < atr * 0.1 || sweep > atr * 3) continue;
    const brk = mc.findIndex((c, i) => i > h2.index && c.close < l1.price);
    if (brk < 0) continue;
    if (minLow(brk) > l1.price - atr * 0.5) continue; // needs a real move away first
    const retestHigh = maxHigh(brk);
    if (retestHigh >= h2.price) continue; // invalidation high taken -> void
    if (live < h0.price - tol || live > h0.price + tol) continue;
    return {
      ...base, grp: "APA",
      detail: `Invalidation pattern: price swept the ${side} at ${formatPrice(back(h0.price))} (to ${formatPrice(back(h2.price))}), broke the last ${long ? "lower high" : "higher low"} at ${formatPrice(back(l1.price))}, and is now retesting ${formatPrice(back(h0.price))}. Stop goes beyond the retest ${side} (new invalidation); ${confirm}`,
      invalidation: back(retestHigh),
      levels: [
        { price: back(h0.price), label: "APA LEVEL" },
        { price: back(h2.price), label: "INVALIDATION" },
      ],
    };
  }

  // --- D) HIDDEN INVALIDATION ----------------------------------------------
  // A swing level that price crossed back and forth (messy), then a strong
  // displacement candle through it, then a retest of the level from the other side.
  for (let k = zz.length - 1; k >= 0; k--) {
    const l0 = zz[k];
    if (l0.type !== "L" || l0.index > n - 12) continue;
    if (l0.index < n - 90) break;
    const lv = l0.price;
    let cross = 0;
    let prevSide = 0;
    for (let i = l0.index + 1; i < n; i++) {
      const s = mc[i].close > lv ? 1 : -1;
      if (prevSide && s !== prevSide) cross++;
      prevSide = s;
    }
    if (cross < 2) continue;
    let j = -1;
    for (let i = n - 1; i > l0.index; i--) {
      if (mc[i].close < lv && mc[i].open - mc[i].close >= atr) { j = i; break; }
    }
    if (j < 0 || j < n - 40) continue;
    const lastAbove = mc.map((c) => c.close > lv).lastIndexOf(true);
    if (lastAbove > j) continue;
    if (mc.slice(j + 1).some((c) => c.close > lv + tol)) continue;
    if (minLow(j) > lv - atr * 0.6) continue;
    if (live < lv - tol || live > lv + tol) continue;
    return {
      ...base, grp: "SR",
      detail: `Hidden invalidation: the level ${formatPrice(back(lv))} was crossed back and forth, then broke with a strong candle, and price is now retesting it from the other side; ${confirm}`,
      invalidation: back(maxHigh(j)),
      levels: [{ price: back(lv), label: "HIDDEN LEVEL" }],
    };
  }

  // --- C) STRUCTURE RETEST -------------------------------------------------
  // A swing extreme that held, a pullback, then a staircase back up to retest
  // the same level (double top / double bottom) without breaking it.
  for (let k = zz.length - 1; k >= 0; k--) {
    const h0 = zz[k];
    if (h0.type !== "H" || h0.index > n - 10) continue;
    if (h0.index < n - 80) break;
    const lvl = h0.price;
    if (mc.slice(h0.index + 1).some((c) => c.close > lvl + tol)) continue;
    if (zz.some((p) => p.type === "H" && p.index > h0.index && p.price > lvl + atr * 0.1)) continue;
    if (lvl - minLow(h0.index + 1) < atr) continue;
    const lows = zz.filter((p) => p.type === "L" && p.index > h0.index);
    if (!lows.length) continue;
    const lowest = lows.reduce((a, b) => (b.price < a.price ? b : a));
    const risingLows = lows.filter((p) => p.index > lowest.index && p.price > lowest.price);
    if (!risingLows.length) continue;
    if (live < lvl - tol || live > lvl + tol) continue;
    return {
      ...base, grp: "SR",
      detail: `Structure retest: price is back at the ${side} ${formatPrice(back(lvl))} after a pullback and a staircase of ${long ? "lower highs" : "higher lows"}; ${confirm}`,
      invalidation: back(Math.max(lvl, ...m15.map((c) => c.high))),
      levels: [{ price: back(lvl), label: "STRUCTURE" }],
    };
  }

  // --- E) HIDDEN STRUCTURE -------------------------------------------------
  // An older, barely-touched order block (last opposite candle + impulse candle)
  // that price left by 3+ ATR and has now returned to.
  const wantType = long ? "DEMAND" : "SUPPLY";
  for (const z of ctx.zones1.filter((zz2) => zz2.type === wantType)) {
    const age = ctx.c1.length - z.index;
    if (age < 24 || z.touches > 1) continue;
    const since = ctx.c1.slice(z.index + 2);
    if (!since.length) continue;
    const away = long ? Math.max(...since.map((c) => c.high)) - z.high : z.low - Math.min(...since.map((c) => c.low));
    if (away < atr * 3) continue;
    if (ctx.live < z.low - tol || ctx.live > z.high + tol) continue;
    return {
      ...base, grp: "SD",
      detail: `Hidden structure: price is back at an older ${long ? "demand" : "supply"} block (${formatPrice(z.low)}-${formatPrice(z.high)}) it left ${(away / atr).toFixed(1)} ATR ago; ${confirm}`,
      invalidation: long ? z.low : z.high,
      zones: [{ ...z, label: "HIDDEN STRUCTURE" }],
      levels: [],
    };
  }
  return null;
}


const STRATEGIES = [stratSMC, stratLiquidity, stratSupplyDemand, stratICT, stratSR, stratAPA];

// =====================================================================
// TOP-DOWN ANALYSIS ENGINE
// Daily + 4H = direction  ->  4H/1H = where (zones, levels)  ->  15m = trigger
// =====================================================================
export function analyzeFromCandles({ symbol, d1Raw, c4Raw, c1Raw, c15Raw, nowMs }) {
  const d1 = completedOnly(d1Raw, nowMs);
  const c4 = completedOnly(c4Raw, nowMs);
  const c1 = completedOnly(c1Raw, nowMs);
  const c15 = completedOnly(c15Raw, nowMs);
  if (d1.length < 12 || c4.length < 30 || c1.length < 60 || c15.length < 60) {
    throw new Error("Not enough candle data returned.");
  }
  const live = c15Raw[c15Raw.length - 1].close;
  const atr15 = calcAtr(c15);
  const atr1 = calcAtr(c1);
  const atr4 = calcAtr(c4);

  const daily = analyzeDailyTrend(d1);
  const h4 = swingTrend(c4, 2);
  const { bias, strength } = computeBias(daily, h4);

  const base = {
    symbol, direction: "WAIT", score: 0, grade: "-", entry: live, stopLoss: 0, takeProfit: 0, slPct: 0,
    hits: [], reasons: [], bias, biasStrength: strength, daily, h4,
  };
  const biasLine = `Top-down: Daily ${daily}, 4H ${h4} -> bias ${bias}${strength === "STRONG" ? " (strong)" : strength === "MODERATE" ? " (moderate)" : ""}`;
  base.reasons.push(biasLine);

  if (bias === "NEUTRAL") {
    base.reasons.push("Daily and 4H are neutral or disagree, so no trade is allowed (top-down filter).");
    return base;
  }

  const dir = bias === "BULLISH" ? "LONG" : "SHORT";
  const long = dir === "LONG";
  const ctx = {
    symbol, dir, live, nowMs, d1, c4, c1, c15, atr15, atr1, atr4,
    zones1: findZones(c1, "1H", atr1),
    zones4: findZones(c4, "4H", atr4),
    levels: buildSrLevels(c1, c4, atr1),
  };

  const hits = [];
  for (const fn of STRATEGIES) {
    try {
      const hit = fn(ctx);
      if (hit) hits.push(hit);
    } catch (error) {
      console.error(`Strategy ${fn.name} failed for ${symbol}:`, error.message);
    }
  }
  if (!hits.length) {
    base.reasons.push(`No strategy found a valid ${dir} setup on the 1H/15m right now.`);
    return base;
  }

  // Stop-loss: use the first strategy whose structure-based stop is inside the limit.
  const buffer = atr15 * 0.25;
  let chosen = null;
  const rejected = [];
  for (const hit of hits) {
    if (long ? live <= hit.invalidation : live >= hit.invalidation) continue;
    let sl = long ? hit.invalidation - buffer : hit.invalidation + buffer;
    const minDist = atr15 * MIN_SL_ATR;
    sl = long ? Math.min(sl, live - minDist) : Math.max(sl, live + minDist);
    const risk = Math.abs(live - sl);
    const slPct = (risk / live) * 100;
    if (!(slPct > 0) || slPct > MAX_SL_PCT) {
      rejected.push(`${hit.short} (${slPct.toFixed(2)}%)`);
      continue;
    }
    chosen = { hit, sl, risk, slPct };
    break;
  }
  if (!chosen) {
    base.reasons.push(`Found: ${hits.map((h) => h.short).join(", ")}, but the stop-loss would be wider than ${MAX_SL_PCT}% ${rejected.length ? `[${rejected.join(", ")}]` : ""}. Skipped to stay away from liquidation risk.`);
    return base;
  }

  const takeProfit = long ? live + chosen.risk * TAKE_PROFIT_R : live - chosen.risk * TAKE_PROFIT_R;
  // Two strategies that describe the same idea (e.g. APA retest + S/R level) count once.
  const distinct = new Set(hits.map((h) => h.grp || h.id)).size;
  const grade = distinct >= 3 ? "A" : distinct === 2 ? "B" : "C";
  const score = Math.min(95, 45 + distinct * 12 + (strength === "STRONG" ? 10 : 0));

  const reasons = [biasLine];
  for (const h of hits) reasons.push(`${h.name}: ${h.detail}`);
  reasons.push(`Stop-loss based on the ${chosen.hit.short} invalidation; take-profit at ${TAKE_PROFIT_R}R.`);

  // Cautions (warnings only, they never change the direction).
  const rsi = calcRsi(c15.map((c) => c.close), 14);
  if (rsi !== null) {
    if (long && rsi >= 70) reasons.push(`Caution: 15m RSI ${rsi.toFixed(0)} is overbought. Entry may be late.`);
    else if (long && rsi >= 65) reasons.push(`Caution: 15m RSI ${rsi.toFixed(0)} is approaching overbought.`);
    else if (!long && rsi <= 30) reasons.push(`Caution: 15m RSI ${rsi.toFixed(0)} is oversold. Entry may be late.`);
    else if (!long && rsi <= 35) reasons.push(`Caution: 15m RSI ${rsi.toFixed(0)} is approaching oversold.`);
  }
  const opp = long ? "SUPPLY" : "DEMAND";
  const obstacles = [
    ...ctx.levels.map((l) => l.price),
    ...[...ctx.zones1, ...ctx.zones4].filter((z) => z.type === opp).map((z) => (long ? z.low : z.high)),
  ].filter((p) => (long ? p > live : p < live));
  if (obstacles.length) {
    const nearest = long ? Math.min(...obstacles) : Math.max(...obstacles);
    const rAway = Math.abs(nearest - live) / chosen.risk;
    if (rAway < 1.5) reasons.push(`Caution: nearest opposing level ${formatPrice(nearest)} is only ${rAway.toFixed(1)}R away, so the target may be hard to reach.`);
  }

  const zones = hits.flatMap((h) => h.zones);
  const levels = hits.flatMap((h) => h.levels);
  return {
    ...base,
    direction: dir, score, grade, entry: live, stopLoss: chosen.sl, takeProfit, slPct: chosen.slPct,
    hits, reasons,
    chart: { candles: c15Raw.slice(-72), zones, levels },
  };
}

export async function analyzeSymbol(symbolInput) {
  const symbol = normalizeSymbol(symbolInput);
  if (!symbol) throw new Error("This bot only trades BTC, ETH and SOL.");
  const d1Raw = await fetchCandlesCached(symbol, "1d", 40);
  const c4Raw = await fetchCandlesCached(symbol, "4h", 150);
  const c1Raw = await fetchCandlesCached(symbol, "1h", 200);
  const c15Raw = await fetchCandles(symbol, "15m", 200, TF_MS["15m"]);
  return analyzeFromCandles({ symbol, d1Raw, c4Raw, c1Raw, c15Raw, nowMs: Date.now() });
}

// =====================================================================
// CHART IMAGE (pure JavaScript, no extra packages needed)
// =====================================================================
const FONT_ROWS = {
  "0": "01110,10001,10011,10101,11001,10001,01110", "1": "00100,01100,00100,00100,00100,00100,01110",
  "2": "01110,10001,00001,00010,00100,01000,11111", "3": "11110,00001,00001,01110,00001,00001,11110",
  "4": "00010,00110,01010,10010,11111,00010,00010", "5": "11111,10000,11110,00001,00001,10001,01110",
  "6": "00110,01000,10000,11110,10001,10001,01110", "7": "11111,00001,00010,00100,01000,01000,01000",
  "8": "01110,10001,10001,01110,10001,10001,01110", "9": "01110,10001,10001,01111,00001,00010,01100",
  A: "01110,10001,10001,11111,10001,10001,10001", B: "11110,10001,10001,11110,10001,10001,11110",
  C: "01110,10001,10000,10000,10000,10001,01110", D: "11110,10001,10001,10001,10001,10001,11110",
  E: "11111,10000,10000,11110,10000,10000,11111", F: "11111,10000,10000,11110,10000,10000,10000",
  G: "01110,10001,10000,10111,10001,10001,01111", H: "10001,10001,10001,11111,10001,10001,10001",
  I: "01110,00100,00100,00100,00100,00100,01110", J: "00111,00010,00010,00010,00010,10010,01100",
  K: "10001,10010,10100,11000,10100,10010,10001", L: "10000,10000,10000,10000,10000,10000,11111",
  M: "10001,11011,10101,10101,10001,10001,10001", N: "10001,11001,10101,10011,10001,10001,10001",
  O: "01110,10001,10001,10001,10001,10001,01110", P: "11110,10001,10001,11110,10000,10000,10000",
  Q: "01110,10001,10001,10001,10101,10010,01101", R: "11110,10001,10001,11110,10100,10010,10001",
  S: "01111,10000,10000,01110,00001,00001,11110", T: "11111,00100,00100,00100,00100,00100,00100",
  U: "10001,10001,10001,10001,10001,10001,01110", V: "10001,10001,10001,10001,10001,01010,00100",
  W: "10001,10001,10001,10101,10101,10101,01010", X: "10001,10001,01010,00100,01010,10001,10001",
  Y: "10001,10001,01010,00100,00100,00100,00100", Z: "11111,00001,00010,00100,01000,10000,11111",
  ".": "00000,00000,00000,00000,00000,01100,01100", ",": "00000,00000,00000,00000,00100,00100,01000",
  ":": "00000,01100,01100,00000,01100,01100,00000", "-": "00000,00000,00000,11111,00000,00000,00000",
  "+": "00000,00100,00100,11111,00100,00100,00000", "/": "00001,00010,00010,00100,01000,01000,10000",
  "%": "11001,11010,00010,00100,01000,01011,10011", "(": "00010,00100,01000,01000,01000,00100,00010",
  ")": "01000,00100,00010,00010,00010,00100,01000", " ": "00000,00000,00000,00000,00000,00000,00000",
};
const FONT = {};
for (const [ch, rows] of Object.entries(FONT_ROWS)) FONT[ch] = rows.split(",").map((r) => parseInt(r, 2));

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();

function crc32(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i++) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function pngChunk(type, data) {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length);
  const td = Buffer.concat([Buffer.from(type, "ascii"), data]);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(td));
  return Buffer.concat([len, td, crc]);
}

function encodePng(w, h, rgb) {
  const stride = w * 3;
  const raw = Buffer.alloc((stride + 1) * h);
  for (let y = 0; y < h; y++) {
    raw[y * (stride + 1)] = 0;
    rgb.copy(raw, y * (stride + 1) + 1, y * stride, (y + 1) * stride);
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(w, 0);
  ihdr.writeUInt32BE(h, 4);
  ihdr[8] = 8; // bit depth
  ihdr[9] = 2; // RGB
  return Buffer.concat([
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    pngChunk("IHDR", ihdr),
    pngChunk("IDAT", zlib.deflateSync(raw)),
    pngChunk("IEND", Buffer.alloc(0)),
  ]);
}

class Canvas {
  constructor(w, h, bg) {
    this.w = w;
    this.h = h;
    this.buf = Buffer.alloc(w * h * 3);
    this.rect(0, 0, w, h, bg);
  }
  rect(x, y, w, h, color, alpha = 1) {
    const x0 = Math.max(0, Math.round(x));
    const y0 = Math.max(0, Math.round(y));
    const x1 = Math.min(this.w, Math.round(x + w));
    const y1 = Math.min(this.h, Math.round(y + h));
    for (let yy = y0; yy < y1; yy++) {
      for (let xx = x0; xx < x1; xx++) {
        const i = (yy * this.w + xx) * 3;
        if (alpha >= 1) {
          this.buf[i] = color[0]; this.buf[i + 1] = color[1]; this.buf[i + 2] = color[2];
        } else {
          this.buf[i] = Math.round(this.buf[i] * (1 - alpha) + color[0] * alpha);
          this.buf[i + 1] = Math.round(this.buf[i + 1] * (1 - alpha) + color[1] * alpha);
          this.buf[i + 2] = Math.round(this.buf[i + 2] * (1 - alpha) + color[2] * alpha);
        }
      }
    }
  }
  hline(x1, x2, y, color, { alpha = 1, dash = 0, thick = 1 } = {}) {
    if (!dash) { this.rect(x1, y, x2 - x1, thick, color, alpha); return; }
    for (let x = x1; x < x2; x += dash * 2) this.rect(x, y, Math.min(dash, x2 - x), thick, color, alpha);
  }
  vline(x, y1, y2, color) {
    this.rect(x, Math.min(y1, y2), 1, Math.abs(y2 - y1) + 1, color);
  }
  text(str, x, y, color, scale = 2) {
    let cx = x;
    for (const raw of String(str).toUpperCase()) {
      const glyph = FONT[raw] || FONT[" "];
      for (let r = 0; r < 7; r++) {
        for (let col = 0; col < 5; col++) {
          if ((glyph[r] >> (4 - col)) & 1) this.rect(cx + col * scale, y + r * scale, scale, scale, color);
        }
      }
      cx += 6 * scale;
    }
  }
}

const CH = {
  bg: [17, 20, 28], grid: [40, 46, 60], text: [200, 206, 220], dim: [130, 138, 155],
  up: [38, 166, 154], down: [239, 83, 80], entry: [240, 200, 60], fvg: [66, 133, 244], level: [170, 120, 255],
};

export function renderSignalChart(result) {
  const W = 960;
  const H = 600;
  const cv = new Canvas(W, H, CH.bg);
  const L = 14;
  const R = W - 170;
  const T = 76;
  const B = H - 36;
  const candles = result.chart.candles;
  const n = candles.length;
  const slot = (R - L) / (n + 10);
  const xOf = (i) => L + slot * (i + 0.5);

  let lo = Math.min(...candles.map((c) => c.low), result.stopLoss, result.entry, result.takeProfit);
  let hi = Math.max(...candles.map((c) => c.high), result.stopLoss, result.entry, result.takeProfit);
  const pad = (hi - lo) * 0.05;
  lo -= pad;
  hi += pad;
  const yOf = (p) => Math.round(B - ((p - lo) / (hi - lo)) * (B - T));
  const inView = (p) => p >= lo && p <= hi;

  // grid + price labels (skipped where the trade labels sit, to avoid overlap)
  const tradeYs = [yOf(result.entry), yOf(result.takeProfit), yOf(result.stopLoss)];
  for (let g = 0; g <= 5; g++) {
    const p = lo + ((hi - lo) * g) / 5;
    const y = yOf(p);
    cv.hline(L, R, y, CH.grid);
    if (tradeYs.every((ty) => Math.abs(ty - y) >= 18)) cv.text(formatPrice(p), R + 8, y - 7, CH.dim, 2);
  }

  // zones
  for (const z of result.chart.zones) {
    if (z.high < lo || z.low > hi) continue;
    const top = yOf(Math.min(z.high, hi));
    const bottom = yOf(Math.max(z.low, lo));
    const idx = z.startTime ? candles.findIndex((c) => c.startTime >= z.startTime) : -1;
    const x0 = idx < 0 ? L : Math.max(L, xOf(idx) - slot / 2);
    const color = z.type === "DEMAND" ? CH.up : z.type === "SUPPLY" ? CH.down : CH.fvg;
    cv.rect(x0, top, R - x0, Math.max(2, bottom - top), color, 0.2);
    cv.hline(x0, R, top, color, { alpha: 0.7 });
    cv.hline(x0, R, bottom, color, { alpha: 0.7 });
    cv.text(z.label || z.type, x0 + 4, top + 3, color, 2);
  }

  // key levels
  const usedLabelYs = [];
  for (const lv of result.chart.levels) {
    if (!inView(lv.price)) continue;
    const y = yOf(lv.price);
    cv.hline(L, R, y, CH.level, { dash: 6 });
    let ly = y - 16;
    // stack labels so two close levels do not print on top of each other
    while (usedLabelYs.some((u) => Math.abs(u - ly) < 16)) ly -= 16;
    usedLabelYs.push(ly);
    cv.text(`${lv.label} ${formatPrice(lv.price)}`, L + 4, ly, CH.level, 2);
  }

  // risk / reward boxes
  const xStart = xOf(n - 1);
  const yEntry = yOf(result.entry);
  const yTp = yOf(result.takeProfit);
  const ySl = yOf(result.stopLoss);
  cv.rect(xStart, Math.min(yEntry, yTp), R - xStart, Math.abs(yTp - yEntry), CH.up, 0.16);
  cv.rect(xStart, Math.min(yEntry, ySl), R - xStart, Math.abs(ySl - yEntry), CH.down, 0.16);
  cv.hline(xStart, R, yTp, CH.up, { thick: 2 });
  cv.hline(xStart, R, ySl, CH.down, { thick: 2 });
  cv.hline(xStart, R, yEntry, CH.entry, { thick: 2 });
  cv.text(`TP ${formatPrice(result.takeProfit)}`, R + 8, yTp - 7, CH.up, 2);
  cv.text(`SL ${formatPrice(result.stopLoss)}`, R + 8, ySl - 7, CH.down, 2);
  cv.text(`IN ${formatPrice(result.entry)}`, R + 8, yEntry - 7, CH.entry, 2);

  // candles
  const bw = Math.max(2, Math.floor(slot * 0.65));
  for (let i = 0; i < n; i++) {
    const c = candles[i];
    const color = c.close >= c.open ? CH.up : CH.down;
    const x = Math.round(xOf(i));
    cv.vline(x, yOf(c.high), yOf(c.low), color);
    const yo = yOf(c.open);
    const yc = yOf(c.close);
    cv.rect(x - Math.floor(bw / 2), Math.min(yo, yc), bw, Math.max(1, Math.abs(yc - yo)), color);
  }

  // time labels (UTC)
  for (let i = 0; i < n; i += 12) {
    const d = new Date(candles[i].startTime);
    const label = `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
    cv.text(label, Math.max(L, Math.round(xOf(i)) - 25), B + 10, CH.dim, 2);
  }

  // title
  const dirColor = result.direction === "LONG" ? CH.up : CH.down;
  cv.text(`${result.symbol} 15M ${result.direction}`, L, 12, dirColor, 3);
  const names = result.hits.map((h) => h.short).join(" + ");
  cv.text(`GRADE ${result.grade}  ${names}`, L, 44, CH.text, 2);
  cv.text("UTC", W - 60, 12, CH.dim, 2);
  return encodePng(W, H, cv.buf);
}

// =====================================================================
// SIGNAL MESSAGES (chart + text)
// =====================================================================
function buildSignalLines(result) {
  const icon = result.direction === "LONG" ? "📈" : result.direction === "SHORT" ? "📉" : "⏳";
  const lines = [`🪙 Pair: ${result.symbol}`];
  if (result.direction === "LONG" || result.direction === "SHORT") {
    lines.push(
      `${icon} Signal: ${result.direction}  |  Grade ${result.grade} (score ${result.score}/100)`,
      `🧩 Strategies: ${result.hits.map((h) => h.name).join(" + ")}`,
      `🎯 Entry: ${formatPrice(result.entry)}`,
      `🟢 TP: ${formatPrice(result.takeProfit)} (${TAKE_PROFIT_R}R)`,
      `🔴 SL: ${formatPrice(result.stopLoss)} (${result.slPct.toFixed(2)}% away)`,
    );
  } else {
    lines.push(`${icon} Signal: WAIT`, `Current price: ${formatPrice(result.entry)}`);
  }
  return lines;
}

function buildCaption(result, auto) {
  const text = [
    auto ? "🚨 *AUTO ALERT*" : "🔍 *ANALYSIS*",
    ...buildSignalLines(result),
    `📐 ${result.reasons[0]}`,
  ].join("\n");
  return text.length > 1000 ? text.slice(0, 997) + "..." : text;
}

function buildDetailsMessage(result) {
  return [
    `🔍 *Why ${result.symbol} ${result.direction}?*`,
    ...result.reasons.map((r) => `• ${r}`),
    "",
    "Grade: A = 3+ strategies agree, B = 2 agree, C = 1 strategy. It is a checklist, not a win probability. Fees and funding are not included.",
  ].join("\n");
}

function tryRenderChart(result) {
  try {
    return renderSignalChart(result);
  } catch (error) {
    console.error("Chart rendering failed:", error.message);
    return null;
  }
}

async function sendPhoto(token, chatId, png, caption) {
  const attempt = async (withMarkdown) => {
    const form = new FormData();
    form.append("chat_id", String(chatId));
    form.append("caption", caption);
    if (withMarkdown) form.append("parse_mode", "Markdown");
    form.append("photo", new Blob([png], { type: "image/png" }), "chart.png");
    const res = await fetch(`${TELEGRAM_API}/bot${token}/sendPhoto`, { method: "POST", body: form });
    return res.json();
  };
  let data = await attempt(true);
  if (!data?.ok) data = await attempt(false);
  if (!data?.ok) console.error("sendPhoto failed:", data?.description);
  return Boolean(data?.ok);
}

async function deliverSignal(token, chatId, result, png, auto = false) {
  const caption = buildCaption(result, auto);
  let sent = false;
  if (png) {
    try {
      sent = await sendPhoto(token, chatId, png, caption);
    } catch (error) {
      console.error("Could not send chart:", error.message);
    }
  }
  if (!sent) await sendMessage(token, chatId, caption);
  await sendMessage(token, chatId, buildDetailsMessage(result));
}

// =====================================================================
// BOS DETECTOR (unchanged)
// =====================================================================
/**
 * Detect a balanced-confirmation Break of Structure on completed candles.
 * A BOS requires a confirmed pivot break by candle close plus directional
 * displacement (body >= 0.6 * ATR(14)). This is a rule-based filter, not a
 * guarantee of continuation.
 */
export function detectBreakOfStructure(candles, options = {}) {
  const pivotStrength = Number.isInteger(options.pivotStrength) ? options.pivotStrength : 3;
  const atrPeriod = Number.isInteger(options.atrPeriod) ? options.atrPeriod : 14;
  const displacementAtr = Number.isFinite(options.displacementAtr) ? options.displacementAtr : 0.6;
  if (!Array.isArray(candles) || candles.length < Math.max(atrPeriod + 2, pivotStrength * 2 + 5)) {
    return { direction: "NONE", reason: "Not enough completed candles to confirm structure." };
  }

  const valid = candles.every((c) =>
    Number.isFinite(c.open) && Number.isFinite(c.high) &&
    Number.isFinite(c.low) && Number.isFinite(c.close)
  );
  if (!valid) return { direction: "NONE", reason: "Candle data contains invalid values." };

  const trueRanges = candles.slice(1).map((c, i) => {
    const prevClose = candles[i].close;
    return Math.max(c.high - c.low, Math.abs(c.high - prevClose), Math.abs(c.low - prevClose));
  });
  const recentTR = trueRanges.slice(-atrPeriod);
  if (recentTR.length < atrPeriod) {
    return { direction: "NONE", reason: "Not enough candles to calculate ATR." };
  }
  const atr = recentTR.reduce((sum, value) => sum + value, 0) / recentTR.length;
  const last = candles[candles.length - 1];
  const previous = candles[candles.length - 2];
  const body = Math.abs(last.close - last.open);
  const displacement = atr > 0 && body >= atr * displacementAtr;

  let bullishPivot = null;
  let bearishPivot = null;
  // The latest candle is the breakout candidate, not a pivot-confirmation candle.
  const lastPivotIndex = candles.length - pivotStrength - 2;

  // Search backward for the most recent unbroken confirmed swing high/low.
  for (let i = lastPivotIndex; i >= pivotStrength; i--) {
    const candidate = candles[i];
    let isHigh = true;
    let isLow = true;
    for (let j = i - pivotStrength; j <= i + pivotStrength; j++) {
      if (j === i) continue;
      if (candles[j].high >= candidate.high) isHigh = false;
      if (candles[j].low <= candidate.low) isLow = false;
    }
    if (isHigh && !bullishPivot) {
      const alreadyBroken = candles.slice(i + pivotStrength + 1, candles.length - 1)
        .some((c) => c.close > candidate.high);
      if (!alreadyBroken) bullishPivot = { index: i, level: candidate.high };
    }
    if (isLow && !bearishPivot) {
      const alreadyBroken = candles.slice(i + pivotStrength + 1, candles.length - 1)
        .some((c) => c.close < candidate.low);
      if (!alreadyBroken) bearishPivot = { index: i, level: candidate.low };
    }
    if (bullishPivot && bearishPivot) break;
  }

  if (bullishPivot && previous.close <= bullishPivot.level &&
      last.close > bullishPivot.level && last.close > last.open &&
      displacement) {
    return {
      direction: "BULLISH",
      level: bullishPivot.level,
      close: last.close,
      candleTime: last.startTime ?? null,
      atr,
      body,
      displacement: true,
      reason: `Completed candle closed above confirmed swing high with bullish displacement (body ${body.toFixed(6)}; ATR ${atr.toFixed(6)}).`,
    };
  }

  if (bearishPivot && previous.close >= bearishPivot.level &&
      last.close < bearishPivot.level && last.close < last.open &&
      displacement) {
    return {
      direction: "BEARISH",
      level: bearishPivot.level,
      close: last.close,
      candleTime: last.startTime ?? null,
      atr,
      body,
      displacement: true,
      reason: `Completed candle closed below confirmed swing low with bearish displacement (body ${body.toFixed(6)}; ATR ${atr.toFixed(6)}).`,
    };
  }

  let reason = "No BOS met all balanced-confirmation rules on the latest completed candle.";
  if ((bullishPivot && last.high > bullishPivot.level && last.close <= bullishPivot.level) ||
      (bearishPivot && last.low < bearishPivot.level && last.close >= bearishPivot.level)) {
    reason = "Possible wick sweep detected, but candle did not close beyond the structure level.";
  } else if ((bullishPivot && last.close > bullishPivot.level) ||
             (bearishPivot && last.close < bearishPivot.level)) {
    reason = displacement
      ? "Price closed beyond a structure level, but direction/pivot confirmation did not qualify."
      : "Price crossed a structure level, but candle displacement was too weak.";
  }
  return { direction: "NONE", reason, atr, body, displacement };
}

async function analyzeBos(symbol, timeframe) {
  const intervalMs = BOS_TIMEFRAMES[timeframe];
  if (!intervalMs) throw new Error(`Unsupported timeframe: ${timeframe}`);
  const rawCandles = await fetchCandles(symbol, timeframe, 120, intervalMs);
  // Kraken includes the currently forming candle; exclude it from confirmation.
  const completedCandles = completedOnly(rawCandles, Date.now());
  const result = detectBreakOfStructure(completedCandles);
  return {
    symbol: normalizeSymbol(symbol) || symbol,
    timeframe,
    candleCount: completedCandles.length,
    ...result,
  };
}

function formatBosMessage(result, auto = false) {
  const bullish = result.direction === "BULLISH";
  const heading = bullish ? "🟢 BULLISH BOS" : "🔴 BEARISH BOS";
  return [
    `${auto ? "🔔 *AUTOMATIC BOS ALERT*" : "🔎 *BOS DETECTOR RESULT*"}`,
    `${heading}`,
    `Pair: ${result.symbol}`,
    `Timeframe: ${result.timeframe}`,
    `Broken swing level: ${formatPrice(result.level)}`,
    `Completed candle close: ${formatPrice(result.close)}`,
    `Confirmation: Candle close + displacement`,
    `Reason: ${result.reason}`,
    "",
    "This is a technical structure alert, not a guaranteed trade or an order execution.",
  ].join("\n");
}

async function loadBosSubscribers() {
  try {
    const saved = JSON.parse(await readFile(BOS_SUBSCRIBERS_FILE, "utf8"));
    for (const id of saved) bosSubscribers.add(String(id));
    console.info(`Loaded ${bosSubscribers.size} BOS alert subscribers.`);
  } catch (error) {
    if (error.code !== "ENOENT") console.error("Could not load BOS subscribers:", error.message);
  }
}

async function saveBosSubscribers() {
  await writeFile(BOS_SUBSCRIBERS_FILE, JSON.stringify([...bosSubscribers]), "utf8");
}

async function runBosAutoScan(token) {
  while (true) {
    if (bosSubscribers.size > 0) {
      for (const symbol of BOS_SYMBOLS) {
        for (const timeframe of BOS_AUTO_TIMEFRAMES) {
          try {
            const result = await analyzeBos(symbol, timeframe);
            if (!["BULLISH", "BEARISH"].includes(result.direction)) continue;
            const key = `${symbol}-${timeframe}-${result.direction}-${result.candleTime}`;
            for (const chatId of bosSubscribers) {
              const alertKey = `${chatId}-${key}`;
              if (lastBosAlertAt.has(alertKey)) continue;
              const cooldownKey = `${chatId}-${symbol}-${timeframe}-${result.direction}`;
              const prior = lastBosAlertAt.get(cooldownKey) || 0;
              if (Date.now() - prior < BOS_ALERT_COOLDOWN_MS) continue;
              lastBosAlertAt.set(alertKey, Date.now());
              lastBosAlertAt.set(cooldownKey, Date.now());
              await sendMessage(token, chatId, formatBosMessage(result, true));
            }
          } catch (error) {
            console.error(`BOS auto-scan error for ${symbol} ${timeframe}:`, error.message);
          }
        }
      }
    }
    await sleep(BOS_SCAN_INTERVAL_MS);
  }
}

// =====================================================================
// TRADE MONITORING (unchanged)
// =====================================================================
async function loadMonitorState() {
  try {
    const saved = await readFile(MONITOR_FILE, "utf8");
    const monitors = JSON.parse(saved);
    for (const monitor of monitors) {
      if (monitor?.id) activeMonitors.set(monitor.id, monitor);
    }
    console.info(`Loaded ${activeMonitors.size} saved monitors.`);
  } catch (error) {
    if (error.code !== "ENOENT") {
      console.error("Could not load monitor state:", error.message);
    }
  }
}

function saveMonitorState() {
  const snapshot = JSON.stringify([...activeMonitors.values()], null, 2);
  persistenceQueue = persistenceQueue
    .catch(() => {})
    .then(() => writeFile(MONITOR_FILE, snapshot, "utf8"));
  return persistenceQueue;
}

async function registerMonitor(token, chatId, result) {
  if (!["LONG", "SHORT"].includes(result.direction)) return;

  const entry = Number(result.entry);
  const stopLoss = Number(result.stopLoss);
  const takeProfit = Number(result.takeProfit);

  if (
    !Number.isFinite(entry) || !Number.isFinite(stopLoss) || !Number.isFinite(takeProfit) ||
    entry <= 0 || stopLoss <= 0 || takeProfit <= 0
  ) {
    await sendMessage(token, chatId, "⚠️ Signal monitoring was not started because its price levels are invalid.");
    return;
  }

  const risk = result.direction === "LONG" ? entry - stopLoss : stopLoss - entry;
  if (risk <= 0) {
    await sendMessage(token, chatId, "⚠️ Signal monitoring was not started because the stop-loss is on the wrong side of the entry.");
    return;
  }

  const existing = [...activeMonitors.values()].find(
    (m) => String(m.chatId) === String(chatId) && m.symbol === result.symbol && m.status === "ACTIVE",
  );
  if (existing) {
    await sendMessage(
      token,
      chatId,
      `ℹ️ Monitoring is already active for ${result.symbol}. I kept the existing monitor to avoid duplicate tracking.`,
    );
    return;
  }

  const monitor = {
    id: `${chatId}-${result.symbol}-${Date.now()}`,
    chatId,
    symbol: result.symbol,
    direction: result.direction,
    entry,
    stopLoss,
    takeProfit,
    risk,
    oneRAlerted: false,
    adverseAlerted: false,
    status: "ACTIVE",
    createdAt: new Date().toISOString(),
    lastCheckedCandle: null,
  };

  activeMonitors.set(monitor.id, monitor);
  await saveMonitorState();

  await sendMessage(
    token,
    chatId,
    [
      `🔔 *Monitoring started: ${monitor.symbol}*`,
      `Direction: ${monitor.direction}`,
      `Reference entry: ${formatPrice(entry)}`,
      `+1R alert: ${formatPrice(monitor.direction === "LONG" ? entry + risk : entry - risk)}`,
      `−0.75R warning: ${formatPrice(monitor.direction === "LONG" ? entry - risk * 0.75 : entry + risk * 0.75)}`,
      "This is monitoring only; no orders will be placed.",
    ].join("\n"),
  );
}

// Checks ONE 1m candle against the monitor. Returns true if the monitor is finished.
async function evaluateMonitorCandle(token, monitor, candle) {
  const isLong = monitor.direction === "LONG";

  const hitStop = isLong ? candle.low <= monitor.stopLoss : candle.high >= monitor.stopLoss;
  const hitTarget = isLong ? candle.high >= monitor.takeProfit : candle.low <= monitor.takeProfit;

  // OHLC data cannot tell us which level was touched first
  // when both the stop-loss and take-profit are inside one candle.
  if (hitStop && hitTarget) {
    await sendMessage(
      token,
      monitor.chatId,
      `⚠️ *${monitor.symbol}: outcome unclear*\nThe same one-minute candle touched both the TP and SL levels. Candle data cannot confirm which was reached first. Monitoring stopped for this signal; check a more detailed price feed before deciding the outcome.`,
    );
    monitor.status = "AMBIGUOUS";
    await saveMonitorState();
    return true;
  }

  if (hitStop) {
    await sendMessage(
      token,
      monitor.chatId,
      `🔴 *${monitor.symbol}: stop-loss level touched*\nDirection: ${monitor.direction}\nSL reference: ${formatPrice(monitor.stopLoss)}\nThis is a price-level alert, not confirmation of an executed trade.`,
    );
    monitor.status = "STOP_TOUCHED";
    await saveMonitorState();
    return true;
  }

  if (hitTarget) {
    await sendMessage(
      token,
      monitor.chatId,
      `🎯 *${monitor.symbol}: take-profit level touched*\nDirection: ${monitor.direction}\nTP reference: ${formatPrice(monitor.takeProfit)}\nThis is a price-level alert, not confirmation of an executed trade.`,
    );
    monitor.status = "TARGET_TOUCHED";
    await saveMonitorState();
    return true;
  }

  const oneRLevel = isLong ? monitor.entry + monitor.risk : monitor.entry - monitor.risk;
  const adverseLevel = isLong ? monitor.entry - monitor.risk * 0.75 : monitor.entry + monitor.risk * 0.75;

  const hitOneR = isLong ? candle.high >= oneRLevel : candle.low <= oneRLevel;
  const hitAdverse = isLong ? candle.low <= adverseLevel : candle.high >= adverseLevel;

  if (hitOneR && hitAdverse && !monitor.oneRAlerted && !monitor.adverseAlerted) {
    await sendMessage(
      token,
      monitor.chatId,
      `⚠️ *${monitor.symbol}: movement order unclear*\nThe same one-minute candle touched both the +1R and −0.75R alert levels. Their order cannot be determined from candle data.`,
    );
    monitor.oneRAlerted = true;
    monitor.adverseAlerted = true;
    await saveMonitorState();
    return false;
  }

  if (hitOneR && !monitor.oneRAlerted) {
    await sendMessage(
      token,
      monitor.chatId,
      `🟢 *${monitor.symbol}: +1R level reached*\nDirection: ${monitor.direction}\nReference entry: ${formatPrice(monitor.entry)}\nAlert level: ${formatPrice(oneRLevel)}`,
    );
    monitor.oneRAlerted = true;
    await saveMonitorState();
  }

  if (hitAdverse && !monitor.adverseAlerted) {
    await sendMessage(
      token,
      monitor.chatId,
      `🟠 *${monitor.symbol}: −0.75R adverse warning*\nDirection: ${monitor.direction}\nReference entry: ${formatPrice(monitor.entry)}\nWarning level: ${formatPrice(adverseLevel)}`,
    );
    monitor.adverseAlerted = true;
    await saveMonitorState();
  }

  return false;
}

async function checkOneMonitor(token, monitor) {
  if (monitor.status !== "ACTIVE") return;

  const candles = await fetchCandles(monitor.symbol, "1m", 15, 60 * 1000);
  if (!candles.length) return;

  // Only look at candles that started AFTER the signal was created,
  // so price action from before the signal can't trigger alerts.
  const firstValid = Math.ceil(Date.parse(monitor.createdAt) / 60000) * 60000;
  const from = Math.max(firstValid, monitor.lastCheckedCandle ?? 0);

  for (const candle of candles) {
    if (candle.startTime < from) continue;
    if (!Number.isFinite(candle.high) || !Number.isFinite(candle.low) || !Number.isFinite(candle.close)) continue;
    const finished = await evaluateMonitorCandle(token, monitor, candle);
    if (finished) return;
  }

  const newest = candles[candles.length - 1].startTime;
  if (newest !== monitor.lastCheckedCandle) {
    monitor.lastCheckedCandle = newest;
    await saveMonitorState();
  }
}

async function runMonitorLoop(token) {
  while (true) {
    try {
      for (const monitor of activeMonitors.values()) {
        if (monitor.status !== "ACTIVE") continue;
        try {
          await checkOneMonitor(token, monitor);
        } catch (error) {
          console.error(`Monitor error for ${monitor.symbol}:`, error.message);
        }
      }
    } catch (error) {
      console.error("Monitoring loop error:", error.message);
    }
    await sleep(15000);
  }
}

// =====================================================================
// AUTO SCAN
// =====================================================================
async function loadSubscribers() {
  try {
    const saved = JSON.parse(await readFile(SUBSCRIBERS_FILE, "utf8"));
    for (const id of saved) autoSubscribers.add(String(id));
    console.info(`Loaded ${autoSubscribers.size} auto-alert subscribers.`);
  } catch (error) {
    if (error.code !== "ENOENT") {
      console.error("Could not load subscribers:", error.message);
    }
  }
}

async function saveSubscribers() {
  await writeFile(SUBSCRIBERS_FILE, JSON.stringify([...autoSubscribers]), "utf8");
}

async function runAutoScan(token) {
  while (true) {
    if (autoSubscribers.size > 0) {
      for (const symbol of TRADE_SYMBOLS) {
        try {
          const result = await analyzeSymbol(symbol);
          if (result.direction === "WAIT") continue;

          let png; // chart is drawn once and shared by all subscribers
          for (const chatId of autoSubscribers) {
            const key = `${chatId}-${symbol}-${result.direction}`;
            const last = lastAlertAt.get(key) || 0;
            if (Date.now() - last < ALERT_COOLDOWN_MS) continue;

            const hasActive = [...activeMonitors.values()].some(
              (m) => String(m.chatId) === String(chatId) && m.symbol === result.symbol && m.status === "ACTIVE",
            );
            if (hasActive) continue;

            lastAlertAt.set(key, Date.now());
            if (png === undefined) png = tryRenderChart(result);
            await deliverSignal(token, chatId, result, png, true);
            await registerMonitor(token, chatId, result);
          }
        } catch (error) {
          console.error(`Auto-scan error for ${symbol}:`, error.message);
        }
      }
    }
    await sleep(AUTO_SCAN_INTERVAL_MS);
  }
}

// =====================================================================
// TELEGRAM
// =====================================================================
async function callTelegramRaw(token, method, body) {
  const res = await fetch(`${TELEGRAM_API}/bot${token}/${method}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return res.json();
}

async function callTelegram(token, method, body) {
  const data = await callTelegramRaw(token, method, body);
  return data.result;
}

async function sendMessage(token, chatId, text) {
  const data = await callTelegramRaw(token, "sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "Markdown",
  });
  // If Telegram rejects the Markdown formatting, resend as plain text.
  if (!data?.ok) {
    await callTelegramRaw(token, "sendMessage", { chat_id: chatId, text });
  }
}

async function handleMessage(token, message) {
  if (!message) return;
  const text = message.text?.trim();
  if (!text || !message.chat?.id) return;
  const match = text.match(/^(\/[a-z]+)(?:@\w+)?(?:\s+(.+))?$/i);
  if (!match) return;

  const command = match[1].toLowerCase();
  const args = (match[2] || "").trim().split(/\s+/).filter(Boolean);
  const chatId = message.chat.id;

  if (command === "/start" || command === "/help") {
    await sendMessage(token, chatId, [
      "🤖 *Welcome to Futures Signal Buddy!*",
      "This bot studies Kraken perpetual-futures data for BTC, ETH and SOL and sends technical-analysis signals with a chart picture. It does NOT open, close, or manage trades for you.",
      "",
      "🚀 *START HERE*",
      "1. Send /analyze BTCUSDT (or ETH / SOL) for a full analysis.",
      "2. Read the direction, entry, stop-loss (SL) and take-profit (TP) on the chart.",
      "3. Use /autoon to get automatic signals when any of the 6 strategies finds a setup.",
      "",
      "📖 *HOW TO READ A RESULT*",
      "• LONG / SHORT: at least one strategy found a setup that agrees with the Daily + 4H bias.",
      "• WAIT: no valid setup, or the top-down bias is unclear. Do not enter.",
      "• Grade A / B / C: 3+ / 2 / 1 strategies agree. It is a checklist, NOT a win probability.",
      "• Monitoring: the bot watches price levels after a signal; it does NOT place orders.",
      "",
      "🧭 *MAIN COMMANDS*",
      "/analyze BTCUSDT — analyze one market (or /analyze for all three)",
      "/signal BTCUSDT — same as /analyze",
      "/price — live prices",
      "/autoon — automatic signals on",
      "/autooff — automatic signals off",
      "/autostatus — alert status",
      "/scanlist — markets and strategies scanned",
      "/stats — tracked signal outcomes",
      "",
      "📈 *BOS COMMANDS*",
      "/bos BTCUSDT — check BOS on 15m",
      "/bos BTCUSDT 1h — check BOS on 1h",
      "/bos BTCUSDT all — check supported timeframes",
      "/bosauto — subscribe to automatic BOS alerts",
      "/bosstop — unsubscribe from BOS alerts",
      "/boshelp — full BOS explanation",
      "",
      "ℹ️ *LEARN THE BOT*",
      "/strategy — the 6 strategies and the top-down method",
      "/timeframes — timeframes used",
      "/risk — important risk information",
      "/about — data source and limitations",
      "",
      "⚠️ Futures trading is risky. Signals can be wrong. Start with demo/paper trading and make your own decisions.",
    ].join("\n"));
    return;
  }

  if (command === "/strategy") {
    await sendMessage(token, chatId, [
      "🧠 *HOW THE BOT WORKS*",
      "*Top-down method*",
      "1. Daily + 4H decide the direction. If they disagree, the bot says WAIT.",
      "2. 4H/1H show where to look (zones, key levels, liquidity).",
      "3. 15m gives the trigger. Only trades in the bias direction are allowed.",
      "",
      "*The 6 strategies (any ONE can trigger a signal)*",
      "• SMC: BOS/CHOCH on 15m, then a retest of the order block (and FVG).",
      "• Liquidity: sweep of equal highs/lows, previous day high/low or swing points, then a close back inside.",
      "• Supply & Demand: price inside a fresh 4H/1H zone with a 15m trigger candle.",
      "• ICT: London/New York kill zone, discount (long) or premium (short), fair value gap tap, 15m trigger.",
      "• Support & Resistance: bounce or break-and-retest at a clustered level.",
      "• APA (structure): Invalidation / New Invalidation, Structure Retest, Hidden Invalidation and Hidden Structure on 1H, with a 15m trigger.",
      "",
      "Grade: A = 3+ strategies agree, B = 2, C = 1. TP is 2.5R; setups with a stop wider than 2.5% are skipped.",
      "This is a rule-based checklist, not a prediction engine. A valid-looking setup can still lose.",
    ].join("\n"));
    return;
  }

  if (command === "/timeframes") {
    await sendMessage(token, chatId, [
      "⏱️ *TIMEFRAMES*",
      "• Daily + 4H — direction (top-down bias).",
      "• 4H + 1H — zones, key levels, liquidity pools.",
      "• 15m — entry trigger and the chart you receive.",
      "",
      "Manual /bos supports: 1m, 5m, 15m, 30m, 1h, 4h and 1d.",
      "Automatic BOS alerts watch: 15m, 30m, 1h, 4h and 1d.",
    ].join("\n"));
    return;
  }

  if (command === "/risk") {
    await sendMessage(token, chatId, [
      "⚠️ *RISK INFORMATION*",
      "• This bot provides analysis and price-level monitoring only. It does not execute trades.",
      `• Futures and leverage can cause rapid losses, including liquidation.`,
      `• The ${MAX_SL_PCT}% maximum stop-distance filter is only a bot screening rule. It is NOT a guarantee, a recommended account risk, or protection from liquidation.`,
      "• The grade and score are not a win rate or probability.",
      "• /stats counts observed TP/SL price touches for tracked signals, not verified trades or account profit. Fees, funding and slippage are not included.",
      "• Check the market and calculate your position size and account risk independently. Consider demo trading first.",
      "",
      "Never risk money you cannot afford to lose.",
    ].join("\n"));
    return;
  }

  if (command === "/about") {
    await sendMessage(token, chatId, [
      "ℹ️ *ABOUT FUTURES SIGNAL BUDDY*",
      "• Market data: Kraken public futures market data.",
      "• Markets: BTCUSDT, ETHUSDT and SOLUSDT only.",
      "• Automatic scan: all 6 strategies on every pair, about every 3 minutes.",
      "• BOS automatic alerts: 15m, 30m, 1h, 4h and 1d.",
      "• No exchange API keys are required by this analysis bot.",
      "• No automatic trade execution is included.",
      "",
      "Market availability and API responses can change.",
    ].join("\n"));
    return;
  }

  if (command === "/boshelp") {
    await sendMessage(token, chatId, [
      "*Valid BOS detector — Balanced confirmation*",
      "/bos BTCUSDT — check 15m",
      "/bos BTCUSDT 1h — check 1h",
      "/bos BTCUSDT all — check all supported timeframes",
      "/bosauto — subscribe to automatic BOS alerts (15m and above)",
      "/bosstop — stop automatic BOS alerts",
      "",
      "Rules: confirmed swing pivot, completed candle close beyond the level, directional candle, and body >= 0.6 x ATR(14). Wick-only breaks do not qualify. Alerts are analysis only, not trade instructions.",
    ].join("\n"));
    return;
  }

  if (command === "/bosauto") {
    bosSubscribers.add(String(chatId));
    await saveBosSubscribers();
    await sendMessage(token, chatId, `✅ Automatic BOS alerts enabled. Watching ${BOS_SYMBOLS.join(", ")} on ${BOS_AUTO_TIMEFRAMES.join(", ")}. Scans run about every 5 minutes. Use /bosstop to unsubscribe.`);
    return;
  }

  if (command === "/bosstop") {
    bosSubscribers.delete(String(chatId));
    await saveBosSubscribers();
    await sendMessage(token, chatId, "🛑 Automatic BOS alerts disabled. Your /autoon signal alerts are unchanged.");
    return;
  }

  if (command === "/bos") {
    if (!args[0]) {
      await sendMessage(token, chatId, "Usage: /bos BTCUSDT [15m|1h|all]. Use /boshelp for details.");
      return;
    }
    const symbol = normalizeSymbol(args[0]);
    if (!symbol) {
      await sendMessage(token, chatId, "This bot only trades BTC, ETH and SOL. Example: /bos BTCUSDT 1h");
      return;
    }
    const requested = (args[1] || "15m").toLowerCase();
    const timeframes = requested === "all" ? Object.keys(BOS_TIMEFRAMES) : [requested];
    const unsupported = timeframes.filter((tf) => !BOS_TIMEFRAMES[tf]);
    if (unsupported.length) {
      await sendMessage(token, chatId, `Unsupported timeframe: ${unsupported.join(", ")}. Use ${Object.keys(BOS_TIMEFRAMES).join(", ")} or all.`);
      return;
    }
    await sendMessage(token, chatId, `🔎 Checking ${symbol} BOS on ${timeframes.join(", ")} using completed candles...`);
    for (const timeframe of timeframes) {
      try {
        const result = await analyzeBos(symbol, timeframe);
        if (["BULLISH", "BEARISH"].includes(result.direction)) {
          await sendMessage(token, chatId, formatBosMessage(result));
        } else {
          await sendMessage(token, chatId, `⚪ *NO VALID BOS*\nPair: ${symbol}\nTimeframe: ${timeframe}\nReason: ${result.reason}`);
        }
      } catch (error) {
        await sendMessage(token, chatId, `⚠️ BOS check failed for ${symbol} ${timeframe}: ${error.message}`);
      }
    }
    return;
  }

  if (command === "/autoon") {
    autoSubscribers.add(String(chatId));
    await saveSubscribers();
    await sendMessage(
      token,
      chatId,
      `✅ Auto signals ON. Scanning ${TRADE_SYMBOLS.join(", ")} with all 6 strategies about every 3 minutes. Each signal comes with a chart. Use /autooff to stop.`,
    );
    return;
  }

  if (command === "/autooff") {
    autoSubscribers.delete(String(chatId));
    await saveSubscribers();
    await sendMessage(token, chatId, "🛑 Auto signals OFF.");
    return;
  }

  if (command === "/autostatus") {
    const on = autoSubscribers.has(String(chatId));
    await sendMessage(token, chatId, `Auto signals: ${on ? "ON" : "OFF"}\nWatching: ${TRADE_SYMBOLS.join(", ")}`);
    return;
  }

  if (command === "/scanlist") {
    await sendMessage(token, chatId, [
      `📋 *Scanned markets (${TRADE_SYMBOLS.length})*`,
      TRADE_SYMBOLS.join(", "),
      "",
      "*Strategies:* SMC, Liquidity, Supply & Demand, ICT, Support & Resistance, APA",
      "*Method:* Daily + 4H bias -> 1H zones/levels -> 15m trigger",
    ].join("\n"));
    return;
  }

  if (command === "/price") {
    const list = args[0] ? [normalizeSymbol(args[0])] : TRADE_SYMBOLS;
    if (list.some((s) => !s)) {
      await sendMessage(token, chatId, "This bot only trades BTC, ETH and SOL. Example: /price BTCUSDT");
      return;
    }
    const lines = ["💵 *Live prices*"];
    for (const sym of list) {
      try {
        const candles = await fetchCandles(sym, "1m", 3, TF_MS["1m"]);
        lines.push(`${sym}: ${formatPrice(candles[candles.length - 1].close)}`);
      } catch (error) {
        lines.push(`${sym}: unavailable (${error.message})`);
      }
    }
    await sendMessage(token, chatId, lines.join("\n"));
    return;
  }

  if (command === "/stats") {
    const mine = [...activeMonitors.values()].filter(
      (m) => String(m.chatId) === String(chatId),
    );
    if (!mine.length) {
      await sendMessage(token, chatId, "📊 No tracked signals yet. Use /analyze or /autoon first.");
      return;
    }

    const bySymbol = new Map();
    let wins = 0;
    let losses = 0;
    let unclear = 0;
    let active = 0;
    let netR = 0;

    for (const m of mine) {
      const rewardR = m.risk > 0 ? Math.abs(m.takeProfit - m.entry) / m.risk : 0;
      const row = bySymbol.get(m.symbol) || { w: 0, l: 0, u: 0, a: 0 };
      if (m.status === "TARGET_TOUCHED") {
        wins++;
        netR += rewardR;
        row.w++;
      } else if (m.status === "STOP_TOUCHED") {
        losses++;
        netR -= 1;
        row.l++;
      } else if (m.status === "AMBIGUOUS") {
        unclear++;
        row.u++;
      } else {
        active++;
        row.a++;
      }
      bySymbol.set(m.symbol, row);
    }

    const closed = wins + losses;
    const winRate = closed ? ((wins / closed) * 100).toFixed(1) : "n/a";
    const lines = [
      "📊 *Signal Stats (your chat)*",
      `Total tracked: ${mine.length}`,
      `✅ TP touched: ${wins}`,
      `❌ SL touched: ${losses}`,
      `⚠️ Unclear: ${unclear}`,
      `⏳ Still active: ${active}`,
      `Win rate (TP vs SL only): ${winRate}${closed ? "%" : ""}`,
      `Net result: ${netR >= 0 ? "+" : ""}${netR.toFixed(2)}R (win = TP size in R, loss = -1R)`,
      "",
      "*By pair:*",
      ...[...bySymbol.entries()].map(
        ([sym, r]) => `${sym}: ${r.w}W / ${r.l}L / ${r.u} unclear / ${r.a} active`,
      ),
      "",
      closed < 20
        ? `Only ${closed} closed signals so far. Wait for 20+ before trusting these numbers.`
        : "Fees, funding and slippage are NOT included.",
    ];
    await sendMessage(token, chatId, lines.join("\n"));
    return;
  }

  if (command === "/analyze" || command === "/signal") {
    const wantsAll = !args[0] || args[0].toLowerCase() === "all";
    const symbols = wantsAll ? TRADE_SYMBOLS : [normalizeSymbol(args[0])];
    if (symbols.some((s) => !s)) {
      await sendMessage(token, chatId, "This bot only trades BTC, ETH and SOL. Example: /analyze BTCUSDT");
      return;
    }
    await sendMessage(
      token,
      chatId,
      `🔍 Running top-down analysis (Daily/4H -> 1H -> 15m) with SMC, Liquidity, S&D, ICT, S/R and APA on ${symbols.join(", ")}...`,
    );
    for (const symbol of symbols) {
      try {
        const result = await analyzeSymbol(symbol);
        if (result.direction === "LONG" || result.direction === "SHORT") {
          const png = tryRenderChart(result);
          await deliverSignal(token, chatId, result, png, false);
          try {
            await registerMonitor(token, chatId, result);
          } catch (error) {
            console.error("Could not register signal monitor:", error.message);
            await sendMessage(token, chatId, "⚠️ The signal was sent, but monitoring could not be started. Check the Replit console.");
          }
        } else {
          await sendMessage(token, chatId, [
            ...buildSignalLines(result),
            "",
            ...result.reasons.map((r) => `• ${r}`),
          ].join("\n"));
        }
      } catch (err) {
        await sendMessage(token, chatId, `⚠️ Error analyzing ${symbol}: ${err.message}`);
      }
    }
  }
}

export async function startBot(token) {
  let offset = 0;
  console.info("Futures Signal Buddy running.");

  // Register Telegram's command menu. Failure should not prevent the bot from starting.
  try {
    await callTelegram(token, "setMyCommands", {
      commands: [
        { command: "start", description: "Welcome and beginner's guide" },
        { command: "help", description: "Show all bot commands" },
        { command: "analyze", description: "Analyze BTC, ETH or SOL with a chart" },
        { command: "signal", description: "Same as /analyze" },
        { command: "price", description: "Live BTC, ETH and SOL prices" },
        { command: "autoon", description: "Turn automatic signals on" },
        { command: "autooff", description: "Turn automatic signals off" },
        { command: "autostatus", description: "Check automatic signal status" },
        { command: "scanlist", description: "Markets and strategies scanned" },
        { command: "stats", description: "Show tracked signal observations" },
        { command: "bos", description: "Check BOS, e.g. /bos BTCUSDT 1h" },
        { command: "bosauto", description: "Subscribe to automatic BOS alerts" },
        { command: "bosstop", description: "Stop automatic BOS alerts" },
        { command: "boshelp", description: "Explain BOS commands and rules" },
        { command: "strategy", description: "The 6 strategies and top-down method" },
        { command: "timeframes", description: "Show analysis timeframes" },
        { command: "risk", description: "Read important risk information" },
        { command: "about", description: "About the bot and its data source" },
      ],
    });
    console.info("Telegram command menu registered.");
  } catch (error) {
    console.error("Could not register Telegram command menu:", error.message);
  }

  await loadMonitorState();
  void runMonitorLoop(token);
  await loadSubscribers();
  void runAutoScan(token);
  await loadBosSubscribers();
  void runBosAutoScan(token);
  while (true) {
    try {
      const updates = await callTelegram(token, "getUpdates", {
        offset,
        timeout: 25,
      });
      if (updates) {
        for (const update of updates) {
          await handleMessage(token, update.message).catch(console.error);
          offset = update.update_id + 1;
        }
      }
    } catch (err) {
      await sleep(3000);
    }
  }
}

async function main() {
  const token = process.env.BOT_TOKEN;
  if (!token) {
    console.error("BOT_TOKEN secret is missing. Add it in Replit Secrets.");
    process.exit(1);
  }
  await startBot(token);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch(console.error);
}
