import { pathToFileURL } from "node:url";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
const TELEGRAM_API = "https://api.telegram.org";
const KRAKEN_CHARTS_API = "https://futures.kraken.com/api/charts/v1";
const MIN_PUBLIC_REQUEST_INTERVAL_MS = 500;
const MONITOR_FILE = resolve(process.cwd(), "trade-monitors.json");
const activeMonitors = new Map();

let persistenceQueue = Promise.resolve();
let nextPublicRequestAt = 0;
const AUTO_SCAN_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const AUTO_SCAN_INTERVAL_MS = 5 * 60 * 1000;
const ALERT_COOLDOWN_MS = 4 * 60 * 60 * 1000;
const SUBSCRIBERS_FILE = resolve(process.cwd(), "auto-subscribers.json");
const autoSubscribers = new Set();
const lastAlertAt = new Map();

// BOS detector settings and independent Telegram alert subscriptions.
const BOS_SUBSCRIBERS_FILE = resolve(process.cwd(), "bos-subscribers.json");
const bosSubscribers = new Set();
const BOS_SCAN_INTERVAL_MS = 5 * 60 * 1000;
const BOS_ALERT_COOLDOWN_MS = 60 * 60 * 1000;
const BOS_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const BOS_TIMEFRAMES = Object.freeze({
  "1m": 60 * 1000,
  "5m": 5 * 60 * 1000,
  "15m": 15 * 60 * 1000,
  "30m": 30 * 60 * 1000,
  "1h": 60 * 60 * 1000,
  "4h": 4 * 60 * 60 * 1000,
  "1d": 24 * 60 * 60 * 1000,
});
const lastBosAlertAt = new Map();

const KRAKEN_MARKETS = Object.freeze({
  BTCUSDT: Object.freeze({ contract: "PF_XBTUSD", pair: "BTC:USD" }),
  ETHUSDT: Object.freeze({ contract: "PF_ETHUSD", pair: "ETH:USD" }),
  SOLUSDT: Object.freeze({ contract: "PF_SOLUSD", pair: "SOL:USD" }),
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
    throw new Error("Kraken API request failed.");
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

async function getKrakenMarket(symbol) {
  const normalized = symbol?.trim().toUpperCase();
  if (!normalized) throw new Error("Invalid symbol.");
  if (KRAKEN_MARKETS[normalized]) {
    return { commandSymbol: normalized, ...KRAKEN_MARKETS[normalized] };
  }
  const base = normalized.replace(/USDT$|USD$/, "");
  return {
    commandSymbol: normalized,
    contract: `PF_${base}USD`,
    pair: `${base}:USD`,
  };
}

async function fetchCandles(symbol, interval, count, intervalMs) {
  const market = await getKrakenMarket(symbol);
  const url = `${KRAKEN_CHARTS_API}/trade/${encodeURIComponent(market.contract)}/${interval}?count=${count}`;
  const rawData = await fetchKrakenJson(url);
  return parseKrakenCandles(rawData, intervalMs);
}
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
  // Kraken may include the currently forming candle; exclude it from confirmation.
  const completedCandles = rawCandles.slice(0, -1);
  const result = detectBreakOfStructure(completedCandles);
  return {
    symbol: symbol.trim().toUpperCase(),
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
  ].join("\\n");
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
        for (const [timeframe, intervalMs] of Object.entries(BOS_TIMEFRAMES)) {
          try {
            const result = await analyzeBos(symbol, timeframe);
            if (!["BULLISH", "BEARISH"].includes(result.direction)) continue;
            const key = `${symbol}-${timeframe}-${result.direction}-${result.candleTime}`;
            for (const chatId of bosSubscribers) {
              const alertKey = `${chatId}-${key}`;
              if (lastBosAlertAt.has(alertKey)) continue;
              const prior = lastBosAlertAt.get(`${chatId}-${symbol}-${timeframe}-${result.direction}`) || 0;
              if (Date.now() - prior < BOS_ALERT_COOLDOWN_MS) continue;
              lastBosAlertAt.set(alertKey, Date.now());
              lastBosAlertAt.set(`${chatId}-${symbol}-${timeframe}-${result.direction}`, Date.now());
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

function identifySupplyDemandZones(candles) {
  const zones = [];
  for (let i = 2; i < candles.length - 1; i++) {
    const curr = candles[i];
    const next = candles[i + 1];

    // Demand Zone (Bullish Order Block)
    if (
      curr.close < curr.open &&
      next.close > next.open &&
      next.close > curr.high
    ) {
      zones.push({ type: "DEMAND", high: curr.high, low: curr.low });
    }
    // Supply Zone (Bearish Order Block)
    else if (
      curr.close > curr.open &&
      next.close < next.open &&
      next.close < curr.low
    ) {
      zones.push({ type: "SUPPLY", high: curr.high, low: curr.low });
    }
  }
  return zones;
}

function detectAdditionalSetups(candles) {
  const c = candles.slice(0, -1); // completed candles only
  if (c.length < 25) return { direction: "NONE", setups: [] };
  const last = c[c.length - 1];
  const prev = c[c.length - 2];
  const lookback = c.slice(-21, -1);
  const priorHigh = Math.max(...lookback.slice(0, -1).map(x => x.high));
  const priorLow = Math.min(...lookback.slice(0, -1).map(x => x.low));
  const setups = [];
  let direction = "NONE";

  // Liquidity sweep: wick beyond a prior extreme, close back inside.
  if (last.low < priorLow && last.close > priorLow && last.close > last.open) {
    setups.push("Liquidity sweep below prior swing low, then bullish close back inside");
    direction = "LONG";
  } else if (last.high > priorHigh && last.close < priorHigh && last.close < last.open) {
    setups.push("Liquidity sweep above prior swing high, then bearish close back inside");
    direction = "SHORT";
  }

  // Break-and-retest: previous candle breaks a recent level; latest candle retests and closes through in breakout direction.
  const range = c.slice(-12, -3);
  if (range.length >= 6) {
    const resistance = Math.max(...range.map(x => x.high));
    const support = Math.min(...range.map(x => x.low));
    if (prev.close > resistance && last.low <= resistance && last.close > resistance && last.close > last.open) {
      setups.push("Bullish break and retest confirmed at prior resistance");
      if (direction === "NONE") direction = "LONG";
    } else if (prev.close < support && last.high >= support && last.close < support && last.close < last.open) {
      setups.push("Bearish break and retest confirmed at prior support");
      if (direction === "NONE") direction = "SHORT";
    }
  }

  // Approximate S/R bounce from repeated recent extremes; trendline-like confirmation is the close through the prior candle extreme.
  const sr = c.slice(-16, -1);
  const support = Math.min(...sr.map(x => x.low));
  const resistance = Math.max(...sr.map(x => x.high));
  const nearSupport = last.low <= support * 1.002 && last.close > support && last.close > last.open;
  const nearResistance = last.high >= resistance * 0.998 && last.close < resistance && last.close < last.open;
  if (nearSupport && last.close > prev.high) {
    setups.push("Support bounce with bullish candle confirmation (trendline/SR proxy)");
    if (direction === "NONE") direction = "LONG";
  } else if (nearResistance && last.close < prev.low) {
    setups.push("Resistance rejection with bearish candle confirmation (trendline/SR proxy)");
    if (direction === "NONE") direction = "SHORT";
  }
  return { direction, setups };
}

function analyzeDailyTrend(dailyCandles) {
  // Use completed daily candles only.

  const candles = dailyCandles.slice(0, -1);

  if (candles.length < 10) return "NEUTRAL";

  // Compare recent 5-candle and preceding 5-candle averages.
  const previous = candles.slice(-10, -5);
  const recent = candles.slice(-5);

  const average = (items, key) =>
    items.reduce((sum, candle) => sum + candle[key], 0) / items.length;

  const previousHigh = average(previous, "high");
  const recentHigh = average(recent, "high");
  const previousLow = average(previous, "low");
  const recentLow = average(recent, "low");
  const previousClose = average(previous, "close");
  const recentClose = average(recent, "close");

  const higherHighs = recentHigh > previousHigh;
  const higherLows = recentLow > previousLow;
  const lowerHighs = recentHigh < previousHigh;
  const lowerLows = recentLow < previousLow;
  const risingClose = recentClose > previousClose;
  const fallingClose = recentClose < previousClose;

  if (higherHighs && higherLows && risingClose) {
    return "BULLISH";
  }

  if (lowerHighs && lowerLows && fallingClose) {
    return "BEARISH";
  }

  return "CONSOLIDATION";
}



// ICT model helpers: candle-based approximations using completed candles only.
// These are rule-based proxies, not a full discretionary ICT chart reading.
function findRecentSwing(candles, side, strength = 2) {
  for (let i = candles.length - strength - 2; i >= strength; i--) {
    const price = side === "HIGH" ? candles[i].high : candles[i].low;
    let pivot = true;
    for (let j = i - strength; j <= i + strength; j++) {
      if (j === i) continue;
      if (side === "HIGH" && candles[j].high >= price) pivot = false;
      if (side === "LOW" && candles[j].low <= price) pivot = false;
    }
    if (pivot) return { index: i, price };
      }
  return null;
}

function analyzeICTModel(dailyCandles, htfCandles, ltfCandles) {
  const daily = dailyCandles.slice(0, -1);
  const htf = htfCandles.slice(0, -1);
  const ltf = ltfCandles.slice(0, -1);
  const result = {
    previousDayHigh: null, previousDayLow: null,
    buySideLiquidity: null, sellSideLiquidity: null,
    liquiditySweep: "NONE", mss: "NONE", fvg: null, orderBlock: null,
    rangeMidpoint: null, dealingRange: "UNKNOWN", killZone: "OFF",
    aligned: false, notes: [],
  };
  if (daily.length) {
    const previousDay = daily[daily.length - 1];
    result.previousDayHigh = previousDay.high;
    result.previousDayLow = previousDay.low;
  }
  if (htf.length < 10 || ltf.length < 10) {
    result.notes.push("Not enough completed candles for reliable ICT context.");
    return result;
  }

  const recentHtf = htf.slice(-20);
  const rangeHigh = Math.max(...recentHtf.map(c => c.high));
  const rangeLow = Math.min(...recentHtf.map(c => c.low));
  result.rangeMidpoint = (rangeHigh + rangeLow) / 2;
  const current = ltf[ltf.length - 1];
  result.dealingRange = current.close < result.rangeMidpoint ? "DISCOUNT" : "PREMIUM";

  const swingHigh = findRecentSwing(htf, "HIGH");
  const swingLow = findRecentSwing(htf, "LOW");
  const tolerance = current.close * 0.0015;
  const highs = recentHtf.filter(c => Math.abs(c.high - rangeHigh) <= tolerance);
  const lows = recentHtf.filter(c => Math.abs(c.low - rangeLow) <= tolerance);
  result.buySideLiquidity = swingHigh?.price ?? rangeHigh;
  result.sellSideLiquidity = swingLow?.price ?? rangeLow;
  if (highs.length >= 2) result.notes.push("Possible equal highs / buy-side liquidity pool mapped.");
  else result.notes.push("Buy-side liquidity mapped from recent swing high.");
  if (lows.length >= 2) result.notes.push("Possible equal lows / sell-side liquidity pool mapped.");
  else result.notes.push("Sell-side liquidity mapped from recent swing low.");

  const pdh = result.previousDayHigh;
  const pdl = result.previousDayLow;
  if ((pdl != null && current.low < pdl && current.close > pdl) ||
      (swingLow && current.low < swingLow.price && current.close > swingLow.price)) {
    result.liquiditySweep = "SELL_SIDE_SWEPT";
    result.notes.push("Sell-side liquidity sweep: wick below a mapped low, then close back above it.");
  } else if ((pdh != null && current.high > pdh && current.close < pdh) ||
             (swingHigh && current.high > swingHigh.price && current.close < swingHigh.price)) {
    result.liquiditySweep = "BUY_SIDE_SWEPT";
    result.notes.push("Buy-side liquidity sweep: wick above a mapped high, then close back below it.");
  } else {
    result.notes.push("No confirmed sweep of prior-day or recent swing liquidity on the latest completed 15m candle.");
  }

  const ltfSwingHigh = findRecentSwing(ltf, "HIGH");
  const ltfSwingLow = findRecentSwing(ltf, "LOW");
  const prior = ltf[ltf.length - 2];
  if (ltfSwingHigh && prior.close <= ltfSwingHigh.price && current.close > ltfSwingHigh.price && current.close > current.open) {
    result.mss = "BULLISH_MSS";
    result.notes.push("Bullish MSS proxy: completed candle closed above a confirmed recent swing high.");
  } else if (ltfSwingLow && prior.close >= ltfSwingLow.price && current.close < ltfSwingLow.price && current.close < current.open) {
    result.mss = "BEARISH_MSS";
    result.notes.push("Bearish MSS proxy: completed candle closed below a confirmed recent swing low.");
  } else {
    result.notes.push("No confirmed MSS on the latest completed 15m candle.");
  }

  // Three-candle fair value gap: non-overlap between candle 1 and candle 3.
  for (let i = ltf.length - 1; i >= Math.max(2, ltf.length - 12); i--) {
    const first = ltf[i - 2];
    const third = ltf[i];
    if (third.low > first.high) {
      result.fvg = { type: "BULLISH_FVG", low: first.high, high: third.low };
      break;
    }
    if (third.high < first.low) {
      result.fvg = { type: "BEARISH_FVG", low: third.high, high: first.low };
      break;
    }
  }
  result.notes.push(result.fvg
    ? `${result.fvg.type.replaceAll("_", " ")} identified at ${formatPrice(result.fvg.low)}–${formatPrice(result.fvg.high)}.`
    : "No recent three-candle FVG found on the 15m timeframe.");

  // Order-block proxy: latest opposing candle before a strong candle that closes beyond its range.
  for (let i = ltf.length - 1; i >= Math.max(1, ltf.length - 10); i--) {
    const impulse = ltf[i];
    const origin = ltf[i - 1];
    const body = Math.abs(impulse.close - impulse.open);
    const avgRange = ltf.slice(Math.max(0, i - 14), i).reduce((sum, c) => sum + (c.high - c.low), 0) / Math.max(1, ltf.slice(Math.max(0, i - 14), i).length);
    if (body < avgRange * 0.8) continue;
    if (impulse.close > impulse.open && origin.close < origin.open && impulse.close > origin.high) {
      result.orderBlock = { type: "BULLISH_ORDER_BLOCK", low: origin.low, high: origin.high };
      break;
    }
    if (impulse.close < impulse.open && origin.close > origin.open && impulse.close < origin.low) {
      result.orderBlock = { type: "BEARISH_ORDER_BLOCK", low: origin.low, high: origin.high };
      break;
    }
  }
  result.notes.push(result.orderBlock
    ? `${result.orderBlock.type.replaceAll("_", " ")} proxy identified at ${formatPrice(result.orderBlock.low)}–${formatPrice(result.orderBlock.high)}.`
    : "No recent displacement-backed order-block proxy found.");

  // Kill-zone status uses New York local time, independent of the hosting server timezone.
  const nyParts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", hour: "2-digit", minute: "2-digit", hour12: false,
  }).formatToParts(new Date());
  const nyHour = Number(nyParts.find(p => p.type === "hour")?.value ?? 0);
  const nyMinute = Number(nyParts.find(p => p.type === "minute")?.value ?? 0);
  const minutes = nyHour * 60 + nyMinute;
  if ((minutes >= 120 && minutes < 300) || (minutes >= 450 && minutes < 660)) result.killZone = "ACTIVE";
  result.notes.push(`ICT kill-zone time filter: ${result.killZone === "ACTIVE" ? "inside a configured London/New York window" : "outside configured London/New York windows"} (informational; not used as a hard filter).`);
  return result;
}

export async function performMultiTimeframeAnalysis(symbol, htfChoice = "4h") {
  const market = await getKrakenMarket(symbol);

  // 1. Daily Bias
  const dailyCandles = await fetchCandles(
    symbol,
    "1d",
    14,
    24 * 60 * 60 * 1000,
  );
  const dailyBias = analyzeDailyTrend(dailyCandles);

  // 2. HTF POI Mapping (4H or 1H)
  const htfInterval = htfChoice === "1h" ? "1h" : "4h";
  const htfIntervalMs = htfChoice === "1h" ? 3600 * 1000 : 4 * 3600 * 1000;
  const htfCandles = await fetchCandles(symbol, htfInterval, 50, htfIntervalMs);
  // Use closed candles and a tiny edge buffer to prevent live price flickering
  const closedCandles = htfCandles.slice(0, -1);
  const zones = identifySupplyDemandZones(closedCandles);
  const currentPrice = closedCandles[closedCandles.length - 1].close;

  let activePoi = null;
  for (const zone of zones) {
    if (currentPrice >= zone.low * 0.999 && currentPrice <= zone.high * 1.001) {
      activePoi = zone;
      break;
    }
  }

  // 3. LTF Confirmation (15m for 4H, 5m for 1H)
  const ltfInterval = htfChoice === "1h" ? "5m" : "15m";
  const ltfIntervalMs = htfChoice === "1h" ? 5 * 60 * 1000 : 15 * 60 * 1000;
  const ltfCandles = await fetchCandles(symbol, ltfInterval, 30, ltfIntervalMs);
  const lastLtf = ltfCandles[ltfCandles.length - 2];
  const prevLtf = ltfCandles[ltfCandles.length - 3];

  let ltfConfirmation = "NONE";
  if (activePoi?.type === "DEMAND" && lastLtf.close > prevLtf.high) {
    ltfConfirmation = "BULLISH_CONFIRMATION";
  } else if (activePoi?.type === "SUPPLY" && lastLtf.close < prevLtf.low) {
    ltfConfirmation = "BEARISH_CONFIRMATION";
  }

  const extraSetups = detectAdditionalSetups(ltfCandles);
  const ict = analyzeICTModel(dailyCandles, htfCandles, ltfCandles);

  // Scoring & Setup Decision
  let score = 40;
  let direction = "WAIT";
  let reasons = [`Daily Bias: ${dailyBias}`];
  reasons.push("Primary strategy: ICT confluence model + existing Supply & Demand / SMC filters");
  reasons.push(`ICT liquidity sweep: ${ict.liquiditySweep}`);
  reasons.push(`ICT MSS: ${ict.mss}`);
  reasons.push(`ICT dealing range: ${ict.dealingRange}; midpoint ${formatPrice(ict.rangeMidpoint || 0)}`);
  reasons.push(...ict.notes);
  if (extraSetups.setups.length) {
    reasons.push(...extraSetups.setups.map(setup => `Additional setup: ${setup}`));
  } else {
    reasons.push("Additional setups: no confirmed liquidity sweep, break-and-retest, or S/R bounce on the latest completed candle");
  }

  if (activePoi) {
    score += 30;
    reasons.push(
      `Price testing ${htfChoice.toUpperCase()} ${activePoi.type} POI Zone`,
    );
  } else {
    reasons.push(`Price floating in equilibrium (No active POI test)`);
  }

  if (ltfConfirmation !== "NONE") {
    score += 30;
    reasons.push(`LTF (${ltfInterval}) confirmation candle received`);
  }

  // ICT strict gate: bias + aligned HTF POI + liquidity sweep + MSS + FVG/OB + premium/discount.
  const bullishIctZone = ict.fvg?.type === "BULLISH_FVG" || ict.orderBlock?.type === "BULLISH_ORDER_BLOCK";
  const bearishIctZone = ict.fvg?.type === "BEARISH_FVG" || ict.orderBlock?.type === "BEARISH_ORDER_BLOCK";
  const bullishIct = dailyBias === "BULLISH" && activePoi?.type === "DEMAND" &&
    ict.liquiditySweep === "SELL_SIDE_SWEPT" && ict.mss === "BULLISH_MSS" &&
    bullishIctZone && ict.dealingRange === "DISCOUNT" && score >= 70;
  const bearishIct = dailyBias === "BEARISH" && activePoi?.type === "SUPPLY" &&
    ict.liquiditySweep === "BUY_SIDE_SWEPT" && ict.mss === "BEARISH_MSS" &&
    bearishIctZone && ict.dealingRange === "PREMIUM" && score >= 70;

  if (bullishIct) {
    direction = "LONG";
    score = Math.min(95, score + 10);
    reasons.push("ICT checklist aligned: bullish bias, demand POI, sell-side sweep, bullish MSS, bullish FVG/OB, and discount pricing.");
  } else if (bearishIct) {
    direction = "SHORT";
    score = Math.min(95, score + 10);
    reasons.push("ICT checklist aligned: bearish bias, supply POI, buy-side sweep, bearish MSS, bearish FVG/OB, and premium pricing.");
  } else {
    direction = "WAIT";
    reasons.push("ICT strict filter not fully aligned. No LONG/SHORT signal until all required confluences agree.");
  }

  let entry = currentPrice;
  let stopLoss = 0;
  let takeProfit = 0;

  if (direction === "LONG" && activePoi) {
    stopLoss = activePoi.low * 0.998;
    const risk = entry - stopLoss;
    takeProfit = entry + risk * 2.5;
  } else if (direction === "SHORT" && activePoi) {
    stopLoss = activePoi.high * 1.002;
    const risk = stopLoss - entry;
    takeProfit = entry - risk * 2.5;
  }

  return {
    symbol: market.commandSymbol,
    direction,
    score,
    entry,
    stopLoss,
    takeProfit,
    reasons,
    ict: {
      liquiditySweep: ict.liquiditySweep,
      mss: ict.mss,
      fvg: ict.fvg,
      orderBlock: ict.orderBlock,
      dealingRange: ict.dealingRange,
      rangeMidpoint: ict.rangeMidpoint,
      previousDayHigh: ict.previousDayHigh,
      previousDayLow: ict.previousDayLow,
      killZone: ict.killZone,
    },
  };
}

function formatPrice(value) {
  if (!value) return "0.00";
  const decimals = value >= 100 ? 2 : value >= 1 ? 4 : 6;
  return value.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

async function loadMonitorState() {
  try {
    const saved = await readFile(MONITOR_FILE, "utf8");
    const monitors = JSON.parse(saved);

    for (const monitor of monitors) {
      if (monitor?.id) {
        activeMonitors.set(monitor.id, monitor);
      }
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
    !Number.isFinite(entry) ||
    !Number.isFinite(stopLoss) ||
    !Number.isFinite(takeProfit) ||
    entry <= 0 ||
    stopLoss <= 0 ||
    takeProfit <= 0
  ) {
    await sendMessage(
      token,
      chatId,
      "⚠️ Signal monitoring was not started because its price levels are invalid.",
    );
    return;
  }

  const risk =
    result.direction === "LONG" ? entry - stopLoss : stopLoss - entry;

  if (risk <= 0) {
    await sendMessage(
      token,
      chatId,
      "⚠️ Signal monitoring was not started because the stop-loss is on the wrong side of the entry.",
    );
    return;
  }

  const existing = [...activeMonitors.values()].find(
    (m) =>
      String(m.chatId) === String(chatId) &&
      m.symbol === result.symbol &&
      m.status === "ACTIVE",
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
      `+1R alert: ${formatPrice(
        monitor.direction === "LONG" ? entry + risk : entry - risk,
      )}`,
      `−0.75R warning: ${formatPrice(
        monitor.direction === "LONG"
          ? entry - risk * 0.75
          : entry + risk * 0.75,
      )}`,
      "This is monitoring only; no orders will be placed.",
    ].join("\n"),
  );
}

async function checkOneMonitor(token, monitor) {
  if (monitor.status !== "ACTIVE") return;

  const candles = await fetchCandles(monitor.symbol, "1m", 3, 60 * 1000);

  if (!candles.length) return;

  // Use the latest available one-minute candle's range.
  const candle = candles[candles.length - 1];

  if (
    !Number.isFinite(candle.high) ||
    !Number.isFinite(candle.low) ||
    !Number.isFinite(candle.close)
  ) {
    return;
  }

  const isLong = monitor.direction === "LONG";

  const hitStop = isLong
    ? candle.low <= monitor.stopLoss
        : candle.high >= monitor.stopLoss;

  const hitTarget = isLong
    ? candle.high >= monitor.takeProfit
    : candle.low <= monitor.takeProfit;

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
    return;
  }

  if (hitStop) {
    await sendMessage(
      token,
      monitor.chatId,
      `🔴 *${monitor.symbol}: stop-loss level touched*\nDirection: ${monitor.direction}\nSL reference: ${formatPrice(monitor.stopLoss)}\nThis is a price-level alert, not confirmation of an executed trade.`,
    );

    monitor.status = "STOP_TOUCHED";
    await saveMonitorState();
    return;
  }

  if (hitTarget) {
    await sendMessage(
      token,
      monitor.chatId,
      `🎯 *${monitor.symbol}: take-profit level touched*\nDirection: ${monitor.direction}\nTP reference: ${formatPrice(monitor.takeProfit)}\nThis is a price-level alert, not confirmation of an executed trade.`,
    );

    monitor.status = "TARGET_TOUCHED";
    await saveMonitorState();
    return;
  }

  const hitOneR = isLong
    ? candle.high >= monitor.entry + monitor.risk

        : candle.low <= monitor.entry - monitor.risk;

  const hitAdverse = isLong
    ? candle.low <= monitor.entry - monitor.risk * 0.75
    : candle.high >= monitor.entry + monitor.risk * 0.75;

  if (
    hitOneR &&
    hitAdverse &&
    !monitor.oneRAlerted &&
    !monitor.adverseAlerted
  ) {
    await sendMessage(
      token,
      monitor.chatId,
      `⚠️ *${monitor.symbol}: movement order unclear*\nThe same one-minute candle touched both the +1R and −0.75R alert levels. Their order cannot be determined from candle data.`,
    );

    monitor.oneRAlerted = true;
    monitor.adverseAlerted = true;
    await saveMonitorState();
    return;
  }

  if (hitOneR && !monitor.oneRAlerted) {
    await sendMessage(
      token,
      monitor.chatId,
      `🟢 *${monitor.symbol}: +1R level reached*\nDirection: ${monitor.direction}\nReference entry: ${formatPrice(monitor.entry)}\nAlert level: ${formatPrice(
        isLong ? monitor.entry + monitor.risk : monitor.entry - monitor.risk,
      )}`,
    );

    monitor.oneRAlerted = true;
    await saveMonitorState();
  }

  if (hitAdverse && !monitor.adverseAlerted) {
    await sendMessage(
      token,
      monitor.chatId,
      `🟠 *${monitor.symbol}: −0.75R adverse warning*\nDirection: ${monitor.direction}\nReference entry: ${formatPrice(monitor.entry)}\nWarning level: ${formatPrice(
        isLong
          ? monitor.entry - monitor.risk * 0.75
          : monitor.entry + monitor.risk * 0.75,
      )}`,
    );

    monitor.adverseAlerted = true;
    await saveMonitorState();
  }

  monitor.lastCheckedCandle = candle.startTime;
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
  await writeFile(
    SUBSCRIBERS_FILE,
    JSON.stringify([...autoSubscribers]),
    "utf8",
  );
}

function buildAlertMessage(result) {
  const icon = result.direction === "LONG" ? "📈" : "📉";
  return [
    `🚨 *AUTO ALERT*`,
    `🪙 Pair: ${result.symbol}`,
    `${icon} Signal: ${result.direction} (Confidence:${result.score}%)`,
    `Timeframe: Daily bias + 4H POI + 15m ICT confirmation`,
    `🎯 Entry: ${formatPrice(result.entry)}`,
    `🟢 TP: ${formatPrice(result.takeProfit)}`,
    `🔴 SL: ${formatPrice(result.stopLoss)}`,
    `Risk:Reward target: 1:2.5`,
    ``,
    ...result.reasons.map((r) => `• ${r}`),
  ].join("\n");
}

async function runAutoScan(token) {
  while (true) {
    if (autoSubscribers.size > 0) {
      for (const symbol of AUTO_SCAN_SYMBOLS) {
        try {
          const result = await performMultiTimeframeAnalysis(symbol, "4h");
          if (result.direction === "WAIT") continue;

          for (const chatId of autoSubscribers) {
            const key = `${chatId}-${symbol}-${result.direction}`;
            const last = lastAlertAt.get(key) || 0;
            if (Date.now() - last < ALERT_COOLDOWN_MS) continue;

            const hasActive = [...activeMonitors.values()].some(
              (m) =>
                String(m.chatId) === String(chatId) &&
                m.symbol === result.symbol &&
                m.status === "ACTIVE",
            );
            if (hasActive) continue;

            lastAlertAt.set(key, Date.now());
            await sendMessage(token, chatId, buildAlertMessage(result));
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

async function callTelegram(token, method, body) {
  const res = await fetch(`${TELEGRAM_API}/bot${token}/${method}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  return data.result;
}

async function sendMessage(token, chatId, text) {
  await callTelegram(token, "sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "Markdown",
  });
}

async function handleMessage(token, message) {
  const text = message.text?.trim();
  if (!text || !message.chat?.id) return;
  const match = text.match(/^(\/[a-z]+)(?:\s+(.+))?$/i);
  if (!match) return;

  const command = match[1].toLowerCase();
  const args = (match[2] || "").trim().split(/\s+/).filter(Boolean);
  const chatId = message.chat.id;

  if (command === "/start" || command === "/help") {
    await sendMessage(
      token,
      chatId,
      "🤖 *Supply & Demand Multi-TF Bot*\n/analyze <symbol> — ICT Top-Down Analysis (Daily bias + 4H POI + 15m liquidity/MSS/FVG/OB)",
    );
    return;
  }

  if (command === "/boshelp") {
    await sendMessage(token, chatId, [
      "*Valid BOS detector — Balanced confirmation*",
      "/bos BTCUSDT — check 15m",
      "/bos BTCUSDT 1h — check 1h",
      "/bos BTCUSDT all — check all supported timeframes",
      "/bosauto — subscribe to automatic BOS alerts",
      "/bosstop — stop automatic BOS alerts",
      "",
      "Rules: confirmed swing pivot, completed candle close beyond the level, directional candle, and body >= 0.6 x ATR(14). Wick-only breaks do not qualify. Alerts are analysis only, not trade instructions.",
    ].join("\\n"));
    return;
  }

  if (command === "/bosauto") {
    bosSubscribers.add(String(chatId));
    await saveBosSubscribers();
    await sendMessage(token, chatId, `✅ Automatic BOS alerts enabled. Watching ${BOS_SYMBOLS.join(", ")} across ${Object.keys(BOS_TIMEFRAMES).join(", ")}. Scans run about every 5 minutes. Use /bosstop to unsubscribe.`);
    return;
  }

  if (command === "/bosstop") {
    bosSubscribers.delete(String(chatId));
    await saveBosSubscribers();
    await sendMessage(token, chatId, "🛑 Automatic BOS alerts disabled. Your existing S&D /auto alerts are unchanged.");
    return;
  }

  if (command === "/bos") {
    if (!args[0]) {
      await sendMessage(token, chatId, "Usage: /bos BTCUSDT [15m|1h|all]. Use /boshelp for details.");
      return;
    }
    const symbol = args[0].toUpperCase();
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
          await sendMessage(token, chatId, `⚪ *NO VALID BOS*\\nPair: ${symbol}\\nTimeframe: ${timeframe}\\nReason: ${result.reason}`);
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
      `✅ Auto alerts ON. Scanning ${AUTO_SCAN_SYMBOLS.join(", ")} every 5 minutes. Use /autooff to stop.`,
    );
    return;
  }

  if (command === "/autooff") {
    autoSubscribers.delete(String(chatId));
    await saveSubscribers();
    await sendMessage(token, chatId, "🛑 Auto alerts OFF.");
    return;
  }

  if (command === "/autostatus") {
    const on = autoSubscribers.has(String(chatId));
    await sendMessage(
      token,
      chatId,
      `Auto alerts: ${on ? "ON" : "OFF"}\nWatching: ${AUTO_SCAN_SYMBOLS.join(", ")}`,
    );
    return;
  }

  if (command === "/analyze" && args.length >= 1) {
    const symbol = args[0].toUpperCase();
    await sendMessage(
      token,
      chatId,
      `🔍 Running ICT model + S&D/SMC: liquidity, MSS, FVG, order block, premium/discount on ${symbol}...`,
    );
    try {
      const result = await performMultiTimeframeAnalysis(symbol, "4h");
      const icon =
        result.direction === "LONG"
          ? "📈"
          : result.direction === "SHORT"
            ? "📉"
            : "⏳";

      const msg = [
        `🪙 Pair: ${result.symbol}`,
        `${icon} Signal: ${result.direction} (Confidence:${result.score}%)`,
        `Timeframe: Daily bias + 4H POI + 15m ICT confirmation`,
        `🎯 Entry: ${formatPrice(result.entry)}`,
        `🟢 TP: ${formatPrice(result.takeProfit)}`,
        `🔴 SL: ${formatPrice(result.stopLoss)}`,
        `Risk:Reward target: 1:2.5`,
        ``,
        `🔍 *Strategy Diagnostics:*`,
        ...result.reasons.map((r) => `• ${r}`),
      ].join("\n");

      await sendMessage(token, chatId, msg);
      if (result.direction === "LONG" || result.direction === "SHORT") {
        try {
          await registerMonitor(token, chatId, result);
        } catch (error) {
          console.error("Could not register signal monitor:", error.message);

          await sendMessage(
            token,
            chatId,
            "⚠️ The signal was sent, but monitoring could not be started. Check the Replit console.",
          );
        }
      }
    } catch (err) {
      await sendMessage(
        token,
        chatId,
        `⚠️ Error analyzing ${symbol}: ${err.message}`,
      );
    }
  }
}

export async function startBot(token) {
  let offset = 0;
  console.info("Multi-Timeframe S&D Bot running.");
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
  await startBot(token);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch(console.error);
      }
