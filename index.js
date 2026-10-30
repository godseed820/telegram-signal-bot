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

// ---------- Settings you can tweak ----------
// Fallback list, used only if the live "top markets" list cannot be loaded.
const AUTO_SCAN_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
// Auto-scan covers the most traded Kraken perpetuals (by 24h volume).
const SCAN_TOP_N = 40;
const SCAN_LIST_REFRESH_MS = 6 * 60 * 60 * 1000;
const KRAKEN_TICKERS_URL = "https://futures.kraken.com/derivatives/api/v3/tickers";
// Non-crypto or stable contracts that should never be scanned.
const EXCLUDED_BASES = new Set([
  "XAU", "XAG", "XPT", "XPD", "WTI", "BRENT", "OIL", "USOIL", "UKOIL", "NGAS",
  "SPX", "NDX", "DJI", "TSLA", "AAPL", "NVDA", "AMZN", "MSFT", "GOOGL", "META",
  "COIN", "MSTR", "EUR", "GBP", "AUD", "JPY", "CHF", "CAD",
  "USDT", "USDC", "DAI", "PYUSD", "USDE",
]);
let scanSymbols = [...AUTO_SCAN_SYMBOLS];
let scanListUpdatedAt = 0;
const AUTO_SCAN_INTERVAL_MS = 5 * 60 * 1000;
const ALERT_COOLDOWN_MS = 4 * 60 * 60 * 1000;
// With 25x leverage, liquidation is roughly 3.5-4% away.
// Signals with a stop-loss wider than this are skipped (turned into WAIT).
const MAX_SL_PCT = 2.5;
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
// Automatic alerts skip 1m and 5m (too noisy). Manual /bos still supports all.
const BOS_AUTO_TIMEFRAMES = ["15m", "30m", "1h", "4h", "1d"];
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

/**
 * Finds supply/demand zones (order blocks) on completed candles.
 * - Zones that price already closed through are dropped (broken).
 * - Returned newest-first so the most recent zone is checked first.
 */
function identifySupplyDemandZones(candles) {
  const zones = [];
  for (let i = 2; i < candles.length - 1; i++) {
    const curr = candles[i];
    const next = candles[i + 1];
    let zone = null;

    // Demand Zone (Bullish Order Block)
    if (curr.close < curr.open && next.close > next.open && next.close > curr.high) {
      zone = { type: "DEMAND", high: curr.high, low: curr.low, index: i };
    }
    // Supply Zone (Bearish Order Block)
    else if (curr.close > curr.open && next.close < next.open && next.close < curr.low) {
      zone = { type: "SUPPLY", high: curr.high, low: curr.low, index: i };
    }
    if (!zone) continue;

    const later = candles.slice(i + 2);
    const broken = zone.type === "DEMAND"
      ? later.some((c) => c.close < zone.low)
      : later.some((c) => c.close > zone.high);
    if (!broken) zones.push(zone);
  }
  return zones.reverse();
}

function detectAdditionalSetups(candles) {
  const c = candles.slice(0, -1); // completed candles only
  if (c.length < 25) return { direction: "NONE", setups: [] };
  const last = c[c.length - 1];
  const prev = c[c.length - 2];
  // The 20 candles right before the latest one.
  const lookback = c.slice(-21, -1);
  const priorHigh = Math.max(...lookback.map((x) => x.high));
  const priorLow = Math.min(...lookback.map((x) => x.low));
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
    const resistance = Math.max(...range.map((x) => x.high));
    const support = Math.min(...range.map((x) => x.low));
    if (prev.close > resistance && last.low <= resistance && last.close > resistance && last.close > last.open) {
      setups.push("Bullish break and retest confirmed at prior resistance");
      if (direction === "NONE") direction = "LONG";
    } else if (prev.close < support && last.high >= support && last.close < support && last.close < last.open) {
      setups.push("Bearish break and retest confirmed at prior support");
      if (direction === "NONE") direction = "SHORT";
    }
  }

  // Approximate S/R bounce. Levels come from candles BEFORE the latest one,
  // so the latest candle cannot "bounce" off its own low/high.
  const sr = c.slice(-17, -1);
  const support = Math.min(...sr.map((x) => x.low));
  const resistance = Math.max(...sr.map((x) => x.high));
  const nearSupport = last.low <= support * 1.002 && last.close > support && last.close > last.open;
  const nearResistance = last.high >= resistance * 0.998 && last.close < resistance && last.close < last.open;
  if (nearSupport && last.close > prev.high) {
    setups.push("Support bounce with bullish candle confirmation (SR proxy, weak signal)");
    if (direction === "NONE") direction = "LONG";
  } else if (nearResistance && last.close < prev.low) {
    setups.push("Resistance rejection with bearish candle confirmation (SR proxy, weak signal)");
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

  if (higherHighs && higherLows && risingClose) return "BULLISH";
  if (lowerHighs && lowerLows && fallingClose) return "BEARISH";
  return "CONSOLIDATION";
}

export async function performMultiTimeframeAnalysis(symbol, htfChoice = "4h") {
  const market = await getKrakenMarket(symbol);

  // 1. Daily Bias
  const dailyCandles = await fetchCandles(symbol, "1d", 14, 24 * 60 * 60 * 1000);
  const dailyBias = analyzeDailyTrend(dailyCandles);

  // 2. HTF candles (4H or 1H) for POI mapping
  const htfInterval = htfChoice === "1h" ? "1h" : "4h";
  const htfIntervalMs = htfChoice === "1h" ? 3600 * 1000 : 4 * 3600 * 1000;
  const htfCandles = await fetchCandles(symbol, htfInterval, 50, htfIntervalMs);
  const closedCandles = htfCandles.slice(0, -1);

  // 3. LTF candles (15m for 4H, 5m for 1H)
  const ltfInterval = htfChoice === "1h" ? "5m" : "15m";
  const ltfIntervalMs = htfChoice === "1h" ? 5 * 60 * 1000 : 15 * 60 * 1000;
  const ltfCandles = await fetchCandles(symbol, ltfInterval, 30, ltfIntervalMs);
  if (ltfCandles.length < 4 || closedCandles.length < 5) {
    throw new Error("Not enough candle data returned.");
  }

  // Live price = close of the still-forming latest LTF candle (not a stale HTF close).
  const livePrice = ltfCandles[ltfCandles.length - 1].close;

  // Zones are newest-first and already exclude broken zones.
  const zones = identifySupplyDemandZones(closedCandles);
  let activePoi = null;
  for (const zone of zones) {
    if (livePrice >= zone.low * 0.999 && livePrice <= zone.high * 1.001) {
      activePoi = zone;
      break;
    }
  }

  // LTF confirmation on the last two COMPLETED candles
  const lastLtf = ltfCandles[ltfCandles.length - 2];
  const prevLtf = ltfCandles[ltfCandles.length - 3];

  let ltfConfirmation = "NONE";
  if (activePoi?.type === "DEMAND" && lastLtf.close > prevLtf.high) {
    ltfConfirmation = "BULLISH_CONFIRMATION";
  } else if (activePoi?.type === "SUPPLY" && lastLtf.close < prevLtf.low) {
    ltfConfirmation = "BEARISH_CONFIRMATION";
  }

  const extraSetups = detectAdditionalSetups(ltfCandles);

  // Setup score = checklist count, NOT a win probability.
  let score = 40;
  let direction = "WAIT";
  const reasons = [`Daily Bias: ${dailyBias}`];
  reasons.push("Primary strategy: Supply & Demand + SMC + BOS confirmation");
  if (extraSetups.setups.length) {
    reasons.push(...extraSetups.setups.map((setup) => `Additional setup: ${setup}`));
  } else {
    reasons.push("Additional setups: no confirmed liquidity sweep, break-and-retest, or S/R bounce on the latest completed candle");
  }

  if (activePoi) {
    score += 30;
    reasons.push(`Price testing ${htfChoice.toUpperCase()} ${activePoi.type} POI Zone`);
  } else {
    reasons.push("Price floating in equilibrium (No active POI test)");
  }

  if (ltfConfirmation !== "NONE") {
    score += 30;
    reasons.push(`LTF (${ltfInterval}) confirmation candle received`);
  }

  if (
    dailyBias === "BULLISH" &&
    activePoi?.type === "DEMAND" &&
    ltfConfirmation === "BULLISH_CONFIRMATION" &&
    score >= 70
  ) {
    direction = "LONG";
  } else if (
    dailyBias === "BEARISH" &&
    activePoi?.type === "SUPPLY" &&
    ltfConfirmation === "BEARISH_CONFIRMATION" &&
    score >= 70
  ) {
    direction = "SHORT";
  }

  // Additional setups are a confluence filter: they can confirm, but cannot override, the daily bias or POI.
  if (direction === "WAIT" && extraSetups.direction === "LONG" && dailyBias === "BULLISH" && activePoi?.type === "DEMAND") {
    direction = "LONG";
    score = Math.min(95, score + 15);
    reasons.push("Extra setup supports the bullish daily bias and demand POI.");
  } else if (direction === "WAIT" && extraSetups.direction === "SHORT" && dailyBias === "BEARISH" && activePoi?.type === "SUPPLY") {
    direction = "SHORT";
    score = Math.min(95, score + 15);
    reasons.push("Extra setup supports the bearish daily bias and supply POI.");
  }

  const entry = livePrice;
  let stopLoss = 0;
  let takeProfit = 0;
  let slPct = 0;

  if (direction === "LONG" && activePoi) {
    stopLoss = activePoi.low * 0.998;
    const risk = entry - stopLoss;
    takeProfit = entry + risk * 2.5;
  } else if (direction === "SHORT" && activePoi) {
    stopLoss = activePoi.high * 1.002;
    const risk = stopLoss - entry;
    takeProfit = entry - risk * 2.5;
  }

  if (direction !== "WAIT") {
    slPct = (Math.abs(entry - stopLoss) / entry) * 100;
    if (!(slPct > 0) || slPct > MAX_SL_PCT) {
      reasons.push(
        `Setup skipped: stop-loss would be ${slPct.toFixed(2)}% away (max allowed ${MAX_SL_PCT}%). Too close to liquidation risk at high leverage.`,
      );
      direction = "WAIT";
      stopLoss = 0;
      takeProfit = 0;
      slPct = 0;
    }
  }

  return {
    symbol: market.commandSymbol,
    direction,
    score,
    entry,
    stopLoss,
    takeProfit,
    slPct,
    reasons,
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

function buildSignalLines(result) {
  const icon = result.direction === "LONG" ? "📈" : result.direction === "SHORT" ? "📉" : "⏳";
  const lines = [
    `🪙 Pair: ${result.symbol}`,
    `${icon} Signal: ${result.direction} (Setup score: ${result.score}/100)`,
  ];
  if (result.direction === "LONG" || result.direction === "SHORT") {
    lines.push(
      `🎯 Entry: ${formatPrice(result.entry)}`,
      `🟢 TP: ${formatPrice(result.takeProfit)}`,
      `🔴 SL: ${formatPrice(result.stopLoss)} (${result.slPct.toFixed(2)}% away)`,
    );
  } else {
    lines.push(`Current price: ${formatPrice(result.entry)}`);
  }
  return lines;
}

function buildAlertMessage(result) {
  return [
    `🚨 *AUTO ALERT*`,
    ...buildSignalLines(result),
    ``,
    ...result.reasons.map((r) => `• ${r}`),
    ``,
    `Setup score is a checklist count, not a win probability. Fees and funding are not included.`,
  ].join("\n");
}

// Loads the most traded Kraken USD perpetuals. Keeps the old list if loading fails.
async function refreshScanSymbols() {
  try {
    await waitForKrakenRequestSlot();
    const res = await fetch(KRAKEN_TICKERS_URL, { signal: AbortSignal.timeout(30000) });
    const body = await res.json();
    if (!res.ok || body?.result === "error" || !Array.isArray(body?.tickers)) {
      throw new Error("Unexpected tickers response.");
    }
    const rows = body.tickers
      .filter((t) => typeof t?.symbol === "string" && /^PF_[A-Z0-9]+USD$/.test(t.symbol) && !t.suspended)
      .map((t) => {
        const base = t.symbol.slice(3, -3);
        const volume = Number(t.volumeQuote) || Number(t.vol24h) * Number(t.last) || 0;
        return { base, volume };
      })
      .filter((r) => r.volume > 0 && !EXCLUDED_BASES.has(r.base))
      .sort((a, b) => b.volume - a.volume)
      .slice(0, SCAN_TOP_N);
    if (rows.length < 5) throw new Error("Too few markets returned.");
    scanSymbols = rows.map((r) => `${r.base === "XBT" ? "BTC" : r.base}USDT`);
    scanListUpdatedAt = Date.now();
    console.info(`Scan list updated: ${scanSymbols.length} markets.`);
  } catch (error) {
    console.error("Could not refresh scan list:", error.message);
    if (!scanSymbols.length) scanSymbols = [...AUTO_SCAN_SYMBOLS];
  }
}

async function runAutoScan(token) {
  while (true) {
    if (autoSubscribers.size > 0) {
      if (Date.now() - scanListUpdatedAt > SCAN_LIST_REFRESH_MS) {
        await refreshScanSymbols();
      }
      for (const symbol of scanSymbols) {
        try {
          const result = await performMultiTimeframeAnalysis(symbol, "4h");
          if (result.direction === "WAIT") continue;

          for (const chatId of autoSubscribers) {
            const key = `${chatId}-${symbol}-${result.direction}`;
            const last = lastAlertAt.get(key) || 0;
            if (Date.now() - last < ALERT_COOLDOWN_MS) continue;

            const hasActive = [...activeMonitors.values()].some(
              (m) => String(m.chatId) === String(chatId) && m.symbol === result.symbol && m.status === "ACTIVE",
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
    await sendMessage(
      token,
      chatId,
      [
        "🤖 *Supply & Demand Multi-TF Bot*",
        "/analyze <symbol> — Top-Down Analysis (Daily + 4H POI + 15m)",
        "/autoon — automatic S&D alerts ON",
        "/autooff — automatic S&D alerts OFF",
        "/autostatus — check auto alert status",
        "/scanlist — markets the auto-scan watches",
        "/stats — wins/losses of tracked signals",
        "/boshelp — BOS detector commands",
      ].join("\n"),
    );
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
    await sendMessage(token, chatId, "🛑 Automatic BOS alerts disabled. Your existing S&D /autoon alerts are unchanged.");
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
      `✅ Auto alerts ON. Scanning the top ${scanSymbols.length} Kraken perpetuals by volume, one full round about every 5 minutes. Use /scanlist to see them and /autooff to stop.`,
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
      `Auto alerts: ${on ? "ON" : "OFF"}\nWatching ${scanSymbols.length} markets. Use /scanlist to see them.`,
    );
    return;
  }

  if (command === "/scanlist") {
    if (!scanListUpdatedAt) await refreshScanSymbols();
    await sendMessage(
      token,
      chatId,
      [
        `📋 *Auto-scan list (${scanSymbols.length})*`,
        scanListUpdatedAt ? "Source: Kraken top markets by 24h volume" : "Source: fallback list (live list could not be loaded)",
        "",
        scanSymbols.join(", "),
      ].join("\n"),
    );
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

  if (command === "/analyze") {
    if (args.length < 1) {
      await sendMessage(token, chatId, "Usage: /analyze BTCUSDT");
      return;
    }
    const symbol = args[0].toUpperCase();
    await sendMessage(
      token,
      chatId,
      `🔍 Running S&D + SMC/BOS + Liquidity Sweep + Break/Retest + Trendline/S/R analysis on ${symbol}...`,
    );
    try {
      const result = await performMultiTimeframeAnalysis(symbol, "4h");

      const msg = [
        ...buildSignalLines(result),
        ``,
        `🔍 *Strategy Diagnostics:*`,
        ...result.reasons.map((r) => `• ${r}`),
        ``,
        `Setup score is a checklist count, not a win probability. Fees and funding are not included.`,
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
      await sendMessage(token, chatId, `⚠️ Error analyzing ${symbol}: ${err.message}`);
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
