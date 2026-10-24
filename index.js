import { pathToFileURL } from "node:url";
import { createHmac } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
const TELEGRAM_API = "https://api.telegram.org";
const KRAKEN_CHARTS_API = "https://futures.kraken.com/api/charts/v1";
const BYBIT_DEMO_API = "https://api-demo.bybit.com";
const BYBIT_RECV_WINDOW = "5000";
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


async function getBybitDemoStatus() {
  const apiKey = process.env.BYBIT_API_KEY;
  const apiSecret = process.env.BYBIT_API_SECRET;

  if (process.env.BYBIT_DEMO_TRADING !== "true") {
    return {
      ok: false,
      message: "Demo mode is not enabled. Set BYBIT_DEMO_TRADING=true in FadeHost."
    };
  }

  if (!apiKey || !apiSecret) {
    return {
      ok: false,
      message: "Missing BYBIT_API_KEY or BYBIT_API_SECRET in FadeHost."
    };
  }

  const timestamp = Date.now().toString();
  const query = "accountType=UNIFIED&coin=USDT";
  const signaturePayload =
    timestamp + apiKey + BYBIT_RECV_WINDOW + query;

  const signature = createHmac("sha256", apiSecret)
    .update(signaturePayload)
    .digest("hex");

  try {
    const response = await fetch(
      `${BYBIT_DEMO_API}/v5/account/wallet-balance?${query}`,
      {
        method: "GET",
        headers: {
          "X-BAPI-API-KEY": apiKey,
          "X-BAPI-TIMESTAMP": timestamp,
          "X-BAPI-RECV-WINDOW": BYBIT_RECV_WINDOW,
          "X-BAPI-SIGN": signature
        },
        signal: AbortSignal.timeout(10000)
      }
    );

    const data = await response.json();

    if (!response.ok || data.retCode !== 0) {
      return {
        ok: false,
        message: `Bybit error ${data.retCode ?? response.status}: ${
          data.retMsg ?? response.statusText
        }`
      };
    }

    const account = data.result?.list?.[0];
    const usdt = account?.coin?.find(
      coin => coin.coin === "USDT"
    );

    return {
      ok: true,
      accountType: account?.accountType ?? "Unknown",
      walletBalance: usdt?.walletBalance ?? "0",
      message: "Connected to Bybit Demo successfully."
    };
  } catch (error) {
    return {
      ok: false,
      message: `Connection failed: ${error.message}`
    };
  }
}


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
  const base = normalized.replace(/USDT$\vert{}USD$/, "");
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


const BYBIT_MARKET_API = "https://api-demo.bybit.com";
const BYBIT_ALLOWED_SYMBOLS = new Set([
  "BTCUSDT",
  "ETHUSDT",
  "SOLUSDT",
]);

function parseBybitCandles(payload, intervalMs) {
  const rows = payload?.result?.list;

  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error("Bybit returned no candle data.");
  }

  return rows
    .map((row) => ({
      startTime: Number(row[0]),
      closeTime: Number(row[0]) + intervalMs,
      open: Number(row[1]),
      high: Number(row[2]),
      low: Number(row[3]),
      close: Number(row[4]),
    }))
    .filter((candle) =>
      Number.isFinite(candle.startTime) &&
      Number.isFinite(candle.open) &&
      Number.isFinite(candle.high) &&
      Number.isFinite(candle.low) &&
      Number.isFinite(candle.close)
    )
    .sort((a, b) => a.startTime - b.startTime);
}

async function fetchBybitCandles(
  symbol,
  interval,
  count,
  intervalMs
) {
  const normalized = symbol?.trim().toUpperCase();

  if (!BYBIT_ALLOWED_SYMBOLS.has(normalized)) {
    throw new Error("Unsupported Bybit test symbol.");
  }

  const intervalMap = {
    "1d": "D",
    "4h": "240",
    "1h": "60",
    "15m": "15",
    "5m": "5",
  };

  const bybitInterval = intervalMap[interval];

  if (!bybitInterval) {
    throw new Error("Unsupported Bybit candle interval.");
  }

  const url = new URL(
    "/v5/market/kline",
    BYBIT_MARKET_API
  );

  url.searchParams.set("category", "linear");
  url.searchParams.set("symbol", normalized);
  url.searchParams.set("interval", bybitInterval);
  url.searchParams.set("limit", String(
    Math.min(1000, Math.max(1, Math.floor(count)))
  ));

  const response = await fetch(url, {
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(
      `Bybit market-data HTTP error: ${response.status}`
    );
  }

  const payload = await response.json();

  if (payload.retCode !== 0) {
    throw new Error(
      `Bybit market-data error ${payload.retCode}: ${payload.retMsg}`
    );
  }

  const candles = parseBybitCandles(payload, intervalMs);

  if (candles.length < 3) {
    throw new Error("Insufficient Bybit candle data.");
  }

  return candles;
}


function identifySupplyDemandZones(candles) {
  const zones = [];
  for (let i = 2; i < candles.length - 1; i++) {
    const curr = candles[i];
    const next = candles[i + 1];

    if (
      curr.close < curr.open &&
      next.close > next.open &&
      next.close > curr.high
    ) {
      zones.push({ type: "DEMAND", high: curr.high, low: curr.low });
    }
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

function analyzeDailyTrend(dailyCandles) {
  const candles = dailyCandles.slice(0, -1);
  if (candles.length < 10) return "NEUTRAL";

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

  if (recentHigh > previousHigh && recentLow > previousLow && recentClose > previousClose) {
    return "BULLISH";
  }
  if (recentHigh < previousHigh && recentLow < previousLow && recentClose < previousClose) {
    return "BEARISH";
  }
  return "CONSOLIDATION";
}


export async function performMultiTimeframeAnalysis(symbol) {
  const normalized = symbol?.trim().toUpperCase();

  if (!BYBIT_ALLOWED_SYMBOLS.has(normalized)) {
    throw new Error("Unsupported scalping symbol.");
  }

  const intervalMs15m = 15 * 60 * 1000;
  const intervalMs5m = 5 * 60 * 1000;

  const [contextRaw, entryRaw] = await Promise.all([
    fetchBybitCandles(normalized, "15m", 100, intervalMs15m),
    fetchBybitCandles(normalized, "5m", 100, intervalMs5m),
  ]);

  const now = Date.now();

  const contextCandles = contextRaw.filter(
    (candle) => candle.closeTime <= now
  );
  const entryCandles = entryRaw.filter(
    (candle) => candle.closeTime <= now
  );

  if (contextCandles.length < 20 || entryCandles.length < 20) {
    throw new Error("Not enough completed Bybit candles.");
  }

  const currentPrice = entryCandles[entryCandles.length - 1].close;
  const zones = identifySupplyDemandZones(contextCandles);

  let activePoi = null;

  for (const zone of zones.slice().reverse()) {
    if (
      currentPrice >= zone.low * 0.999 &&
      currentPrice <= zone.high * 1.001
    ) {
      activePoi = zone;
      break;
    }
  }

  const last = entryCandles[entryCandles.length - 1];
  const previous = entryCandles[entryCandles.length - 2];

  const bullishBos = last.close > previous.high;
  const bearishBos = last.close < previous.low;

  let direction = "WAIT";
  let score = 40;
  const reasons = [
    "Market data: Bybit Demo public candles",
    "Context: completed 15m candles",
    "Entry confirmation: completed 5m candles",
  ];

  if (!activePoi) {
    reasons.push("No active 15m supply/demand zone at current price.");
  } else {
    score += 20;
    reasons.push(`Price testing 15m ${activePoi.type} zone.`);
  }

  if (activePoi?.type === "DEMAND" && bullishBos) {
    direction = "LONG";
    score += 30;
    reasons.push("Bullish 5m candle breakout detected.");
  } else if (activePoi?.type === "SUPPLY" && bearishBos) {
    direction = "SHORT";
    score += 30;
    reasons.push("Bearish 5m candle breakout detected.");
  } else {
    reasons.push("No matching 5m confirmation; waiting.");
  }

  let entry = currentPrice;
  let stopLoss = 0;
  let takeProfit = 0;

  if (direction === "LONG" && activePoi) {
    stopLoss = activePoi.low * 0.998;
    const risk = entry - stopLoss;

    if (risk > 0) {
      takeProfit = entry + risk * 2;
    } else {
      direction = "WAIT";
    }
  } else if (direction === "SHORT" && activePoi) {
    stopLoss = activePoi.high * 1.002;
    const risk = stopLoss - entry;

    if (risk > 0) {
      takeProfit = entry - risk * 2;
    } else {
      direction = "WAIT";
    }
  }

  return {
    symbol: normalized,
    direction,
    score,
    entry,
    stopLoss,
    takeProfit,
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
  } catch (error) {
    if (error.code !== "ENOENT") console.error("Could not load monitor state:", error.message);
  }
}

function saveMonitorState() {
  const snapshot = JSON.stringify([...activeMonitors.values()], null, 2);
  persistenceQueue = persistenceQueue.catch(() => {}).then(() => writeFile(MONITOR_FILE, snapshot, "utf8"));
  return persistenceQueue;
}

async function registerMonitor(token, chatId, result) {
  if (!["LONG", "SHORT"].includes(result.direction)) return;
  const entry = Number(result.entry);
  const stopLoss = Number(result.stopLoss);
  const takeProfit = Number(result.takeProfit);

  if (!Number.isFinite(entry) || !Number.isFinite(stopLoss) || !Number.isFinite(takeProfit) || entry <= 0 || stopLoss <= 0 || takeProfit <= 0) return;

  const risk = result.direction === "LONG" ? entry - stopLoss : stopLoss - entry;
  if (risk <= 0) return;

  const existing = [...activeMonitors.values()].find(
    (m) => String(m.chatId) === String(chatId) && m.symbol === result.symbol && m.status === "ACTIVE"
  );
  if (existing) return;

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
}

async function checkOneMonitor(token, monitor) {
  if (monitor.status !== "ACTIVE") return;
  const candles = await fetchCandles(monitor.symbol, "1m", 3, 60 * 1000);
  if (!candles.length) return;
  const candle = candles[candles.length - 1];

  const isLong = monitor.direction === "LONG";
  const hitStop = isLong ? candle.low <= monitor.stopLoss : candle.high >= monitor.stopLoss;
  const hitTarget = isLong ? candle.high >= monitor.takeProfit : candle.low <= monitor.takeProfit;

  if (hitStop && hitTarget) {
    monitor.status = "AMBIGUOUS";
    await saveMonitorState();
    return;
  }
  if (hitStop) {
    monitor.status = "STOP_TOUCHED";
    await saveMonitorState();
    return;
  }
  if (hitTarget) {
    monitor.status = "TARGET_TOUCHED";
    await saveMonitorState();
    return;
  }
}

async function runMonitorLoop(token) {
  while (true) {
    try {
      for (const monitor of activeMonitors.values()) {
        if (monitor.status !== "ACTIVE") continue;
        try { await checkOneMonitor(token, monitor); } catch (e) {}
      }
    } catch (e) {}
    await sleep(15000);
  }
}

async function loadSubscribers() {
  try {
    const saved = JSON.parse(await readFile(SUBSCRIBERS_FILE, "utf8"));
    for (const id of saved) autoSubscribers.add(String(id));
  } catch (error) {
    if (error.code !== "ENOENT") console.error("Could not load subscribers:", error.message);
  }
}

async function saveSubscribers() {
  await writeFile(SUBSCRIBERS_FILE, JSON.stringify([...autoSubscribers]), "utf8");
}

function buildAlertMessage(result) {
  const icon = result.direction === "LONG" ? "📈" : "📉";
  return [
    `🚨 *AUTO ALERT*`,
    `🪙 Pair: ${result.symbol}`,
    `${icon} Signal: ${result.direction} (Confidence:${result.score}%)`,
    `🎯 Entry: ${formatPrice(result.entry)}`,
    `🟢 TP: ${formatPrice(result.takeProfit)}`,
    `🔴 SL: ${formatPrice(result.stopLoss)}`,
    ``,
    ...result.reasons.map((r) => `• ${r}`),
  ].join("\n");
}


export function detectBreakOfStructure(candles, options = {}) {
  const pivotStrength = options.pivotStrength ?? 3;
  const atrPeriod = options.atrPeriod ?? 14;
  const displacementAtr = options.displacementAtr ?? 0.6;

  if (
    !Array.isArray(candles) ||
    !Number.isInteger(pivotStrength) ||
    pivotStrength < 1 ||
    !Number.isInteger(atrPeriod) ||
    atrPeriod < 1 ||
    !Number.isFinite(displacementAtr) ||
    displacementAtr < 0 ||
    candles.length < Math.max(atrPeriod + 2, pivotStrength * 2 + 5)
  ) {
    return {
      direction: "NONE",
      reason: "Not enough valid candles or invalid detector settings."
    };
  }

  const valid = candles.every(c =>
    Number.isFinite(c.open) &&
    Number.isFinite(c.high) &&
    Number.isFinite(c.low) &&
    Number.isFinite(c.close) &&
    c.high >= Math.max(c.open, c.close, c.low) &&
    c.low <= Math.min(c.open, c.close, c.high)
  );

  if (!valid) {
    return {
      direction: "NONE",
      reason: "Invalid candle data."
    };
  }

  // The caller must supply completed candles only.
  const last = candles[candles.length - 1];

  const trueRanges = candles.slice(1).map((c, i) =>
    Math.max(
      c.high - c.low,
      Math.abs(c.high - candles[i].close),
      Math.abs(c.low - candles[i].close)
    )
  );

  const recentTR = trueRanges.slice(-atrPeriod);
  const atr = recentTR.reduce((sum, v) => sum + v, 0) / recentTR.length;
  const body = Math.abs(last.close - last.open);

  if (!(atr > 0)) {
    return { direction: "NONE", reason: "ATR is zero or invalid." };
  }

  let swingHigh = null;
  let swingLow = null;

  // Leave the newest candle as the breakout candidate.
  const lastPivotIndex = candles.length - pivotStrength - 2;

  for (let i = lastPivotIndex; i >= pivotStrength; i--) {
    const candidate = candles[i];
    let isHigh = true;
    let isLow = true;

    for (let j = i - pivotStrength; j <= i + pivotStrength; j++) {
      if (j === i) continue;

      if (candles[j].high >= candidate.high) isHigh = false;
      if (candles[j].low <= candidate.low) isLow = false;
    }

    if (isHigh && !swingHigh) {
      const brokenEarlier = candles
        .slice(i + pivotStrength + 1, candles.length - 1)
        .some(c => c.close > candidate.high);

      if (!brokenEarlier) {
        swingHigh = { index: i, level: candidate.high };
      }
    }

    if (isLow && !swingLow) {
      const brokenEarlier = candles
        .slice(i + pivotStrength + 1, candles.length - 1)
        .some(c => c.close < candidate.low);

      if (!brokenEarlier) {
        swingLow = { index: i, level: candidate.low };
      }
    }

    if (swingHigh && swingLow) break;
  }

  const bullishBreak = swingHigh && last.close > swingHigh.level;
  const bearishBreak = swingLow && last.close < swingLow.level;

  if (bullishBreak && bearishBreak) {
    return {
      direction: "NONE",
      reason: "Conflicting structure breaks."
    };
  }

  if (!bullishBreak && !bearishBreak) {
    const sweptHigh = swingHigh &&
      last.high > swingHigh.level &&
      last.close <= swingHigh.level;

    const sweptLow = swingLow &&
      last.low < swingLow.level &&
      last.close >= swingLow.level;

    return {
      direction: "NONE",
      reason: sweptHigh || sweptLow
        ? "Wick sweep without candle-close confirmation."
        : "No confirmed swing break."
    };
  }

  if (body < atr * displacementAtr) {
    return {
      direction: "NONE",
      reason: "Breakout lacks sufficient ATR displacement."
    };
  }

  if (bullishBreak && last.close <= last.open) {
    return {
      direction: "NONE",
      reason: "Bullish BOS requires a bullish candle."
    };
  }

  if (bearishBreak && last.close >= last.open) {
    return {
      direction: "NONE",
      reason: "Bearish BOS requires a bearish candle."
    };
  }

  const bullish = Boolean(bullishBreak);

  return {
    direction: bullish ? "BULLISH" : "BEARISH",
    level: bullish ? swingHigh.level : swingLow.level,
    candleTime: last.startTime ?? null,
    reason: `Confirmed ${bullish ? "bullish" : "bearish"} BOS: close beyond swing with displacement.`
  };
}


function analyzeBreakOfStructure(candles, symbol, timeframe = "15m") {
  if (!candles || candles.length < 20) return null;

  const completedCandles = candles.slice(0, -1);
  const currentCandle = candles[candles.length - 1];

  let recentSwingHigh = -Infinity;
  let recentSwingLow = Infinity;

  const lookback = completedCandles.slice(-15);
  for (const c of lookback) {
    if (c.high > recentSwingHigh) recentSwingHigh = c.high;
    if (c.low < recentSwingLow) recentSwingLow = c.low;
  }

  const currentBody = Math.abs(currentCandle.close - currentCandle.open);
  const currentRange = currentCandle.high - currentCandle.low;
  const isDisplacement = currentRange > 0 && (currentBody / currentRange) >= 0.55;

  // 1. BULLISH BOS CHECK
  if (currentCandle.close > recentSwingHigh) {
    return [
      `🚨 *BREAK OF STRUCTURE (BOS)*`,
      `Pair: ${symbol}`,
      `Timeframe: ${timeframe}`,
      `Broken swing high: ${formatPrice(recentSwingHigh)}`,
      `Candle close: ${formatPrice(currentCandle.close)}`,
      `Displacement: ${isDisplacement ? "Confirmed" : "Weak"}`,
      `Wick-only break: Rejected`,
      `Reasons for confirmation`,
      `• Completed candle closed above the previous confirmed swing high.`,
      `• Candle body met the displacement threshold.`,
      `• Break passed the balanced confirmation rules.`,
      `Result: Potential bullish structure continuation. Wait for your entry setup; BOS alone is not an automatic buy signal.`
    ].join("\n");
  }

  // 2. BEARISH BOS CHECK
  if (currentCandle.close < recentSwingLow) {
    return [
      `🚨 *BREAK OF STRUCTURE (BOS)*`,
      `Pair: ${symbol}`,
      `Timeframe: ${timeframe}`,
      `Broken swing low: ${formatPrice(recentSwingLow)}`,
      `Candle close: ${formatPrice(currentCandle.close)}`,
      `Displacement: ${isDisplacement ? "Confirmed" : "Weak"}`,
      `Wick-only break: Rejected`,
      `Reasons for confirmation`,
      `• Completed candle closed below the previous confirmed swing low.`,
      `• Candle body met the displacement threshold.`,
      `• Break passed the balanced confirmation rules.`,
      `Result: Potential bearish structure continuation. Wait for your entry setup; BOS alone is not an automatic sell signal.`
    ].join("\n");
  }

  return null;
}
async function runAutoScan(token) {
  while (true) {
    if (autoSubscribers.size > 0) {
      for (const symbol of AUTO_SCAN_SYMBOLS) {
        try {
          // 15-Minute Break of Structure Check
          const bosCandles = await fetchCandles(symbol, "15m", 30, 15 * 60 * 1000);
          const bosAlertText = analyzeBreakOfStructure(bosCandles, symbol, "15 minutes");

          if (bosAlertText) {
            for (const chatId of autoSubscribers) {
              const key = `BOS-${chatId}-${symbol}`;
              const last = lastAlertAt.get(key) || 0;
              
              if (Date.now() - last < ALERT_COOLDOWN_MS) continue;

              lastAlertAt.set(key, Date.now());
              await sendMessage(token, chatId, bosAlertText);
            }
          }
          
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
            "🤖 *Supply & Demand & BOS Bot*\n/analyze <symbol> — Top-Down Analysis\n/bos <symbol> — Check 15m Break of Structure\n/autoon — Enable auto scans\n/autooff — Disable auto scans\n/autostatus — Check auto scans\n/demostatus — Check Bybit Demo connection",);
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

  
  if (command === "/bybitcheck") {
    await sendMessage(
      token,
      chatId,
      "🔎 Testing Bybit Demo market candles..."
    );

    const symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
    const results = [];

    for (const symbol of symbols) {
      try {
        const candles = await fetchBybitCandles(
          symbol,
          "15m",
          10,
          15 * 60 * 1000
        );

        const completed = candles.filter(
          (candle) => candle.closeTime <= Date.now()
        );

        if (completed.length < 2) {
          throw new Error("Not enough completed candles.");
        }

        const last = completed[completed.length - 1];

        results.push(
          `✅ ${symbol}: OK\n` +
          `Last completed 15m close: ${last.close}`
        );
      } catch (error) {
        results.push(
          `❌ ${symbol}: ${error.message}`
        );
      }
    }

    await sendMessage(
      token,
      chatId,
      "📊 Bybit Market Data Test\n\n" +
      results.join("\n\n") +
      "\n\n🔒 Public market data only. No orders placed."
    );

    return;
  }
  
  if (command === "/demostatus") {
    await sendMessage(
      token,
      chatId,
      "🔎 Checking Bybit Demo connection... No orders will be placed."
    );

    const status = await getBybitDemoStatus();

    if (!status.ok) {
      await sendMessage(
        token,
        chatId,
        `❌ Bybit Demo check failed:\n${status.message}`
      );
      return;
    }

    await sendMessage(
      token,
      chatId,
      `✅ ${status.message}\nAccount: ${status.accountType}\nUSDT wallet balance: ${status.walletBalance}\n\n🔒 Read-only check. No trades placed.`
    );
    return;
  }
  
  
  // On-demand manual BOS test command
  

if (text === "/bos" || text.startsWith("/bos ")) {
  const symbolInput = text.split(/\s+/)[1] || "BTCUSDT";
  const symbol = symbolInput.toUpperCase();

  const allowedSymbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];

  if (!allowedSymbols.includes(symbol)) {
    await sendMessage(
      token,
      chatId,
      "Unsupported symbol. Use /bos BTCUSDT, /bos ETHUSDT, or /bos SOLUSDT."
    );
    return;
  }

  try {
    await sendMessage(
      token,
      chatId,
      `Checking confirmed 15m BOS for ${symbol}...`
    );

    const candles = await fetchCandles(
      symbol,
      "15m",
      100,
      15 * 60 * 1000
    );

    if (!Array.isArray(candles) || candles.length < 25) {
      await sendMessage(
        token,
        chatId,
        `BOS check unavailable for ${symbol}: insufficient candle data.`
      );
      return;
    }

    // Assumes fetchCandles includes the currently forming candle.
    const completedCandles = candles.slice(0, -1);

    const result = detectBreakOfStructure(completedCandles, {
      pivotStrength: 3,
      atrPeriod: 14,
      displacementAtr: 0.6
    });

    if (!result || result.direction === "NONE") {
      await sendMessage(
        token,
        chatId,
        `NO CONFIRMED BOS — ${symbol}\n` +
        `Timeframe: 15m\n` +
        `Reason: ${result?.reason || "No valid structure break."}`
      );
      return;
    }

    const direction =
      result.direction === "BULLISH"
        ? "BULLISH BOS"
        : "BEARISH BOS";

    const level = Number.isFinite(result.level)
      ? result.level.toLocaleString("en-US", {
          maximumFractionDigits: 8
        })
      : "Unavailable";

    await sendMessage(
      token,
      chatId,
      `${direction}\n` +
      `Pair: ${symbol}\n` +
      `Timeframe: 15m\n` +
      `Broken swing level: ${level}\n` +
      `Reason: ${result.reason}\n\n` +
      `Signals only — no trade was placed.`
    );
  } catch (error) {
    console.error(`/bos failed for ${symbol}:`, error);

    await sendMessage(
      token,
      chatId,
      `BOS check failed for ${symbol}. Please try again later.`
    );
  }

  return;
}

  

  if (command === "/analyze" && args.length >= 1) {
    const symbol = args[0].toUpperCase();
    await sendMessage(
      token,
      chatId,
      `🔍 Running S&D Multi-TF Analysis on ${symbol}...`,
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
        `🎯 Entry: ${formatPrice(result.entry)}`,
        `🟢 TP: ${formatPrice(result.takeProfit)}`,
        `🔴 SL: ${formatPrice(result.stopLoss)}`,
        ``,
        `🔍 *Institutional Diagnostics:*`,
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
            "⚠️ The signal was sent, but monitoring could not be started. Check the console.",
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
  console.info("Multi-Timeframe S&D & BOS Bot running.");
  await loadMonitorState();
  void runMonitorLoop(token);
  await loadSubscribers();
  void runAutoScan(token);
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

async function getBybitDemoStatus() {
  const apiKey = process.env.BYBIT_API_KEY;
  const apiSecret = process.env.BYBIT_API_SECRET;

  if (process.env.BYBIT_DEMO_TRADING !== "true") {
    return {
      ok: false,
      message: "Demo mode is not enabled. Set BYBIT_DEMO_TRADING=true in FadeHost."
    };
  }

  if (!apiKey || !apiSecret) {
    return {
      ok: false,
      message: "Missing BYBIT_API_KEY or BYBIT_API_SECRET in FadeHost."
    };
  }

  const timestamp = Date.now().toString();
  const query = "accountType=UNIFIED&coin=USDT";
  const signaturePayload =
    timestamp + apiKey + BYBIT_RECV_WINDOW + query;

  const signature = createHmac("sha256", apiSecret)
    .update(signaturePayload)
    .digest("hex");

  try {
    const response = await fetch(
      `${BYBIT_DEMO_API}/v5/account/wallet-balance?${query}`,
      {
        method: "GET",
        headers: {
          "X-BAPI-API-KEY": apiKey,
          "X-BAPI-TIMESTAMP": timestamp,
          "X-BAPI-RECV-WINDOW": BYBIT_RECV_WINDOW,
          "X-BAPI-SIGN": signature
        },
        signal: AbortSignal.timeout(10000)
      }
    );

    const data = await response.json();

    if (!response.ok || data.retCode !== 0) {
      return {
        ok: false,
        message: `Bybit error ${data.retCode ?? response.status}: ${
          data.retMsg ?? response.statusText
        }`
      };
    }

    const account = data.result?.list?.[0];
    const usdt = account?.coin?.find(
      coin => coin.coin === "USDT"
    );

    return {
      ok: true,
      accountType: account?.accountType ?? "Unknown",
      walletBalance: usdt?.walletBalance ?? "0",
      message: "Connected to Bybit Demo successfully."
    };
  } catch (error) {
    return {
      ok: false,
      message: `Connection failed: ${error.message}`
    };
  }
}


// Automatic execution is deliberately OFF until /tradeon is used in Telegram.
let autoTradingEnabled = false;
const lastExecutedSignalAt = new Map();
const TRADE_LEVERAGE = 25;
const MAX_MARGIN_USDT = 5;
const MAX_WALLET_RISK_FRACTION = 0.10;
const EXECUTION_COOLDOWN_MS = 4 * 60 * 60 * 1000;

function bybitSignature(timestamp, apiKey, recvWindow, payload, secret) {
  return createHmac("sha256", secret)
    .update(timestamp + apiKey + recvWindow + payload)
    .digest("hex");
}

async function bybitPrivateRequest(method, path, paramsOrBody = {}) {
  const apiKey = process.env.BYBIT_API_KEY;
  const apiSecret = process.env.BYBIT_API_SECRET;
  if (process.env.BYBIT_DEMO_TRADING !== "true" || !apiKey || !apiSecret) {
    throw new Error("Bybit Demo mode or API credentials are not configured.");
  }
  const timestamp = Date.now().toString();
  const query = method === "GET"
    ? new URLSearchParams(Object.entries(paramsOrBody).sort(([a], [b]) => a.localeCompare(b))).toString()
    : "";
  const bodyText = method === "GET" ? "" : JSON.stringify(paramsOrBody);
  const payload = method === "GET" ? query : bodyText;
  const signature = bybitSignature(timestamp, apiKey, BYBIT_RECV_WINDOW, payload, apiSecret);
  const url = `${BYBIT_DEMO_API}${path}${query ? `?${query}` : ""}`;
  const response = await fetch(url, {
    method,
    headers: {
      "X-BAPI-API-KEY": apiKey,
      "X-BAPI-TIMESTAMP": timestamp,
      "X-BAPI-RECV-WINDOW": BYBIT_RECV_WINDOW,
      "X-BAPI-SIGN": signature,
      "X-BAPI-SIGN-TYPE": "2",
      ...(method === "GET" ? {} : { "Content-Type": "application/json" }),
    },
    ...(method === "GET" ? {} : { body: bodyText }),
    signal: AbortSignal.timeout(15000),
  });
  const data = await response.json();
  const unchangedLeverage = path === "/v5/position/set-leverage" && data.retCode === 110043;
  if (!response.ok || (data.retCode !== 0 && !unchangedLeverage)) {
    throw new Error(`Bybit ${data.retCode ?? response.status}: ${data.retMsg ?? response.statusText}`);
  }
  return data.result;
}

function decimalsFromStep(step) {
  const text = String(step);
  return text.includes(".") ? text.split(".")[1].replace(/0+$/, "").length : 0;
}

async function getOpenBybitPosition(symbol) {
  const result = await bybitPrivateRequest("GET", "/v5/position/list", {
    category: "linear", symbol, settleCoin: "USDT",
  });
  return (result?.list || []).find(position => Number(position.size) > 0) || null;
}

async function executeDemoTrade(signal) {
  if (!autoTradingEnabled) return { skipped: true, message: "Automatic trading is OFF." };
  if (!BYBIT_ALLOWED_SYMBOLS.has(signal.symbol)) return { skipped: true, message: "Symbol is not on the allowed list." };
  if (!(["LONG", "SHORT"].includes(signal.direction))) return { skipped: true, message: "Signal is not a trade." };
  const entry = Number(signal.entry);
  const stopLoss = Number(signal.stopLoss);
  const takeProfit = Number(signal.takeProfit);
  if (![entry, stopLoss, takeProfit].every(Number.isFinite) || entry <= 0 || stopLoss <= 0 || takeProfit <= 0 ||
      (signal.direction === "LONG" && !(stopLoss < entry && takeProfit > entry)) ||
      (signal.direction === "SHORT" && !(takeProfit < entry && stopLoss > entry))) {
    return { skipped: true, message: "Signal entry/SL/TP failed validation." };
  }

  const cooldownKey = `${signal.symbol}-${signal.direction}`;
  if (Date.now() - (lastExecutedSignalAt.get(cooldownKey) || 0) < EXECUTION_COOLDOWN_MS) {
    return { skipped: true, message: "Same-direction signal is in the 4-hour execution cooldown." };
  }
  const existing = await getOpenBybitPosition(signal.symbol);
  if (existing) return { skipped: true, message: `Position already open for ${signal.symbol}; no duplicate order placed.` };

  const status = await getBybitDemoStatus();
  if (!status.ok) throw new Error(status.message);
  const walletBalance = Number(status.walletBalance);
  const marginUsdt = Math.min(MAX_MARGIN_USDT, walletBalance * MAX_WALLET_RISK_FRACTION);
  if (!Number.isFinite(walletBalance) || marginUsdt <= 0) throw new Error("Demo wallet balance is not sufficient.");

  const [tickerResult, instrumentResult] = await Promise.all([
    bybitPrivateRequest("GET", "/v5/market/tickers", { category: "linear", symbol: signal.symbol }),
    bybitPrivateRequest("GET", "/v5/market/instruments-info", { category: "linear", symbol: signal.symbol }),
  ]);
  const lastPrice = Number(tickerResult?.list?.[0]?.lastPrice);
  const instrument = instrumentResult?.list?.[0];
  const qtyStep = Number(instrument?.lotSizeFilter?.qtyStep);
  const minQty = Number(instrument?.lotSizeFilter?.minOrderQty);
  const minNotional = Number(instrument?.lotSizeFilter?.minNotionalValue || 0);
  const tickSize = Number(instrument?.priceFilter?.tickSize);
  if (![lastPrice, qtyStep, minQty, tickSize].every(v => Number.isFinite(v) && v > 0)) {
    throw new Error("Could not validate Bybit price/quantity rules.");
  }
  const notional = marginUsdt * TRADE_LEVERAGE;
  const rawQty = notional / lastPrice;
  const qty = Math.floor(rawQty / qtyStep) * qtyStep;
  const qtyText = qty.toFixed(decimalsFromStep(qtyStep));
  if (qty < minQty || qty * lastPrice < minNotional) {
    return { skipped: true, message: `Safe size is below Bybit minimum for ${signal.symbol}; no order placed.` };
  }
  const priceDecimals = decimalsFromStep(tickSize);
  const tp = (Math.round(takeProfit / tickSize) * tickSize).toFixed(priceDecimals);
  const sl = (Math.round(stopLoss / tickSize) * tickSize).toFixed(priceDecimals);
  if ((signal.direction === "LONG" && !(Number(sl) < lastPrice && Number(tp) > lastPrice)) ||
      (signal.direction === "SHORT" && !(Number(tp) < lastPrice && Number(sl) > lastPrice))) {
    return { skipped: true, message: "Live Demo price moved past the signal SL/TP; no order placed." };
  }

  await bybitPrivateRequest("POST", "/v5/position/set-leverage", {
    category: "linear", symbol: signal.symbol,
    buyLeverage: String(TRADE_LEVERAGE), sellLeverage: String(TRADE_LEVERAGE),
  });
  const order = await bybitPrivateRequest("POST", "/v5/order/create", {
    category: "linear", symbol: signal.symbol,
    side: signal.direction === "LONG" ? "Buy" : "Sell",
    orderType: "Market", qty: qtyText, positionIdx: 0,
    tpslMode: "Full", takeProfit: tp, stopLoss: sl,
    tpTriggerBy: "MarkPrice", slTriggerBy: "MarkPrice",
    tpOrderType: "Market", slOrderType: "Market",
  });
  lastExecutedSignalAt.set(cooldownKey, Date.now());
  return { skipped: false, orderId: order?.orderId || "accepted", qty: qtyText, leverage: TRADE_LEVERAGE, marginUsdt: marginUsdt.toFixed(2), tp, sl };
}


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
  const base = normalized.replace(/USDT$\vert{}USD$/, "");
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


const BYBIT_MARKET_API = "https://api-demo.bybit.com";
const BYBIT_ALLOWED_SYMBOLS = new Set([
  "BTCUSDT",
  "ETHUSDT",
  "SOLUSDT",
]);

function parseBybitCandles(payload, intervalMs) {
  const rows = payload?.result?.list;

  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error("Bybit returned no candle data.");
  }

  return rows
    .map((row) => ({
      startTime: Number(row[0]),
      closeTime: Number(row[0]) + intervalMs,
      open: Number(row[1]),
      high: Number(row[2]),
      low: Number(row[3]),
      close: Number(row[4]),
    }))
    .filter((candle) =>
      Number.isFinite(candle.startTime) &&
      Number.isFinite(candle.open) &&
      Number.isFinite(candle.high) &&
      Number.isFinite(candle.low) &&
      Number.isFinite(candle.close)
    )
    .sort((a, b) => a.startTime - b.startTime);
}

async function fetchBybitCandles(
  symbol,
  interval,
  count,
  intervalMs
) {
  const normalized = symbol?.trim().toUpperCase();

  if (!BYBIT_ALLOWED_SYMBOLS.has(normalized)) {
    throw new Error("Unsupported Bybit test symbol.");
  }

  const intervalMap = {
    "1d": "D",
    "4h": "240",
    "1h": "60",
    "15m": "15",
    "5m": "5",
  };

  const bybitInterval = intervalMap[interval];

  if (!bybitInterval) {
    throw new Error("Unsupported Bybit candle interval.");
  }

  const url = new URL(
    "/v5/market/kline",
    BYBIT_MARKET_API
  );

  url.searchParams.set("category", "linear");
  url.searchParams.set("symbol", normalized);
  url.searchParams.set("interval", bybitInterval);
  url.searchParams.set("limit", String(
    Math.min(1000, Math.max(1, Math.floor(count)))
  ));

  const response = await fetch(url, {
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(
      `Bybit market-data HTTP error: ${response.status}`
    );
  }

  const payload = await response.json();

  if (payload.retCode !== 0) {
    throw new Error(
      `Bybit market-data error ${payload.retCode}: ${payload.retMsg}`
    );
  }

  const candles = parseBybitCandles(payload, intervalMs);

  if (candles.length < 3) {
    throw new Error("Insufficient Bybit candle data.");
  }

  return candles;
}


function identifySupplyDemandZones(candles) {
  const zones = [];
  for (let i = 2; i < candles.length - 1; i++) {
    const curr = candles[i];
    const next = candles[i + 1];

    if (
      curr.close < curr.open &&
      next.close > next.open &&
      next.close > curr.high
    ) {
      zones.push({ type: "DEMAND", high: curr.high, low: curr.low });
    }
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

function analyzeDailyTrend(dailyCandles) {
  const candles = dailyCandles.slice(0, -1);
  if (candles.length < 10) return "NEUTRAL";

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

  if (recentHigh > previousHigh && recentLow > previousLow && recentClose > previousClose) {
    return "BULLISH";
  }
  if (recentHigh < previousHigh && recentLow < previousLow && recentClose < previousClose) {
    return "BEARISH";
  }
  return "CONSOLIDATION";
}


export /**
 * CRT: use the previous completed 4H candle as the range and the latest
 * completed 15M candle for a liquidity sweep + close back inside confirmation.
 * This function only creates alerts; it never places an order.
 */
export async function analyzeCRT(symbol) {
  const normalized = symbol?.trim().toUpperCase();
  if (!BYBIT_ALLOWED_SYMBOLS.has(normalized)) {
    throw new Error(`Unsupported CRT symbol. Use: ${[...BYBIT_ALLOWED_SYMBOLS].join(", ")}`);
  }

  const now = Date.now();
  const [raw4h, raw15m] = await Promise.all([
    fetchBybitCandles(normalized, "4h", 10, 4 * 60 * 60 * 1000),
    fetchBybitCandles(normalized, "15m", 20, 15 * 60 * 1000),
  ]);
  const candles4h = raw4h.filter(c => c.closeTime <= now);
  const candles15m = raw15m.filter(c => c.closeTime <= now);
  if (candles4h.length < 2 || candles15m.length < 3) {
    throw new Error("Not enough completed candles for CRT analysis.");
  }

  // The latest fully closed 4H candle defines the CRT range.
  const range = candles4h[candles4h.length - 1];
  const signalCandle = candles15m[candles15m.length - 1];
  const rangeHigh = range.high;
  const rangeLow = range.low;
  const sweptHigh = signalCandle.high > rangeHigh && signalCandle.close < rangeHigh;
  const sweptLow = signalCandle.low < rangeLow && signalCandle.close > rangeLow;

  if (!sweptHigh && !sweptLow) {
    return {
      symbol: normalized, direction: "WAIT", rangeHigh, rangeLow,
      entry: signalCandle.close, signalCandleTime: signalCandle.startTime,
      message: `⏳ CRT: No confirmed 15M sweep-and-close-back-inside setup for ${normalized}.
4H range high: ${formatPrice(rangeHigh)}

4H range low: ${formatPrice(rangeLow)}`
    };
  }

  // If one candle somehow sweeps both sides, avoid guessing direction.
  if (sweptHigh && sweptLow) {
    return {
      symbol: normalized, direction: "WAIT", rangeHigh, rangeLow,
      entry: signalCandle.close, signalCandleTime: signalCandle.startTime,
      message: `⏳ CRT: Both sides were swept on the same 15M candle for ${normalized}; no signal.`
    };
  }

  const direction = sweptLow ? "LONG" : "SHORT";
  const entry = signalCandle.close;
  const stopLoss = sweptLow ? signalCandle.low : signalCandle.high;
  const takeProfit = sweptLow ? rangeHigh : rangeLow;
  const risk = Math.abs(entry - stopLoss);
  const reward = Math.abs(takeProfit - entry);
  if (!risk || !reward || reward / risk < 1) {
    return {
      symbol: normalized, direction: "WAIT", rangeHigh, rangeLow, entry,
      signalCandleTime: signalCandle.startTime,
      message: `⏳ CRT: Sweep detected on ${normalized}, but the entry-to-target risk/reward is below 1:1. No signal.`
    };
  }

  const sideText = direction === "LONG" ? "Bullish" : "Bearish";
  const message = [
    `🎯 *CRT ${sideText} SIGNAL — ${normalized}*`,
    `⏱️ Range: previous completed 4H candle`,
    `🔎 Confirmation: completed 15M liquidity sweep + close back inside`,
    `📍 Direction: *${direction}*`,
    `Entry reference: ${formatPrice(entry)}`,
    `Stop-loss reference: ${formatPrice(stopLoss)}`,
    `Take-profit target: ${formatPrice(takeProfit)} (opposite side of 4H range)`,
    `4H range high: ${formatPrice(rangeHigh)}`,
    `4H range low: ${formatPrice(rangeLow)}`,
    `Estimated R:R: ${(reward / risk).toFixed(2)}:1`,
    `⚠️ Signal only — no trade was placed. Verify the setup before acting.`
  ].join("\n");

  return {
    symbol: normalized, direction, entry, stopLoss, takeProfit,
    rangeHigh, rangeLow, signalCandleTime: signalCandle.startTime,
    score: 70, reasons: [
      `Previous completed 4H candle range: ${formatPrice(rangeLow)}–${formatPrice(rangeHigh)}.`,
      sweptLow ? "15M candle swept the 4H low and closed back above it." : "15M candle swept the 4H high and closed back below it.",
      "CRT is alert-only; automatic order execution is not used."
    ],
    message
  };
}

async function performMultiTimeframeAnalysis(symbol) {
  const normalized = symbol?.trim().toUpperCase();

  if (!BYBIT_ALLOWED_SYMBOLS.has(normalized)) {
    throw new Error("Unsupported scalping symbol.");
  }

  const intervalMs15m = 15 * 60 * 1000;
  const intervalMs5m = 5 * 60 * 1000;

  const [contextRaw, entryRaw] = await Promise.all([
    fetchBybitCandles(normalized, "15m", 100, intervalMs15m),
    fetchBybitCandles(normalized, "5m", 100, intervalMs5m),
  ]);

  const now = Date.now();

  const contextCandles = contextRaw.filter(
    (candle) => candle.closeTime <= now
  );
  const entryCandles = entryRaw.filter(
    (candle) => candle.closeTime <= now
  );

  if (contextCandles.length < 20 || entryCandles.length < 20) {
    throw new Error("Not enough completed Bybit candles.");
  }

  const currentPrice = entryCandles[entryCandles.length - 1].close;
  const zones = identifySupplyDemandZones(contextCandles);

  let activePoi = null;

  for (const zone of zones.slice().reverse()) {
    if (
      currentPrice >= zone.low * 0.999 &&
      currentPrice <= zone.high * 1.001
    ) {
      activePoi = zone;
      break;
    }
  }

  const last = entryCandles[entryCandles.length - 1];
  const previous = entryCandles[entryCandles.length - 2];

  const bullishBos = last.close > previous.high;
  const bearishBos = last.close < previous.low;

  let direction = "WAIT";
  let score = 40;
  const reasons = [
    "Market data: Bybit Demo public candles",
    "Context: completed 15m candles",
    "Entry confirmation: completed 5m candles",
  ];

  if (!activePoi) {
    reasons.push("No active 15m supply/demand zone at current price.");
  } else {
    score += 20;
    reasons.push(`Price testing 15m ${activePoi.type} zone.`);
  }

  if (activePoi?.type === "DEMAND" && bullishBos) {
    direction = "LONG";
    score += 30;
    reasons.push("Bullish 5m candle breakout detected.");
  } else if (activePoi?.type === "SUPPLY" && bearishBos) {
    direction = "SHORT";
    score += 30;
    reasons.push("Bearish 5m candle breakout detected.");
  } else {
    reasons.push("No matching 5m confirmation; waiting.");
  }

  let entry = currentPrice;
  let stopLoss = 0;
  let takeProfit = 0;

  if (direction === "LONG" && activePoi) {
    stopLoss = activePoi.low * 0.998;
    const risk = entry - stopLoss;

    if (risk > 0) {
      takeProfit = entry + risk * 2;
    } else {
      direction = "WAIT";
    }
  } else if (direction === "SHORT" && activePoi) {
    stopLoss = activePoi.high * 1.002;
    const risk = stopLoss - entry;

    if (risk > 0) {
      takeProfit = entry - risk * 2;
    } else {
      direction = "WAIT";
    }
  }

  return {
    symbol: normalized,
    direction,
    score,
    entry,
    stopLoss,
    takeProfit,
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
  } catch (error) {
    if (error.code !== "ENOENT") console.error("Could not load monitor state:", error.message);
  }
}

function saveMonitorState() {
  const snapshot = JSON.stringify([...activeMonitors.values()], null, 2);
  persistenceQueue = persistenceQueue.catch(() => {}).then(() => writeFile(MONITOR_FILE, snapshot, "utf8"));
  return persistenceQueue;
}

async function registerMonitor(token, chatId, result) {
  if (!["LONG", "SHORT"].includes(result.direction)) return;
  const entry = Number(result.entry);
  const stopLoss = Number(result.stopLoss);
  const takeProfit = Number(result.takeProfit);

  if (!Number.isFinite(entry) || !Number.isFinite(stopLoss) || !Number.isFinite(takeProfit) || entry <= 0 || stopLoss <= 0 || takeProfit <= 0) return;

  const risk = result.direction === "LONG" ? entry - stopLoss : stopLoss - entry;
  if (risk <= 0) return;

  const existing = [...activeMonitors.values()].find(
    (m) => String(m.chatId) === String(chatId) && m.symbol === result.symbol && m.status === "ACTIVE"
  );
  if (existing) return;

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
}

async function checkOneMonitor(token, monitor) {
  if (monitor.status !== "ACTIVE") return;
  const candles = await fetchCandles(monitor.symbol, "1m", 3, 60 * 1000);
  if (!candles.length) return;
  const candle = candles[candles.length - 1];

  const isLong = monitor.direction === "LONG";
  const hitStop = isLong ? candle.low <= monitor.stopLoss : candle.high >= monitor.stopLoss;
  const hitTarget = isLong ? candle.high >= monitor.takeProfit : candle.low <= monitor.takeProfit;

  if (hitStop && hitTarget) {
    monitor.status = "AMBIGUOUS";
    await saveMonitorState();
    return;
  }
  if (hitStop) {
    monitor.status = "STOP_TOUCHED";
    await saveMonitorState();
    return;
  }
  if (hitTarget) {
    monitor.status = "TARGET_TOUCHED";
    await saveMonitorState();
    return;
  }
}

async function runMonitorLoop(token) {
  while (true) {
    try {
      for (const monitor of activeMonitors.values()) {
        if (monitor.status !== "ACTIVE") continue;
        try { await checkOneMonitor(token, monitor); } catch (e) {}
      }
    } catch (e) {}
    await sleep(15000);
  }
}

async function loadSubscribers() {
  try {
    const saved = JSON.parse(await readFile(SUBSCRIBERS_FILE, "utf8"));
    for (const id of saved) autoSubscribers.add(String(id));
  } catch (error) {
    if (error.code !== "ENOENT") console.error("Could not load subscribers:", error.message);
  }
}

async function saveSubscribers() {
  await writeFile(SUBSCRIBERS_FILE, JSON.stringify([...autoSubscribers]), "utf8");
}

function buildAlertMessage(result) {
  const icon = result.direction === "LONG" ? "📈" : "📉";
  return [
    `🚨 *AUTO ALERT*`,
    `🪙 Pair: ${result.symbol}`,
    `${icon} Signal: ${result.direction} (Confidence:${result.score}%)`,
    `🎯 Entry: ${formatPrice(result.entry)}`,
    `🟢 TP: ${formatPrice(result.takeProfit)}`,
    `🔴 SL: ${formatPrice(result.stopLoss)}`,
    ``,
    ...result.reasons.map((r) => `• ${r}`),
  ].join("\n");
}


export function detectBreakOfStructure(candles, options = {}) {
  const pivotStrength = options.pivotStrength ?? 3;
  const atrPeriod = options.atrPeriod ?? 14;
  const displacementAtr = options.displacementAtr ?? 0.6;

  if (
    !Array.isArray(candles) ||
    !Number.isInteger(pivotStrength) ||
    pivotStrength < 1 ||
    !Number.isInteger(atrPeriod) ||
    atrPeriod < 1 ||
    !Number.isFinite(displacementAtr) ||
    displacementAtr < 0 ||
    candles.length < Math.max(atrPeriod + 2, pivotStrength * 2 + 5)
  ) {
    return {
      direction: "NONE",
      reason: "Not enough valid candles or invalid detector settings."
    };
  }

  const valid = candles.every(c =>
    Number.isFinite(c.open) &&
    Number.isFinite(c.high) &&
    Number.isFinite(c.low) &&
    Number.isFinite(c.close) &&
    c.high >= Math.max(c.open, c.close, c.low) &&
    c.low <= Math.min(c.open, c.close, c.high)
  );

  if (!valid) {
    return {
      direction: "NONE",
      reason: "Invalid candle data."
    };
  }

  // The caller must supply completed candles only.
  const last = candles[candles.length - 1];

  const trueRanges = candles.slice(1).map((c, i) =>
    Math.max(
      c.high - c.low,
      Math.abs(c.high - candles[i].close),
      Math.abs(c.low - candles[i].close)
    )
  );

  const recentTR = trueRanges.slice(-atrPeriod);
  const atr = recentTR.reduce((sum, v) => sum + v, 0) / recentTR.length;
  const body = Math.abs(last.close - last.open);

  if (!(atr > 0)) {
    return { direction: "NONE", reason: "ATR is zero or invalid." };
  }

  let swingHigh = null;
  let swingLow = null;

  // Leave the newest candle as the breakout candidate.
  const lastPivotIndex = candles.length - pivotStrength - 2;

  for (let i = lastPivotIndex; i >= pivotStrength; i--) {
    const candidate = candles[i];
    let isHigh = true;
    let isLow = true;

    for (let j = i - pivotStrength; j <= i + pivotStrength; j++) {
      if (j === i) continue;

      if (candles[j].high >= candidate.high) isHigh = false;
      if (candles[j].low <= candidate.low) isLow = false;
    }

    if (isHigh && !swingHigh) {
      const brokenEarlier = candles
        .slice(i + pivotStrength + 1, candles.length - 1)
        .some(c => c.close > candidate.high);

      if (!brokenEarlier) {
        swingHigh = { index: i, level: candidate.high };
      }
    }

    if (isLow && !swingLow) {
      const brokenEarlier = candles
        .slice(i + pivotStrength + 1, candles.length - 1)
        .some(c => c.close < candidate.low);

      if (!brokenEarlier) {
        swingLow = { index: i, level: candidate.low };
      }
    }

    if (swingHigh && swingLow) break;
  }

  const bullishBreak = swingHigh && last.close > swingHigh.level;
  const bearishBreak = swingLow && last.close < swingLow.level;

  if (bullishBreak && bearishBreak) {
    return {
      direction: "NONE",
      reason: "Conflicting structure breaks."
    };
  }

  if (!bullishBreak && !bearishBreak) {
    const sweptHigh = swingHigh &&
      last.high > swingHigh.level &&
      last.close <= swingHigh.level;

    const sweptLow = swingLow &&
      last.low < swingLow.level &&
      last.close >= swingLow.level;

    return {
      direction: "NONE",
      reason: sweptHigh || sweptLow
        ? "Wick sweep without candle-close confirmation."
        : "No confirmed swing break."
    };
  }

  if (body < atr * displacementAtr) {
    return {
      direction: "NONE",
      reason: "Breakout lacks sufficient ATR displacement."
    };
  }

  if (bullishBreak && last.close <= last.open) {
    return {
      direction: "NONE",
      reason: "Bullish BOS requires a bullish candle."
    };
  }

  if (bearishBreak && last.close >= last.open) {
    return {
      direction: "NONE",
      reason: "Bearish BOS requires a bearish candle."
    };
  }

  const bullish = Boolean(bullishBreak);

  return {
    direction: bullish ? "BULLISH" : "BEARISH",
    level: bullish ? swingHigh.level : swingLow.level,
    candleTime: last.startTime ?? null,
    reason: `Confirmed ${bullish ? "bullish" : "bearish"} BOS: close beyond swing with displacement.`
  };
}


function analyzeBreakOfStructure(candles, symbol, timeframe = "15m") {
  if (!candles || candles.length < 20) return null;

  const completedCandles = candles.slice(0, -1);
  const currentCandle = candles[candles.length - 1];

  let recentSwingHigh = -Infinity;
  let recentSwingLow = Infinity;

  const lookback = completedCandles.slice(-15);
  for (const c of lookback) {
    if (c.high > recentSwingHigh) recentSwingHigh = c.high;
    if (c.low < recentSwingLow) recentSwingLow = c.low;
  }

  const currentBody = Math.abs(currentCandle.close - currentCandle.open);
  const currentRange = currentCandle.high - currentCandle.low;
  const isDisplacement = currentRange > 0 && (currentBody / currentRange) >= 0.55;

  // 1. BULLISH BOS CHECK
  if (currentCandle.close > recentSwingHigh) {
    return [
      `🚨 *BREAK OF STRUCTURE (BOS)*`,
      `Pair: ${symbol}`,
      `Timeframe: ${timeframe}`,
            `Broken swing high: ${formatPrice(recentSwingHigh)}`,
      `Candle close: ${formatPrice(currentCandle.close)}`,
      `Displacement: ${isDisplacement ? "Confirmed" : "Weak"}`,
      `Wick-only break: Rejected`,
      `Reasons for confirmation`,
      `• Completed candle closed above the previous confirmed swing high.`,
      `• Candle body met the displacement threshold.`,
      `• Break passed the balanced confirmation rules.`,
      `Result: Potential bullish structure continuation. Wait for your entry setup; BOS alone is not an automatic buy signal.`
    ].join("\n");
  }

  // 2. BEARISH BOS CHECK
  if (currentCandle.close < recentSwingLow) {
    return [
      `🚨 *BREAK OF STRUCTURE (BOS)*`,
      `Pair: ${symbol}`,
      `Timeframe: ${timeframe}`,
      `Broken swing low: ${formatPrice(recentSwingLow)}`,
      `Candle close: ${formatPrice(currentCandle.close)}`,
      `Displacement: ${isDisplacement ? "Confirmed" : "Weak"}`,
      `Wick-only break: Rejected`,
      `Reasons for confirmation`,
      `• Completed candle closed below the previous confirmed swing low.`,
      `• Candle body met the displacement threshold.`,
      `• Break passed the balanced confirmation rules.`,
      `Result: Potential bearish structure continuation. Wait for your entry setup; BOS alone is not an automatic sell signal.`
    ].join("\n");
  }

  return null;
}
async function runAutoScan(token) {
  while (true) {
    if (autoSubscribers.size > 0) {
      for (const symbol of AUTO_SCAN_SYMBOLS) {
        try {
          // 15-Minute Break of Structure Check
          const bosCandles = await fetchCandles(symbol, "15m", 30, 15 * 60 * 1000);
          const bosAlertText = analyzeBreakOfStructure(bosCandles, symbol, "15 minutes");

          if (bosAlertText) {
            for (const chatId of autoSubscribers) {
              const key = `BOS-${chatId}-${symbol}`;
              const last = lastAlertAt.get(key) || 0;
              
              if (Date.now() - last < ALERT_COOLDOWN_MS) continue;

              lastAlertAt.set(key, Date.now());
              await sendMessage(token, chatId, bosAlertText);
            }
          }
          
          // CRT alerts are independent of the existing S&D strategy and are alert-only.
          try {
            const crtResult = await analyzeCRT(symbol);
            if (crtResult.direction === "LONG" || crtResult.direction === "SHORT") {
              for (const chatId of autoSubscribers) {
                const crtKey = `CRT-${chatId}-${symbol}-${crtResult.signalCandleTime}`;
                if (lastAlertAt.has(crtKey)) continue;
                lastAlertAt.set(crtKey, Date.now());
                await sendMessage(token, chatId, crtResult.message);
              }
            }
          } catch (crtError) {
            console.error(`CRT scan error for ${symbol}:`, crtError.message);
          }

          const result = await performMultiTimeframeAnalysis(symbol, "4h");
          if (result.direction === "WAIT") continue;

          if (autoTradingEnabled) {
            try {
              const execution = await executeDemoTrade(result);
              if (!execution.skipped) {
                for (const chatId of autoSubscribers) {
                  await sendMessage(token, chatId,
                    `🧪 *BYBIT DEMO ORDER ACCEPTED*\n${result.symbol} ${result.direction}\nQty: ${execution.qty}\nLeverage: ${execution.leverage}x\nEstimated margin cap: $${execution.marginUsdt}\nTP: ${execution.tp}\nSL: ${execution.sl}\nOrder ID: ${execution.orderId}\nDemo only; verify position in Bybit.`);
                }
              }
            } catch (executionError) {
              console.error(`Demo order error for ${symbol}:`, executionError.message);
              for (const chatId of autoSubscribers) {
                await sendMessage(token, chatId, `⚠️ Demo order not placed for ${symbol}: ${executionError.message}`);
              }
            }
          }

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
            "🤖 *Supply & Demand, BOS & CRT Bot*\n/analyze <symbol> — Top-Down Analysis\n/crt <symbol> — Check CRT (4H range → 15M confirmation)\n/bos <symbol> — Check 15m Break of Structure\n/autoon — Enable auto scans\n/autooff — Disable auto scans\n/autostatus — Check auto scans\n/demostatus — Check Bybit Demo connection\n/tradeon — Enable Demo auto-trading\n/tradeoff — Disable Demo auto-trading\n/tradestatus — Check trade execution",);
    return;
  }

  if (command === "/tradeon") {
    const status = await getBybitDemoStatus();
    if (!status.ok) {
      await sendMessage(token, chatId, `❌ Cannot enable trading: ${status.message}`);
      return;
    }
    autoTradingEnabled = true;
    await sendMessage(token, chatId,
      `🧪 Bybit DEMO auto-trading is ON.\nAllowed pairs: ${AUTO_SCAN_SYMBOLS.join(", ")}\nLeverage: ${TRADE_LEVERAGE}x\nMargin cap: lesser of $${MAX_MARGIN_USDT} or 10% of wallet per trade.\nNo live endpoint is used. Keep /autoon enabled for scanning. Use /tradeoff to stop new orders.`);
    return;
  }

  if (command === "/tradeoff") {
    autoTradingEnabled = false;
    await sendMessage(token, chatId, "🛑 Demo auto-trading OFF. This stops new orders; it does not close any open position.");
    return;
  }

  if (command === "/tradestatus") {
    await sendMessage(token, chatId,
      `Demo auto-trading: ${autoTradingEnabled ? "ON" : "OFF"}\nPairs: ${AUTO_SCAN_SYMBOLS.join(", ")}\nLeverage: ${TRADE_LEVERAGE}x\nMax margin: lesser of $${MAX_MARGIN_USDT} or 10% of wallet.\nAutomatic trading resets to OFF after every restart.`);
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

  
  if (command === "/bybitcheck") {
    await sendMessage(
      token,
      chatId,
      "🔎 Testing Bybit Demo market candles..."
    );

    const symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
    const results = [];

    for (const symbol of symbols) {
      try {
        const candles = await fetchBybitCandles(
          symbol,
          "15m",
          10,
          15 * 60 * 1000
        );

        const completed = candles.filter(
          (candle) => candle.closeTime <= Date.now()
        );

        if (completed.length < 2) {
          throw new Error("Not enough completed candles.");
        }

        const last = completed[completed.length - 1];

        results.push(
          `✅ ${symbol}: OK\n` +
          `Last completed 15m close: ${last.close}`
        );
      } catch (error) {
        results.push(
          `❌ ${symbol}: ${error.message}`
        );
      }
    }

    await sendMessage(
      token,
      chatId,
      "📊 Bybit Market Data Test\n\n" +
      results.join("\n\n") +
      "\n\n🔒 Public market data only. No orders placed."
    );

    return;
  }
  
  if (command === "/demostatus") {
    await sendMessage(
      token,
      chatId,
      "🔎 Checking Bybit Demo connection... No orders will be placed."
    );

    const status = await getBybitDemoStatus();

    if (!status.ok) {
      await sendMessage(
        token,
        chatId,
        `❌ Bybit Demo check failed:\n${status.message}`
      );
      return;
    }

    await sendMessage(
      token,
      chatId,
      `✅ ${status.message}\nAccount: ${status.accountType}\nUSDT wallet balance: ${status.walletBalance}\n\n🔒 Read-only check. No trades placed.`
    );
    return;
  }
  
  
  // On-demand manual BOS test command
  

if (text === "/bos" || text.startsWith("/bos ")) {
  const symbolInput = text.split(/\s+/)[1] || "BTCUSDT";
  const symbol = symbolInput.toUpperCase();

  const allowedSymbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];

  if (!allowedSymbols.includes(symbol)) {
    await sendMessage(
      token,
      chatId,
      "Unsupported symbol. Use /bos BTCUSDT, /bos ETHUSDT, or /bos SOLUSDT."
    );
    return;
  }

  try {
    await sendMessage(
      token,
      chatId,
      `Checking confirmed 15m BOS for ${symbol}...`
    );

    const candles = await fetchCandles(
      symbol,
      "15m",
      100,
      15 * 60 * 1000
    );

    if (!Array.isArray(candles) || candles.length < 25) {
      await sendMessage(
        token,
        chatId,
        `BOS check unavailable for ${symbol}: insufficient candle data.`
      );
      return;
    }

    // Assumes fetchCandles includes the currently forming candle.
    const completedCandles = candles.slice(0, -1);

    const result = detectBreakOfStructure(completedCandles, {
      pivotStrength: 3,
      atrPeriod: 14,
      displacementAtr: 0.6
    });

    if (!result || result.direction === "NONE") {
      await sendMessage(
        token,
        chatId,
        `NO CONFIRMED BOS — ${symbol}\n` +
        `Timeframe: 15m\n` +
        `Reason: ${result?.reason || "No valid structure break."}`
      );
      return;
    }

    const direction =
      result.direction === "BULLISH"
        ? "BULLISH BOS"
        : "BEARISH BOS";

    const level = Number.isFinite(result.level)
      ? result.level.toLocaleString("en-US", {
          maximumFractionDigits: 8
        })
      : "Unavailable";

    await sendMessage(
      token,
      chatId,
      `${direction}\n` +
      `Pair: ${symbol}\n` +
      `Timeframe: 15m\n` +
      `Broken swing level: ${level}\n` +
      `Reason: ${result.reason}\n\n` +
      `Signals only — no trade was placed.`
    );
  } catch (error) {
    console.error(`/bos failed for ${symbol}:`, error);

    await sendMessage(
      token,
      chatId,
      `BOS check failed for ${symbol}. Please try again later.`
    );
  }

  return;
}

  

  if (command === "/crt" && args.length >= 1) {
    const symbol = args[0].toUpperCase();
    await sendMessage(token, chatId, `🔍 Checking CRT for ${symbol} (4H range → 15M confirmation)...`);
    try {
      const result = await analyzeCRT(symbol);
      await sendMessage(token, chatId, result.message);
    } catch (error) {
      await sendMessage(token, chatId, `⚠️ CRT analysis failed for ${symbol}: ${error.message}`);
    }
    return;
  }

  if (command === "/analyze" && args.length >= 1) {
    const symbol = args[0].toUpperCase();
    await sendMessage(
      token,
      chatId,
      `🔍 Running S&D Multi-TF Analysis on ${symbol}...`,
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
        `🎯 Entry: ${formatPrice(result.entry)}`,
        `🟢 TP: ${formatPrice(result.takeProfit)}`,
        `🔴 SL: ${formatPrice(result.stopLoss)}`,
        ``,
        `🔍 *Institutional Diagnostics:*`,
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
            "⚠️ The signal was sent, but monitoring could not be started. Check the console.",
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
  console.info("Multi-Timeframe S&D & BOS Bot running.");
  await loadMonitorState();
  void runMonitorLoop(token);
  await loadSubscribers();
  void runAutoScan(token);
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
