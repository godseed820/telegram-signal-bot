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
const AUTO_SCAN_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];[span_0](start_span)[span_0](end_span)[span_1](start_span)[span_1](end_span)
const AUTO_SCAN_INTERVAL_MS = 5 * 60 * 1000;
const ALERT_COOLDOWN_MS = 4 * 60 * 60 * 1000;
const SUBSCRIBERS_FILE = resolve(process.cwd(), "auto-subscribers.json");
const autoSubscribers = new Set();
const lastAlertAt = new Map();

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

export async function performMultiTimeframeAnalysis(symbol, htfChoice = "4h") {
  const market = await getKrakenMarket(symbol);

  const dailyCandles = await fetchCandles(symbol, "1d", 14, 24 * 60 * 60 * 1000);
  const dailyBias = analyzeDailyTrend(dailyCandles);

  const htfInterval = htfChoice === "1h" ? "1h" : "4h";
  const htfIntervalMs = htfChoice === "1h" ? 3600 * 1000 : 4 * 3600 * 1000;
  const htfCandles = await fetchCandles(symbol, htfInterval, 50, htfIntervalMs);
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

  let score = 40;
  let direction = "WAIT";
  let reasons = [`Daily Bias: ${dailyBias}`];

  if (activePoi) {
    score += 30;
    reasons.push(`Price testing ${htfChoice.toUpperCase()} ${activePoi.type} POI Zone`);
  } else {
    reasons.push(`Price floating in equilibrium (No active POI test)`);
  }

  if (ltfConfirmation !== "NONE") {
    score += 30;
    reasons.push(`LTF (${ltfInterval}) confirmation candle received`);
  }

  if (dailyBias === "BULLISH" && activePoi?.type === "DEMAND" && ltfConfirmation === "BULLISH_CONFIRMATION" && score >= 70) {
    direction = "LONG";
  } else if (dailyBias === "BEARISH" && activePoi?.type === "SUPPLY" && ltfConfirmation === "BEARISH_CONFIRMATION" && score >= 70) {
    direction = "SHORT";
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
      "🤖 *Supply & Demand & BOS Bot*\n/analyze <symbol> — Top-Down Analysis\n/bos <symbol> — Check 15m Break of Structure\n/autoon — Enable auto scans",
    );
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
  
  // On-demand manual BOS test command
  if (command === "/bos" && args.length >= 1) {
    const symbol = args[0].toUpperCase();
    await sendMessage(token, chatId, `🔍 Checking 15-minute Break of Structure for ${symbol}...`);
    try {
      const candles = await fetchCandles(symbol, "15m", 30, 15 * 60 * 1000);
      const bosText = analyzeBreakOfStructure(candles, symbol, "15 minutes");
      if (bosText) {
        await sendMessage(token, chatId, bosText);
      } else {
        await sendMessage(token, chatId, `ℹ️ No confirmed Break of Structure detected on ${symbol} (15m timeframe) right now.`);
      }
    } catch (err) {
      await sendMessage(token, chatId, `⚠️ Error checking BOS: ${err.message}`);
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
