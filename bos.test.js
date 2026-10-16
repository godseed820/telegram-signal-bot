import test from "node:test";
import assert from "node:assert/strict";
import { detectBreakOfStructure } from "./index.js";

function bullishCandles() {
  const candles = [];
  for (let i = 0; i < 30; i++) {
    let candle = { startTime: i, open: 100, close: 100.3, high: 101, low: 99 };
    if (i === 20) candle = { startTime: i, open: 105, close: 106, high: 110, low: 104 };
    if (i >= 21 && i < 29) candle = { startTime: i, open: 104, close: 104.2, high: 105, low: 103 };
    if (i === 29) candle = { startTime: i, open: 108, close: 112, high: 113, low: 107 };
    candles.push(candle);
  }
  return candles;
}

function bearishCandles() {
  const candles = [];
  for (let i = 0; i < 30; i++) {
    let candle = { startTime: i, open: 100, close: 99.7, high: 101, low: 99 };
    if (i === 20) candle = { startTime: i, open: 95, close: 94, high: 96, low: 90 };
    if (i >= 21 && i < 29) candle = { startTime: i, open: 94, close: 93.8, high: 95, low: 92 };
    if (i === 29) candle = { startTime: i, open: 92, close: 88, high: 93, low: 87 };
    candles.push(candle);
  }
  return candles;
}

test("confirms bullish BOS when a completed candle closes above a swing high with displacement", () => {
  const result = detectBreakOfStructure(bullishCandles());
  assert.equal(result.direction, "BULLISH");
  assert.equal(result.level, 110);
});

test("confirms bearish BOS when a completed candle closes below a swing low with displacement", () => {
  const result = detectBreakOfStructure(bearishCandles());
  assert.equal(result.direction, "BEARISH");
  assert.equal(result.level, 90);
});

test("rejects a wick-only break", () => {
  const candles = bullishCandles();
  candles[29] = { startTime: 29, open: 104, close: 109, high: 111, low: 103 };
  const result = detectBreakOfStructure(candles);
  assert.equal(result.direction, "NONE");
  assert.match(result.reason, /wick sweep/i);
});

test("does not confirm BOS when candle data is insufficient", () => {
  const result = detectBreakOfStructure(bullishCandles().slice(0, 10));
  assert.equal(result.direction, "NONE");
  assert.match(result.reason, /Not enough completed candles/i);
});
