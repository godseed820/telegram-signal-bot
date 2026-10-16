// bos.js

/**
 * Detects a valid Break of Structure (BOS) based on candle swing points.
 * @param {Array} candles - Array of candle objects [{ startTime, open, high, low, close }, ...]
 * @returns {Object|null} - Returns BOS details if triggered, otherwise null
 */
export function detectBreakOfStructure(candles) {
  if (!candles || candles.length < 20) return null;

  // Use completed historical candles to find structural swing points
  const completedCandles = candles.slice(0, -1);
  const currentCandle = candles[candles.length - 1];

  let recentSwingHigh = -Infinity;
  let recentSwingLow = Infinity;

  // Look back at the last 15 completed candles to find local structure levels
  const lookback = completedCandles.slice(-15);
  for (const c of lookback) {
    if (c.high > recentSwingHigh) recentSwingHigh = c.high;
    if (c.low < recentSwingLow) recentSwingLow = c.low;
  }

  // Bullish BOS: Current candle closes cleanly above the recent swing high
  if (currentCandle.close > recentSwingHigh) {
    return {
      type: "BULLISH_BOS",
      level: recentSwingHigh,
      price: currentCandle.close,
      message: `🚀 *BULLISH BREAK OF STRUCTURE (BOS)*\nPrice broke and closed above swing high at *${recentSwingHigh.toFixed(2)}*.\nCurrent Close: *${currentCandle.close.toFixed(2)}*`
    };
  }

  // Bearish BOS: Current candle closes cleanly below the recent swing low
  if (currentCandle.close < recentSwingLow) {
    return {
      type: "BEARISH_BOS",
      level: recentSwingLow,
      price: currentCandle.close,
      message: `🩸 *BEARISH BREAK OF STRUCTURE (BOS)*\nPrice broke and closed below swing low at *${recentSwingLow.toFixed(2)}*.\nCurrent Close: *${currentCandle.close.toFixed(2)}*`
    };
  }

  return null;
}
