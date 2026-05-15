-- =============================================================================
-- Transform klines: RSI(14) + MACD(12,26,9) in ClickHouse (ELT)
-- =============================================================================
-- Runs AFTER load_klines has inserted raw OHLCV into crypto_db.klines.
-- Reads raw rows from klines, fetches context for indicator warm-up,
-- computes RSI + MACD, and INSERTs back into klines (ReplacingMergeTree
-- replaces raw rows with transformed ones).
--
-- Parameters (Python-side substitution):
--   {symbol}         : trading pair e.g. BTCUSDT
--   {month}          : YYYY-MM partition
--   {month_int}      : YYYYMM integer for partition filter
--   {watermark_ms}   : epoch ms of last transformed timestamp (0 = bootstrap)
--   {context_rows}   : number of context rows for warm-up
-- =============================================================================

INSERT INTO crypto_db.klines
WITH
-- 1. Context rows from ClickHouse for indicator warm-up -----------------------
context AS (
    SELECT
        toInt64(toUnixTimestamp(timestamp)) * 1000 AS open_time_ms,
        open, high, low, close, volume, quote_volume, trades
    FROM crypto_db.klines FINAL
    WHERE symbol = '{symbol}'
    ORDER BY timestamp DESC
    LIMIT {context_rows}
),

-- 2. Raw rows from klines (loaded by load_klines, not yet transformed) -----
new_raw AS (
    SELECT
        toInt64(toUnixTimestamp(timestamp)) * 1000 AS open_time_ms,
        open, high, low, close, volume, quote_volume, trades
    FROM crypto_db.klines
    WHERE symbol = '{symbol}'
      AND toYYYYMM(timestamp) = {month_int}
      AND open_time_ms > {watermark_ms}
),

-- 3. Combine context + new data, sorted ---------------------------------------
combined AS (
    SELECT
        open_time_ms,
        open, high, low, close, volume, quote_volume, trades
    FROM (
        SELECT * FROM context
        UNION ALL
        SELECT * FROM new_raw
    )
    ORDER BY open_time_ms
),

-- 4. Compute delta ------------------------------------------------------------
deltas AS (
    SELECT *,
        close - lagInFrame(close, 1) OVER (ORDER BY open_time_ms) AS delta
    FROM combined
),

-- 5. RSI(14) + EMA(12) + EMA(26) ---------------------------------------------
-- RSI: SMA-based rolling(14).mean(), matches Pandas implementation.
--   ffill/bfill/fillna(0) is approximated by coalesce(..., 0); first ~13
--   rows per symbol will be 0 instead of ffill/bfill (acceptable divergence).
-- MACD halflife = -ln(2)/ln(1 - 2/(span+1)):
--   EMA(12): halflife=4.149, EMA(26): halflife=9.006
-- Time unit: row index, matching Pandas row-step EWM even when candles have gaps.
indicators AS (
    SELECT *,
        coalesce(
            100 - (100 / (1 +
                avg(if(delta > 0, delta, 0)) OVER rsi_w /
                nullIf(avg(if(delta < 0, -delta, 0)) OVER rsi_w, 0)
            )),
            0
        ) AS rsi_14,
        exponentialMovingAverage(4.149)(close, ema_idx)
            OVER (ORDER BY ema_idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ema12,
        exponentialMovingAverage(9.006)(close, ema_idx)
            OVER (ORDER BY ema_idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ema26
    FROM (
        SELECT *,
            row_number() OVER (ORDER BY open_time_ms) - 1 AS ema_idx
        FROM deltas
    )
    WINDOW rsi_w AS (ORDER BY open_time_ms ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
),

-- 6. MACD line + signal(9) ----------------------------------------------------
-- EMA(9): halflife = -ln(2)/ln(1-2/10) = 3.106
with_macd AS (
    SELECT *,
        ema12 - ema26 AS macd,
        exponentialMovingAverage(3.106)(ema12 - ema26, ema_idx)
            OVER (ORDER BY ema_idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS macd_signal
    FROM indicators
)

-- 7. Output only new rows (ReplacingMergeTree replaces raw rows) ------------
SELECT
    '{symbol}' AS symbol,
    toDateTime(intDiv(open_time_ms, 1000), 'UTC') AS timestamp,
    open, high, low, close, volume, quote_volume, trades,
    rsi_14, macd, macd_signal
FROM with_macd
WHERE open_time_ms > {watermark_ms}
