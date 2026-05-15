-- =============================================================================
-- Transform klines: RSI(14) + MACD(12,26,9) in ClickHouse
-- =============================================================================
-- Reads raw CSV from MinIO via s3(), fetches context rows from ClickHouse
-- for indicator warm-up, computes indicators, and writes processed Parquet
-- to the MinIO processed bucket.
--
-- Parameters (Python-side substitution):
--   {symbol}             : trading pair e.g. BTCUSDT
--   {month}              : YYYY-MM partition to read
--   {watermark_ms}       : epoch ms of last loaded timestamp (0 if bootstrap)
--   {context_rows}       : number of context rows for warm-up
--   {bucket_raw}         : MinIO raw bucket name
--   {bucket_processed}   : MinIO processed bucket name
--   {s3_endpoint}        : MinIO S3 endpoint URL
--   {s3_access_key}      : MinIO access key
--   {s3_secret_key}      : MinIO secret key
-- =============================================================================

INSERT INTO FUNCTION s3(
    '{s3_endpoint}/{bucket_processed}/klines/{symbol}/{month}.parquet',
    '{s3_access_key}', '{s3_secret_key}',
    'Parquet'
)
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

-- 2. New data from MinIO CSV --------------------------------------------------
new_raw AS (
    SELECT
        toInt64(if(open_time > 1000000000000000, intDiv(open_time, 1000), open_time)) AS open_time_ms,
        open, high, low, close, volume, quote_volume, toUInt32(trades) AS trades
    FROM s3(
        '{s3_endpoint}/{bucket_raw}/klines/{symbol}/{month}.csv',
        '{s3_access_key}', '{s3_secret_key}',
        'CSVWithNames',
        'open_time Int64, open Float64, high Float64, low Float64, close Float64,
         volume Float64, close_time Int64, quote_volume Float64, trades Int64,
         taker_buy_base Float64, taker_buy_quote Float64'
    )
    WHERE open_time_ms > {watermark_ms}
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

-- 7. Output only new rows (written to MinIO processed bucket) -----------------
SELECT
    '{symbol}' AS symbol,
    toDateTime(intDiv(open_time_ms, 1000), 'UTC') AS timestamp,
    open, high, low, close, volume, quote_volume, trades,
    rsi_14, macd, macd_signal
FROM with_macd
WHERE open_time_ms > {watermark_ms}
