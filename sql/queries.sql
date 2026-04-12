-- =============================================================================
-- Sample Queries - Crypto Data Warehouse (ClickHouse, LLM-only)
-- =============================================================================

-- Query 1 - Snapshot mới nhất theo symbol: close + ticker + advisory
SELECT
    s.symbol,
    s.base_asset,
    k.latest_kline_time,
    k.latest_close,
    t.price_change_pct AS change_24h_pct,
    l.signal,
    l.confidence,
    l.reason,
    l.generated_at
FROM symbols s
LEFT JOIN (
    SELECT symbol, argMax(timestamp, timestamp) AS latest_kline_time, argMax(close, timestamp) AS latest_close
    FROM klines
    GROUP BY symbol
) k ON s.symbol = k.symbol
LEFT JOIN (
    SELECT symbol, argMax(price_change_pct, snapshot_time) AS price_change_pct
    FROM ticker_24h
    GROUP BY symbol
) t ON s.symbol = t.symbol
LEFT JOIN (
    SELECT
        symbol,
        argMax(signal, generated_at) AS signal,
        argMax(confidence, generated_at) AS confidence,
        argMax(reason, generated_at) AS reason,
        max(generated_at) AS generated_at
    FROM llm_signals
    GROUP BY symbol
) l ON s.symbol = l.symbol
ORDER BY s.symbol;

-- Query 2 - Phân bố tín hiệu theo giờ trong 7 ngày
SELECT
    toStartOfHour(generated_at) AS hour,
    countIf(signal = 'BUY') AS buy_count,
    countIf(signal = 'SELL') AS sell_count,
    countIf(signal = 'HOLD') AS hold_count,
    round(avg(confidence), 2) AS avg_confidence
FROM llm_signals
WHERE generated_at >= now() - INTERVAL 7 DAY
GROUP BY hour
ORDER BY hour DESC;

-- Query 3 - Top 10 coins theo quote volume 24h snapshot mới nhất
SELECT
    t.symbol,
    t.quote_volume_24h,
    round(t.quote_volume_24h / 1000000000, 2) AS quote_volume_billion_usd
FROM ticker_24h t
INNER JOIN (
    SELECT symbol, max(snapshot_time) AS max_st
    FROM ticker_24h
    GROUP BY symbol
) x ON t.symbol = x.symbol AND t.snapshot_time = x.max_st
ORDER BY t.quote_volume_24h DESC
LIMIT 10;

-- Query 4a - Top gainers 24h (latest per symbol)
SELECT
    t.symbol,
    t.price_change_pct,
    t.quote_volume_24h
FROM ticker_24h t
INNER JOIN (
    SELECT symbol, max(snapshot_time) AS max_st
    FROM ticker_24h
    GROUP BY symbol
) x ON t.symbol = x.symbol AND t.snapshot_time = x.max_st
ORDER BY t.price_change_pct DESC
LIMIT 10;

-- Query 4b - Top losers 24h (latest per symbol)
SELECT
    t.symbol,
    t.price_change_pct,
    t.quote_volume_24h
FROM ticker_24h t
INNER JOIN (
    SELECT symbol, max(snapshot_time) AS max_st
    FROM ticker_24h
    GROUP BY symbol
) x ON t.symbol = x.symbol AND t.snapshot_time = x.max_st
ORDER BY t.price_change_pct ASC
LIMIT 10;

-- Query 5 - Spread trung bình theo ngày trong 30 ngày
SELECT
    symbol,
    toDate(snapshot_time) AS day,
    round(avg(spread_pct), 6) AS avg_spread_pct,
    round(max(spread_pct), 6) AS max_spread_pct,
    round(min(spread_pct), 6) AS min_spread_pct
FROM ticker_24h
WHERE snapshot_time >= now() - INTERVAL 30 DAY
GROUP BY symbol, day
ORDER BY day DESC, symbol;

-- Query 6 - RSI mới nhất mỗi symbol
SELECT
    symbol,
    argMax(timestamp, timestamp) AS latest_time,
    argMax(rsi_14, timestamp) AS latest_rsi
FROM klines
GROUP BY symbol
ORDER BY latest_rsi DESC;

-- Query 7 - LLM advisory history theo symbol (7 ngày)
SELECT
    generated_at,
    signal,
    confidence,
    reason,
    key_risk,
    rsi_14,
    trend_6h,
    trend_6h_pct,
    ob_imbalance,
    price_change_pct,
    vol_change_pct
FROM llm_signals
WHERE symbol = 'BTCUSDT'
  AND generated_at >= now() - INTERVAL 7 DAY
ORDER BY generated_at DESC;

-- Query 8 - Order book imbalance theo symbol/time (30 ngày)
SELECT
    symbol,
    timestamp,
    total_bid_volume,
    total_ask_volume,
    imbalance
FROM order_book_snapshot
WHERE symbol = 'BTCUSDT'
  AND timestamp >= now() - INTERVAL 30 DAY
ORDER BY timestamp DESC;

-- Query 9 - KPI advisory tổng quan (7 ngày)
SELECT
    count() AS total_signals,
    round(avg(confidence), 2) AS avg_confidence,
    round(countIf(signal = 'BUY') * 100.0 / count(), 2) AS buy_ratio_pct,
    round(countIf(signal = 'SELL') * 100.0 / count(), 2) AS sell_ratio_pct,
    round(countIf(signal = 'HOLD') * 100.0 / count(), 2) AS hold_ratio_pct
FROM llm_signals
WHERE generated_at >= now() - INTERVAL 7 DAY;
