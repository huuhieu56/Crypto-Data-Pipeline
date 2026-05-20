-- =============================================================================
-- Sample Queries - Crypto Data Warehouse (ClickHouse)
-- =============================================================================

-- Query 1 - Snapshot mới nhất theo symbol: close + ticker
SELECT
    s.symbol,
    s.base_asset,
    k.latest_kline_time,
    k.latest_close,
    t.price_change_pct AS change_24h_pct
FROM symbols s
LEFT JOIN (
    SELECT symbol, argMax(open_time, open_time) AS latest_kline_time, argMax(close, open_time) AS latest_close
    FROM klines
    GROUP BY symbol
) k ON s.symbol = k.symbol
LEFT JOIN (
    SELECT symbol, argMax(price_change_pct, snapshot_time) AS price_change_pct
    FROM ticker_24h
    GROUP BY symbol
) t ON s.symbol = t.symbol
ORDER BY s.symbol;

-- Query 2 - Top 10 coins theo quote volume 24h snapshot mới nhất
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

-- Query 3 - Top gainers 24h (latest per symbol)
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

-- Query 4 - Top losers 24h (latest per symbol)
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
    argMax(open_time, open_time) AS latest_time,
    argMax(rsi_14, open_time) AS latest_rsi
FROM klines
GROUP BY symbol
ORDER BY latest_rsi DESC;

-- Query 7 - Live Liquidity Pressure: snapshot mới nhất
SELECT
    symbol,
    timestamp AS updated,
    obi,
    depth_bid_volume AS bid_volume,
    depth_ask_volume AS ask_volume,
    bid_ask_ratio,
    spread_pct,
    best_bid,
    best_ask,
    mid_price,
    nearest_bid_wall_price,
    nearest_bid_wall_volume,
    nearest_ask_wall_price,
    nearest_ask_wall_volume
FROM order_book_snapshot FINAL
WHERE symbol = 'BTCUSDT'
ORDER BY timestamp DESC
LIMIT 1;
