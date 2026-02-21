-- =============================================================================
-- Sample Queries - Crypto Data Warehouse
-- =============================================================================
-- Các query mẫu để phân tích dữ liệu và phục vụ Grafana dashboard
-- =============================================================================

-- Query 1 - Lấy thông tin coin kèm giá hiện tại và dự báo mới nhất
SELECT
    s.symbol,
    s.base_asset,
    k.timestamp AS latest_kline_time,
    k.close AS latest_close,
    p.predicted_at,
    p.target_time,
    p.predicted_close,
    t.price_change_pct AS change_24h_pct
FROM symbols s
    JOIN LATERAL (
        SELECT timestamp, close
        FROM klines k
        WHERE
            k.symbol = s.symbol
        ORDER BY timestamp DESC
        LIMIT 1
    ) k ON true
    LEFT JOIN LATERAL (
        SELECT
            predicted_at, target_time, predicted_close
        FROM predictions p
        WHERE
            p.symbol = s.symbol
        ORDER BY predicted_at DESC, step_index DESC
        LIMIT 1
    ) p ON true
    LEFT JOIN LATERAL (
        SELECT price_change_pct
        FROM ticker_24h t
        WHERE
            t.symbol = s.symbol
        ORDER BY snapshot_time DESC
        LIMIT 1
    ) t ON true
ORDER BY s.symbol;

-- Query 2 - Đánh giá model accuracy theo symbol (30 ngày gần nhất)
SELECT
	p.symbol,
	ROUND(AVG(ABS(p.actual_close - p.predicted_close))::numeric, 6) AS mae,
	ROUND(SQRT(AVG(POWER(p.actual_close - p.predicted_close, 2)))::numeric, 6) AS rmse,
	ROUND(
		AVG(ABS(p.actual_close - p.predicted_close) / NULLIF(p.actual_close, 0))::numeric * 100,
		4
	) AS mape_pct,
	ROUND((COUNT(*) FILTER (WHERE ABS(p.error_pct) < 1) * 100.0 / NULLIF(COUNT(*), 0))::numeric, 2) AS acc_lt_1pct
FROM predictions p
WHERE p.actual_close IS NOT NULL
  AND p.target_time >= NOW() - INTERVAL '30 days'
GROUP BY p.symbol
ORDER BY mape_pct ASC NULLS LAST;

-- Query 3 - Top 10 coins theo quote volume 24h snapshot mới nhất
SELECT
	s.symbol,
	s.base_asset,
	t.quote_volume_24h,
	ROUND((t.quote_volume_24h::numeric / 1000000000), 2) AS quote_volume_billion_usd
FROM ticker_24h t
JOIN symbols s ON t.symbol = s.symbol
WHERE t.snapshot_time = (SELECT MAX(snapshot_time) FROM ticker_24h)
ORDER BY t.quote_volume_24h DESC
LIMIT 10;

-- Query 4a - Top gainers 24h (snapshot mới nhất)
SELECT s.symbol, s.base_asset, t.price_change_pct, t.quote_volume_24h
FROM ticker_24h t
    JOIN symbols s ON t.symbol = s.symbol
WHERE
    t.snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM ticker_24h
    )
ORDER BY t.price_change_pct DESC
LIMIT 10;

-- Query 4b - Top losers 24h (snapshot mới nhất)
SELECT s.symbol, s.base_asset, t.price_change_pct, t.quote_volume_24h
FROM ticker_24h t
    JOIN symbols s ON t.symbol = s.symbol
WHERE
    t.snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM ticker_24h
    )
ORDER BY t.price_change_pct ASC
LIMIT 10;

-- Query 5 - Liquidity metric (spread_pct) theo symbol trong 30 ngày
SELECT
	t.symbol,
	DATE_TRUNC('day', t.snapshot_time) AS day,
	ROUND(AVG(t.spread_pct)::numeric, 6) AS avg_spread_pct,
	ROUND(MAX(t.spread_pct)::numeric, 6) AS max_spread_pct,
	ROUND(MIN(t.spread_pct)::numeric, 6) AS min_spread_pct
FROM ticker_24h t
WHERE t.snapshot_time >= NOW() - INTERVAL '30 days'
GROUP BY t.symbol, DATE_TRUNC('day', t.snapshot_time)
ORDER BY day DESC, t.symbol;

-- Query 6 - RSI heatmap cho toàn bộ coins tại nến mới nhất mỗi symbol
SELECT s.symbol, s.base_asset, k.timestamp, k.rsi_14
FROM symbols s
    JOIN LATERAL (
        SELECT timestamp, rsi_14
        FROM klines k
        WHERE
            k.symbol = s.symbol
        ORDER BY timestamp DESC
        LIMIT 1
    ) k ON true
ORDER BY k.rsi_14 DESC;

-- Query 7 - Actual vs Predicted comparison theo symbol (7 ngày)
SELECT p.target_time, p.actual_close, p.predicted_close, p.error_pct, p.model_version
FROM predictions p
WHERE
    p.symbol = 'BTCUSDT'
    AND p.actual_close IS NOT NULL
    AND p.target_time >= NOW() - INTERVAL '7 days'
ORDER BY p.target_time;

-- Query 8 - Order book imbalance theo symbol/time (30 ngày)
SELECT o.symbol, o.timestamp, o.total_bid_volume, o.total_ask_volume, o.imbalance
FROM order_book_snapshot o
WHERE
    o.symbol = 'BTCUSDT'
    AND o.timestamp >= NOW() - INTERVAL '30 days'
ORDER BY o.timestamp;

-- Query 9 - Model performance metrics tổng quan hệ thống (30 ngày)
SELECT
	ROUND(AVG(ABS(actual_close - predicted_close))::numeric, 6) AS mae,
	ROUND(SQRT(AVG(POWER(actual_close - predicted_close, 2)))::numeric, 6) AS rmse,
	ROUND(
		AVG(ABS(actual_close - predicted_close) / NULLIF(actual_close, 0))::numeric * 100,
		4
	) AS mape_pct,
	ROUND((COUNT(*) FILTER (WHERE ABS(error_pct) < 1) * 100.0 / NULLIF(COUNT(*), 0))::numeric, 2) AS acc_lt_1pct
FROM predictions
WHERE actual_close IS NOT NULL
  AND target_time >= NOW() - INTERVAL '30 days';