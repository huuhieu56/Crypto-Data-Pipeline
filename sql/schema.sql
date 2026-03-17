-- =============================================================================
-- Database Schema - Crypto Data Warehouse (ClickHouse)
-- =============================================================================
-- Star Schema với 1 Dimension table và 4 Fact tables:
--   - symbols (Dimension): Thông tin 50 coins
--   - klines (Fact): Dữ liệu nến 1 phút + technical indicators
--   - ticker_24h (Fact): Thống kê 24h hàng ngày + best bid/ask + spread
--   - order_book_snapshot (Fact): Snapshot order book
--   - predictions (Fact): Kết quả dự báo từ LSTM model (mỗi giờ)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS crypto_db;

-- 1. Bảng Dimensions: Symbols (Thông tin Coin)
CREATE TABLE IF NOT EXISTS crypto_db.symbols (
    symbol String,
    base_asset String,
    quote_asset String,
    status String,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol);

-- 2. Bảng Fact: Klines (Dữ liệu nến & Chỉ báo)
CREATE TABLE IF NOT EXISTS crypto_db.klines (
    symbol String,
    timestamp DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    quote_volume Float64,
    trades UInt32,
    rsi_14 Nullable(Float64),
    macd Nullable(Float64),
    macd_signal Nullable(Float64)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

-- 3. Bảng Fact: Ticker 24h (Thống kê ngày)
CREATE TABLE IF NOT EXISTS crypto_db.ticker_24h (
    symbol String,
    snapshot_time DateTime,
    price_change Float64,
    price_change_pct Float64,
    high_24h Float64,
    low_24h Float64,
    volume_24h Float64,
    quote_volume_24h Float64,
    trade_count UInt32,
    bid_price Float64,
    ask_price Float64,
    spread_pct Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, snapshot_time);

-- 4. Bảng Fact: Order Book Snapshot (Sổ lệnh)
CREATE TABLE IF NOT EXISTS crypto_db.order_book_snapshot (
    symbol String,
    timestamp DateTime,
    total_bid_volume Float64,
    total_ask_volume Float64,
    imbalance Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, timestamp);

-- 5. Bảng Fact: Predictions (Kết quả dự báo AI)
CREATE TABLE IF NOT EXISTS crypto_db.predictions (
    symbol String,
    predicted_at DateTime,
    step_index UInt32,
    target_time DateTime,
    predicted_close Float64,
    model_version String,
    actual_close Nullable(Float64),
    error_pct Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, predicted_at, step_index);