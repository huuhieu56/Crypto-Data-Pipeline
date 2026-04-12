-- =============================================================================
-- Database Schema - Crypto Data Warehouse (ClickHouse)
-- =============================================================================
-- Star schema with one dimension table and market/advisory fact tables.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS crypto_db;

-- 1) Dimension: Symbols
CREATE TABLE IF NOT EXISTS crypto_db.symbols (
    symbol String,
    base_asset String,
    quote_asset String,
    status String,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol);

-- 2) Fact: Klines (1-minute candles + indicators)
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

-- 3) Fact: Ticker 24h snapshot
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

-- 4) Fact: Order book snapshot
CREATE TABLE IF NOT EXISTS crypto_db.order_book_snapshot (
    symbol String,
    timestamp DateTime,
    total_bid_volume Float64,
    total_ask_volume Float64,
    imbalance Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, timestamp);

-- 5) Fact: LLM advisory signals
CREATE TABLE IF NOT EXISTS crypto_db.llm_signals (
    symbol String,
    generated_at DateTime,

    signal LowCardinality(String),
    confidence UInt8,
    reason String,
    key_risk Nullable(String),

    rsi_14 Nullable(Float64),
    macd_cross LowCardinality(String),
    ob_imbalance Nullable(Float64),
    vol_change_pct Nullable(Float64),
    price_change_pct Nullable(Float64),

    data_window_minutes UInt16 DEFAULT 360,
    trend_6h LowCardinality(String),
    trend_6h_pct Nullable(Float64),

    llm_provider LowCardinality(String),
    model_version String,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(generated_at)
ORDER BY (symbol, generated_at);
