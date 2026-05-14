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
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (symbol, snapshot_time);

-- 4) Fact: Order book snapshot
CREATE TABLE IF NOT EXISTS crypto_db.order_book_snapshot (
    symbol String,
    timestamp DateTime,
    total_bid_volume Float64,
    total_ask_volume Float64,
    imbalance Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

-- 5) Chat history for AI chatbot
CREATE TABLE IF NOT EXISTS crypto_db.chat_history (
    session_id String,
    message_id String,
    timestamp DateTime DEFAULT now(),
    symbol String,
    role String,
    content String,
    timeframe Nullable(String),
    context_summary String DEFAULT ''
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (session_id, timestamp);

