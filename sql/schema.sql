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

INSERT INTO crypto_db.symbols (symbol, base_asset, quote_asset, status)
SELECT symbol, base_asset, quote_asset, status
FROM values(
    'symbol String, base_asset String, quote_asset String, status String',
    ('BTCUSDT', 'BTC', 'USDT', 'TRADING'),
    ('ETHUSDT', 'ETH', 'USDT', 'TRADING'),
    ('BNBUSDT', 'BNB', 'USDT', 'TRADING'),
    ('SOLUSDT', 'SOL', 'USDT', 'TRADING'),
    ('XRPUSDT', 'XRP', 'USDT', 'TRADING'),
    ('DOGEUSDT', 'DOGE', 'USDT', 'TRADING'),
    ('ADAUSDT', 'ADA', 'USDT', 'TRADING'),
    ('TRXUSDT', 'TRX', 'USDT', 'TRADING'),
    ('LINKUSDT', 'LINK', 'USDT', 'TRADING'),
    ('MATICUSDT', 'MATIC', 'USDT', 'BREAK'),
    ('AVAXUSDT', 'AVAX', 'USDT', 'TRADING'),
    ('TONUSDT', 'TON', 'USDT', 'TRADING'),
    ('SHIBUSDT', 'SHIB', 'USDT', 'TRADING'),
    ('XLMUSDT', 'XLM', 'USDT', 'TRADING'),
    ('BCHUSDT', 'BCH', 'USDT', 'TRADING'),
    ('DOTUSDT', 'DOT', 'USDT', 'TRADING'),
    ('UNIUSDT', 'UNI', 'USDT', 'TRADING'),
    ('LTCUSDT', 'LTC', 'USDT', 'TRADING'),
    ('HBARUSDT', 'HBAR', 'USDT', 'TRADING'),
    ('PEPEUSDT', 'PEPE', 'USDT', 'TRADING'),
    ('NEARUSDT', 'NEAR', 'USDT', 'TRADING'),
    ('APTUSDT', 'APT', 'USDT', 'TRADING'),
    ('ICPUSDT', 'ICP', 'USDT', 'TRADING'),
    ('ETCUSDT', 'ETC', 'USDT', 'TRADING'),
    ('STXUSDT', 'STX', 'USDT', 'TRADING'),
    ('RENDERUSDT', 'RENDER', 'USDT', 'TRADING'),
    ('CROUSDT', 'CRO', 'USDT', 'BREAK'),
    ('ATOMUSDT', 'ATOM', 'USDT', 'TRADING'),
    ('VETUSDT', 'VET', 'USDT', 'TRADING'),
    ('ARBUSDT', 'ARB', 'USDT', 'TRADING'),
    ('INJUSDT', 'INJ', 'USDT', 'TRADING'),
    ('IMXUSDT', 'IMX', 'USDT', 'TRADING'),
    ('OPUSDT', 'OP', 'USDT', 'TRADING'),
    ('GRTUSDT', 'GRT', 'USDT', 'TRADING'),
    ('THETAUSDT', 'THETA', 'USDT', 'TRADING'),
    ('FILUSDT', 'FIL', 'USDT', 'TRADING'),
    ('ARUSDT', 'AR', 'USDT', 'TRADING'),
    ('MKRUSDT', 'MKR', 'USDT', 'BREAK'),
    ('WIFUSDT', 'WIF', 'USDT', 'TRADING'),
    ('RUNEUSDT', 'RUNE', 'USDT', 'TRADING'),
    ('FTMUSDT', 'FTM', 'USDT', 'BREAK'),
    ('ALGOUSDT', 'ALGO', 'USDT', 'TRADING'),
    ('FLOWUSDT', 'FLOW', 'USDT', 'TRADING'),
    ('XTZUSDT', 'XTZ', 'USDT', 'TRADING'),
    ('AXSUSDT', 'AXS', 'USDT', 'TRADING'),
    ('SANDUSDT', 'SAND', 'USDT', 'TRADING'),
    ('MANAUSDT', 'MANA', 'USDT', 'TRADING'),
    ('NEOUSDT', 'NEO', 'USDT', 'TRADING'),
    ('EOSUSDT', 'EOS', 'USDT', 'BREAK'),
    ('AAVEUSDT', 'AAVE', 'USDT', 'TRADING')
)
WHERE symbol NOT IN (SELECT symbol FROM crypto_db.symbols);

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
