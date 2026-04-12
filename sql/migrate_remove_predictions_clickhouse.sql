-- Migration: remove legacy predictions and switch to LLM advisory schema.

DROP TABLE IF EXISTS crypto_db.predictions;
DROP TABLE IF EXISTS crypto_db.llm_signals;

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
