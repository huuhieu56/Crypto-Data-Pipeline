-- =============================================================================
-- Database Schema - Crypto Data Warehouse
-- =============================================================================
-- Star Schema với 1 Dimension table và 4 Fact tables:
--   - symbols (Dimension): Thông tin 50 coins
--   - klines (Fact): Dữ liệu nến 1 phút + technical indicators
--   - ticker_24h (Fact): Thống kê 24h hàng ngày + best bid/ask + spread
--   - order_book_snapshot (Fact): Snapshot order book
--   - predictions (Fact): Kết quả dự báo từ LSTM model (mỗi giờ)
--
-- Chạy: psql -U postgres -d crypto_dw -f sql/schema.sql
-- =============================================================================

-- 1. Bảng Dimensions: Symbols (Thông tin Coin)
CREATE TABLE IF NOT EXISTS symbols (
    symbol VARCHAR(20) PRIMARY KEY,
    base_asset VARCHAR(10),
    quote_asset VARCHAR(10),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Bảng Fact: Klines (Dữ liệu nến & Chỉ báo)
CREATE TABLE IF NOT EXISTS klines (
    symbol VARCHAR(20),
    timestamp TIMESTAMP,
    open NUMERIC(30, 10),
    high NUMERIC(30, 10),
    low NUMERIC(30, 10),
    close NUMERIC(30, 10),
    volume NUMERIC(30, 10),
    quote_volume NUMERIC(30, 10),
    trades INT,
    rsi_14 NUMERIC(10, 4),
    macd NUMERIC(20, 8),
    macd_signal NUMERIC(20, 8),
    
    PRIMARY KEY (symbol, timestamp),
    CONSTRAINT fk_klines_symbol FOREIGN KEY (symbol) REFERENCES symbols(symbol)
);

-- 3. Bảng Fact: Ticker 24h (Thống kê ngày)
CREATE TABLE IF NOT EXISTS ticker_24h (
    symbol VARCHAR(20),
    snapshot_time TIMESTAMP,
    price_change NUMERIC(30, 10),
    price_change_pct NUMERIC(10, 4),
    high_24h NUMERIC(30, 10),
    low_24h NUMERIC(30, 10),
    volume_24h NUMERIC(30, 10),
    quote_volume_24h NUMERIC(30, 10),
    trade_count INT,
    bid_price NUMERIC(30, 10),
    ask_price NUMERIC(30, 10),
    spread_pct NUMERIC(10, 4),
    
    PRIMARY KEY (symbol, snapshot_time),
    CONSTRAINT fk_ticker_symbol FOREIGN KEY (symbol) REFERENCES symbols(symbol)
);

-- 4. Bảng Fact: Order Book Snapshot (Sổ lệnh)
CREATE TABLE IF NOT EXISTS order_book_snapshot (
    symbol VARCHAR(20),
    timestamp TIMESTAMP,
    total_bid_volume NUMERIC(30, 10),
    total_ask_volume NUMERIC(30, 10),
    imbalance NUMERIC(10, 4),
    
    PRIMARY KEY (symbol, timestamp),
    CONSTRAINT fk_ob_symbol FOREIGN KEY (symbol) REFERENCES symbols(symbol)
);

-- 5. Bảng Fact: Predictions (Kết quả dự báo AI)
CREATE TABLE IF NOT EXISTS predictions (
    symbol VARCHAR(20),
    predicted_at TIMESTAMP, -- Thời điểm chạy model (mỗi giờ)
    step_index INT,         -- Phút thứ i trong 60 phút tới (1-60)
    target_time TIMESTAMP,  -- Thời gian thực tế được dự báo
    predicted_close NUMERIC(30, 10),
    model_version VARCHAR(50),
    actual_close NUMERIC(30, 10), -- Điền sau khi có giá thật
    error_pct NUMERIC(10, 4),     -- Điền sau khi có giá thật
    
    PRIMARY KEY (symbol, predicted_at, step_index),
    CONSTRAINT fk_pred_symbol FOREIGN KEY (symbol) REFERENCES symbols(symbol)
);

-- =============================================================================
-- Indexes (Tối ưu tốc độ truy vấn)
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_klines_symbol_time ON klines(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_target_time ON predictions(target_time);
CREATE INDEX IF NOT EXISTS idx_ticker_snapshot_time ON ticker_24h(snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_order_book_snapshot_time ON order_book_snapshot(timestamp DESC);