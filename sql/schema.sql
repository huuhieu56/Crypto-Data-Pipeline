-- =============================================================================
-- Database Schema - Crypto Data Warehouse
-- =============================================================================
-- Star Schema với 1 Dimension table và 3 Fact tables:
--   - symbols (Dimension): Thông tin 50 coins
--   - klines (Fact): Dữ liệu nến 1 phút + technical indicators
--   - ticker_24h (Fact): Thống kê 24h hàng ngày
--   - predictions (Fact): Kết quả dự báo từ LSTM model
--
-- Chạy: psql -U postgres -d crypto_dw -f sql/schema.sql
-- =============================================================================

-- TODO: CREATE TABLE symbols
-- PK: symbol
-- Columns: symbol, base_asset, quote_asset, status, created_at

-- TODO: CREATE TABLE klines
-- PK: (symbol, timestamp)
-- FK: symbol -> symbols.symbol
-- Columns: symbol, timestamp, open, high, low, close, volume, rsi_14, macd, macd_signal

-- TODO: CREATE TABLE ticker_24h
-- PK: (symbol, snapshot_time)
-- FK: symbol -> symbols.symbol
-- Columns: symbol, snapshot_time, price_change, price_change_pct, high_24h, low_24h, volume_24h, quote_volume_24h, trade_count

-- TODO: CREATE TABLE predictions
-- PK: (symbol, predicted_at, step_index)
-- FK: symbol -> symbols.symbol
-- Columns: symbol, predicted_at, step_index, target_time, predicted_close, model_version, actual_close, error_pct

-- TODO: CREATE INDEXES
-- idx_klines_symbol_time
-- idx_predictions_target_time
-- idx_ticker_snapshot_time