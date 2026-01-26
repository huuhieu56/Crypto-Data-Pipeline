# =============================================================================
# Load Script - Ghi dữ liệu vào PostgreSQL
# =============================================================================
# Chức năng:
#   1. Load symbols: Ghi thông tin coins vào bảng symbols
#   2. Load klines: Ghi dữ liệu nến + features vào bảng klines
#   3. Load ticker_24h: Ghi thống kê 24h vào bảng ticker_24h
#
# Input:
#   - data/raw/symbols.json
#   - data/processed/features.parquet
#   - data/raw/ticker_24h.csv
#
# Output: PostgreSQL tables (symbols, klines, ticker_24h)
#
# Sử dụng:
#   python scripts/load.py
# =============================================================================

# TODO: Import libraries (psycopg2, sqlalchemy, pandas)

# TODO: Implement get_db_connection()
# - Tạo connection đến PostgreSQL

# TODO: Implement load_symbols()
# - Đọc symbols.json
# - Insert/Update vào bảng symbols

# TODO: Implement load_klines()
# - Đọc features.parquet
# - Append vào bảng klines

# TODO: Implement load_ticker_24h()
# - Đọc ticker_24h.csv
# - Append vào bảng ticker_24h

# TODO: Implement main()
