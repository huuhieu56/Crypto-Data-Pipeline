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
# Lưu ý:
#   - Data Types: Các cột giá (price) và spread của các coin như SHIB, PEPE 
#     có giá trị cực nhỏ (e.g., 6.42e-06). 
#         => BẮT BUỘC dùng kiểu dữ liệu NUMERIC hoặc DECIMAL(20, 10) trong Postgres.
#         => TRÁNH dùng FLOAT/REAL nếu cần độ chính xác tuyệt đối cho tiền tệ.
#   - Đảm bảo khi chạy lại script không bị trùng lặp dữ liệu (Duplicate).
#     Sử dụng logic "UPSERT" (INSERT ON CONFLICT).
#   - Performance: Với bảng klines (~78M records), cần đánh Index cho cột 
#     (symbol, timestamp) để tối ưu tốc độ Query cho Grafana.
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
# - Note: Ép kiểu dữ liệu (Data Type Casting) rõ ràng trước khi ghi vào DB để tránh Postgres hiểu nhầm scientific notation (e.g., 4e-07) là string.
# - Append vào bảng ticker_24h

# TODO: Implement main()
