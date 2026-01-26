# =============================================================================
# Extract Script - Thu thập dữ liệu từ Binance API
# =============================================================================
# Chức năng:
#   1. Extract Symbols: Lấy thông tin 50 coins từ /exchangeInfo
#   2. Extract Klines: Lấy dữ liệu nến 1 phút từ /klines
#   3. Extract Ticker 24h: Lấy thống kê 24h từ /ticker/24hr
#
# Output:
#   - data/raw/symbols.json
#   - data/raw/{SYMBOL}.csv (50 files)
#   - data/raw/ticker_24h.csv
#
# Sử dụng:
#   python scripts/extract.py --start-date 2023-01-01 --end-date 2026-01-01
# =============================================================================

# TODO: Import libraries

# TODO: Implement extract_symbols()
# - Gọi Binance /exchangeInfo API
# - Lọc 50 coins theo danh sách
# - Lưu vào data/raw/symbols.json

# TODO: Implement extract_klines()
# - Gọi Binance /klines API hoặc Binance Data Vision
# - Download dữ liệu 3 năm cho mỗi coin
# - Lưu vào data/raw/{SYMBOL}.csv

# TODO: Implement extract_ticker_24h()
# - Gọi Binance /ticker/24hr API
# - Lưu vào data/raw/ticker_24h.csv

# TODO: Implement main()
