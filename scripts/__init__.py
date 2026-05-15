# =============================================================================
# Scripts Package - Crypto Data Pipeline
# =============================================================================
# Module chứa các scripts chính cho ELT (Extract → Load → Transform):
#   - extract.py: Entry point/orchestrator thu thập dữ liệu từ Binance
#   - extract_modules/: Logic extract theo từng loại dữ liệu
#   - load.py: Load raw data vào ClickHouse (chạy trước transform)
#   - transform.py: Tính RSI(14) + MACD(12,26,9) trong ClickHouse SQL
# =============================================================================
