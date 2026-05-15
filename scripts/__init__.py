# =============================================================================
# Scripts Package - Crypto Data Pipeline
# =============================================================================
# Module chứa các scripts chính cho ELT:
#   - extract.py: Entry point/orchestrator thu thập dữ liệu từ Binance
#   - extract_modules/: Logic extract theo từng loại dữ liệu
#   - transform.py: Tính RSI(14) + MACD(12,26,9) qua ClickHouse SQL
#   - load.py: Ghi dữ liệu vào ClickHouse
# =============================================================================
