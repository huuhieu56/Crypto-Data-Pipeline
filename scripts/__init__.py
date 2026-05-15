# =============================================================================
# Scripts Package - Crypto Data Pipeline
# =============================================================================
# Module chứa các scripts chính cho ELT:
#   - extract.py: Entry point/orchestrator thu thập dữ liệu từ Binance
#   - extract_modules/: Logic extract theo từng loại dữ liệu
#   - load.py: Transform (ClickHouse SQL) + ghi dữ liệu vào ClickHouse
# =============================================================================
