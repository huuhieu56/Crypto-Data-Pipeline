# =============================================================================
# Scripts Package - Crypto Data Pipeline
# =============================================================================
# Module chứa các scripts chính cho ETL:
#   - extract.py: Entry point/orchestrator thu thập dữ liệu từ Binance
#   - extract_modules/: Logic extract theo từng loại dữ liệu
#   - transform.py: Xử lý dữ liệu với Spark
#   - load.py: Ghi dữ liệu vào ClickHouse
# =============================================================================
