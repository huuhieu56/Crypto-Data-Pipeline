# =============================================================================
# Daily ETL DAG - Apache Airflow
# =============================================================================
# Schedule: 02:00 AM mỗi ngày
# Timeout: 2 giờ
#
# Tasks:
#   1. extract_klines: Download dữ liệu nến mới từ Binance
#   2. extract_ticker: Download thống kê 24h
#   3. transform: Chạy Spark job tính RSI, MACD
#   4. load_klines: Ghi vào PostgreSQL bảng klines
#   5. load_ticker: Ghi vào PostgreSQL bảng ticker_24h
#
# Dependencies:
#   extract_klines ─┬─▶ transform ──▶ load_klines
#   extract_ticker ─┴──────────────▶ load_ticker
# =============================================================================

# TODO: Import Airflow libraries

# TODO: Define default_args

# TODO: Define DAG
# - dag_id='daily_etl'
# - schedule='0 2 * * *'
# - catchup=False

# TODO: Define tasks
# - extract_klines_task
# - extract_ticker_task
# - transform_task
# - load_klines_task
# - load_ticker_task

# TODO: Set task dependencies
