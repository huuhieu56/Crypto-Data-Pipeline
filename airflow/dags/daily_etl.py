# =============================================================================
# Daily ETL DAG - Apache Airflow
# =============================================================================
# Schedule: 02:00 AM mỗi ngày
# Timeout: 2 giờ
#
# Tasks:
#   1. extract_klines: Download dữ liệu nến mới từ Binance
#   2. extract_ticker: Download thống kê 24h + best bid/ask
#   3. extract_order_book: Snapshot order book (5–15 phút)
#   3. transform: Chạy Spark job tính RSI, MACD
#   4. load_klines: Ghi vào PostgreSQL bảng klines
#   5. load_ticker: Ghi vào PostgreSQL bảng ticker_24h
#   6. load_order_book: Ghi vào PostgreSQL bảng order_book_snapshot
#
# Dependencies:
#   extract_klines ─┬─▶ transform ──▶ load_klines
#   extract_ticker ─┴──────────────▶ load_ticker
#   extract_order_book ─────────────▶ load_order_book
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
# - extract_order_book_task
# - transform_task
# - load_klines_task
# - load_ticker_task
# - load_order_book_task

# TODO: Set task dependencies
