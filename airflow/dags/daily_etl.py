# =============================================================================
# Daily ETL DAG - Apache Airflow
# =============================================================================
# Schedule: 02:00 AM mỗi ngày
# Timeout: 2 giờ
#
# Tasks:
#   1. pre_extract: Self-healing gap detection + recovery (bulk / backfill)
#   2. extract_klines: Download dữ liệu nến mới từ Binance REST API
#   3. extract_ticker: Download thống kê 24h + best bid/ask
#   4. transform: Chạy Spark job tính RSI, MACD
#   5. load_klines: Ghi vào PostgreSQL bảng klines (upsert)
#   6. load_ticker: Ghi vào PostgreSQL bảng ticker_24h
#
# Dependencies:
#   pre_extract ──▶ extract_klines ─┬─▶ transform ──▶ load_klines
#                  extract_ticker  ─┴──────────────▶ load_ticker
#
# Note: order_book_snapshot chạy riêng (tần suất 5–15 phút),
#       không thuộc daily batch ETL.
# =============================================================================

# TODO: Import Airflow libraries

# TODO: Define default_args

# TODO: Define DAG
# - dag_id='daily_etl'
# - schedule='0 2 * * *'
# - catchup=False

# TODO: Define tasks
# - pre_extract_task
# - extract_klines_task
# - extract_ticker_task
# - transform_task
# - load_klines_task
# - load_ticker_task

# TODO: Set task dependencies
