# =============================================================================
# Minutely Extract DAG - Apache Airflow
# =============================================================================
# Schedule: Mỗi phút (* * * * *)
# Timeout: 50 giây (phải xong trước phút tiếp theo)
#
# Tasks:
#   1. extract_klines: Gọi Binance REST API lấy 1 nến mới nhất / coin
#   2. transform: Spark tính RSI(14), MACD trên dữ liệu 1-min
#   3. load_klines: Ghi vào PostgreSQL bảng klines (Spark JDBC upsert)
#
# Dependencies:
#   extract_klines ──▶ transform ──▶ load_klines
#
# Note:
#   - ETL nặng chỉ trong lần chạy đầu tiên (bulk 3 năm × 50 coins ~ 78M rows)
#   - Incremental mỗi phút chỉ xử lý ~50 rows (1 nến/coin) → rất nhẹ
#   - pre_extract (self-healing) chạy riêng 1 lần khi setup, không thuộc DAG này
#   - ticker_24h extract chạy riêng (daily snapshot)
# =============================================================================

# TODO: Import Airflow libraries

# TODO: Define default_args

# TODO: Define DAG
# - dag_id='minutely_extract'
# - schedule='* * * * *'    # Mỗi phút
# - catchup=False
# - max_active_runs=1       # Tránh overlap nếu job trước chưa xong

# TODO: Define tasks
# - extract_klines_task (GET /klines?limit=1 cho 50 coins → 50 API calls)
# - transform_task (Spark: tính RSI-14, MACD trên window dữ liệu)
# - load_klines_task (Spark JDBC upsert vào bảng klines)

# TODO: Set task dependencies
# extract_klines >> transform >> load_klines
