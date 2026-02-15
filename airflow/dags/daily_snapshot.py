# =============================================================================
# Daily Snapshot DAG - Apache Airflow
# =============================================================================
# Schedule: 00:00 AM mỗi ngày (0 0 * * *)
# Timeout: 15 phút
#
# Tasks:
#   1. extract_ticker: GET /ticker/24hr + /ticker/bookTicker → ticker_24h
#   2. extract_order_book: GET /depth → order_book_snapshot
#   3. load_ticker: Spark JDBC ghi vào bảng ticker_24h
#   4. load_order_book: Spark JDBC ghi vào bảng order_book_snapshot
#
# Dependencies:
#   extract_ticker ──▶ load_ticker
#   extract_order_book ──▶ load_order_book
#   (2 nhánh chạy song song)
#
# Note:
#   - ticker_24h là rolling 24h snapshot → lấy 1 lần/ngày là đủ
#   - order_book_snapshot: 1 snapshot/ngày (đơn giản hóa cho scope đồ án)
#   - Cả 2 API chỉ cần 1-2 requests (trả về tất cả 50 coins cùng lúc)
# =============================================================================

# TODO: Import Airflow libraries

# TODO: Define default_args

# TODO: Define DAG
# - dag_id='daily_snapshot'
# - schedule='0 0 * * *'    # Midnight daily
# - catchup=False

# TODO: Define tasks
# - extract_ticker_task (GET /ticker/24hr + /ticker/bookTicker → CSV)
# - extract_order_book_task (GET /depth cho 50 coins → CSV)
# - load_ticker_task (Spark JDBC → bảng ticker_24h)
# - load_order_book_task (Spark JDBC → bảng order_book_snapshot)

# TODO: Set task dependencies
# extract_ticker >> load_ticker
# extract_order_book >> load_order_book
