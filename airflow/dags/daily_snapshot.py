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

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
	"owner": "crypto-pipeline",
	"depends_on_past": False,
	"email_on_failure": False,
	"email_on_retry": False,
	"retries": 2,
	"retry_delay": timedelta(minutes=3),
	"execution_timeout": timedelta(minutes=15),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
	dag_id="daily_snapshot",
	default_args=default_args,
	description="Daily market snapshots: ticker_24h and order_book_snapshot",
	schedule="0 0 * * *",
	start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
	catchup=False,
	max_active_runs=1,
	tags=["extract", "snapshot", "daily"],
) as dag:

	project_root = "{{ var.value.get('project_root', '/opt/project') }}"

	extract_ticker_task = BashOperator(
		task_id="extract_ticker",
		bash_command=(
			f"cd {project_root} && "
			"python -c \"from config.symbols import SYMBOLS, SYMBOLS_STATUS; "
			"from scripts.extract import extract_ticker_24h; "
			"trading=[s for s in SYMBOLS if SYMBOLS_STATUS.get(s, 'TRADING')=='TRADING']; "
			"extract_ticker_24h(trading)\""
		),
	)

	extract_order_book_task = BashOperator(
		task_id="extract_order_book",
		bash_command=(
			f"cd {project_root} && "
			"python -c \"from config.symbols import SYMBOLS, SYMBOLS_STATUS; "
			"from scripts.extract import extract_order_book_snapshot; "
			"trading=[s for s in SYMBOLS if SYMBOLS_STATUS.get(s, 'TRADING')=='TRADING']; "
			"extract_order_book_snapshot(trading)\""
		),
	)

	load_ticker_task = BashOperator(
		task_id="load_ticker",
		bash_command=f"cd {project_root} && python scripts/load.py --only ticker",
	)

	load_order_book_task = BashOperator(
		task_id="load_order_book",
		bash_command=f"cd {project_root} && python scripts/load.py --only orderbook",
	)

	extract_ticker_task >> load_ticker_task
	extract_order_book_task >> load_order_book_task
