# =============================================================================
# Minutely ETL DAG - Apache Airflow
# =============================================================================
# Schedule: Mỗi phút (* * * * *)
# Timeout: 50 giây (phải xong trước phút tiếp theo)
#
# Tasks:
#   1. extract_klines: Gọi Binance REST API lấy 1 nến mới nhất / coin
#   2. extract_ticker: GET /ticker/24hr + /ticker/bookTicker → ticker_24h
#   3. extract_order_book: GET /depth → order_book_snapshot
#   4. transform: Tính RSI(14), MACD trên dữ liệu 1-min
#   5. load_klines: Ghi vào ClickHouse bảng klines
#   6. load_ticker: Ghi vào ClickHouse bảng ticker_24h
#   7. load_order_book: Ghi vào ClickHouse bảng order_book_snapshot
#
# Dependencies:
#   extract_klines ──▶ transform ──▶ load_klines
#   extract_ticker ────────────────▶ load_ticker
#   extract_order_book ────────────▶ load_order_book
#
# Note:
#   - Mini-batch / near-streaming: toàn bộ pipeline chạy mỗi phút
#   - Lần chạy đầu tiên: auto-bootstrap 3 năm data từ Data Vision (~78M rows)
#   - Incremental mỗi phút chỉ xử lý ~50 rows (1 nến/coin) → rất nhẹ
#   - ticker_24h + order_book: mỗi lần chỉ 1–2 API requests (trả về tất cả
#     50 coins cùng lúc) → overhead rất thấp
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
	"retries": 0,
	"execution_timeout": timedelta(minutes=60),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
	dag_id="minutely_etl",
	default_args=default_args,
	description="Mini-batch ETL mỗi phút: klines, ticker_24h, order_book",
	schedule="* * * * *",
	start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
	catchup=False,
	max_active_runs=1,
	tags=["etl", "minutely", "crypto"],
) as dag:

	project_root = "{{ var.value.get('project_root', '/opt/project') }}"

	# --- Extract ---------------------------------------------------------

	extract_klines_task = BashOperator(
		task_id="extract_klines",
		bash_command=(
			f"cd {project_root} && "
			"python -c \"from config.symbols import SYMBOLS; "
			"from scripts.extract import extract_recent_klines; "
			"extract_recent_klines(SYMBOLS)\""
		),
	)

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

	# --- Transform -------------------------------------------------------

	transform_task = BashOperator(
		task_id="transform",
		bash_command=f"cd {project_root} && python scripts/transform.py",
	)

	# --- Load ------------------------------------------------------------

	load_klines_task = BashOperator(
		task_id="load_klines",
		bash_command=f"cd {project_root} && python scripts/load.py --only klines",
	)

	load_ticker_task = BashOperator(
		task_id="load_ticker",
		bash_command=f"cd {project_root} && python scripts/load.py --only ticker",
	)

	load_order_book_task = BashOperator(
		task_id="load_order_book",
		bash_command=f"cd {project_root} && python scripts/load.py --only orderbook",
	)

	# --- Dependencies ----------------------------------------------------

	extract_klines_task >> transform_task >> load_klines_task
	extract_ticker_task >> load_ticker_task
	extract_order_book_task >> load_order_book_task
