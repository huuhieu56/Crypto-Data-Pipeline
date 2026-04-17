# =============================================================================
# Minutely Extract DAG - Apache Airflow
# =============================================================================
# Schedule: Mỗi phút (* * * * *)
# Timeout: 50 giây (phải xong trước phút tiếp theo)
#
# Tasks:
#   1. extract_klines: Gọi Binance REST API lấy 1 nến mới nhất / coin
#   2. transform: Spark tính RSI(14), MACD trên dữ liệu 1-min
#   3. load_klines: Ghi vào ClickHouse bảng klines
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
	"execution_timeout": timedelta(minutes=3),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
	dag_id="minutely_extract",
	default_args=default_args,
	description="Incremental minutely ETL for klines features and load",
	schedule="* * * * *",
	start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
	catchup=False,
	max_active_runs=1,
	tags=["etl", "extract", "minutely"],
) as dag:

	project_root = "{{ var.value.get('project_root', '/opt/project') }}"

	extract_klines_task = BashOperator(
		task_id="extract_klines",
		bash_command=(
			f"cd {project_root} && "
			"python -c \"from config.symbols import SYMBOLS; "
			"from scripts.extract import extract_recent_klines; "
			"extract_recent_klines(SYMBOLS)\""
		),
	)

	transform_task = BashOperator(
		task_id="transform",
		bash_command=f"cd {project_root} && python scripts/transform.py",
	)

	load_klines_task = BashOperator(
		task_id="load_klines",
		bash_command=f"cd {project_root} && python scripts/load.py --only klines",
	)

	extract_klines_task >> transform_task >> load_klines_task
