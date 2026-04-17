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
#   5. load_klines: Ghi vào ClickHouse bảng klines
#   6. load_ticker: Ghi vào ClickHouse bảng ticker_24h
#
# Dependencies:
#   pre_extract ──▶ extract_klines ─┬─▶ transform ──▶ load_klines
#                  extract_ticker  ─┴──────────────▶ load_ticker
#
# Note: order_book_snapshot chạy riêng (tần suất 5–15 phút),
#       không thuộc daily batch ETL.
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
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="daily_etl",
    default_args=default_args,
    description="Daily ETL: pre-extract, klines/ticker extract, transform, load",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "daily", "crypto"],
) as dag:

    project_root = "{{ var.value.get('project_root', '/opt/project') }}"

    pre_extract_task = BashOperator(
        task_id="pre_extract",
        bash_command=f"cd {project_root} && python scripts/pre_extract.py",
    )

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

    transform_task = BashOperator(
        task_id="transform",
        bash_command="cd {{ var.value.get('project_root', '/opt/project') }} && python scripts/transform.py --date {{ ds }}",
    )

    load_klines_task = BashOperator(
        task_id="load_klines",
        bash_command=f"cd {project_root} && python scripts/load.py --only klines",
    )

    load_ticker_task = BashOperator(
        task_id="load_ticker",
        bash_command=f"cd {project_root} && python scripts/load.py --only ticker",
    )

    pre_extract_task >> [extract_klines_task, extract_ticker_task]
    extract_klines_task >> transform_task >> load_klines_task
    extract_ticker_task >> load_ticker_task
