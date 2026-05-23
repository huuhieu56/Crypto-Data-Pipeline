# =============================================================================
# Crypto News ETL DAG - Apache Airflow
# =============================================================================
# Schedule: Mỗi 15 phút (*/15 * * * *)
#
# Tasks:
#   1. extract_crypto_news: Fetch crypto news từ GNews API → MinIO (raw)
#   2. transform_crypto_news: Clean text, extract entities → MinIO (processed)
#   3. load_crypto_news: MinIO (processed) → ClickHouse crypto_news
#
# Dependencies:
#   extract_crypto_news ──▶ transform_crypto_news ──▶ load_crypto_news
#
# Note:
#   - Mỗi lần chạy = 1 API request (10 articles)
#   - Free tier: 100 req/day, 15-min interval = 96 req/day → OK
# =============================================================================

from datetime import timedelta

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
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="crypto_news_etl",
    default_args=default_args,
    description="Crypto news ETL: extract từ GNews → transform → load vào ClickHouse",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "gnews", "crypto", "news"],
) as dag:

    project_root = "{{ var.value.get('project_root', '/opt/project') }}"

    extract_news_task = BashOperator(
        task_id="extract_crypto_news",
        bash_command=(
            f"cd {project_root} && "
            "python scripts/extract_modules/extract_crypto_news.py"
        ),
    )

    transform_news_task = BashOperator(
        task_id="transform_crypto_news",
        bash_command=f"cd {project_root} && python scripts/transform.py --only news",
    )

    load_news_task = BashOperator(
        task_id="load_crypto_news",
        bash_command=f"cd {project_root} && python scripts/load.py --only news",
    )

    extract_news_task >> transform_news_task >> load_news_task
