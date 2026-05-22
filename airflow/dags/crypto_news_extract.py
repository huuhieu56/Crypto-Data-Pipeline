# =============================================================================
# Crypto News Extract DAG - Apache Airflow
# =============================================================================
# Schedule: Mỗi 15 phút (*/15 * * * *)
#
# Tasks:
#   1. extract_crypto_news: Fetch crypto news từ GNews API
#      → Parse articles → Ghi raw Parquet vào MinIO
#
# Note:
#   - Chỉ làm Extract phase (raw → MinIO)
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
    "execution_timeout": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="crypto_news_extract",
    default_args=default_args,
    description="Extract crypto news từ GNews API mỗi 15 phút",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["extract", "gnews", "crypto", "news"],
) as dag:

    project_root = "{{ var.value.get('project_root', '/opt/project') }}"

    extract_news_task = BashOperator(
        task_id="extract_crypto_news",
        bash_command=(
            f"cd {project_root} && "
            "python -c \"from scripts.extract_modules.extract_crypto_news "
            "import extract_crypto_news; extract_crypto_news()\""
        ),
    )
