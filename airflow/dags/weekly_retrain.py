# =============================================================================
# Weekly Retrain DAG - Apache Airflow
# =============================================================================
# Schedule: 03:00 AM Chủ Nhật hàng tuần
# Timeout: 4 giờ
#
# Tasks:
#   1. train_model: Huấn luyện model LSTM trên toàn bộ dữ liệu lịch sử
#
# Dependencies:
#   train_model (single task — training script handles all steps internally)
#
# Note:
#   - Train trên toàn bộ dữ liệu 1-min klines lịch sử (~78M rows)
#   - scripts/train.py handles: query DB → create sequences → train → evaluate → save
#   - ~1.58M samples/coin × 50 coins = ~79M training samples
# =============================================================================

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "crypto-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="weekly_retrain",
    default_args=default_args,
    description="Retrain LSTM model hàng tuần trên toàn bộ dữ liệu lịch sử",
    schedule="0 3 * * 0",  # Sunday 3 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "retrain", "weekly"],
) as dag:

    train_model = BashOperator(
        task_id="train_model",
        bash_command=(
            "cd {{ var.value.get('project_root', '/opt/crypto-pipeline') }} && "
            "python scripts/train.py --model-version v1"
        ),
    )

    # Single task — no downstream dependencies needed
    train_model
