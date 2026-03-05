# =============================================================================
# Hourly Inference DAG - Apache Airflow
# =============================================================================
# Schedule: Đầu mỗi giờ (0 * * * *)
# Timeout: 10 phút
#
# Tasks:
#   1. run_inference: Dùng 360 nến 1-min gần nhất → dự báo 60 nến tiếp theo
#   2. update_actuals: Cập nhật actual_close cho dự báo đã qua
#
# Dependencies:
#   run_inference ──▶ update_actuals
#
# Note:
#   - Mỗi giờ predict 1 lần: 50 coins × 60 steps = 3,000 records/lần
#   - Input: 360 nến 1-min gần nhất (6h lookback) với 7 features
#   - Output: 60 nến 1-min tiếp theo (1h ahead) — predicted close price
#   - Scalping/intraday: trader có thể action dựa trên 1h prediction
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
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="hourly_inference",
    default_args=default_args,
    description="Dự báo giá 60 phút mỗi giờ cho 50 coins",
    schedule="0 * * * *",  # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "inference", "hourly"],
) as dag:

    project_root = "{{ var.value.get('project_root', '/opt/crypto-pipeline') }}"

    run_inference = BashOperator(
        task_id="run_inference",
        bash_command=f"cd {project_root} && python scripts/inference.py --model-version v1",
    )

    update_actuals = BashOperator(
        task_id="update_actuals",
        bash_command=f"cd {project_root} && python scripts/update_actuals.py",
    )

    # Task dependencies
    run_inference >> update_actuals
