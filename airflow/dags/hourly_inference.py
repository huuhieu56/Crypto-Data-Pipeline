# =============================================================================
# Hourly LLM Signal DAG - Apache Airflow
# =============================================================================
# Schedule: Every hour (0 * * * *)
# Task: Generate BUY/SELL/HOLD advisory signals from recent market context.
# =============================================================================

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")


default_args = {
    "owner": "crypto-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=10),
}


with DAG(
    dag_id="hourly_inference",
    default_args=default_args,
    description="Generate hourly LLM advisory signals for crypto symbols",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["llm", "signal", "hourly"],
) as dag:

    project_root = "{{ var.value.get('project_root', '/opt/crypto-pipeline') }}"

    llm_signal = BashOperator(
        task_id="llm_signal",
        bash_command=f"cd {project_root} && python scripts/llm_signal.py",
        execution_timeout=timedelta(minutes=8),
    )

    llm_signal
