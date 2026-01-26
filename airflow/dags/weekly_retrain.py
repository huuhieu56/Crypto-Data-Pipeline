# =============================================================================
# Weekly Retrain DAG - Apache Airflow
# =============================================================================
# Schedule: 03:00 AM Chủ Nhật hàng tuần
# Timeout: 4 giờ
#
# Tasks:
#   1. prepare_data: Query dữ liệu từ DB, tạo training sequences
#   2. train: Huấn luyện model LSTM (GPU)
#   3. evaluate: Đánh giá model trên test set
#   4. save_model: Lưu model nếu performance tốt hơn
#
# Dependencies:
#   prepare_data ──▶ train ──▶ evaluate ──▶ save_model
# =============================================================================

# TODO: Import Airflow libraries

# TODO: Define default_args

# TODO: Define DAG
# - dag_id='weekly_retrain'
# - schedule='0 3 * * 0'  # Sunday 3 AM
# - catchup=False

# TODO: Define tasks
# - prepare_data_task
# - train_task
# - evaluate_task
# - save_model_task

# TODO: Set task dependencies
