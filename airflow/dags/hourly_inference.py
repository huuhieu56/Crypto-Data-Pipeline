# =============================================================================
# Hourly Inference DAG - Apache Airflow
# =============================================================================
# Schedule: Mỗi giờ (xx:05)
# Timeout: 15 phút
#
# Tasks:
#   1. load_model: Load model LSTM từ .pth file
#   2. predict: Dự báo 60 nến tiếp theo cho 50 coins
#   3. save_predictions: Ghi 3,000 predictions vào PostgreSQL
#   4. update_actuals: (Delayed 1h) Cập nhật actual_close và error
#
# Dependencies:
#   load_model ──▶ predict ──▶ save_predictions
#                                      │
#                                      ▼ (after 1 hour)
#                              update_actuals
# =============================================================================

# TODO: Import Airflow libraries

# TODO: Define default_args

# TODO: Define DAG
# - dag_id='hourly_inference'
# - schedule='5 * * * *'  # 5 minutes past every hour
# - catchup=False

# TODO: Define tasks
# - load_model_task
# - predict_task
# - save_predictions_task
# - update_actuals_task (với ExternalTaskSensor hoặc delay)

# TODO: Set task dependencies
