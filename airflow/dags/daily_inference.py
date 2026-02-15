# =============================================================================
# Daily Inference DAG - Apache Airflow
# =============================================================================
# Schedule: 02:30 AM daily (sau daily_etl)
# Timeout: 15 phút
#
# Tasks:
#   1. load_model: Load model LSTM từ .pth file
#   2. predict: Dùng 60 ngày lịch sử → dự báo 7 ngày tiếp theo (50 coins)
#   3. save_predictions: Ghi 350 predictions vào PostgreSQL
#   4. update_actuals: Cập nhật actual_close cho dự báo đã qua
#
# Dependencies:
#   load_model ──▶ predict ──▶ save_predictions ──▶ update_actuals
# =============================================================================

# TODO: Import Airflow libraries

# TODO: Define default_args

# TODO: Define DAG
# - dag_id='daily_inference'
# - schedule='30 2 * * *'  # 02:30 AM daily, sau daily_etl (02:00)
# - catchup=False

# TODO: Define tasks
# - load_model_task
# - predict_task (50 coins × 7 days = 350 records/ngày)
# - save_predictions_task
# - update_actuals_task (cập nhật predictions cũ đã có actual price)

# TODO: Set task dependencies
