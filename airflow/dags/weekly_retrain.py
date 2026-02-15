# =============================================================================
# Weekly Retrain DAG - Apache Airflow
# =============================================================================
# Schedule: 03:00 AM Chủ Nhật hàng tuần
# Timeout: 4 giờ
#
# Tasks:
#   1. prepare_data: Query dữ liệu từ DB, tạo training sequences
#   2. train: Huấn luyện model LSTM (GPU) - input 360 nến, output 60 nến
#   3. evaluate: Đánh giá model trên test set
#   4. save_model: Lưu model nếu performance tốt hơn
#
# Dependencies:
#   prepare_data ──▶ train ──▶ evaluate ──▶ save_model
#
# Note:
#   - Train trên toàn bộ dữ liệu 1-min klines lịch sử (~78M rows)
#   - Spark đọc data từ PostgreSQL, tạo sliding windows (360→60)
#   - ~1.58M samples/coin × 50 coins = ~79M training samples
# =============================================================================

# TODO: Import Airflow libraries

# TODO: Define default_args

# TODO: Define DAG
# - dag_id='weekly_retrain'
# - schedule='0 3 * * 0'  # Sunday 3 AM
# - catchup=False

# TODO: Define tasks
# - prepare_data_task (Spark: query klines, tạo sequences 360→60)
# - train_task (PyTorch LSTM: hidden=128, dropout=0.2, epochs=50)
# - evaluate_task (MAE, RMSE, MAPE trên test set)
# - save_model_task (Lưu .pth nếu tốt hơn model hiện tại)

# TODO: Set task dependencies
