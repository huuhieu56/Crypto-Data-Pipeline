# =============================================================================
# Inference Script - Chạy dự báo giá với model LSTM (mỗi giờ)
# =============================================================================
# Chức năng:
#   1. Load model: Đọc file .pth đã train
#   2. Get latest data: Lấy 360 nến 1-min gần nhất (6h lookback)
#   3. Predict: Dự báo giá close 60 nến tiếp theo (1h ahead)
#   4. Save predictions: Ghi vào bảng predictions
#
# Input:
#   - models/lstm_v1.pth
#   - 360 nến 1-min gần nhất từ PostgreSQL (Spark JDBC read)
#
# Output:
#   - 60 predictions/coin ghi vào bảng predictions
#   - Total: 50 coins × 60 = 3,000 records/lần chạy
#
# Schedule: Đầu mỗi giờ (0 * * * *), trigger bởi hourly_inference DAG
#
# Sử dụng:
#   python scripts/inference.py --model-version v1
# =============================================================================

# TODO: Import libraries

# TODO: Implement load_model()
# - Load model từ .pth file
# - Move to GPU if available

# TODO: Implement get_latest_data()
# - Spark JDBC query 360 nến 1-min gần nhất cho mỗi coin
# - 7 features: open, high, low, close, volume, rsi_14, macd
# - Normalize features

# TODO: Implement predict()
# - Forward pass qua model
# - Denormalize predictions
# - Output shape: (50, 60) — 50 coins × 60 phút

# TODO: Implement save_predictions()
# - Insert predictions vào PostgreSQL (Spark JDBC)
# - Ghi predicted_at, target_time (60 phút), predicted_close, model_version

# TODO: Implement main()
