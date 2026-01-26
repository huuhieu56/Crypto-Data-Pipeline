# =============================================================================
# Inference Script - Chạy dự báo giá với model LSTM
# =============================================================================
# Chức năng:
#   1. Load model: Đọc file .pth đã train
#   2. Get latest data: Query 60 nến gần nhất từ PostgreSQL
#   3. Predict: Dự báo 60 nến tiếp theo
#   4. Save predictions: Ghi vào bảng predictions
#
# Input:
#   - models/lstm_v1.pth
#   - 60 nến gần nhất từ bảng klines
#
# Output:
#   - 60 predictions/coin ghi vào bảng predictions
#   - Total: 50 coins × 60 = 3,000 records/lần chạy
#
# Sử dụng:
#   python scripts/inference.py --model-version v1
# =============================================================================

# TODO: Import libraries

# TODO: Implement load_model()
# - Load model từ .pth file
# - Move to GPU if available

# TODO: Implement get_latest_data()
# - Query 60 nến gần nhất cho mỗi coin
# - Normalize features

# TODO: Implement predict()
# - Forward pass qua model
# - Denormalize predictions

# TODO: Implement save_predictions()
# - Insert predictions vào PostgreSQL
# - Ghi predicted_at, target_time, predicted_close, model_version

# TODO: Implement main()
