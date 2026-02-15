# =============================================================================
# Train Script - Huấn luyện model LSTM với PyTorch
# =============================================================================
# Chức năng:
#   1. Chuẩn bị dữ liệu: Spark đọc klines 1-min từ PostgreSQL
#   2. Xây dựng model: LSTM 2 layers, hidden_size=128, dropout=0.2
#   3. Training: Adam optimizer, MSE loss, early stopping (patience=10)
#   4. Evaluation: Tính MAE, RMSE, MAPE trên test set
#   5. Lưu model: Xuất file .pth nếu performance tốt hơn
#
# Model Architecture:
#   - Input: (batch_size, 360, 7) — 360 nến 1-min (6h), 7 features
#   - LSTM: 2 layers, hidden_size=128, dropout=0.2
#   - Output: (batch_size, 60) — predicted close price 60 phút tiếp theo
#
# Tham số lấy từ config.config.MODEL_CONFIG.
#
# Sử dụng:
#   python scripts/train.py --epochs 50 --batch-size 64
# =============================================================================

# TODO: Import PyTorch libraries

# TODO: Implement CryptoDataset class
# - __init__: Spark JDBC load klines 1-min từ PostgreSQL
# - __getitem__: Trả về (input_sequence[360 nến], target[60 nến])
# - __len__: ~1.58M samples/coin × 50 coins = ~79M samples

# TODO: Implement prepare_data()
# - Spark JDBC query klines từ PostgreSQL
# - 7 features: open, high, low, close, volume, rsi_14, macd
# - Normalize features (MinMaxScaler)
# - Split train/val/test (70/15/15)
# - Tạo DataLoader (batch_size=64)

# TODO: Implement train_epoch()
# - Loop qua batches
# - Forward, backward, update weights

# TODO: Implement evaluate()
# - Tính MAE, RMSE, MAPE

# TODO: Implement main()
# - Parse arguments
# - Prepare data
# - Train loop với early stopping (patience=10)
# - Save best model to models/
