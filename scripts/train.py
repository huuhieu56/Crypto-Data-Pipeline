# =============================================================================
# Train Script - Huấn luyện model LSTM với PyTorch
# =============================================================================
# Chức năng:
#   1. Chuẩn bị dữ liệu: Query từ PostgreSQL, tạo sequences
#   2. Xây dựng model: LSTM với 2 layers, hidden_size=128
#   3. Training: Adam optimizer, MSE loss, early stopping
#   4. Evaluation: Tính MAE, RMSE, MAPE trên test set
#   5. Lưu model: Xuất file .pth nếu performance tốt hơn
#
# Model Architecture:
#   - Input: (batch_size, 60, 7) - 60 timesteps, 7 features
#   - LSTM: 2 layers, hidden_size=128, dropout=0.2
#   - Output: (batch_size, 60) - predicted close prices
#
# Sử dụng:
#   python scripts/train.py --epochs 50 --batch-size 64
# =============================================================================

# TODO: Import PyTorch libraries

# TODO: Implement LSTMModel class
# - __init__: Định nghĩa LSTM layers
# - forward: Định nghĩa forward pass

# TODO: Implement CryptoDataset class
# - __init__: Load data từ PostgreSQL
# - __getitem__: Trả về (input_sequence, target_sequence)
# - __len__: Trả về số samples

# TODO: Implement prepare_data()
# - Query dữ liệu từ PostgreSQL
# - Normalize features
# - Split train/val/test (70/15/15)
# - Tạo DataLoader

# TODO: Implement train_epoch()
# - Loop qua batches
# - Forward, backward, update weights

# TODO: Implement evaluate()
# - Tính MAE, RMSE, MAPE

# TODO: Implement main()
# - Parse arguments
# - Prepare data
# - Train loop với early stopping
# - Save best model
