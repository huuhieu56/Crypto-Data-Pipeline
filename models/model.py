# =============================================================================
# LSTM Model Definition - Crypto 1-min Price Prediction
# =============================================================================
# Model Architecture:
#   - Input: (batch_size, 360, 7) - 360 nến 1-min (6h), 7 features/nến
#   - LSTM: 2 layers, hidden_size=128, dropout=0.2
#   - Output: (batch_size, 60) - predicted close price cho 60 phút tiếp theo
#
# Features (7): trên nến 1 phút
#   1. open
#   2. high
#   3. low
#   4. close
#   5. volume
#   6. rsi_14  (RSI 14 periods = 14 phút — chỉ báo scalping)
#   7. macd    (MACD 12/26/9 — chỉ báo day trading)
#
# Tham số lấy từ config.config.MODEL_CONFIG (single source of truth).
# =============================================================================

# TODO: Import PyTorch

# TODO: Define LSTMModel class
# - __init__(self, input_size=7, hidden_size=128, num_layers=2, output_size=60, dropout=0.2)
# - forward(self, x) -> predictions

# TODO: Define utility functions
# - save_model(model, filepath)
# - load_model(filepath)
