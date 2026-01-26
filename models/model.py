# =============================================================================
# LSTM Model Definition - Crypto Price Prediction
# =============================================================================
# Model Architecture:
#   - Input: (batch_size, 60, 7) - 60 timesteps, 7 features
#   - LSTM: 2 layers, hidden_size=128, dropout=0.2
#   - Output: (batch_size, 60) - predicted close prices for next 60 minutes
#
# Features (7):
#   1. open
#   2. high
#   3. low
#   4. close
#   5. volume
#   6. rsi_14
#   7. macd
# =============================================================================

# TODO: Import PyTorch

# TODO: Define LSTMModel class
# - __init__(self, input_size=7, hidden_size=128, num_layers=2, output_size=60, dropout=0.2)
# - forward(self, x) -> predictions

# TODO: Define utility functions
# - save_model(model, filepath)
# - load_model(filepath)
