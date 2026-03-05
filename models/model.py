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

from __future__ import annotations

from pathlib import Path
from typing import Any

import torch
import torch.nn as nn

from config.config import MODEL_CONFIG, MODELS_DIR
from utils.logger import get_logger
from utils.exceptions import ModelNotFoundError

logger = get_logger(__name__)


class LSTMModel(nn.Module):
    """Multi-step LSTM for crypto price forecasting.

    Defaults are sourced from ``MODEL_CONFIG`` but can be overridden
    per-instance so the same class works for experimentation.
    """

    def __init__(
        self,
        input_size: int = MODEL_CONFIG["features"],
        hidden_size: int = MODEL_CONFIG["hidden_size"],
        num_layers: int = MODEL_CONFIG["num_layers"],
        output_size: int = MODEL_CONFIG["output_window"],
        dropout: float = MODEL_CONFIG["dropout"],
    ) -> None:
        super().__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Forward pass.

        Args:
            x: (batch, seq_len, input_size)

        Returns:
            (batch, output_size) — predicted close prices.
        """
        # lstm_out: (batch, seq_len, hidden_size)
        lstm_out, _ = self.lstm(x)

        # Take only the last timestep's hidden state
        last_hidden = lstm_out[:, -1, :]  # (batch, hidden_size)
        out = self.dropout(last_hidden)
        out = self.fc(out)  # (batch, output_size)
        return out


# ---------------------------------------------------------------------------
# Save / Load utilities
# ---------------------------------------------------------------------------

def save_model(
    model: LSTMModel,
    scaler: Any,
    filepath: Path | str | None = None,
    metadata: dict | None = None,
) -> Path:
    """Save model state_dict, scaler, and metadata in a single checkpoint.

    Args:
        model: Trained LSTMModel instance.
        scaler: Fitted MinMaxScaler (from data_utils.normalize_data).
        filepath: Target .pth path. Defaults to ``MODELS_DIR / 'lstm_v1.pth'``.
        metadata: Optional dict (e.g. epoch, metrics, model_version).

    Returns:
        Path to the saved checkpoint.
    """
    filepath = Path(filepath) if filepath else MODELS_DIR / "lstm_v1.pth"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    checkpoint = {
        "model_state_dict": model.state_dict(),
        "model_config": {
            "input_size": model.lstm.input_size,
            "hidden_size": model.hidden_size,
            "num_layers": model.num_layers,
            "output_size": model.fc.out_features,
            "dropout": model.dropout.p,
        },
        "scaler": scaler,
        "metadata": metadata or {},
    }
    torch.save(checkpoint, filepath)
    logger.info("Model saved to %s", filepath)
    return filepath


def load_model(
    filepath: Path | str | None = None,
    device: torch.device | str | None = None,
) -> tuple[LSTMModel, Any, dict]:
    """Load a checkpoint and reconstruct LSTMModel.

    Args:
        filepath: Path to .pth file. Defaults to ``MODELS_DIR / 'lstm_v1.pth'``.
        device: Target device ('cpu', 'cuda'). Auto-detects if None.

    Returns:
        (model, scaler, metadata)

    Raises:
        ModelNotFoundError: If the checkpoint file does not exist.
    """
    filepath = Path(filepath) if filepath else MODELS_DIR / "lstm_v1.pth"
    if not filepath.exists():
        raise ModelNotFoundError(f"Model file not found: {filepath}")

    if device is None:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    else:
        device = torch.device(device)

    checkpoint = torch.load(filepath, map_location=device, weights_only=False)

    cfg = checkpoint["model_config"]
    model = LSTMModel(
        input_size=cfg["input_size"],
        hidden_size=cfg["hidden_size"],
        num_layers=cfg["num_layers"],
        output_size=cfg["output_size"],
        dropout=cfg["dropout"],
    )
    model.load_state_dict(checkpoint["model_state_dict"])
    model.to(device)
    model.eval()

    scaler = checkpoint["scaler"]
    metadata = checkpoint.get("metadata", {})

    logger.info(
        "Model loaded from %s (device=%s, metadata=%s)",
        filepath, device, metadata,
    )
    return model, scaler, metadata
