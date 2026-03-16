"""LSTM model for 1-minute crypto price prediction.

Architecture:
    Input:  (batch, input_window, 7) — 7 OHLCV + indicator features
    LSTM:   2 layers, hidden_size=128, dropout=0.2
    Output: (batch, output_window) — predicted close for N minutes ahead

Each coin trains separate weights: lstm_{symbol}_{version}.pth

Features (7): open, high, low, close, volume, rsi_14, macd
All hyperparameters are read from config.config.MODEL_CONFIG.
"""

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


# --- Custom Loss Function (directional penalty) -----------------------------

class DirectionalLoss(nn.Module):
    """MSE + directional penalty for time-series prediction.

    Standard MSE on normalized data treats a $100 move on BTC ($71K)
    as ~0.001 error — essentially zero.  This loss adds a penalty when
    the model predicts the wrong *direction* of price movement.

    Loss = MSE(pred, target) + weight × DirectionalPenalty

    DirectionalPenalty = fraction of timesteps where
        sign(pred_{t} - pred_{t-1}) ≠ sign(target_{t} - target_{t-1})
    """

    def __init__(self, directional_weight: float = 0.3) -> None:
        super().__init__()
        self.mse = nn.MSELoss()
        self.directional_weight = directional_weight

    def forward(
        self,
        pred: torch.Tensor,
        target: torch.Tensor,
    ) -> torch.Tensor:
        """Compute combined loss.

        Args:
            pred: (batch, output_window) predicted values.
            target: (batch, output_window) ground truth values.
        """
        mse_loss = self.mse(pred, target)

        if self.directional_weight <= 0 or pred.shape[1] < 2:
            return mse_loss

        # Direction: diff along time axis
        pred_diff = pred[:, 1:] - pred[:, :-1]
        target_diff = target[:, 1:] - target[:, :-1]

        # Penalty: fraction of wrong direction predictions
        # sign mismatch → 1, correct → 0
        wrong_dir = (torch.sign(pred_diff) != torch.sign(target_diff)).float()
        dir_penalty = wrong_dir.mean()

        return mse_loss + self.directional_weight * dir_penalty


# --- Save / Load Utilities ---------------------------------------------------

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
