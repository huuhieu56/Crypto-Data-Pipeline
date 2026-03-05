# =============================================================================
# Train Script - Huấn luyện model LSTM với PyTorch
# =============================================================================
# Chức năng:
#   1. Chuẩn bị dữ liệu: Query klines 1-min từ PostgreSQL
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

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import time

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

from config.config import MODEL_CONFIG, FEATURE_COLUMNS, MODELS_DIR
from config.symbols import ACTIVE_SYMBOLS
from models.model import LSTMModel, save_model
from utils.data_utils import (
    validate_data,
    normalize_data,
    create_sequences,
)
from utils.db_utils import get_engine
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class CryptoDataset(Dataset):
    """Wraps pre-built (X, y) tensors for DataLoader."""

    def __init__(self, X: np.ndarray, y: np.ndarray) -> None:
        self.X = torch.tensor(X, dtype=torch.float32)
        self.y = torch.tensor(y, dtype=torch.float32)

    def __len__(self) -> int:
        return len(self.X)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        return self.X[idx], self.y[idx]


# ---------------------------------------------------------------------------
# Data Preparation
# ---------------------------------------------------------------------------

def prepare_data(
    symbols: list[str] | None = None,
    batch_size: int = MODEL_CONFIG["batch_size"],
) -> tuple[DataLoader, DataLoader, DataLoader, object]:
    """Query klines from PostgreSQL, build train/val/test DataLoaders.

    Returns:
        (train_loader, val_loader, test_loader, scaler)
    """
    symbols = symbols or ACTIVE_SYMBOLS
    engine = get_engine()

    placeholders = ", ".join(f"'{s}'" for s in symbols)
    feature_str = ", ".join(FEATURE_COLUMNS)

    query = (
        f"SELECT symbol, timestamp, {feature_str} "
        f"FROM klines "
        f"WHERE symbol IN ({placeholders}) "
        f"  AND rsi_14 IS NOT NULL "
        f"  AND macd IS NOT NULL "
        f"ORDER BY symbol, timestamp"
    )

    logger.info("Querying klines for %d symbols...", len(symbols))
    df = pd.read_sql(query, engine)
    logger.info("Loaded %s rows from klines", f"{len(df):,}")

    # Validate
    df = validate_data(df, FEATURE_COLUMNS)
    if len(df) == 0:
        raise ValueError("No valid data after validation")

    # Normalize features
    scaled_df, scaler = normalize_data(df, FEATURE_COLUMNS)

    # Build sequences per symbol, then concatenate
    input_window = MODEL_CONFIG["input_window"]
    output_window = MODEL_CONFIG["output_window"]
    all_X, all_y = [], []

    for symbol in scaled_df["symbol"].unique():
        sym_df = scaled_df[scaled_df["symbol"] == symbol].sort_values("timestamp")
        if len(sym_df) < input_window + output_window:
            logger.warning(
                "%s: only %d rows, need %d — skipping",
                symbol, len(sym_df), input_window + output_window,
            )
            continue
        X, y = create_sequences(
            sym_df, input_window, output_window,
            feature_cols=FEATURE_COLUMNS, target_col="close",
        )
        all_X.append(X)
        all_y.append(y)

    if not all_X:
        raise ValueError("No sequences created — not enough data")

    X_all = np.concatenate(all_X)
    y_all = np.concatenate(all_y)
    logger.info("Total sequences: %s", f"{len(X_all):,}")

    # Split train / val / test
    n = len(X_all)
    train_end = int(n * MODEL_CONFIG["train_ratio"])
    val_end = train_end + int(n * MODEL_CONFIG["val_ratio"])

    train_ds = CryptoDataset(X_all[:train_end], y_all[:train_end])
    val_ds = CryptoDataset(X_all[train_end:val_end], y_all[train_end:val_end])
    test_ds = CryptoDataset(X_all[val_end:], y_all[val_end:])

    logger.info(
        "Split: train=%d, val=%d, test=%d",
        len(train_ds), len(val_ds), len(test_ds),
    )

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=batch_size, shuffle=False)
    test_loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False)

    return train_loader, val_loader, test_loader, scaler


# ---------------------------------------------------------------------------
# Training & Evaluation
# ---------------------------------------------------------------------------

def train_one_epoch(
    model: LSTMModel,
    loader: DataLoader,
    criterion: nn.Module,
    optimizer: torch.optim.Optimizer,
    device: torch.device,
) -> float:
    """Run one training epoch. Returns average loss."""
    model.train()
    total_loss = 0.0
    n_batches = 0

    for X_batch, y_batch in loader:
        X_batch = X_batch.to(device)
        y_batch = y_batch.to(device)

        optimizer.zero_grad()
        predictions = model(X_batch)
        loss = criterion(predictions, y_batch)
        loss.backward()
        optimizer.step()

        total_loss += loss.item()
        n_batches += 1

    return total_loss / max(n_batches, 1)


def evaluate(
    model: LSTMModel,
    loader: DataLoader,
    criterion: nn.Module,
    device: torch.device,
) -> dict[str, float]:
    """Evaluate model on a dataset. Returns dict with loss, MAE, RMSE, MAPE."""
    model.eval()
    total_loss = 0.0
    all_preds, all_targets = [], []

    with torch.no_grad():
        for X_batch, y_batch in loader:
            X_batch = X_batch.to(device)
            y_batch = y_batch.to(device)

            predictions = model(X_batch)
            loss = criterion(predictions, y_batch)
            total_loss += loss.item()

            all_preds.append(predictions.cpu().numpy())
            all_targets.append(y_batch.cpu().numpy())

    preds = np.concatenate(all_preds)
    targets = np.concatenate(all_targets)
    n_batches = max(len(loader), 1)

    mae = float(np.mean(np.abs(preds - targets)))
    rmse = float(np.sqrt(np.mean((preds - targets) ** 2)))
    # MAPE — avoid division by zero
    mask = np.abs(targets) > 1e-8
    mape = float(np.mean(np.abs((preds[mask] - targets[mask]) / targets[mask])) * 100) if mask.any() else 0.0

    return {
        "loss": total_loss / n_batches,
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Train LSTM model for crypto price prediction")
    p.add_argument("--epochs", type=int, default=MODEL_CONFIG["epochs"], help="Number of epochs")
    p.add_argument("--batch-size", type=int, default=MODEL_CONFIG["batch_size"], help="Batch size")
    p.add_argument("--lr", type=float, default=MODEL_CONFIG["learning_rate"], help="Learning rate")
    p.add_argument("--symbols", nargs="+", default=None, help="Override symbols (e.g. BTCUSDT ETHUSDT)")
    p.add_argument("--model-version", default="v1", help="Model version tag (default: v1)")
    return p


def main() -> None:
    args = _build_parser().parse_args()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Device: %s", device)

    # 1. Prepare data
    train_loader, val_loader, test_loader, scaler = prepare_data(
        symbols=args.symbols,
        batch_size=args.batch_size,
    )

    # 2. Initialize model
    model = LSTMModel().to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)

    total_params = sum(p.numel() for p in model.parameters())
    logger.info("Model params: %s", f"{total_params:,}")

    # 3. Training loop with early stopping
    best_val_loss = float("inf")
    patience_counter = 0
    patience = MODEL_CONFIG["early_stopping_patience"]
    best_epoch = 0

    logger.info("=== Training started: %d epochs, patience=%d ===", args.epochs, patience)
    t0 = time.time()

    for epoch in range(1, args.epochs + 1):
        train_loss = train_one_epoch(model, train_loader, criterion, optimizer, device)
        val_metrics = evaluate(model, val_loader, criterion, device)

        logger.info(
            "Epoch %d/%d | train_loss=%.6f | val_loss=%.6f | MAE=%.6f | RMSE=%.6f | MAPE=%.4f%%",
            epoch, args.epochs, train_loss,
            val_metrics["loss"], val_metrics["mae"],
            val_metrics["rmse"], val_metrics["mape"],
        )

        # Early stopping check
        if val_metrics["loss"] < best_val_loss:
            best_val_loss = val_metrics["loss"]
            patience_counter = 0
            best_epoch = epoch

            # Save best model
            model_path = MODELS_DIR / f"lstm_{args.model_version}.pth"
            save_model(
                model, scaler, model_path,
                metadata={
                    "model_version": args.model_version,
                    "epoch": epoch,
                    "val_loss": val_metrics["loss"],
                    "val_mae": val_metrics["mae"],
                    "val_rmse": val_metrics["rmse"],
                    "val_mape": val_metrics["mape"],
                },
            )
        else:
            patience_counter += 1
            if patience_counter >= patience:
                logger.info(
                    "Early stopping at epoch %d (best=%d, val_loss=%.6f)",
                    epoch, best_epoch, best_val_loss,
                )
                break

    elapsed = time.time() - t0
    logger.info("Training completed in %.1f seconds", elapsed)

    # 4. Evaluate on test set
    test_metrics = evaluate(model, test_loader, criterion, device)
    logger.info(
        "=== Test Results: loss=%.6f | MAE=%.6f | RMSE=%.6f | MAPE=%.4f%% ===",
        test_metrics["loss"], test_metrics["mae"],
        test_metrics["rmse"], test_metrics["mape"],
    )


if __name__ == "__main__":
    main()
