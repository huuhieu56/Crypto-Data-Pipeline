# =============================================================================
# Train Script - Huấn luyện model LSTM với PyTorch (Per-Coin Weights)
# =============================================================================
# Chức năng:
#   1. Chuẩn bị dữ liệu: Query N nến gần nhất từ PostgreSQL (cho 1 symbol)
#   2. Xây dựng model: LSTM 2 layers, hidden_size=128, dropout=0.2
#   3. Training: Adam optimizer, MSE loss, early stopping (patience=10)
#   4. Evaluation: Tính MAE, RMSE, MAPE trên test set
#   5. Lưu model: Xuất file .pth riêng cho mỗi coin
#
# Model Architecture:
#   - Input: (batch_size, 600, 7) — 600 nến 1-min (10h), 7 features
#   - LSTM: 2 layers, hidden_size=128, dropout=0.2
#   - Output: (batch_size, 60) — predicted close price 60 phút tiếp theo
#
# Mỗi coin có weight riêng: lstm_{symbol}_{version}.pth
# Mặc định chỉ train BTCUSDT, override qua --symbols.
#
# Sử dụng:
#   python scripts/train.py --epochs 50 --batch-size 32
#   python scripts/train.py --symbols BTCUSDT ETHUSDT
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
# Data Preparation — chỉ load N nến gần nhất cho 1 symbol
# ---------------------------------------------------------------------------

def prepare_data_for_symbol(
    symbol: str,
    batch_size: int = MODEL_CONFIG["batch_size"],
    n_candles: int = MODEL_CONFIG["n_candles_to_load"],
) -> tuple[DataLoader, DataLoader, DataLoader, object]:
    """Query N nến gần nhất từ PostgreSQL cho 1 symbol, build DataLoaders.

    Returns:
        (train_loader, val_loader, test_loader, scaler)
    """
    engine = get_engine()
    feature_str = ", ".join(FEATURE_COLUMNS)

    # Chỉ load n_candles nến gần nhất — tránh load toàn bộ lịch sử
    query = (
        f"SELECT symbol, timestamp, {feature_str} "
        f"FROM klines "
        f"WHERE symbol = '{symbol}' "
        f"  AND rsi_14 IS NOT NULL "
        f"  AND macd IS NOT NULL "
        f"ORDER BY timestamp DESC "
        f"LIMIT {n_candles}"
    )

    logger.info("[%s] Querying %d latest candles...", symbol, n_candles)
    df = pd.read_sql(query, engine)

    if len(df) == 0:
        raise ValueError(f"[{symbol}] No data returned from query")

    # Re-sort ascending (query returned DESC)
    df = df.sort_values("timestamp").reset_index(drop=True)
    logger.info("[%s] Loaded %s rows", symbol, f"{len(df):,}")

    # Validate
    df = validate_data(df, FEATURE_COLUMNS)
    if len(df) == 0:
        raise ValueError(f"[{symbol}] No valid data after validation")

    # Normalize features
    scaled_df, scaler = normalize_data(df, FEATURE_COLUMNS)

    # Build sequences
    input_window = MODEL_CONFIG["input_window"]
    output_window = MODEL_CONFIG["output_window"]

    min_rows = input_window + output_window
    if len(scaled_df) < min_rows:
        raise ValueError(
            f"[{symbol}] Only {len(scaled_df)} rows, need at least {min_rows}"
        )

    X, y = create_sequences(
        scaled_df, input_window, output_window,
        feature_cols=FEATURE_COLUMNS, target_col="close",
    )

    logger.info("[%s] Total sequences: %s", symbol, f"{len(X):,}")

    # Split train / val / test
    n = len(X)
    train_end = int(n * MODEL_CONFIG["train_ratio"])
    val_end = train_end + int(n * MODEL_CONFIG["val_ratio"])

    train_ds = CryptoDataset(X[:train_end], y[:train_end])
    val_ds = CryptoDataset(X[train_end:val_end], y[train_end:val_end])
    test_ds = CryptoDataset(X[val_end:], y[val_end:])

    logger.info(
        "[%s] Split: train=%d, val=%d, test=%d",
        symbol, len(train_ds), len(val_ds), len(test_ds),
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
# Train a single symbol
# ---------------------------------------------------------------------------

def train_symbol(
    symbol: str,
    args: argparse.Namespace,
    device: torch.device,
) -> dict[str, float] | None:
    """Train LSTM for one symbol. Returns test metrics or None on failure."""
    logger.info("=" * 60)
    logger.info("=== Training: %s ===", symbol)
    logger.info("=" * 60)

    try:
        train_loader, val_loader, test_loader, scaler = prepare_data_for_symbol(
            symbol=symbol,
            batch_size=args.batch_size,
        )
    except ValueError as exc:
        logger.error("[%s] Skipping — %s", symbol, exc)
        return None

    # Initialize model
    model = LSTMModel().to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)

    total_params = sum(p.numel() for p in model.parameters())
    logger.info("[%s] Model params: %s", symbol, f"{total_params:,}")

    # Training loop with early stopping
    best_val_loss = float("inf")
    patience_counter = 0
    patience = MODEL_CONFIG["early_stopping_patience"]
    best_epoch = 0

    logger.info(
        "[%s] Training started: %d epochs, patience=%d",
        symbol, args.epochs, patience,
    )
    t0 = time.time()

    for epoch in range(1, args.epochs + 1):
        train_loss = train_one_epoch(model, train_loader, criterion, optimizer, device)
        val_metrics = evaluate(model, val_loader, criterion, device)

        logger.info(
            "[%s] Epoch %d/%d | train_loss=%.6f | val_loss=%.6f | MAE=%.6f | RMSE=%.6f | MAPE=%.4f%%",
            symbol, epoch, args.epochs, train_loss,
            val_metrics["loss"], val_metrics["mae"],
            val_metrics["rmse"], val_metrics["mape"],
        )

        # Early stopping check
        if val_metrics["loss"] < best_val_loss:
            best_val_loss = val_metrics["loss"]
            patience_counter = 0
            best_epoch = epoch

            # Save best model — per-coin weight file
            model_path = MODELS_DIR / f"lstm_{symbol}_{args.model_version}.pth"
            save_model(
                model, scaler, model_path,
                metadata={
                    "symbol": symbol,
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
                    "[%s] Early stopping at epoch %d (best=%d, val_loss=%.6f)",
                    symbol, epoch, best_epoch, best_val_loss,
                )
                break

    elapsed = time.time() - t0
    logger.info("[%s] Training completed in %.1f seconds", symbol, elapsed)

    # Evaluate on test set
    test_metrics = evaluate(model, test_loader, criterion, device)
    logger.info(
        "[%s] Test Results: loss=%.6f | MAE=%.6f | RMSE=%.6f | MAPE=%.4f%%",
        symbol, test_metrics["loss"], test_metrics["mae"],
        test_metrics["rmse"], test_metrics["mape"],
    )

    return test_metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Train LSTM model for crypto price prediction (per-coin weights)")
    p.add_argument("--epochs", type=int, default=MODEL_CONFIG["epochs"], help="Number of epochs")
    p.add_argument("--batch-size", type=int, default=MODEL_CONFIG["batch_size"], help="Batch size")
    p.add_argument("--lr", type=float, default=MODEL_CONFIG["learning_rate"], help="Learning rate")
    p.add_argument(
        "--symbols", nargs="+",
        default=MODEL_CONFIG["default_train_symbols"],
        help="Symbols to train (default: BTCUSDT only)",
    )
    p.add_argument("--model-version", default="v1", help="Model version tag (default: v1)")
    return p


def main() -> None:
    args = _build_parser().parse_args()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Device: %s", device)
    logger.info("Symbols to train: %s", args.symbols)

    results: dict[str, dict[str, float]] = {}

    for symbol in args.symbols:
        test_metrics = train_symbol(symbol, args, device)
        if test_metrics is not None:
            results[symbol] = test_metrics

    # Summary
    logger.info("=" * 60)
    logger.info("=== Training Summary ===")
    logger.info("=" * 60)
    for symbol, metrics in results.items():
        logger.info(
            "%s: loss=%.6f | MAE=%.6f | RMSE=%.6f | MAPE=%.4f%%",
            symbol, metrics["loss"], metrics["mae"],
            metrics["rmse"], metrics["mape"],
        )

    failed = set(args.symbols) - set(results.keys())
    if failed:
        logger.warning("Failed/skipped symbols: %s", failed)


if __name__ == "__main__":
    main()
