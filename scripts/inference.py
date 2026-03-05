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
#   - 360 nến 1-min gần nhất từ PostgreSQL
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

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import torch

from config.config import MODEL_CONFIG, FEATURE_COLUMNS, MODELS_DIR
from config.symbols import ACTIVE_SYMBOLS
from models.model import load_model
from utils.data_utils import normalize_data, denormalize_data
from utils.db_utils import get_engine, upsert_on_conflict_nothing
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def get_latest_data(
    engine,
    symbol: str,
    input_window: int = MODEL_CONFIG["input_window"],
    feature_cols: list[str] = FEATURE_COLUMNS,
) -> pd.DataFrame | None:
    """Query the last *input_window* klines for a symbol.

    Returns DataFrame with *feature_cols* or None if insufficient data.
    """
    feature_str = ", ".join(feature_cols)
    query = (
        f"SELECT {feature_str} FROM klines "
        f"WHERE symbol = '{symbol}' "
        f"  AND rsi_14 IS NOT NULL AND macd IS NOT NULL "
        f"ORDER BY timestamp DESC "
        f"LIMIT {input_window}"
    )
    df = pd.read_sql(query, engine)

    if len(df) < input_window:
        logger.warning(
            "%s: only %d/%d klines available — skipping",
            symbol, len(df), input_window,
        )
        return None

    # Reverse to chronological order (oldest first)
    return df.iloc[::-1].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------

def predict_symbol(
    model: torch.nn.Module,
    scaler,
    data: pd.DataFrame,
    feature_cols: list[str],
    device: torch.device,
) -> np.ndarray:
    """Run inference for one symbol.

    Args:
        model: Loaded LSTMModel (eval mode).
        scaler: Fitted MinMaxScaler from training.
        data: DataFrame with *feature_cols* (input_window rows).
        feature_cols: Column names matching the scaler.
        device: Torch device.

    Returns:
        1-D array of *output_window* denormalized predicted close prices.
    """
    # Normalize using training scaler
    scaled_df, _ = normalize_data(data, feature_cols, scaler=scaler)

    # Build input tensor: (1, input_window, n_features)
    x = torch.tensor(
        scaled_df[feature_cols].values, dtype=torch.float32,
    ).unsqueeze(0).to(device)

    # Forward pass
    model.eval()
    with torch.no_grad():
        pred_scaled = model(x).cpu().numpy().flatten()

    # Denormalize predictions (close is the target column)
    close_idx = feature_cols.index("close")
    pred_prices = denormalize_data(pred_scaled, scaler, close_idx)
    return pred_prices


# ---------------------------------------------------------------------------
# Save Predictions
# ---------------------------------------------------------------------------

def save_predictions(
    engine,
    predictions_df: pd.DataFrame,
) -> None:
    """Insert predictions into PostgreSQL using pandas upsert."""
    predictions_df.to_sql(
        "predictions",
        engine,
        if_exists="append",
        index=False,
        method=upsert_on_conflict_nothing,
    )
    logger.info("Saved %d prediction rows", len(predictions_df))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run LSTM inference for crypto price prediction")
    p.add_argument("--model-version", default="v1", help="Model version (default: v1)")
    p.add_argument("--symbols", nargs="+", default=None, help="Override symbols")
    return p


def main() -> None:
    args = _build_parser().parse_args()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Device: %s", device)

    # 1. Load model
    model_path = MODELS_DIR / f"lstm_{args.model_version}.pth"
    model, scaler, metadata = load_model(model_path, device)
    logger.info(
        "Loaded model %s (metadata=%s)",
        args.model_version, metadata,
    )

    # 2. Run inference for each symbol
    symbols = args.symbols or ACTIVE_SYMBOLS
    engine = get_engine()
    predicted_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    all_rows: list[dict] = []

    for idx, symbol in enumerate(symbols, 1):
        data = get_latest_data(engine, symbol)
        if data is None:
            continue

        try:
            pred_prices = predict_symbol(
                model, scaler, data, FEATURE_COLUMNS, device,
            )
        except Exception as exc:
            logger.error("[%d/%d] %s: prediction failed: %s", idx, len(symbols), symbol, exc)
            continue

        # Build prediction rows
        for step in range(len(pred_prices)):
            target_time = predicted_at + timedelta(minutes=step + 1)
            all_rows.append({
                "symbol": symbol,
                "predicted_at": predicted_at,
                "step_index": step + 1,
                "target_time": target_time,
                "predicted_close": float(pred_prices[step]),
                "model_version": args.model_version,
                "actual_close": None,
                "error_pct": None,
            })

        logger.info(
            "[%d/%d] %s: predicted %d steps",
            idx, len(symbols), symbol, len(pred_prices),
        )

    # 3. Save predictions
    if all_rows:
        predictions_df = pd.DataFrame(all_rows)
        save_predictions(engine, predictions_df)
        logger.info(
            "=== Inference complete: %d symbols, %d predictions ===",
            len(set(r["symbol"] for r in all_rows)), len(all_rows),
        )
    else:
        logger.warning("No predictions generated")


if __name__ == "__main__":
    main()
