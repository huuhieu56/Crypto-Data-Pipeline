"""Inference script — run LSTM predictions for the Crypto Data Pipeline.

Workflow:
    1. Load model weights (lstm_{symbol}_{version}.pth) per coin
    2. Fetch the most recent candles from PostgreSQL
    3. Predict close prices for the next output_window minutes
    4. Save predictions to the database

Schedule: start of each hour (0 * * * *) via hourly_inference DAG.

Usage:
    python scripts/inference.py
    python scripts/inference.py --symbols BTCUSDT ETHUSDT
"""

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
from models.model import load_model
from utils.data_utils import normalize_data, denormalize_data, compute_log_returns, returns_to_price
from utils.db_utils import get_engine, upsert_on_conflict_nothing
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def get_latest_data(
    engine,
    symbol: str,
    cutoff_time: datetime | None = None,
    input_window: int = MODEL_CONFIG["input_window"],
    feature_cols: list[str] = FEATURE_COLUMNS,
) -> pd.DataFrame | None:
    """Query the last *input_window* klines for a symbol, up to *cutoff_time*.

    When predict_returns=True, fetches extra rows to compensate for
    the row lost during log-returns computation.

    Args:
        cutoff_time: Only use klines with timestamp <= cutoff_time.
                     If None, use all available data.

    Returns DataFrame with *feature_cols* or None if insufficient data.
    """
    # Need extra rows if we'll compute log-returns (drops 1 row)
    predict_returns = MODEL_CONFIG.get("predict_returns", False)
    n_rows = input_window + (1 if predict_returns else 0)

    feature_str = ", ".join(feature_cols)
    time_filter = f"AND timestamp <= '{cutoff_time}'" if cutoff_time else ""
    query = (
        f"SELECT {feature_str} FROM klines "
        f"WHERE symbol = '{symbol}' "
        f"  AND rsi_14 IS NOT NULL AND macd IS NOT NULL "
        f"  {time_filter} "
        f"ORDER BY timestamp DESC "
        f"LIMIT {n_rows}"
    )
    df = pd.read_sql(query, engine)

    if len(df) < n_rows:
        logger.warning(
            "%s: only %d/%d klines available (cutoff=%s) — skipping",
            symbol, len(df), n_rows, cutoff_time,
        )
        return None

    # Reverse to chronological order (oldest first)
    return df.iloc[::-1].reset_index(drop=True)


# --- Prediction --------------------------------------------------------------

def predict_symbol(
    model: torch.nn.Module,
    scaler,
    data: pd.DataFrame,
    feature_cols: list[str],
    device: torch.device,
    predict_returns: bool = False,
    last_close_price: float | None = None,
) -> np.ndarray:
    """Run inference for one symbol.

    Args:
        model: Loaded LSTMModel (eval mode).
        scaler: Fitted scaler from training.
        data: DataFrame with *feature_cols* (input_window rows).
        feature_cols: Column names matching the scaler.
        device: Torch device.
        predict_returns: If True, model predicts log-returns.
        last_close_price: Last known close price (anchor for returns→price).

    Returns:
        1-D array of *output_window* denormalized predicted close prices.
    """
    # Phase 1: Convert to log-returns if needed
    if predict_returns:
        data = compute_log_returns(data, price_cols=["open", "high", "low", "close"])

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

    # Denormalize predictions
    close_idx = feature_cols.index("close")
    pred_values = denormalize_data(pred_scaled, scaler, close_idx)

    # Phase 1: Convert log-returns → absolute prices
    if predict_returns and last_close_price is not None:
        pred_prices = returns_to_price(pred_values, last_close_price)
    else:
        pred_prices = pred_values

    return pred_prices


# --- Save Predictions --------------------------------------------------------

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
# Inference for one hour slot
# ---------------------------------------------------------------------------

def infer_one_hour(
    engine,
    predicted_at: datetime,
    symbols: list[str],
    model_version: str,
    device: torch.device,
) -> list[dict]:
    """Run inference for a single hour slot.

    Args:
        predicted_at: The hour boundary (e.g. 09:00) — uses data up to this time
                      and predicts 60 minutes after.

    Returns:
        List of prediction row dicts.
    """
    rows: list[dict] = []

    for idx, symbol in enumerate(symbols, 1):
        # 1. Load per-coin model
        model_path = MODELS_DIR / f"lstm_{symbol}_{model_version}.pth"
        if not model_path.exists():
            logger.warning(
                "  [%d/%d] %s: model file not found — skipping",
                idx, len(symbols), symbol,
            )
            continue

        try:
            model, scaler, metadata = load_model(model_path, device)
        except Exception as exc:
            logger.error("  [%d/%d] %s: failed to load model: %s", idx, len(symbols), symbol, exc)
            continue

        # 2. Get data up to the hour boundary
        data = get_latest_data(engine, symbol, cutoff_time=predicted_at)
        if data is None:
            continue

        # Phase 1: Get the last close price for returns→price conversion
        predict_returns = metadata.get("predict_returns", False)
        last_close_price = float(data["close"].iloc[-1]) if predict_returns else None

        # 3. Predict
        try:
            pred_prices = predict_symbol(
                model, scaler, data, FEATURE_COLUMNS, device,
                predict_returns=predict_returns,
                last_close_price=last_close_price,
            )
        except Exception as exc:
            logger.error("  [%d/%d] %s: prediction failed: %s", idx, len(symbols), symbol, exc)
            continue

        # 4. Build prediction rows
        for step in range(len(pred_prices)):
            target_time = predicted_at + timedelta(minutes=step + 1)
            rows.append({
                "symbol": symbol,
                "predicted_at": predicted_at,
                "step_index": step + 1,
                "target_time": target_time,
                "predicted_close": float(pred_prices[step]),
                "model_version": model_version,
                "actual_close": None,
                "error_pct": None,
            })

        logger.info(
            "  [%d/%d] %s: predicted %d steps (%.2f → %.2f)",
            idx, len(symbols), symbol, len(pred_prices),
            pred_prices[0], pred_prices[-1],
        )

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run LSTM inference with 24h backfill")
    p.add_argument("--model-version", default="v1", help="Model version (default: v1)")
    p.add_argument(
        "--symbols", nargs="+",
        default=MODEL_CONFIG["default_train_symbols"],
        help="Symbols to predict (default: BTCUSDT only)",
    )
    p.add_argument(
        "--backfill-hours", type=int, default=24,
        help="Number of past hours to backfill (default: 24)",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Device: %s", device)

    symbols = args.symbols
    engine = get_engine()

    # Lấy latest kline → snap to hour
    anchor_row = pd.read_sql(
        "SELECT MAX(timestamp) AS latest FROM klines WHERE symbol = 'BTCUSDT'",
        engine,
    )
    if anchor_row.empty or pd.isna(anchor_row["latest"].iloc[0]):
        logger.error("No klines data found — cannot determine anchor time")
        return

    latest_kline = pd.Timestamp(anchor_row["latest"].iloc[0]).to_pydatetime()
    current_hour = latest_kline.replace(minute=0, second=0, microsecond=0)
    logger.info("Latest kline: %s → current hour: %s", latest_kline.strftime("%Y-%m-%d %H:%M"), current_hour)

    # Generate 24 hourly slots (current_hour - 23h, ..., current_hour)
    slots = [current_hour - timedelta(hours=h) for h in range(args.backfill_hours - 1, -1, -1)]

    # Check which slots already have predictions
    existing = pd.read_sql(
        f"SELECT DISTINCT predicted_at FROM predictions "
        f"WHERE symbol = '{symbols[0]}' "
        f"  AND predicted_at >= '{slots[0]}' "
        f"  AND predicted_at <= '{slots[-1]}'",
        engine,
    )
    existing_set = set(pd.to_datetime(existing["predicted_at"]).dt.to_pydatetime())

    missing_slots = [s for s in slots if s not in existing_set]
    logger.info(
        "Backfill check: %d/%d slots missing in last %dh",
        len(missing_slots), len(slots), args.backfill_hours,
    )

    if not missing_slots:
        logger.info("All %d hourly slots already have predictions — nothing to do", len(slots))
        return

    # Run inference for each missing slot
    all_rows: list[dict] = []
    for slot_idx, slot in enumerate(missing_slots, 1):
        logger.info(
            "[Slot %d/%d] %s → predicting %s:01 to %s:00",
            slot_idx, len(missing_slots),
            slot.strftime("%Y-%m-%d %H:%M"),
            slot.strftime("%H"),
            (slot + timedelta(hours=1)).strftime("%H"),
        )
        rows = infer_one_hour(engine, slot, symbols, args.model_version, device)
        all_rows.extend(rows)

    # Save all predictions
    if all_rows:
        predictions_df = pd.DataFrame(all_rows)
        save_predictions(engine, predictions_df)
        logger.info(
            "=== Inference complete: %d slots × %d symbols = %d predictions ===",
            len(missing_slots), len(symbols), len(all_rows),
        )
    else:
        logger.warning("No predictions generated")


if __name__ == "__main__":
    main()

