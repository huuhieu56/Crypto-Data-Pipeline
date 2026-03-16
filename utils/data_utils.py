"""Data utilities for the Crypto Data Pipeline.

Provides klines I/O helpers (MinIO Parquet), date/month calculations,
log-return transformations, and scikit-learn normalization.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from config.config import RAW_DATA_DIR, MINIO_CONFIG
from config.symbols import SYMBOLS_STATUS, BREAK_DATES
from utils.logger import get_logger
from utils.storage import storage

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]


# --- Shared Helpers ----------------------------------------------------------

def get_target_end(symbol: str) -> datetime:
    """TRADING → now (UTC), BREAK → break_date."""
    break_date_str = BREAK_DATES.get(symbol)
    if break_date_str and SYMBOLS_STATUS.get(symbol) != "TRADING":
        return datetime.strptime(break_date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc,
        )
    return datetime.now(timezone.utc)


# --- MinIO Parquet I/O (optimized for large klines) -------------------------

def _read_minio_last_open_time(key: str) -> pd.Timestamp | None:
    """Read the last open_time from a Parquet file on MinIO."""
    try:
        table = storage.download_parquet(BUCKET_RAW, key)
        if table.num_rows == 0:
            return None
        df = table.column("open_time").to_pandas()
        last_val = df.iloc[-1]
        return pd.Timestamp(last_val)
    except Exception:
        return None


def merge_and_save_klines(object_key: str, new_df: pd.DataFrame) -> int:
    """Merge *new_df* into existing Parquet on MinIO (dedup by open_time).

    Optimization (common case — incremental / chronological append):
      If new data is strictly AFTER existing data, append to the
      Parquet file without full dedup.  Falls back to full dedup
      only when overlap is detected (e.g. backfill re-download).

    Args:
        object_key: MinIO key e.g. "BTCUSDT.parquet"
        new_df: DataFrame of new klines records

    Returns total record count.
    """
    new_df = new_df.drop_duplicates(subset=["open_time"], keep="last")
    new_df = new_df.sort_values("open_time").reset_index(drop=True)
    new_table = pa.Table.from_pandas(new_df, preserve_index=False)

    if not storage.object_exists(BUCKET_RAW, object_key):
        storage.upload_parquet(BUCKET_RAW, object_key, new_table)
        return len(new_df)

    # --- fast path: append-only when no overlap ---
    last_existing = _read_minio_last_open_time(object_key)
    if last_existing is not None:
        new_first = pd.Timestamp(new_df["open_time"].iloc[0])
        if new_first > last_existing:
            old_table = storage.download_parquet(BUCKET_RAW, object_key)
            combined = pa.concat_tables([old_table, new_table])
            storage.upload_parquet(BUCKET_RAW, object_key, combined)
            logger.debug(
                "Fast append: +%d rows -> %s",
                len(new_df), object_key,
            )
            return combined.num_rows

    # --- slow path: overlap -> full dedup ---
    logger.debug("Full merge (overlap): %s", object_key)
    old_table = storage.download_parquet(BUCKET_RAW, object_key)
    old_df = old_table.to_pandas()
    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined = (
        combined
        .drop_duplicates(subset=["open_time"], keep="last")
        .sort_values("open_time")
        .reset_index(drop=True)
    )
    combined_table = pa.Table.from_pandas(combined, preserve_index=False)
    storage.upload_parquet(BUCKET_RAW, object_key, combined_table)
    return len(combined)


def get_last_timestamp(symbol: str) -> int | None:
    """Get last open_time (ms) from a symbol's Parquet on MinIO."""
    key = f"{symbol}.parquet"
    if not storage.object_exists(BUCKET_RAW, key):
        return None
    ts = _read_minio_last_open_time(key)
    if ts is None:
        return None
    try:
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return int(ts.timestamp() * 1000)
    except Exception as exc:
        logger.error(
            "Cannot parse timestamp from %s: %s", symbol, exc,
        )
        return None


# --- Date / Month Utilities (Data Vision) -----------------------------------

def get_target_months(months_back: int) -> list[tuple[int, int]]:
    """Last *months_back* completed months as (year, month) tuples."""
    end_date = datetime.now(timezone.utc) - relativedelta(months=1)
    return [
        ((end_date - relativedelta(months=i)).year,
         (end_date - relativedelta(months=i)).month)
        for i in range(months_back)
    ]


def get_months_between(
    start_dt: datetime,
    end_dt: datetime,
) -> list[tuple[int, int]]:
    """Complete months between *start_dt* and *end_dt*."""
    months: list[tuple[int, int]] = []
    cursor = start_dt.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0,
    ) + relativedelta(months=1)
    while True:
        next_month = cursor + relativedelta(months=1)
        if next_month > end_dt:
            break
        months.append((cursor.year, cursor.month))
        cursor = next_month
    return months


# --- ML Data Helpers ---------------------------------------------------------

def validate_data(
    df: pd.DataFrame,
    required_cols: list[str],
) -> pd.DataFrame:
    """Check for missing columns, null values, and duplicates.

    Drops rows with nulls in *required_cols* and logs warnings.
    Returns cleaned DataFrame (original is not mutated).
    """
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.copy()
    null_counts = df[required_cols].isnull().sum()
    total_nulls = null_counts.sum()
    if total_nulls > 0:
        logger.warning(
            "Dropping %d rows with nulls:\n%s",
            total_nulls,
            null_counts[null_counts > 0].to_string(),
        )
        df = df.dropna(subset=required_cols)

    dup_count = df.duplicated().sum()
    if dup_count > 0:
        logger.warning("Dropping %d duplicate rows", dup_count)
        df = df.drop_duplicates()

    return df.reset_index(drop=True)


# --- Log-Return Helpers ------------------------------------------------------

def compute_log_returns(
    df: pd.DataFrame,
    price_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Convert price columns to log-returns: log(p_t / p_{t-1}).

    Non-price columns (volume, rsi_14, macd) are passed through unchanged.
    The first row becomes NaN and is dropped.

    Args:
        df: DataFrame with price columns.
        price_cols: Columns to convert. Default: open, high, low, close.

    Returns:
        DataFrame with log-returns (1 row shorter).
    """
    if price_cols is None:
        price_cols = ["open", "high", "low", "close"]

    df = df.copy()
    for col in price_cols:
        if col in df.columns:
            df[col] = np.log(df[col] / df[col].shift(1))

    # Drop the first row (NaN from shift)
    df = df.iloc[1:].reset_index(drop=True)
    return df


def returns_to_price(
    log_returns: np.ndarray,
    last_price: float,
) -> np.ndarray:
    """Convert predicted log-returns back to absolute prices.

    Args:
        log_returns: 1-D array of predicted log-returns.
        last_price: The last known actual price (anchor point).

    Returns:
        1-D array of predicted prices.
    """
    # Cumulative sum of log-returns, then exponentiate
    cum_returns = np.cumsum(log_returns)
    prices = last_price * np.exp(cum_returns)
    return prices


# --- Normalization -----------------------------------------------------------

def normalize_data(
    df: pd.DataFrame,
    feature_cols: list[str],
    scaler: StandardScaler | MinMaxScaler | None = None,
) -> tuple[pd.DataFrame, StandardScaler | MinMaxScaler]:
    """Normalize *feature_cols* using StandardScaler (z-score).

    StandardScaler preserves relative magnitude of changes,
    unlike MinMaxScaler which compresses everything to [0, 1].

    Args:
        df: Input DataFrame.
        feature_cols: Columns to normalize.
        scaler: Pre-fitted scaler (inference mode). If None, fit a new one.

    Returns:
        (scaled_df, scaler) — scaled_df is a copy, original unchanged.
    """
    df = df.copy()
    if scaler is None:
        scaler = StandardScaler()
        df[feature_cols] = scaler.fit_transform(df[feature_cols])
    else:
        df[feature_cols] = scaler.transform(df[feature_cols])
    return df, scaler


def denormalize_data(
    values: np.ndarray,
    scaler: StandardScaler | MinMaxScaler,
    col_index: int,
) -> np.ndarray:
    """Inverse-transform a single feature column back to original scale.

    Works with both StandardScaler and MinMaxScaler.

    Args:
        values: 1-D array of scaled values.
        scaler: Fitted scaler (same used in normalize_data).
        col_index: Index of the target column within the scaler's feature set.

    Returns:
        1-D array in original scale.
    """
    n_features = scaler.n_features_in_
    dummy = np.zeros((len(values), n_features))
    dummy[:, col_index] = values
    inversed = scaler.inverse_transform(dummy)
    return inversed[:, col_index]


def create_sequences(
    data: np.ndarray | pd.DataFrame,
    input_window: int,
    output_window: int,
    feature_cols: list[str] | None = None,
    target_col: str = "close",
) -> tuple[np.ndarray, np.ndarray]:
    """Build sliding-window sequences for LSTM training.

    Args:
        data: 2-D array or DataFrame with features (rows = timesteps).
        input_window: Number of past timesteps per sample (e.g. 120).
        output_window: Number of future timesteps to predict (e.g. 60).
        feature_cols: Column names when *data* is a DataFrame.
        target_col: Column to predict (default: 'close').

    Returns:
        (X, y) where:
          X shape: (n_samples, input_window, n_features)
          y shape: (n_samples, output_window)
    """
    if isinstance(data, pd.DataFrame):
        if feature_cols is None:
            feature_cols = list(data.columns)
        target_idx = feature_cols.index(target_col)
        arr = data[feature_cols].values.astype(np.float32)
    else:
        arr = np.asarray(data, dtype=np.float32)
        target_idx = (
            feature_cols.index(target_col) if feature_cols else 3
        )  # default: 'close' is col-3 in FEATURE_COLUMNS

    total = len(arr)
    n_samples = total - input_window - output_window + 1
    if n_samples <= 0:
        raise ValueError(
            f"Not enough data ({total} rows) for "
            f"input_window={input_window} + output_window={output_window}"
        )

    X = np.empty((n_samples, input_window, arr.shape[1]), dtype=np.float32)
    y = np.empty((n_samples, output_window), dtype=np.float32)

    for i in range(n_samples):
        X[i] = arr[i : i + input_window]
        y[i] = arr[i + input_window : i + input_window + output_window, target_idx]

    logger.info(
        "Created %s sequences: X=%s, y=%s",
        f"{n_samples:,}", X.shape, y.shape,
    )
    return X, y
