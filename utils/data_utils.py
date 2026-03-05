# =============================================================================
# Data Utilities - Crypto Data Pipeline
# =============================================================================

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from config.config import RAW_DATA_DIR
from config.symbols import SYMBOLS_STATUS, BREAK_DATES
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers (used by pre_extract.py & extract.py)
# ---------------------------------------------------------------------------

def get_target_end(symbol: str) -> datetime:
    """TRADING → now (UTC), BREAK → break_date."""
    break_date_str = BREAK_DATES.get(symbol)
    if break_date_str and SYMBOLS_STATUS.get(symbol) != "TRADING":
        return datetime.strptime(break_date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc,
        )
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# CSV I/O helpers (optimized for large klines files)
# ---------------------------------------------------------------------------

def _tail_open_time(csv_path: Path) -> str | None:
    """Read the last row's open_time from a sorted CSV.  O(1) I/O."""
    try:
        with open(csv_path, "rb") as f:
            header = f.readline().decode().strip()
            cols = header.split(",")
            ot_idx = cols.index("open_time")

            f.seek(0, 2)
            size = f.tell()
            if size <= len(header) + 2:
                return None

            chunk = min(512, size)
            f.seek(-chunk, 2)
            tail = f.read().decode().strip()

        last_line = tail.rsplit("\n", 1)[-1]
        if not last_line or last_line == header:
            return None
        return last_line.split(",")[ot_idx]
    except Exception:
        return None


def _count_lines(csv_path: Path) -> int:
    """Count data rows in CSV (header excluded).  Fast buffered byte scan."""
    count = 0
    with open(csv_path, "rb") as f:
        while True:
            buf = f.read(1 << 20)  # 1 MB chunks
            if not buf:
                break
            count += buf.count(b"\n")
    return max(count - 1, 0)  # subtract header


def merge_and_save_klines(csv_path: Path, new_df: pd.DataFrame) -> int:
    """Merge *new_df* into existing CSV (dedup by open_time, sort, save).

    Optimization (common case — incremental / chronological append):
      If new data is strictly AFTER existing data, append-only without
      re-reading the entire CSV.  Falls back to full dedup only when
      overlap is detected (e.g. backfill re-download).

    Returns total record count (int) instead of DataFrame to avoid
    holding millions of rows in memory.
    """
    new_df = new_df.drop_duplicates(subset=["open_time"], keep="last")
    new_df = new_df.sort_values("open_time").reset_index(drop=True)

    if not csv_path.exists() or csv_path.stat().st_size < 50:
        new_df.to_csv(csv_path, index=False)
        return len(new_df)

    # --- fast path: append-only when no overlap ---
    last_existing = _tail_open_time(csv_path)
    if last_existing is not None:
        new_first = str(new_df["open_time"].iloc[0])
        if pd.Timestamp(new_first) > pd.Timestamp(last_existing):
            new_df.to_csv(csv_path, mode="a", header=False, index=False)
            logger.debug(
                "Fast append: +%d rows -> %s", len(new_df), csv_path.name,
            )
            return _count_lines(csv_path)

    # --- slow path: overlap -> full dedup ---
    logger.debug("Full merge (overlap): %s", csv_path.name)
    old_df = pd.read_csv(csv_path)
    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined = (
        combined
        .drop_duplicates(subset=["open_time"], keep="last")
        .sort_values("open_time")
        .reset_index(drop=True)
    )
    combined.to_csv(csv_path, index=False)
    return len(combined)


def get_last_timestamp(symbol: str) -> int | None:
    """Get last open_time (ms) from a symbol's sorted CSV.  O(1) I/O."""
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"
    if not csv_path.exists():
        return None
    raw = _tail_open_time(csv_path)
    if raw is None:
        return None
    try:
        ts = pd.to_datetime(raw, utc=True)
        return int(ts.timestamp() * 1000)
    except Exception as exc:
        logger.error(
            "Cannot parse timestamp '%s' from %s: %s", raw, symbol, exc,
        )
        return None


# ---------------------------------------------------------------------------
# Date / month utilities (Data Vision)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# ML Data Helpers — used by scripts/train.py & scripts/inference.py
# ---------------------------------------------------------------------------

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


def normalize_data(
    df: pd.DataFrame,
    feature_cols: list[str],
    scaler: MinMaxScaler | None = None,
) -> tuple[pd.DataFrame, MinMaxScaler]:
    """Min-Max normalize *feature_cols* in [0, 1].

    Args:
        df: Input DataFrame.
        feature_cols: Columns to normalize.
        scaler: Pre-fitted scaler (inference mode). If None, fit a new one.

    Returns:
        (scaled_df, scaler) — scaled_df is a copy, original unchanged.
    """
    df = df.copy()
    if scaler is None:
        scaler = MinMaxScaler()
        df[feature_cols] = scaler.fit_transform(df[feature_cols])
    else:
        df[feature_cols] = scaler.transform(df[feature_cols])
    return df, scaler


def denormalize_data(
    values: np.ndarray,
    scaler: MinMaxScaler,
    col_index: int,
) -> np.ndarray:
    """Inverse-transform a single feature column back to original scale.

    Args:
        values: 1-D array of scaled values.
        scaler: Fitted MinMaxScaler (same used in normalize_data).
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
        input_window: Number of past timesteps per sample (e.g. 360).
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
