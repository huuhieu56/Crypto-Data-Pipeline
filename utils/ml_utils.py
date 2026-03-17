"""ML utilities for the Crypto Data Pipeline.

Normalization, log-return transforms, sequence building, and data validation
used by training and inference scripts.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from utils.logger import get_logger

logger = get_logger(__name__)


# --- Data Validation --------------------------------------------------------

def validate_data(
    df: pd.DataFrame,
    required_cols: list[str],
) -> pd.DataFrame:
    """Check for missing columns, null values, and duplicates.

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

    Non-price columns are passed through unchanged.
    The first row becomes NaN and is dropped.
    """
    if price_cols is None:
        price_cols = ["open", "high", "low", "close"]

    df = df.copy()
    for col in price_cols:
        if col in df.columns:
            df[col] = np.log(df[col] / df[col].shift(1))

    df = df.iloc[1:].reset_index(drop=True)
    return df


def returns_to_price(
    log_returns: np.ndarray,
    last_price: float,
) -> np.ndarray:
    """Convert predicted log-returns back to absolute prices."""
    cum_returns = np.cumsum(log_returns)
    return last_price * np.exp(cum_returns)


# --- Normalization -----------------------------------------------------------

def normalize_data(
    df: pd.DataFrame,
    feature_cols: list[str],
    scaler: StandardScaler | MinMaxScaler | None = None,
) -> tuple[pd.DataFrame, StandardScaler | MinMaxScaler]:
    """Normalize *feature_cols* using StandardScaler (z-score)."""
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
    """Inverse-transform a single feature column back to original scale."""
    n_features = scaler.n_features_in_
    dummy = np.zeros((len(values), n_features))
    dummy[:, col_index] = values
    inversed = scaler.inverse_transform(dummy)
    return inversed[:, col_index]


# --- Sequence Building -------------------------------------------------------

def create_sequences(
    data: np.ndarray | pd.DataFrame,
    input_window: int,
    output_window: int,
    feature_cols: list[str] | None = None,
    target_col: str = "close",
) -> tuple[np.ndarray, np.ndarray]:
    """Build sliding-window sequences for LSTM training.

    Returns (X, y) where:
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
        )

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
