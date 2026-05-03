"""Transform raw klines -> features with technical indicators.

Reads monthly partitions from MinIO, computes RSI(14) and MACD(12,26,9)
using ClickHouse context for warm-up, outputs with DB column names.

Usage:
    python scripts/transform.py
    python scripts/transform.py --symbols BTCUSDT ETHUSDT
"""

from __future__ import annotations

import sys
from pathlib import Path
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
import pyarrow as pa

from config.config import (
    MINIO_CONFIG, PARALLELISM, INDICATOR_CONTEXT_ROWS,
)
from config.symbols import SYMBOLS
from utils.logger import get_logger
from utils.exceptions import TransformError
from utils.data_utils import partition_key, features_key
from utils.db_utils import ch_query_df, get_last_timestamps
from utils.storage import storage, discover_month_partitions

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]
BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]
TRANSFORM_MAX_WORKERS = PARALLELISM["transform_max_workers"]

# Output columns -- already in DB column names
OUTPUT_COLUMNS = [
    "symbol", "timestamp", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "rsi_14", "macd", "macd_signal",
]


def calculate_indicators(
    raw_df: pd.DataFrame,
    context_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute RSI(14) and MACD(12,26,9).

    If context_df is provided, prepend it for indicator warm-up.
    Returns only rows from raw_df (context rows are dropped).

    Pure function -- no database dependency.
    """
    if context_df is not None and not context_df.empty:
        combined = pd.concat([context_df, raw_df], ignore_index=True)
    else:
        combined = raw_df.copy()

    combined = combined.sort_values("open_time").drop_duplicates(
        subset=["open_time"], keep="last"
    ).reset_index(drop=True)

    close = combined["close"]

    # RSI (14)
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    loss = loss.replace(0, np.nan)
    rs = gain / loss
    combined["rsi_14"] = 100 - (100 / (1 + rs))

    # MACD (12, 26, 9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    combined["macd"] = ema12 - ema26
    combined["macd_signal"] = combined["macd"].ewm(span=9, adjust=False).mean()

    # Fill NaN
    combined = combined.ffill().bfill().fillna(0)

    # Keep only rows from raw_df (drop context rows)
    if context_df is not None and not context_df.empty:
        cutoff = context_df["open_time"].max()
        combined = combined[combined["open_time"] > cutoff]

    return combined


def _get_ch_context(symbol: str, n_rows: int = INDICATOR_CONTEXT_ROWS) -> pd.DataFrame:
    """Query the last n_rows klines from ClickHouse for indicator warm-up.

    Returns DataFrame with raw column names (open_time as timestamp alias).
    """
    try:
        df = ch_query_df(
            f"SELECT timestamp AS open_time, open, high, low, close, volume, "
            f"quote_volume, trades "
            f"FROM klines FINAL "
            f"WHERE symbol = '{symbol}' "
            f"ORDER BY timestamp DESC "
            f"LIMIT {n_rows}"
        )
        if df.empty:
            return df
        df["open_time"] = pd.to_datetime(df["open_time"])
        return df.sort_values("open_time").reset_index(drop=True)
    except Exception as exc:
        logger.debug("ClickHouse context unavailable for %s: %s", symbol, exc)
        return pd.DataFrame()


def _read_monthly_partition(symbol: str, month_str: str) -> pd.DataFrame:
    """Read a monthly raw partition from MinIO."""
    key = partition_key(symbol, month_str)
    if not storage.object_exists(BUCKET_RAW, key):
        return pd.DataFrame()

    table = storage.download_parquet(BUCKET_RAW, key)
    pdf = table.to_pandas()
    del table
    pdf["open_time"] = pd.to_datetime(pdf["open_time"], utc=False)
    return pdf.sort_values("open_time").reset_index(drop=True)


def _process_symbol(
    symbol: str,
    month_str: str,
    last_loaded_ts: pd.Timestamp | None,
) -> pd.DataFrame | None:
    """Read raw partition + CH context, compute indicators, return new rows only."""

    # 1. Read current month's partition
    raw_df = _read_monthly_partition(symbol, month_str)
    if raw_df.empty:
        return None

    # 2. Filter to only rows after last loaded timestamp
    if last_loaded_ts is not None:
        # Normalize tz: last_loaded_ts is tz-aware (UTC), open_time may be tz-naive
        cmp_ts = last_loaded_ts.tz_localize(None) if last_loaded_ts.tzinfo else last_loaded_ts
        new_mask = raw_df["open_time"] > cmp_ts
        if not new_mask.any():
            return None

    # 3. Get context from ClickHouse for indicator warm-up
    ctx_df = _get_ch_context(symbol)

    # 4. Compute indicators (pure function -- context is optional)
    combined = calculate_indicators(raw_df, context_df=ctx_df)

    if combined.empty:
        return None

    # 5. Add symbol column
    combined["symbol"] = symbol

    # 6. Rename to DB column names and select output columns
    combined = combined.rename(columns={"open_time": "timestamp"})

    out_cols = [c for c in OUTPUT_COLUMNS if c in combined.columns]
    return combined[out_cols].copy()


def _upload_features(symbol: str, pdf: pd.DataFrame, month_str: str) -> None:
    """Upload processed features to MinIO (monthly partition)."""
    key = features_key(symbol, month_str)

    for c in pdf.columns:
        if pd.api.types.is_datetime64_any_dtype(pdf[c]):
            pdf[c] = pdf[c].dt.as_unit("us")

    table = pa.Table.from_pandas(pdf, preserve_index=False)
    storage.upload_parquet(BUCKET_PROCESSED, key, table)


# --- Main Entry Point --------------------------------------------------------

def transform_data(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> str | None:
    """Incremental transform: compute indicators for new data only.

    If month_str is provided, processes only that month.
    Otherwise, auto-discovers all months with raw data per symbol.
    """
    symbols = symbols or SYMBOLS

    logger.info("Transform started: %d symbols, month=%s", len(symbols), month_str or "auto-discover")

    # Batch query: get last loaded timestamps from ClickHouse
    last_ts_map = get_last_timestamps(symbols)

    total_new_rows = 0
    symbols_updated = 0
    max_workers = min(TRANSFORM_MAX_WORKERS, len(symbols))

    def _do_symbol(symbol: str) -> tuple[str, int]:
        raw_ts = last_ts_map.get(symbol)
        last_loaded = pd.to_datetime(raw_ts, unit="ms", utc=True) if raw_ts else None

        if month_str:
            months = [month_str]
        elif last_loaded is not None:
            # Only check months that could have new data
            # (the month of last loaded data + current month)
            last_month = last_loaded.strftime("%Y-%m")
            current_month = pd.Timestamp.now(tz="UTC").strftime("%Y-%m")
            months = sorted(set([last_month, current_month]))
        else:
            # No data loaded yet -- discover all months (first run)
            months = discover_month_partitions(BUCKET_RAW, "klines", symbol)

        sym_rows = 0
        for month in months:
            result_df = _process_symbol(symbol, month, last_loaded)
            if result_df is not None and not result_df.empty:
                _upload_features(symbol, result_df, month)
                sym_rows += len(result_df)
                del result_df
        return symbol, sym_rows

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_do_symbol, sym): sym for sym in symbols}
        for future in as_completed(future_map):
            sym = future_map[future]
            try:
                symbol, n_rows = future.result()
                if n_rows > 0:
                    symbols_updated += 1
                    total_new_rows += n_rows
                    logger.info("[Transform] %s: +%s rows", symbol, f"{n_rows:,}")
            except Exception as exc:
                logger.error("[Transform] %s: ERROR -- %s", sym, exc)

    if total_new_rows == 0:
        logger.info("No new rows to transform")
        return None

    logger.info(
        "Transform complete: %s new rows across %d symbols",
        f"{total_new_rows:,}", symbols_updated,
    )
    return "features/"


# --- CLI ---------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Transform raw klines -> features Parquet",
    )
    parser.add_argument(
        "--symbols", nargs="+", metavar="SYM", default=None,
        help="process only these symbols",
    )
    parser.add_argument(
        "--month", type=str, default=None,
        help="process specific month (YYYY-MM, default: current month)",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    active_symbols = args.symbols or SYMBOLS

    logger.info("Transform started -- symbols=%d", len(active_symbols))

    exit_code = 0
    try:
        out_path = transform_data(symbols=active_symbols, month_str=args.month)
    except TransformError as exc:
        logger.error("Transform failed: %s", exc)
        exit_code = 1
    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
        exit_code = 1

    raise SystemExit(exit_code)
