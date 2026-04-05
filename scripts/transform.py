"""Transform raw klines → features with technical indicators.

Reads daily partitions from MinIO, computes RSI(14) and MACD(12,26,9)
using ClickHouse context for warm-up, outputs with DB column names.

Usage:
    python scripts/transform.py
    python scripts/transform.py --symbols BTCUSDT ETHUSDT
    python scripts/transform.py --full-rebuild
"""

from __future__ import annotations

import sys
from pathlib import Path
import argparse
import gc
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
import pyarrow as pa

from config.config import (
    MINIO_CONFIG, PARALLELISM, INDICATOR_CONTEXT_ROWS,
    PARTITION_DATE_FORMAT, KLINES_COLUMNS,
)
from config.symbols import SYMBOLS
from utils.logger import get_logger
from utils.exceptions import TransformError
from utils.data_utils import partition_key, delta_key
from utils.db_utils import ch_query_df, get_last_timestamps
from utils.storage import storage

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]
BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]
TRANSFORM_MAX_WORKERS = PARALLELISM["transform_max_workers"]

# Output columns — already in DB column names
OUTPUT_COLUMNS = [
    "symbol", "timestamp", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "rsi_14", "macd", "macd_signal",
]


def calculate_indicators(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute RSI(14) and MACD(12,26,9). Output uses DB column names."""
    pdf = pdf.sort_values("open_time")
    close = pdf["close"]

    # RSI (14)
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    loss = loss.replace(0, np.nan)
    rs = gain / loss
    pdf["rsi_14"] = 100 - (100 / (1 + rs))

    # MACD (12, 26, 9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    pdf["macd"] = ema12 - ema26
    pdf["macd_signal"] = pdf["macd"].ewm(span=9, adjust=False).mean()

    # Fill NaN
    pdf = pdf.ffill().bfill().fillna(0)
    return pdf


def _get_ch_context(symbol: str, n_rows: int = INDICATOR_CONTEXT_ROWS) -> pd.DataFrame:
    """Query the last n_rows klines from ClickHouse for indicator warm-up.

    Returns DataFrame with raw column names (open_time as timestamp alias).
    """
    try:
        df = ch_query_df(
            f"SELECT timestamp AS open_time, open, high, low, close, volume, "
            f"quote_volume, trades "
            f"FROM klines "
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


def _read_today_partition(symbol: str, date_str: str) -> pd.DataFrame:
    """Read today's raw partition from MinIO."""
    key = partition_key(symbol, date_str)
    if not storage.object_exists(BUCKET_RAW, key):
        return pd.DataFrame()

    table = storage.download_parquet(BUCKET_RAW, key)
    pdf = table.to_pandas()
    del table
    pdf["open_time"] = pd.to_datetime(pdf["open_time"], utc=False)
    return pdf.sort_values("open_time").reset_index(drop=True)


def _process_symbol(
    symbol: str,
    date_str: str,
    last_loaded_ts: pd.Timestamp | None,
) -> pd.DataFrame | None:
    """Read raw partition + CH context, compute indicators, return new rows only."""

    # 1. Read today's partition
    raw_df = _read_today_partition(symbol, date_str)
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

    # 4. Concat context + raw for indicator calculation
    raw_cols = list(raw_df.columns)
    if not ctx_df.empty:
        ctx_trimmed = ctx_df[[c for c in raw_cols if c in ctx_df.columns]].copy()
        combined = pd.concat([ctx_trimmed, raw_df], ignore_index=True)
    else:
        combined = raw_df.copy()

    combined = combined.sort_values("open_time").drop_duplicates(
        subset=["open_time"], keep="last"
    ).reset_index(drop=True)
    combined["symbol"] = symbol

    # 5. Calculate indicators
    combined = calculate_indicators(combined)

    # 6. Keep only new rows (after last loaded)
    if last_loaded_ts is not None:
        cmp_ts = last_loaded_ts.tz_localize(None) if last_loaded_ts.tzinfo else last_loaded_ts
        combined = combined[combined["open_time"] > cmp_ts]

    if combined.empty:
        return None

    # 7. Rename to DB column names and select output columns
    combined = combined.rename(columns={"open_time": "timestamp"})

    out_cols = [c for c in OUTPUT_COLUMNS if c in combined.columns]
    return combined[out_cols].copy()


def _upload_delta(symbol: str, pdf: pd.DataFrame, date_str: str) -> None:
    """Upload processed delta to MinIO."""
    key = delta_key(symbol, date_str)

    for c in pdf.columns:
        if pd.api.types.is_datetime64_any_dtype(pdf[c]):
            pdf[c] = pdf[c].dt.as_unit("us")

    table = pa.Table.from_pandas(pdf, preserve_index=False)
    storage.upload_parquet(BUCKET_PROCESSED, key, table)


# --- Main Entry Points -------------------------------------------------------

def transform_data(
    symbols: list[str] | None = None,
    date_str: str | None = None,
) -> str | None:
    """Incremental transform: compute indicators for new data only."""
    symbols = symbols or SYMBOLS
    date_str = date_str or datetime.now(timezone.utc).strftime(PARTITION_DATE_FORMAT)

    logger.info("Transform started: %d symbols, date=%s", len(symbols), date_str)

    # Batch query: get last loaded timestamps from ClickHouse
    last_ts_map = get_last_timestamps(symbols)

    # Bootstrap mode: if klines table is empty, transform full history once.
    if not last_ts_map:
        logger.warning(
            "ClickHouse has no klines data for selected symbols; "
            "switching to full history transform"
        )
        return transform_full_rebuild(symbols=symbols)

    total_new_rows = 0
    symbols_updated = 0
    n_symbols = len(symbols)
    max_workers = min(TRANSFORM_MAX_WORKERS, n_symbols)

    def _do_symbol(symbol: str) -> tuple[str, pd.DataFrame | None]:
        raw_ts = last_ts_map.get(symbol)
        last_loaded = pd.to_datetime(raw_ts, unit="ms", utc=True) if raw_ts else None
        return symbol, _process_symbol(symbol, date_str, last_loaded)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_do_symbol, sym): sym for sym in symbols}
        for future in as_completed(future_map):
            sym = future_map[future]
            try:
                symbol, result_df = future.result()
                if result_df is not None and not result_df.empty:
                    _upload_delta(symbol, result_df, date_str)
                    symbols_updated += 1
                    n_rows = len(result_df)
                    total_new_rows += n_rows
                    logger.info("[Transform] %s: +%s rows", symbol, f"{n_rows:,}")
                    del result_df
            except Exception as exc:
                logger.error("[Transform] %s: ERROR — %s", sym, exc)

    if total_new_rows == 0:
        logger.info("No new rows to transform")
        return None

    logger.info(
        "Transform complete: %s new rows across %d symbols",
        f"{total_new_rows:,}", symbols_updated,
    )
    return "features_delta/"


def transform_full_rebuild(symbols: list[str] | None = None) -> str | None:
    """Full rebuild: re-process all symbols from all raw partitions."""
    symbols = symbols or SYMBOLS
    logger.info("Full transform rebuild: %d symbols", len(symbols))

    total_rows = 0
    for idx, symbol in enumerate(symbols, 1):
        # List all partition files for this symbol
        prefix = f"klines/{symbol}/"
        keys = storage.list_objects(BUCKET_RAW, prefix)
        parquet_keys = sorted(k for k in keys if k.endswith(".parquet"))

        if not parquet_keys:
            continue

        # Read and concat all partitions
        dfs = []
        for key in parquet_keys:
            try:
                table = storage.download_parquet(BUCKET_RAW, key)
                dfs.append(table.to_pandas())
                del table
            except Exception as exc:
                logger.error("[Rebuild] %s/%s: %s", symbol, key, exc)

        if not dfs:
            continue

        pdf = pd.concat(dfs, ignore_index=True)
        del dfs

        pdf["open_time"] = pd.to_datetime(pdf["open_time"], utc=False)
        pdf = pdf.sort_values("open_time").drop_duplicates(
            subset=["open_time"], keep="last"
        ).reset_index(drop=True)
        pdf["symbol"] = symbol

        # Calculate indicators
        pdf = calculate_indicators(pdf)

        # Rename and select output columns
        pdf = pdf.rename(columns={"open_time": "timestamp"})
        out_cols = [c for c in OUTPUT_COLUMNS if c in pdf.columns]
        pdf = pdf[out_cols]

        # Upload as single features file (for full rebuild)
        for c in pdf.columns:
            if pd.api.types.is_datetime64_any_dtype(pdf[c]):
                pdf[c] = pdf[c].dt.as_unit("us")

        table = pa.Table.from_pandas(pdf, preserve_index=False)
        storage.upload_parquet(BUCKET_PROCESSED, f"features/{symbol}.parquet", table)

        rows = len(pdf)
        total_rows += rows
        logger.info("[Rebuild %d/%d] %s: %s rows", idx, len(symbols), symbol, f"{rows:,}")

        del pdf
        gc.collect()

    if total_rows == 0:
        raise TransformError("No raw data found for rebuild")

    logger.info("Transform complete (full rebuild): %s total rows", f"{total_rows:,}")
    return "features/"


# --- CLI ---------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Transform raw klines → features Parquet",
    )
    parser.add_argument(
        "--symbols", nargs="+", metavar="SYM", default=None,
        help="process only these symbols",
    )
    parser.add_argument(
        "--full-rebuild", action="store_true",
        help="force full rebuild from all raw data",
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="process specific date (YYYY-MM-DD, default: today)",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    active_symbols = args.symbols or SYMBOLS

    logger.info("Transform started — symbols=%d", len(active_symbols))

    exit_code = 0
    try:
        if args.full_rebuild:
            out_path = transform_full_rebuild(symbols=active_symbols)
        else:
            out_path = transform_data(symbols=active_symbols, date_str=args.date)
    except TransformError as exc:
        logger.error("Transform failed: %s", exc)
        exit_code = 1
    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
        exit_code = 1

    raise SystemExit(exit_code)