"""Extract klines from Binance Data Vision and REST API."""

from __future__ import annotations

import pandas as pd
import pyarrow as pa

from config.config import MINIO_CONFIG, MONTHS_BACK
from config.symbols import SYMBOLS
from utils.binance_utils import download_klines_month, fetch_klines_paginated
from utils.data_utils import get_target_end, get_target_months, partition_key
from utils.db_utils import get_last_timestamps
from utils.logger import get_logger
from utils.storage import append_to_partition, storage

logger = get_logger(__name__)
BUCKET_RAW = MINIO_CONFIG["bucket_raw"]


def download_data_vision(
    symbol: str,
    months: list[tuple[int, int]],
) -> int | None:
    """Download klines from Data Vision month-by-month.

    Each month is written to MinIO immediately after download to avoid
    accumulating all months in memory.  Writes monthly partitions:
    klines/{SYMBOL}/{YYYY-MM}.parquet

    Returns total record count, or None if no data was downloaded.
    """
    sorted_months = sorted(months)
    if not sorted_months:
        return None

    total = 0
    for y, m in sorted_months:
        try:
            df = download_klines_month(symbol, y, m)
            if df is not None and not df.empty:
                month_str = f"{y}-{m:02d}"
                key = partition_key(symbol, month_str)
                df = df.sort_values("open_time")
                for c in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[c]):
                        df[c] = df[c].dt.as_unit("us")
                table = pa.Table.from_pandas(df, preserve_index=False)
                storage.upload_parquet(BUCKET_RAW, key, table)
                logger.info("[%s] %d-%02d: %s rows", symbol, y, m, f"{len(df):,}")
                total += len(df)
        except Exception as exc:
            logger.error("[%s] %d-%02d: ERROR %s", symbol, y, m, exc)

    logger.info(
        "[%s] Bulk complete: %d/%d months, %s records",
        symbol, len(sorted_months), len(sorted_months), f"{total:,}",
    )
    return total if total > 0 else None


def extract_bulk(
    symbols: list[str] | None = None,
    months_back: int = MONTHS_BACK,
) -> dict[str, int]:
    """Force re-download all symbols from Data Vision."""
    symbols = symbols or SYMBOLS
    target_months = get_target_months(months_back)
    results: dict[str, int] = {}

    logger.info(
        "=== Bulk Extract: %d symbols x %d months ===",
        len(symbols), months_back,
    )

    for sym in symbols:
        try:
            total = download_data_vision(sym, target_months)
            if total is not None:
                results[sym] = total
                logger.info("[%s] %s records", sym, f"{total:,}")
        except Exception as exc:
            logger.error("[%s] FAILED - %s", sym, exc)

    return results


def extract_recent_klines(
    symbols: list[str],
) -> dict[str, pd.DataFrame]:
    """Incremental klines update via REST API.

    On first run (empty DB): auto-bootstraps by downloading historical data
    via Data Vision, then fills the gap to present via REST API.
    """
    if not symbols:
        return {}

    results: dict[str, pd.DataFrame] = {}

    # 1 batch query for all symbols instead of N file downloads
    last_timestamps = get_last_timestamps(symbols)

    # --- Bootstrap: empty DB -> bulk download 3 years from Data Vision -------
    symbols_needing_bootstrap = [s for s in symbols if s not in last_timestamps]
    if symbols_needing_bootstrap:
        logger.info(
            "=== Bootstrap: %d/%d symbols have no data - "
            "downloading %d months of history via Data Vision ===",
            len(symbols_needing_bootstrap), len(symbols), MONTHS_BACK,
        )
        target_months = get_target_months(MONTHS_BACK)

        for sym in symbols_needing_bootstrap:
            try:
                total = download_data_vision(sym, target_months)
                if total is not None:
                    logger.info("[BOOTSTRAP] %s: %s records", sym, f"{total:,}")
            except Exception as exc:
                logger.error("[BOOTSTRAP] %s: FAILED - %s", sym, exc)

        # Refresh watermarks after bulk download
        last_timestamps = get_last_timestamps(symbols)
        logger.info("=== Bootstrap complete - filling gap to present ===")

    # --- Incremental: fill from last watermark to now ------------------------
    for idx, symbol in enumerate(symbols, 1):
        try:
            last_ts = last_timestamps.get(symbol)
            if last_ts is None:
                continue

            target_end = get_target_end(symbol)
            end_time = int(target_end.timestamp() * 1000)

            if last_ts >= end_time:
                continue

            new_df = fetch_klines_paginated(symbol, last_ts, end_time)
            if new_df is None or new_df.empty:
                continue

            append_to_partition(BUCKET_RAW, "klines", symbol, new_df, dedup_col="open_time")
            results[symbol] = new_df
            logger.info(
                "[%d/%d] %s: +%d rows",
                idx, len(symbols), symbol, len(new_df),
            )
        except Exception as exc:
            logger.error("[%d/%d] %s: %s", idx, len(symbols), symbol, exc)

    return results
