"""Extract klines from Binance Data Vision and REST API."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from config.config import MINIO_CONFIG, MONTHS_BACK
from config.symbols import SYMBOLS, SYMBOLS_STATUS
from utils.binance_utils import download_klines_month, fetch_klines_paginated
from utils.data_utils import get_target_end, get_target_months
from utils.db_utils import get_last_timestamps
from utils.logger import get_logger
from utils.storage import append_to_partition_csv

logger = get_logger(__name__)
BUCKET_RAW = MINIO_CONFIG["bucket_raw"]
KLINE_INTERVAL_MS = 60_000


def _latest_closed_kline_open_time_ms(target_end: datetime) -> int:
    """Return the open timestamp of the latest fully closed 1m kline."""
    end_ms = int(target_end.timestamp() * 1000)
    return (end_ms // KLINE_INTERVAL_MS) * KLINE_INTERVAL_MS - KLINE_INTERVAL_MS


def _incremental_end_time_ms(symbol: str, target_end: datetime) -> int:
    """Resolve REST API endTime using kline open timestamps as watermarks."""
    if SYMBOLS_STATUS.get(symbol, "TRADING") == "TRADING":
        return _latest_closed_kline_open_time_ms(target_end)
    return int(target_end.timestamp() * 1000)


def download_data_vision(
    symbol: str,
    months: list[tuple[int, int]],
) -> int | None:
    """Download klines from Data Vision month-by-month.

    Each month is written to MinIO as CSV immediately after download.
    Writes monthly partitions: klines/{SYMBOL}/{YYYY-MM}.csv

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
                append_to_partition_csv(BUCKET_RAW, "klines", symbol, df, dedup_col="open_time")
                logger.info("[%s] %d-%02d: %s rows", symbol, y, m, f"{len(df):,}")
                total += len(df)
        except Exception as exc:
            logger.error("[%s] %d-%02d: ERROR %s", symbol, y, m, exc)

    logger.info(
        "[%s] Data Vision complete: %d/%d months, %s records",
        symbol, len(sorted_months), len(sorted_months), f"{total:,}",
    )
    return total if total > 0 else None


def extract_bulk(
    symbols: list[str] | None = None,
    months_back: int = MONTHS_BACK,
    *,
    log_context: str = "BULK",
) -> dict[str, int]:
    """Force re-download all symbols from Data Vision."""
    symbols = symbols or SYMBOLS
    target_months = get_target_months(months_back)
    results: dict[str, int] = {}

    logger.info(
        "=== %s: Data Vision bulk extract for %d symbols x %d months ===",
        log_context, len(symbols), months_back,
    )

    for sym in symbols:
        try:
            total = download_data_vision(sym, target_months)
            if total is not None:
                results[sym] = total
                logger.info("[%s] %s: %s records", log_context, sym, f"{total:,}")
        except Exception as exc:
            logger.error("[%s] %s: FAILED - %s", log_context, sym, exc)

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

    # Bootstrap: empty DB -> bulk download MONTHS_BACK from Data Vision
    symbols_needing_bootstrap = [s for s in symbols if s not in last_timestamps]
    if symbols_needing_bootstrap:
        logger.info(
            "=== Bootstrap: %d/%d symbols have no data - "
            "downloading %d months of history via Data Vision ===",
            len(symbols_needing_bootstrap), len(symbols), MONTHS_BACK,
        )
        extract_bulk(
            symbols_needing_bootstrap,
            months_back=MONTHS_BACK,
            log_context="BOOTSTRAP",
        )

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
            end_time = _incremental_end_time_ms(symbol, target_end)

            if last_ts >= end_time:
                continue

            new_df = fetch_klines_paginated(symbol, last_ts, end_time)
            if new_df is None or new_df.empty:
                continue

            # Append to CSV partitions grouped by open_time month
            append_to_partition_csv(BUCKET_RAW, "klines", symbol, new_df, dedup_col="open_time")
            results[symbol] = new_df
            logger.info(
                "[%d/%d] %s: +%d rows",
                idx, len(symbols), symbol, len(new_df),
            )
        except Exception as exc:
            logger.error("[%d/%d] %s: %s", idx, len(symbols), symbol, exc)

    return results
