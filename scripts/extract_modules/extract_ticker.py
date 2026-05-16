"""Extract ticker snapshots from Binance REST API.

Extract stage only: fetch raw data from Binance, filter by symbol,
store raw columns (camelCase) in MinIO. No merge, no rename, no
computation — those belong in Transform.
"""

from __future__ import annotations

import pandas as pd

from config.config import MINIO_CONFIG
from utils.binance_utils import get_book_ticker, get_ticker_24h
from utils.exceptions import ExtractError
from utils.logger import get_logger
from utils.storage import append_to_partition

logger = get_logger(__name__)
BUCKET_RAW = MINIO_CONFIG["bucket_raw"]


def extract_ticker_24h(symbols: list[str]) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Fetch ticker/24hr and bookTicker, store each as raw Parquet in MinIO.

    Returns (ticker_df, book_df) with raw Binance column names (camelCase).
    Merge + rename + computed columns happen in Transform.
    """
    if not symbols:
        return None, None

    symbols_set = set(symbols)

    try:
        ticker_raw = get_ticker_24h()
    except Exception as exc:
        raise ExtractError(f"Failed to fetch ticker/24hr: {exc}") from exc

    ticker_df = pd.DataFrame(ticker_raw)
    ticker_df = ticker_df[ticker_df["symbol"].isin(symbols_set)].copy()

    try:
        book_raw = get_book_ticker()
    except Exception as exc:
        raise ExtractError(f"Failed to fetch bookTicker: {exc}") from exc

    book_df = pd.DataFrame(book_raw)
    book_df = book_df[book_df["symbol"].isin(symbols_set)].copy()

    # Write raw ticker per symbol
    for symbol, group_df in ticker_df.groupby("symbol"):
        append_to_partition(
            BUCKET_RAW, "ticker_raw", symbol,
            group_df.reset_index(drop=True), dedup_col=None,
        )

    # Write raw book_ticker per symbol
    for symbol, group_df in book_df.groupby("symbol"):
        append_to_partition(
            BUCKET_RAW, "book_ticker_raw", symbol,
            group_df.reset_index(drop=True), dedup_col=None,
        )

    logger.info(
        "Saved ticker_raw (+%d) and book_ticker_raw (+%d), %d symbols",
        len(ticker_df), len(book_df), len(symbols_set),
    )
    return ticker_df, book_df
