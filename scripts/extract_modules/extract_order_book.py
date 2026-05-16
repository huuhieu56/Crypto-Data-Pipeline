"""Extract order book snapshots from Binance REST API.

Extract stage: fetch raw depth data, store bids/asks arrays in MinIO.
Volume aggregation + imbalance computation happens in Load/Transform.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from config.config import MINIO_CONFIG, ORDER_BOOK_LIMIT
from utils.binance_utils import get_order_book, sleep_between_requests
from utils.logger import get_logger
from utils.storage import append_to_partition

logger = get_logger(__name__)
BUCKET_RAW = MINIO_CONFIG["bucket_raw"]


def extract_order_book_snapshot(symbols: list[str]) -> pd.DataFrame | None:
    """Fetch order book depth and store raw bids/asks in MinIO.

    Returns raw DataFrame with columns: symbol, timestamp, bids, asks.
    Volume aggregation + imbalance computation happens in Transform.
    """
    if not symbols:
        return None

    timestamp = datetime.now(timezone.utc)
    records = []

    for symbol in symbols:
        try:
            data = get_order_book(symbol, limit=ORDER_BOOK_LIMIT)
            records.append({
                "symbol": symbol,
                "timestamp": timestamp,
                "bids": data.get("bids", []),
                "asks": data.get("asks", []),
            })
        except Exception as exc:
            logger.error("Order book failed for %s: %s", symbol, exc)

    sleep_between_requests()

    if not records:
        logger.error("No order book data collected")
        return None

    df = pd.DataFrame(records)

    for symbol, group_df in df.groupby("symbol"):
        append_to_partition(
            BUCKET_RAW, "order_book", symbol,
            group_df.reset_index(drop=True), dedup_col="timestamp",
        )
    logger.info("Saved order_book raw (+%d records, %d symbols)", len(df), df["symbol"].nunique())
    return df


def _is_float(s: str) -> bool:
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False
