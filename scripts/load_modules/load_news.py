"""Load transformed crypto news Parquet from MinIO into ClickHouse.

Read path:  crypto-processed/crypto_news/gnews/{YYYY-MM}.parquet
Target:     ClickHouse crypto_db.crypto_news table

Usage:
    python -m scripts.load_modules.load_news
    python -m scripts.load_modules.load_news --month 2026-05
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import argparse

import pandas as pd

from config.config import MINIO_CONFIG
from utils.data_utils import validate_month_str
from utils.db_utils import ch_insert_df, ch_query_df
from utils.exceptions import LoadError
from utils.logger import get_logger
from utils.storage import discover_month_partitions, storage

logger = get_logger(__name__)

BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]


def _get_watermark() -> pd.Timestamp | None:
    """Get max published_at from ClickHouse crypto_news table."""
    try:
        df = ch_query_df("SELECT max(published_at) AS max_ts FROM crypto_news")
        if df.empty or pd.isna(df.iloc[0]["max_ts"]):
            return None
        ts = pd.Timestamp(df.iloc[0]["max_ts"])
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts
    except Exception as exc:
        logger.warning("Could not get watermark: %s", exc)
        return None


def load_news(month_str: str | None = None) -> None:
    """Load transformed news Parquet from MinIO → ClickHouse.

    Reads from crypto-processed/crypto_news/gnews/, filters by watermark,
    and inserts into ClickHouse crypto_news table.
    """
    if month_str is not None:
        month_str = validate_month_str(month_str)

    watermark = _get_watermark()
    if watermark:
        logger.info("[Load] crypto_news: watermark = %s", watermark)

    months = (
        [month_str]
        if month_str
        else discover_month_partitions(BUCKET_PROCESSED, "crypto_news", "gnews")
    )
    if not months:
        logger.info("[Load] crypto_news: no partitions found")
        return

    total_inserted = 0
    total_skipped = 0
    errors = 0

    logger.info("[Load] crypto_news: %d partition(s)", len(months))

    for month in months:
        key = f"crypto_news/gnews/{month}.parquet"
        try:
            if not storage.object_exists(BUCKET_PROCESSED, key):
                logger.debug("[Load] crypto_news/%s: no data", month)
                total_skipped += 1
                continue

            df = storage.download_parquet(BUCKET_PROCESSED, key).to_pandas()
            if df.empty:
                total_skipped += 1
                continue

            raw_count = len(df)

            # Filter by watermark
            if watermark:
                ts = pd.to_datetime(df["published_at"], utc=True)
                df = df[ts > watermark]
                if df.empty:
                    logger.debug(
                        "[Load] crypto_news/%s: %d rows filtered (watermark)",
                        month, raw_count,
                    )
                    total_skipped += 1
                    continue

            inserted = ch_insert_df("crypto_news", df)
            total_inserted += inserted
            logger.info("[Load] crypto_news/%s: %d rows inserted", month, inserted)

        except Exception as exc:
            errors += 1
            logger.error("[Load] crypto_news/%s: ERROR -- %s", month, exc)

    if total_inserted == 0 and errors > 0:
        raise LoadError(f"crypto_news: all {errors} partition(s) failed, 0 loaded")
    logger.info(
        "[Load] crypto_news complete: %s inserted, %d skipped, %d errors",
        f"{total_inserted:,}", total_skipped, errors,
    )


# --- CLI ---------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load crypto news into ClickHouse")
    p.add_argument(
        "--month", type=validate_month_str, default=None,
        help="Process specific month (YYYY-MM)",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    load_news(month_str=args.month)


if __name__ == "__main__":
    main()
