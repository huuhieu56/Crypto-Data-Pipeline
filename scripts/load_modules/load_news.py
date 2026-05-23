"""Load transformed crypto news Parquet from MinIO into ClickHouse.

Read path:  crypto-processed/crypto_news/gnews/{YYYY-MM}.parquet
Target:     ClickHouse crypto_db.crypto_news table

Usage:
    python -m scripts.load_modules.load_news
    python -m scripts.load_modules.load_news --month 2026-05
"""

from __future__ import annotations

from config.config import MINIO_CONFIG
from scripts.load import load_table

BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]


def load_news(month_str: str | None = None) -> None:
    """Load transformed news Parquet from MinIO → ClickHouse.

    Uses extracted_at as watermark (not published_at) to avoid duplicates
    when re-processing articles with the same published_at timestamp.
    """
    load_table(
        None, BUCKET_PROCESSED, "crypto_news", "extracted_at",
        month_str=month_str, per_symbol=False, sub_prefix="gnews",
    )
