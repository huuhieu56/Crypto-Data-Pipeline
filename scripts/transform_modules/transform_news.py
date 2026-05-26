"""Transform GNews crypto news — extract entities, deduplicate, filter.

Raw path:   crypto-raw/crypto_news/gnews/{YYYY-MM}.parquet
Output path: crypto-processed/crypto_news/gnews/{YYYY-MM}.parquet

Usage:
    python -m scripts.transform_modules.transform_news
    python -m scripts.transform_modules.transform_news --symbols BTCUSDT ETHUSDT
    python -m scripts.transform_modules.transform_news --month 2026-05
"""

from __future__ import annotations

import re
import argparse

import pandas as pd

from config.config import MINIO_CONFIG
from config.symbols import CRYPTO_ALIASES
from utils.data_utils import validate_month_str
from utils.exceptions import TransformError
from utils.logger import get_logger
from utils.news_filters import filter_dataframe
from utils.storage import append_to_partition, discover_month_partitions, read_month_data, storage

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]
BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]

# Pre-compile alias patterns once (22 aliases × per-article regex avoided)
_ALIAS_PATTERNS = [
    (re.compile(rf"\b{re.escape(alias)}\b", re.IGNORECASE), symbol)
    for alias, symbol in CRYPTO_ALIASES.items()
]


def extract_symbols(text: str) -> list[str]:
    """Extract tracked crypto symbols mentioned in text."""
    if not isinstance(text, str):
        return []
    found = set()
    for pattern, symbol in _ALIAS_PATTERNS:
        if pattern.search(text):
            found.add(symbol)
    return sorted(found)


def transform_news(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """ETL: transform GNews articles → Parquet.

    Reads raw from MinIO (crypto_news/gnews/), extracts mentioned crypto
    symbols, deduplicates by article_id, filters spam, and writes
    transformed Parquet to crypto_news/ in processed bucket.
    """
    if month_str is not None:
        month_str = validate_month_str(month_str)

    target_cols = [
        "article_id", "title", "description", "content",
        "url", "image_url", "source_name", "source_url",
        "published_at", "search_query", "extracted_at",
        "symbols",
    ]
    total_processed = 0
    errors = 0

    logger.info("[Transform] crypto_news: start")

    raw_keys = storage.list_objects(BUCKET_RAW, prefix="crypto_news/gnews/")

    months = (
        [month_str]
        if month_str
        else discover_month_partitions(BUCKET_RAW, "crypto_news", "gnews", keys=raw_keys)
    )
    if not months:
        logger.info("[Transform] crypto_news: no raw partitions found")
        return

    for month in months:
        try:
            df = read_month_data(BUCKET_RAW, "crypto_news", "gnews", month, keys=raw_keys)
            if df.empty:
                continue

            before_dedup = len(df)

            # Deduplicate by article_id
            df = df.drop_duplicates(subset=["article_id"])

            # Filter spam, irrelevant, low-quality articles
            before_filter = len(df)
            df = filter_dataframe(df)
            if df.empty:
                logger.debug("[Transform] crypto_news/%s: all articles filtered out", month)
                continue
            if len(df) < before_filter:
                logger.info("[Transform] crypto_news/%s: filtered %d → %d articles",
                           month, before_filter, len(df))

            # Extract mentioned symbols from title + description + content
            df["symbols"] = (
                df["title"].fillna("") + " "
                + df["description"].fillna("") + " "
                + df["content"].fillna("")
            ).apply(extract_symbols)

            # Filter if specific symbols requested
            if symbols:
                symbol_set = set(symbols)
                df = df[df["symbols"].apply(lambda s: bool(symbol_set & set(s)))]
                if df.empty:
                    logger.debug("[Transform] crypto_news/%s: no articles match symbols", month)
                    continue

            # Ensure target columns exist
            for col in target_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[target_cols]

            append_to_partition(
                BUCKET_PROCESSED, "crypto_news", "gnews",
                df, dedup_col="article_id", month_str=month,
            )
            total_processed += 1
            logger.info(
                "[Transform] crypto_news/%s: %d rows (dedup: %d → %d)",
                month, len(df), before_dedup, len(df),
            )

        except Exception as exc:
            errors += 1
            logger.error("[Transform] crypto_news/%s: ERROR -- %s", month, exc)

    if total_processed == 0 and errors > 0:
        raise TransformError(f"crypto_news: all {errors} partition(s) failed")
    logger.info("[Transform] crypto_news complete: %d partitions, %d errors", total_processed, errors)


# --- CLI ---------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Transform GNews crypto news")
    p.add_argument(
        "--symbols", nargs="*", default=None,
        help="Filter to articles mentioning these symbols",
    )
    p.add_argument(
        "--month", type=validate_month_str, default=None,
        help="Process specific month (YYYY-MM)",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    transform_news(symbols=args.symbols, month_str=args.month)


if __name__ == "__main__":
    main()
