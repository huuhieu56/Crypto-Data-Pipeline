"""Load transformed crypto news Parquet from MinIO into ClickHouse.

Read path:  crypto-processed/crypto_news/{YYYY-MM}.parquet
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

from config.config import MINIO_CONFIG
from scripts.load import _load_table
from utils.data_utils import validate_month_str

BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]


def load_news(month_str: str | None = None) -> None:
    """Load transformed news Parquet from MinIO → ClickHouse."""
    _load_table(
        None, BUCKET_PROCESSED, "crypto_news", "published_at",
        month_str=month_str, per_symbol=False, sub_prefix="gnews",
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
