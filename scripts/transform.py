"""Transform script — compute technical indicators on raw klines.

Reads raw CSV from MinIO, computes RSI(14) + MACD(12,26,9) via
ClickHouse SQL, and writes processed Parquet to MinIO processed bucket.

Usage:
    python scripts/transform.py
    python scripts/transform.py --symbols BTCUSDT ETHUSDT
    python scripts/transform.py --month 2026-05
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse

import pandas as pd

from config.config import (
    MINIO_CONFIG,
    CLICKHOUSE_S3_ENDPOINT,
    INDICATOR_CONTEXT_ROWS,
)
from config.symbols import SYMBOLS
from utils.data_utils import validate_month_str
from utils.db_utils import get_ch_client, get_table_watermarks
from utils.exceptions import TransformError
from utils.logger import get_logger
from utils.storage import storage, discover_month_partitions

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]
BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]


def transform_klines(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Compute RSI(14) + MACD(12,26,9) on raw klines via ClickHouse SQL.

    Reads raw CSV from MinIO, fetches context rows from ClickHouse for
    indicator warm-up, computes indicators, and writes processed Parquet
    to MinIO processed bucket.
    """
    symbols = symbols or SYMBOLS
    if month_str is not None:
        month_str = validate_month_str(month_str)
    wm_map = get_table_watermarks("klines", "timestamp", symbols)
    total_processed = 0
    errors = 0

    logger.info(
        "[Transform] klines: %d symbols, ClickHouse SQL -> MinIO processed",
        len(symbols),
    )

    sql_path = Path(__file__).resolve().parent.parent / "sql" / "transform_klines.sql"
    sql_template = sql_path.read_text()

    s3_access = MINIO_CONFIG["access_key"]
    s3_secret = MINIO_CONFIG["secret_key"]

    for symbol in symbols:
        watermark_ms = wm_map.get(symbol, 0)
        context_rows = INDICATOR_CONTEXT_ROWS

        months = (
            [month_str]
            if month_str
            else discover_month_partitions(BUCKET_RAW, "klines", symbol, extension=".csv")
        )
        if not months:
            logger.debug("[Transform] klines %s: no raw CSV partitions found", symbol)
            continue

        if watermark_ms > 0:
            watermark_month = pd.to_datetime(watermark_ms, unit="ms", utc=True).strftime("%Y-%m")
            months = [m for m in months if m >= watermark_month]

        for month in months:
            raw_key = f"klines/{symbol}/{month}.csv"
            if not storage.object_exists(BUCKET_RAW, raw_key):
                logger.debug("[Transform] klines %s/%s: no raw CSV, skipped", symbol, month)
                continue

            processed_key = f"klines/{symbol}/{month}.parquet"

            sql = sql_template.format(
                symbol=symbol,
                month=month,
                watermark_ms=watermark_ms,
                context_rows=context_rows,
                bucket_raw=BUCKET_RAW,
                bucket_processed=BUCKET_PROCESSED,
                s3_endpoint=CLICKHOUSE_S3_ENDPOINT,
                s3_access_key=s3_access,
                s3_secret_key=s3_secret,
            )

            try:
                client = get_ch_client()
                client.query(sql)
                if storage.object_exists(BUCKET_PROCESSED, processed_key):
                    logger.info(
                        "[Transform] klines %s/%s: processed Parquet written",
                        symbol, month,
                    )
                else:
                    logger.info(
                        "[Transform] klines %s/%s: no new rows (up to date)",
                        symbol, month,
                    )
                total_processed += 1
            except Exception as exc:
                errors += 1
                logger.error(
                    "[Transform] klines %s/%s: ERROR -- %s", symbol, month, exc,
                )

    if total_processed == 0 and errors > 0:
        raise TransformError(f"klines: all {errors} partition(s) failed, 0 transformed")
    logger.info(
        "[Transform] klines complete: %d partitions processed, %d errors",
        total_processed, errors,
    )


# --- CLI ---------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Transform raw klines -> indicators")
    p.add_argument(
        "--symbols", nargs="*", default=None,
        help="Symbols to transform (default: all 50 coins)",
    )
    p.add_argument(
        "--month", type=validate_month_str, default=None,
        help="Process specific month (YYYY-MM). Default: auto-discover all months.",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    transform_klines(
        symbols=args.symbols or None,
        month_str=args.month,
    )


if __name__ == "__main__":
    main()
