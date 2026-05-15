"""Transform script — compute technical indicators on raw klines (ELT).

Runs AFTER load_klines has inserted raw OHLCV into ClickHouse klines.
Reads raw rows from klines, computes RSI(14) + MACD(12,26,9) via
ClickHouse SQL, and INSERTs back into klines (ReplacingMergeTree
replaces raw rows with transformed ones).

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
    INDICATOR_CONTEXT_ROWS,
    MINIO_CONFIG,
)
from config.symbols import SYMBOLS
from utils.data_utils import validate_month_str
from utils.db_utils import get_ch_client, get_table_watermarks
from utils.exceptions import TransformError
from utils.logger import get_logger
from utils.storage import discover_month_partitions

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]


def transform_klines(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Compute RSI(14) + MACD(12,26,9) on raw klines via ClickHouse SQL (ELT).

    Reads raw rows from crypto_db.klines (loaded by load_klines), fetches
    context rows for indicator warm-up, computes indicators, and INSERTs
    back into klines. ReplacingMergeTree deduplicates by (symbol, timestamp).
    """
    symbols = symbols or SYMBOLS
    if month_str is not None:
        month_str = validate_month_str(month_str)
    wm_map = get_table_watermarks("klines", "timestamp", symbols)
    total_processed = 0
    errors = 0

    logger.info(
        "[Transform] klines: %d symbols, ClickHouse SQL in-DB compute",
        len(symbols),
    )

    sql_path = Path(__file__).resolve().parent.parent / "sql" / "transform_klines.sql"
    sql_template = sql_path.read_text()

    for symbol in symbols:
        watermark_ms = wm_map.get(symbol, 0)
        context_rows = INDICATOR_CONTEXT_ROWS

        months = (
            [month_str]
            if month_str
            else discover_month_partitions(BUCKET_RAW, "klines", symbol, extension=".csv")
        )
        if not months:
            logger.debug("[Transform] klines %s: no partitions found", symbol)
            continue

        if watermark_ms > 0:
            watermark_month = pd.to_datetime(watermark_ms, unit="ms", utc=True).strftime("%Y-%m")
            months = [m for m in months if m >= watermark_month]

        for month in months:
            month_int = int(month.replace("-", ""))

            sql = sql_template.format(
                symbol=symbol,
                month=month,
                month_int=month_int,
                watermark_ms=watermark_ms,
                context_rows=context_rows,
            )

            try:
                client = get_ch_client()
                client.query(sql)
                result = client.query(
                    f"SELECT count() AS n FROM crypto_db.klines FINAL "
                    f"WHERE symbol = '{symbol}' AND toYYYYMM(timestamp) = {month_int}"
                )
                n = result.result_rows[0][0] if result.result_rows else 0
                logger.info(
                    "[Transform] klines %s/%s: %d rows in partition",
                    symbol, month, n,
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
    p = argparse.ArgumentParser(description="Transform raw klines -> indicators (ELT)")
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
