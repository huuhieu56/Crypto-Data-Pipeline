"""Load script — write transformed data to ClickHouse (ETL: load after transform).

All tables: transformed Parquet from MinIO → ClickHouse.

Usage:
    python scripts/load.py
    python scripts/load.py --only klines
    python scripts/load.py --skip ticker orderbook
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import pandas as pd

from config.config import MINIO_CONFIG
from config.symbols import SYMBOLS, SYMBOL_REGISTRY
from utils.logger import get_logger
from utils.exceptions import LoadError
from utils.data_utils import validate_month_str
from utils.db_utils import (
    ch_insert_df,
    get_table_watermarks,
)
from utils.storage import storage, discover_month_partitions

logger = get_logger(__name__)

BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]


# --- Helpers ------------------------------------------------------------------

def _filter_by_watermark(
    df: pd.DataFrame,
    ts_col_name: str,
    watermark_ms: int | None,
) -> pd.DataFrame:
    """Filter DataFrame to only rows after the watermark timestamp."""
    if watermark_ms is None:
        return df
    cutoff = pd.to_datetime(watermark_ms, unit="ms", utc=True)
    ts_col = pd.to_datetime(df[ts_col_name])
    if ts_col.dt.tz is None:
        ts_col = ts_col.dt.tz_localize("UTC")
    return df[ts_col > cutoff]


# --- Generic Loader -----------------------------------------------------------

def _load_table(
    symbols: list[str] | None,
    bucket: str,
    table_name: str,
    ts_col: str,
    month_str: str | None = None,
) -> None:
    """Generic loader: download Parquet from MinIO, filter by watermark, insert into ClickHouse."""
    if symbols is None:
        symbols = SYMBOLS
    if month_str is not None:
        month_str = validate_month_str(month_str)
    wm_map = get_table_watermarks(table_name, ts_col, symbols)
    total_inserted = 0
    total_skipped = 0
    errors = 0

    logger.info("[Load] %s: %d symbols, partitions=%s", table_name, len(symbols), month_str or "auto-discover")

    for symbol in symbols:
        months = (
            [month_str]
            if month_str
            else discover_month_partitions(bucket, table_name, symbol)
        )
        if not months:
            logger.debug("[Load] %s %s: no partitions found", table_name, symbol)
            continue

        for month in months:
            key = f"{table_name}/{symbol}/{month}.parquet"
            try:
                table = storage.download_parquet(bucket, key)
                df = table.to_pandas()
                del table

                if df.empty:
                    logger.debug("[Load] %s %s/%s: empty, skipped", table_name, symbol, month)
                    total_skipped += 1
                    continue

                raw_count = len(df)
                df = _filter_by_watermark(df, ts_col, wm_map.get(symbol))
                if df.empty:
                    logger.debug("[Load] %s %s/%s: %d rows filtered (watermark), skipped", table_name, symbol, month, raw_count)
                    total_skipped += 1
                    continue

                inserted = ch_insert_df(table_name, df)
                total_inserted += inserted
                logger.info("[Load] %s %s/%s: %d rows inserted", table_name, symbol, month, inserted)

            except Exception as exc:
                errors += 1
                logger.error("[Load] %s %s/%s: ERROR -- %s", table_name, symbol, month, exc)

    if total_inserted == 0 and errors > 0:
        raise LoadError(f"{table_name}: all {errors} partition(s) failed, 0 loaded")
    logger.info("[Load] %s complete: %s inserted, %d skipped, %d errors", table_name, f"{total_inserted:,}", total_skipped, errors)


# --- Load Functions -----------------------------------------------------------

def load_symbols() -> None:
    """Load symbols into ClickHouse from SYMBOL_REGISTRY."""
    logger.info("Loading symbols into database")
    try:
        records = []
        for sym, info in SYMBOL_REGISTRY.items():
            quote = "USDT" if sym.endswith("USDT") else ""
            base = sym[: -len(quote)] if quote else sym
            records.append({
                "symbol": sym,
                "base_asset": base,
                "quote_asset": quote,
                "status": info.get("status", "TRADING"),
            })
        df = pd.DataFrame(records)

        if "created_at" in df.columns:
            df = df.drop(columns=["created_at"])

        inserted = ch_insert_df("symbols", df)
        logger.info("Loaded %d symbols", inserted)
    except Exception as exc:
        raise LoadError(f"Failed to load symbols: {exc}") from exc


def load_klines(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Load transformed klines Parquet from MinIO → ClickHouse."""
    _load_table(symbols, BUCKET_PROCESSED, "klines", "open_time", month_str=month_str)


def load_ticker(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Load transformed ticker Parquet from MinIO → ClickHouse."""
    _load_table(symbols, BUCKET_PROCESSED, "ticker_24h", "snapshot_time", month_str=month_str)


def load_order_book(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Load transformed order book Parquet from MinIO → ClickHouse."""
    _load_table(symbols, BUCKET_PROCESSED, "order_book_snapshot", "timestamp", month_str=month_str)


# --- CLI ---------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load data into ClickHouse")
    p.add_argument(
        "--only", nargs="+",
        choices=["symbols", "klines", "ticker", "orderbook", "news"],
        help="Load only these tables",
    )
    p.add_argument(
        "--skip", nargs="*", default=[],
        choices=["symbols", "klines", "ticker", "orderbook", "news"],
        help="Skip these tables",
    )
    p.add_argument(
        "--month", type=validate_month_str, default=None,
        help="Process specific month (YYYY-MM). Default: auto-discover all months.",
    )
    return p


def main(
    only: set[str] | None = None,
    skip: set[str] | None = None,
    month_str: str | None = None,
) -> None:
    skip = skip or set()

    logger.info("=== Load pipeline started ===")

    def should_load(name: str) -> bool:
        if only and name not in only:
            return False
        return name not in skip

    if should_load("symbols"):
        load_symbols()
    if should_load("ticker"):
        load_ticker(month_str=month_str)
    if should_load("orderbook"):
        load_order_book(month_str=month_str)
    if should_load("klines"):
        load_klines(month_str=month_str)
    if should_load("news"):
        from scripts.load_modules.load_news import load_news
        load_news(month_str=month_str)

    logger.info("=== Load pipeline complete ===")


if __name__ == "__main__":
    args = _build_parser().parse_args()
    main(
        only=set(args.only) if args.only else None,
        skip=set(args.skip),
        month_str=args.month,
    )
