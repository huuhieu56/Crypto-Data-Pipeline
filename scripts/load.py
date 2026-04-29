"""Load script — write processed data to ClickHouse.

Reads monthly features Parquet from MinIO, batch inserts
into ClickHouse via native clickhouse-connect protocol.

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
from utils.db_utils import init_schema, ch_insert_df, get_table_watermarks
from utils.storage import storage

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]
BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]


# --- Load Functions ----------------------------------------------------------

def load_symbols() -> None:
    """Load symbols into ClickHouse from SYMBOL_REGISTRY."""
    logger.info("Loading symbols into database")
    try:
        # Check MinIO first
        key = "symbols.json"
        if storage.object_exists(BUCKET_RAW, key):
            data = storage.download_json(BUCKET_RAW, key)
            if isinstance(data, dict) and "symbols" in data:
                df = pd.DataFrame(data["symbols"])
            else:
                df = pd.DataFrame(data)
            target_cols = ["symbol", "base_asset", "quote_asset", "status"]
            df = df[[c for c in target_cols if c in df.columns]]
        else:
            # Generate from SYMBOL_REGISTRY
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


def _discover_months(bucket: str, prefix: str, symbol: str) -> list[str]:
    """Find all month partitions for a symbol in MinIO."""
    keys = storage.list_objects(bucket, prefix=f"{prefix}/{symbol}/")
    months = []
    for k in keys:
        if k.endswith(".parquet"):
            month = k.split("/")[-1].replace(".parquet", "")
            months.append(month)
    return sorted(months)


# --- Loaders -----------------------------------------------------------------


def load_klines(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Load klines features into ClickHouse (per-partition to avoid OOM)."""
    symbols = symbols or SYMBOLS
    wm_map = get_table_watermarks("klines", "timestamp", symbols)

    total_inserted = 0
    errors = 0

    for symbol in symbols:
        months = [month_str] if month_str else _discover_months(BUCKET_PROCESSED, "features", symbol)
        for month in months:
            key = f"features/{symbol}/{month}.parquet"
            try:
                table = storage.download_parquet(BUCKET_PROCESSED, key)
                df = table.to_pandas()
                del table

                if df.empty:
                    continue

                df["timestamp"] = pd.to_datetime(df["timestamp"])
                if "trades" in df.columns:
                    df["trades"] = pd.to_numeric(df["trades"], errors="coerce").fillna(0).astype("uint32")

                df = _filter_by_watermark(df, "timestamp", wm_map.get(symbol))
                if df.empty:
                    continue

                inserted = ch_insert_df("klines", df)
                total_inserted += inserted

                # Cleanup after successful insert
                try:
                    storage.remove_object(BUCKET_PROCESSED, key)
                except Exception:
                    pass

            except Exception as exc:
                errors += 1
                logger.error("[Load] klines %s/%s: %s", symbol, month, exc)

    if total_inserted == 0 and errors > 0:
        raise LoadError(f"klines: all {errors} partition(s) failed, 0 loaded")

    logger.info("Loaded %s NEW klines rows (%d errors)", f"{total_inserted:,}", errors)


def load_ticker(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Load ticker snapshots into ClickHouse (per-partition)."""
    symbols = symbols or SYMBOLS
    wm_map = get_table_watermarks("ticker_24h", "snapshot_time", symbols)

    total_inserted = 0
    errors = 0

    for symbol in symbols:
        months = [month_str] if month_str else _discover_months(BUCKET_RAW, "ticker_24h", symbol)
        for month in months:
            key = f"ticker_24h/{symbol}/{month}.parquet"
            try:
                table = storage.download_parquet(BUCKET_RAW, key)
                df = table.to_pandas()
                del table

                if df.empty:
                    continue

                df["snapshot_time"] = pd.to_datetime(df["snapshot_time"])
                for col in ["price_change", "price_change_pct", "high_24h", "low_24h",
                            "volume_24h", "quote_volume_24h", "bid_price", "ask_price", "spread_pct"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                if "trade_count" in df.columns:
                    df["trade_count"] = pd.to_numeric(df["trade_count"], errors="coerce").fillna(0).astype("uint32")

                df = _filter_by_watermark(df, "snapshot_time", wm_map.get(symbol))
                if df.empty:
                    continue

                target_cols = [
                    "symbol", "snapshot_time", "price_change", "price_change_pct",
                    "high_24h", "low_24h", "volume_24h", "quote_volume_24h",
                    "trade_count", "bid_price", "ask_price", "spread_pct",
                ]
                df = df[[c for c in target_cols if c in df.columns]]
                total_inserted += ch_insert_df("ticker_24h", df)
            except Exception as exc:
                errors += 1
                logger.error("[Load] ticker %s/%s: %s", symbol, month, exc)

    if total_inserted == 0 and errors > 0:
        raise LoadError(f"ticker_24h: all {errors} partition(s) failed, 0 loaded")

    logger.info("Loaded %s NEW ticker rows (%d errors)", f"{total_inserted:,}", errors)


def load_order_book(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Load order book snapshots into ClickHouse (per-partition)."""
    symbols = symbols or SYMBOLS
    wm_map = get_table_watermarks("order_book_snapshot", "timestamp", symbols)

    total_inserted = 0
    errors = 0

    for symbol in symbols:
        months = [month_str] if month_str else _discover_months(BUCKET_RAW, "order_book", symbol)
        for month in months:
            key = f"order_book/{symbol}/{month}.parquet"
            try:
                table = storage.download_parquet(BUCKET_RAW, key)
                df = table.to_pandas()
                del table

                if df.empty:
                    continue

                df["timestamp"] = pd.to_datetime(df["timestamp"])
                for col in ["total_bid_volume", "total_ask_volume", "imbalance"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                df = _filter_by_watermark(df, "timestamp", wm_map.get(symbol))
                if df.empty:
                    continue

                target_cols = [
                    "symbol", "timestamp", "total_bid_volume",
                    "total_ask_volume", "imbalance",
                ]
                df = df[[c for c in target_cols if c in df.columns]]
                total_inserted += ch_insert_df("order_book_snapshot", df)
            except Exception as exc:
                errors += 1
                logger.error("[Load] order_book %s/%s: %s", symbol, month, exc)

    if total_inserted == 0 and errors > 0:
        raise LoadError(f"order_book_snapshot: all {errors} partition(s) failed, 0 loaded")

    logger.info("Loaded %s NEW order book rows (%d errors)", f"{total_inserted:,}", errors)


# --- CLI ---------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load data into ClickHouse")
    p.add_argument(
        "--only", nargs="+",
        choices=["symbols", "klines", "ticker", "orderbook"],
        help="Load only these tables",
    )
    p.add_argument(
        "--skip", nargs="*", default=[],
        choices=["symbols", "klines", "ticker", "orderbook"],
        help="Skip these tables",
    )
    p.add_argument(
        "--month", type=str, default=None,
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
    init_schema()

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

    logger.info("=== Load pipeline complete ===")


if __name__ == "__main__":
    args = _build_parser().parse_args()
    main(
        only=set(args.only) if args.only else None,
        skip=set(args.skip),
        month_str=args.month,
    )