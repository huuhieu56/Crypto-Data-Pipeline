"""Load script — write processed data to ClickHouse.

Reads delta Parquet from MinIO (daily partitions), batch inserts
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
from datetime import datetime, timezone

import pandas as pd

from config.config import MINIO_CONFIG, PARTITION_DATE_FORMAT
from config.symbols import SYMBOLS, SYMBOL_REGISTRY
from utils.logger import get_logger
from utils.exceptions import LoadError
from utils.db_utils import init_schema, ch_insert_df, ch_query_df
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


def load_klines(
    symbols: list[str] | None = None,
    date_str: str | None = None,
) -> None:
    """Load klines from MinIO delta partitions into ClickHouse.

    Reads features_delta/{SYMBOL}/{date}.parquet for each symbol,
    batch inserts all data in one ClickHouse call.
    No column renaming needed — Transform outputs DB column names.
    """
    symbols = symbols or SYMBOLS
    date_str = date_str or datetime.now(timezone.utc).strftime(PARTITION_DATE_FORMAT)

    # Collect all delta data
    all_dfs: list[pd.DataFrame] = []

    for symbol in symbols:
        key = f"features_delta/{symbol}/{date_str}.parquet"
        if not storage.object_exists(BUCKET_PROCESSED, key):
            continue

        try:
            table = storage.download_parquet(BUCKET_PROCESSED, key)
            df = table.to_pandas()
            del table

            if df.empty:
                continue

            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            if "trades" in df.columns:
                df["trades"] = pd.to_numeric(df["trades"], errors="coerce").fillna(0).astype("uint32")

            all_dfs.append(df)
        except Exception as exc:
            logger.error("[Load] %s: error — %s", symbol, exc)

    if not all_dfs:
        # Bootstrap: no delta yet but table is empty -> load full features once.
        try:
            cnt_df = ch_query_df("SELECT count() AS cnt FROM klines")
            current_rows = int(cnt_df.iloc[0]["cnt"]) if not cnt_df.empty else 0
        except Exception as exc:
            logger.warning("Cannot verify klines row count: %s", exc)
            current_rows = -1

        if current_rows == 0:
            feature_keys = [
                k for k in storage.list_objects(BUCKET_PROCESSED, "features/")
                if k.endswith(".parquet")
            ]
            if feature_keys:
                logger.info(
                    "No delta data for %s and klines table is empty; "
                    "auto-loading from full features/ (%d files)",
                    date_str,
                    len(feature_keys),
                )
                load_klines_full_rebuild()
                return

        logger.info("No delta data to load for %s", date_str)
        return

    # Single batch insert (much faster than 50 individual inserts)
    combined = pd.concat(all_dfs, ignore_index=True)
    inserted = ch_insert_df("klines", combined)
    logger.info("Klines loaded: %s rows (%s)", f"{inserted:,}", date_str)


def load_klines_full_rebuild() -> None:
    """Load full features for all symbols (after transform --full-rebuild)."""
    keys = storage.list_objects(BUCKET_PROCESSED, "features/")
    parquet_keys = [k for k in keys if k.endswith(".parquet")]

    if not parquet_keys:
        logger.warning("No features/ parquet found — skipping")
        return

    logger.info("Loading full features: %d files", len(parquet_keys))
    total_rows = 0

    for key in parquet_keys:
        try:
            table = storage.download_parquet(BUCKET_PROCESSED, key)
            df = table.to_pandas()
            del table

            if df.empty:
                continue

            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            if "trades" in df.columns:
                df["trades"] = pd.to_numeric(df["trades"], errors="coerce").fillna(0).astype("uint32")

            inserted = ch_insert_df("klines", df)
            total_rows += inserted
        except Exception as exc:
            logger.error("[Load] %s: error — %s", key, exc)

    logger.info("Full rebuild loaded: %s total rows", f"{total_rows:,}")


def load_ticker() -> None:
    """Load ticker_24h from MinIO CSV into ClickHouse."""
    key = "ticker_24h.csv"
    if not storage.object_exists(BUCKET_RAW, key):
        logger.warning("Ticker file not found — skipping")
        return

    logger.info("Loading ticker from MinIO")
    try:
        df = storage.download_csv_df(BUCKET_RAW, key)
        if df.empty:
            return

        if "snapshot_time" in df.columns:
            df["snapshot_time"] = pd.to_datetime(df["snapshot_time"])

        target_cols = [
            "symbol", "snapshot_time", "price_change", "price_change_pct",
            "high_24h", "low_24h", "volume_24h", "quote_volume_24h",
            "trade_count", "bid_price", "ask_price", "spread_pct",
        ]
        df = df[[c for c in target_cols if c in df.columns]]

        if "trade_count" in df.columns:
            df["trade_count"] = pd.to_numeric(df["trade_count"], errors="coerce").fillna(0).astype("uint32")

        for col in df.select_dtypes(include=["object"]).columns:
            if col != "symbol":
                df[col] = pd.to_numeric(df[col], errors="coerce")

        inserted = ch_insert_df("ticker_24h", df)
        logger.info("Loaded %d ticker rows", inserted)
    except Exception as exc:
        raise LoadError(f"Failed to load ticker: {exc}") from exc


def load_order_book() -> None:
    """Load order_book_snapshot from MinIO CSV into ClickHouse."""
    key = "order_book_snapshot.csv"
    if not storage.object_exists(BUCKET_RAW, key):
        logger.warning("Order book file not found — skipping")
        return

    logger.info("Loading order book from MinIO")
    try:
        df = storage.download_csv_df(BUCKET_RAW, key)
        if df.empty:
            return

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        target_cols = ["symbol", "timestamp", "total_bid_volume", "total_ask_volume", "imbalance"]
        df = df[[c for c in target_cols if c in df.columns]]

        for col in ["total_bid_volume", "total_ask_volume", "imbalance"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        inserted = ch_insert_df("order_book_snapshot", df)
        logger.info("Loaded %d order book rows", inserted)
    except Exception as exc:
        raise LoadError(f"Failed to load order book: {exc}") from exc


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
        "--full-rebuild", action="store_true",
        help="Load from features/ instead of features_delta/",
    )
    return p


def main(
    only: set[str] | None = None,
    skip: set[str] | None = None,
    full_rebuild: bool = False,
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
        load_ticker()
    if should_load("orderbook"):
        load_order_book()
    if should_load("klines"):
        if full_rebuild:
            load_klines_full_rebuild()
        else:
            load_klines()

    logger.info("=== Load pipeline complete ===")


if __name__ == "__main__":
    args = _build_parser().parse_args()
    main(
        only=set(args.only) if args.only else None,
        skip=set(args.skip),
        full_rebuild=args.full_rebuild,
    )