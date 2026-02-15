# =============================================================================
# Load Script - Ghi du lieu vao PostgreSQL
# =============================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
import pandas as pd

from config.config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    KLINES_COLUMNS,
    KLINES_RENAME_MAP,
)
from utils.logger import get_logger
from utils.exceptions import LoadError
from utils.db_utils import (
    get_engine,
    init_schema,
    upsert_on_conflict_nothing,
    get_spark_session,
    spark_write_jdbc,
    spark_upsert_jdbc,
)

logger = get_logger(__name__)


# =============================================================================
# Load functions
# =============================================================================

def load_symbols(engine) -> None:
    """Load symbols tu JSON vao bang symbols (Pandas, file nho)."""
    json_path = RAW_DATA_DIR / "symbols.json"
    if not json_path.exists():
        logger.warning("Symbols file not found: %s — skipping", json_path)
        return

    logger.info("Loading symbols from %s", json_path.name)
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict) and "symbols" in data:
            df = pd.DataFrame(data["symbols"])
        else:
            df = pd.DataFrame(data)

        # Giu dung 4 cot khop voi schema bang symbols
        target_cols = ["symbol", "base_asset", "quote_asset", "status"]
        df = df[[c for c in target_cols if c in df.columns]]

        df.to_sql(
            "symbols",
            engine,
            if_exists="append",
            index=False,
            method=upsert_on_conflict_nothing,
        )
        logger.info("Loaded %d symbols", len(df))
    except Exception as exc:
        raise LoadError(f"Failed to load symbols: {exc}") from exc


def load_klines(spark) -> None:
    """Load klines tu Parquet vao bang klines (Spark, file lon).

    Uses temp-table upsert: write to _tmp_klines -> INSERT ... ON CONFLICT
    DO NOTHING -> drop temp.  Safe for re-runs without duplicate key errors.
    """
    parquet_path = str(PROCESSED_DATA_DIR / "features.parquet")
    logger.info("Loading klines from %s via Spark", parquet_path)

    try:
        df = spark.read.parquet(parquet_path)

        # Rename columns theo mapping tu config
        for old_name, new_name in KLINES_RENAME_MAP.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        # Chon dung cac cot can thiet
        existing_cols = [c for c in KLINES_COLUMNS if c in df.columns]
        df_final = df.select(*existing_cols)

        record_count = df_final.count()
        logger.info("Writing %s records to table 'klines'", f"{record_count:,}")

        spark_upsert_jdbc(
            df_final,
            table="klines",
            conflict_columns=["symbol", "timestamp"],
        )
        logger.info("Klines loaded successfully")

    except Exception as exc:
        raise LoadError(f"Failed to load klines: {exc}") from exc


def load_ticker(engine) -> None:
    """Load ticker_24h tu CSV vao bang ticker_24h (Pandas)."""
    csv_path = RAW_DATA_DIR / "ticker_24h.csv"
    if not csv_path.exists():
        logger.warning("Ticker file not found: %s — skipping", csv_path)
        return

    logger.info("Loading ticker from %s", csv_path.name)
    try:
        df = pd.read_csv(csv_path)
        numeric_cols = ["price_change", "high_24h", "low_24h", "bid_price", "ask_price"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

        df.to_sql(
            "ticker_24h",
            engine,
            if_exists="append",
            index=False,
            method=upsert_on_conflict_nothing,
        )
        logger.info("Loaded %d ticker records", len(df))
    except Exception as exc:
        raise LoadError(f"Failed to load ticker: {exc}") from exc


def load_orderbook(engine) -> None:
    """Load order_book_snapshot tu CSV vao bang order_book_snapshot (Pandas)."""
    csv_path = RAW_DATA_DIR / "order_book_snapshot.csv"
    if not csv_path.exists():
        logger.warning("Order book file not found: %s — skipping", csv_path)
        return

    logger.info("Loading order book from %s", csv_path.name)
    try:
        df = pd.read_csv(csv_path)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        df.to_sql(
            "order_book_snapshot",
            engine,
            if_exists="append",
            index=False,
            method=upsert_on_conflict_nothing,
        )
        logger.info("Loaded %d order book records", len(df))
    except Exception as exc:
        raise LoadError(f"Failed to load order book: {exc}") from exc


# =============================================================================
# Main
# =============================================================================

# =============================================================================
# CLI
# =============================================================================
def _build_parser() -> "argparse.ArgumentParser":
    import argparse

    parser = argparse.ArgumentParser(
        description="Load data into PostgreSQL",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        choices=["symbols", "klines", "ticker", "orderbook"],
        default=None,
        help="load only specific tables (default: all)",
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        choices=["symbols", "klines", "ticker", "orderbook"],
        default=[],
        help="skip specific tables",
    )
    return parser


def main(only: set[str] | None = None, skip: set[str] | None = None) -> None:
    skip = skip or set()
    logger.info("=== Load pipeline started ===")

    def _should_load(table: str) -> bool:
        if only is not None:
            return table in only
        return table not in skip

    # 1. Init DB schema
    engine = get_engine()
    init_schema(engine)

    # 2. Load bang nho bang Pandas
    if _should_load("symbols"):
        load_symbols(engine)
    if _should_load("ticker"):
        load_ticker(engine)
    if _should_load("orderbook"):
        load_orderbook(engine)

    # 3. Load bang lon bang Spark
    if _should_load("klines"):
        spark = get_spark_session("CryptoLoad")
        try:
            load_klines(spark)
        finally:
            spark.stop()
            logger.info("SparkSession stopped")

    logger.info("=== Load pipeline finished ===")


if __name__ == "__main__":
    args = _build_parser().parse_args()
    main(
        only=set(args.only) if args.only else None,
        skip=set(args.skip),
    )