# =============================================================================
# Load Script - Ghi du lieu vao PostgreSQL
# =============================================================================

import sys
from pathlib import Path
import argparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
import pandas as pd
from pyspark.sql.functions import col, lit, to_timestamp, max as spark_max

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
    spark_upsert_jdbc,
)

logger = get_logger(__name__)
LOAD_STATE_PATH = PROCESSED_DATA_DIR / "load_state.json"


def _parquet_has_data_files(parquet_path: Path) -> bool:
    if not parquet_path.exists() or not parquet_path.is_dir():
        return False
    for p in parquet_path.rglob("*"):
        if p.is_file() and (p.name.startswith("part-") or p.suffix == ".parquet"):
            return True
    return False


def _load_state() -> dict[str, str]:
    if not LOAD_STATE_PATH.exists():
        return {}
    try:
        with open(LOAD_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception as exc:
        logger.warning("Cannot read load state (%s), reset state", exc)
    return {}


def _save_state(state: dict[str, str]) -> None:
    with open(LOAD_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)


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


def load_klines(spark, parquet_source: Path | None = None) -> None:
    """Load klines tu Parquet vao bang klines (Spark JDBC upsert).

    Uses temp-table upsert: write to _tmp_klines -> INSERT ... ON CONFLICT
    DO NOTHING -> drop temp.  Safe for re-runs without duplicate key errors.
    """
    delta_path = PROCESSED_DATA_DIR / "features_delta.parquet"
    full_path = PROCESSED_DATA_DIR / "features.parquet"

    if parquet_source is not None:
        parquet_path = str(parquet_source)
    else:
        parquet_path = str(delta_path if delta_path.exists() else full_path)

    if not _parquet_has_data_files(Path(parquet_path)):
        logger.warning("No features parquet found (delta/full) — skipping klines load")
        return

    source_name = "delta" if parquet_path == str(delta_path) else "full"
    logger.info("Loading klines from %s via Spark", parquet_path)
    logger.info("Klines source: %s", source_name)

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
        if record_count == 0:
            logger.info("No rows to load into 'klines'")
            return
        logger.info("Writing %s records to table 'klines'", f"{record_count:,}")

        spark_upsert_jdbc(
            df_final,
            table="klines",
            conflict_columns=["symbol", "timestamp"],
        )
        logger.info("Klines loaded successfully")

    except Exception as exc:
        raise LoadError(f"Failed to load klines: {exc}") from exc


def _cast_columns(df, cast_map: dict[str, str]):
    for col_name, target_type in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(target_type))
    return df


def _load_snapshot_csv_via_spark(
    spark,
    *,
    csv_path: Path,
    table_name: str,
    time_col: str,
    state_key: str,
    expected_cols: list[str],
    cast_map: dict[str, str],
    conflict_columns: list[str],
    empty_msg: str,
) -> None:
    if not csv_path.exists():
        logger.warning("%s file not found: %s — skipping", table_name, csv_path)
        return

    state = _load_state()
    df = spark.read.option("header", True).csv(str(csv_path))

    if time_col in df.columns:
        df = df.withColumn(time_col, to_timestamp(col(time_col)))

    last_loaded = state.get(state_key)
    if last_loaded and time_col in df.columns:
        df = df.filter(col(time_col) > lit(last_loaded).cast("timestamp"))

    df = _cast_columns(df, cast_map)

    existing_cols = [c for c in expected_cols if c in df.columns]
    if not existing_cols:
        logger.warning("No expected columns found for table '%s'", table_name)
        return

    df_final = df.select(*existing_cols)
    row_count = df_final.count()
    if row_count == 0:
        logger.info(empty_msg)
        return

    spark_upsert_jdbc(
        df_final,
        table=table_name,
        conflict_columns=conflict_columns,
    )
    logger.info("Loaded %d %s records", row_count, table_name)

    if time_col in df_final.columns:
        max_snapshot = df_final.select(spark_max(time_col).alias("mx")).collect()[0]["mx"]
        if max_snapshot is not None:
            state[state_key] = max_snapshot.isoformat()
            _save_state(state)


def load_ticker(spark) -> None:
    """Load ticker_24h tu CSV vao bang ticker_24h (Spark JDBC upsert)."""
    logger.info("Loading ticker from ticker_24h.csv")
    try:
        _load_snapshot_csv_via_spark(
            spark,
            csv_path=RAW_DATA_DIR / "ticker_24h.csv",
            table_name="ticker_24h",
            time_col="snapshot_time",
            state_key="ticker_24h_last_snapshot_time",
            expected_cols=[
                "symbol", "snapshot_time", "price_change", "price_change_pct",
                "high_24h", "low_24h", "volume_24h", "quote_volume_24h",
                "trade_count", "bid_price", "ask_price", "spread_pct",
            ],
            cast_map={
                "price_change": "double",
                "price_change_pct": "double",
                "high_24h": "double",
                "low_24h": "double",
                "volume_24h": "double",
                "quote_volume_24h": "double",
                "bid_price": "double",
                "ask_price": "double",
                "spread_pct": "double",
                "trade_count": "int",
            },
            conflict_columns=["symbol", "snapshot_time"],
            empty_msg="No new ticker rows to load",
        )
    except Exception as exc:
        raise LoadError(f"Failed to load ticker: {exc}") from exc


def load_orderbook(spark) -> None:
    """Load order_book_snapshot tu CSV vao bang order_book_snapshot (Spark JDBC upsert)."""
    logger.info("Loading order book from order_book_snapshot.csv")
    try:
        _load_snapshot_csv_via_spark(
            spark,
            csv_path=RAW_DATA_DIR / "order_book_snapshot.csv",
            table_name="order_book_snapshot",
            time_col="timestamp",
            state_key="order_book_last_timestamp",
            expected_cols=["symbol", "timestamp", "total_bid_volume", "total_ask_volume", "imbalance"],
            cast_map={
                "total_bid_volume": "double",
                "total_ask_volume": "double",
                "imbalance": "double",
            },
            conflict_columns=["symbol", "timestamp"],
            empty_msg="No new order book rows to load",
        )
    except Exception as exc:
        raise LoadError(f"Failed to load order book: {exc}") from exc


def _resolve_klines_source(klines_source: str) -> Path | None:
    delta_path = PROCESSED_DATA_DIR / "features_delta.parquet"
    full_path = PROCESSED_DATA_DIR / "features.parquet"
    delta_ok = _parquet_has_data_files(delta_path)
    full_ok = _parquet_has_data_files(full_path)

    if klines_source == "delta":
        if not delta_ok:
            logger.warning("Requested --klines-source=delta but delta parquet missing")
            return None
        return delta_path

    if klines_source == "full":
        if not full_ok:
            logger.warning("Requested --klines-source=full but full parquet missing")
            return None
        return full_path

    if delta_ok:
        return delta_path
    if full_ok:
        return full_path
    return None


# =============================================================================
# Main
# =============================================================================

# =============================================================================
# CLI
# =============================================================================
def _build_parser() -> argparse.ArgumentParser:

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
    parser.add_argument(
        "--klines-source",
        choices=["delta", "full", "auto"],
        default="auto",
        help="source parquet for klines load (default: auto=delta if exists else full)",
    )
    return parser


def main(
    only: set[str] | None = None,
    skip: set[str] | None = None,
    klines_source: str = "auto",
) -> None:
    skip = skip or set()
    logger.info("=== Load pipeline started ===")

    def _should_load(table: str) -> bool:
        if only is not None:
            return table in only
        return table not in skip

    # 1. Init DB schema
    engine = get_engine()
    init_schema(engine)

    # 2. Load symbols
    if _should_load("symbols"):
        load_symbols(engine)

    spark_needed = _should_load("ticker") or _should_load("orderbook") or _should_load("klines")
    if not spark_needed:
        logger.info("=== Load pipeline finished ===")
        return

    spark = get_spark_session("CryptoLoad")
    try:
        if _should_load("ticker"):
            load_ticker(spark)
        if _should_load("orderbook"):
            load_orderbook(spark)

        if _should_load("klines"):
            target = _resolve_klines_source(klines_source)
            if target is None:
                logger.warning("No parquet source available for klines load")
            else:
                logger.info("Selected klines parquet source: %s", target)
                load_klines(spark, parquet_source=target)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")

    logger.info("=== Load pipeline finished ===")


if __name__ == "__main__":
    args = _build_parser().parse_args()
    main(
        only=set(args.only) if args.only else None,
        skip=set(args.skip),
        klines_source=args.klines_source,
    )