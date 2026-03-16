import sys
from pathlib import Path
import json
import argparse
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import pandas as pd
import numpy as np
import pyarrow.parquet as pq

from config.config import (
    RAW_DATA_DIR, PROCESSED_DATA_DIR, MINIO_CONFIG,
    PARALLELISM, INDICATOR_CONTEXT_ROWS,
)
from config.symbols import SYMBOLS
from utils.logger import get_logger
from utils.exceptions import TransformError
from utils.db_utils import get_spark_session
from utils.data_utils import get_last_timestamp
from utils.storage import storage

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]
BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]
TRANSFORM_MAX_WORKERS = PARALLELISM["transform_max_workers"]
_S3A_RAW = f"s3a://{BUCKET_RAW}"
_S3A_PROCESSED = f"s3a://{BUCKET_PROCESSED}"

INPUT_SCHEMA = StructType([
    StructField("open_time", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("close_time", TimestampType(), True),
    StructField("quote_volume", DoubleType(), True),
    StructField("trades", LongType(), True),
    StructField("taker_buy_base", DoubleType(), True),
    StructField("taker_buy_quote", DoubleType(), True),
    StructField("symbol", StringType(), True),
])

# StructType.add() mutates in-place; must create a separate copy
OUTPUT_SCHEMA = StructType(list(INPUT_SCHEMA.fields) + [
    StructField("RSI", DoubleType(), True),
    StructField("MACD", DoubleType(), True),
    StructField("MACD_signal", DoubleType(), True),
])

FULL_FEATURES_PATH = PROCESSED_DATA_DIR / "features.parquet"
DELTA_FEATURES_PATH = PROCESSED_DATA_DIR / "features_delta.parquet"
TRANSFORM_STATE_KEY = "transform_state.json"

# S3A paths (Spark reads/writes via S3A protocol)
S3A_FULL_FEATURES = f"{_S3A_PROCESSED}/features.parquet"
S3A_DELTA_FEATURES = f"{_S3A_PROCESSED}/features_delta.parquet"


def _parquet_has_data_files(parquet_path: Path) -> bool:
    if not parquet_path.exists() or not parquet_path.is_dir():
        return False
    for p in parquet_path.rglob("*"):
        if p.is_file() and (p.name.startswith("part-") or p.suffix == ".parquet"):
            return True
    return False


def _cleanup_empty_parquet_dir(parquet_path: Path) -> None:
    if parquet_path.exists() and parquet_path.is_dir() and not _parquet_has_data_files(parquet_path):
        logger.warning("Removing empty parquet directory: %s", parquet_path)
        shutil.rmtree(parquet_path, ignore_errors=True)


def calculate_indicators_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    """Tinh RSI(14) va MACD(12,26,9) cho mot group symbol."""
    pdf = pdf.sort_values("open_time")
    close = pdf["close"]

    # RSI (14)
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    loss = loss.replace(0, np.nan)
    rs = gain / loss
    pdf["RSI"] = 100 - (100 / (1 + rs))

    # MACD (12, 26, 9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    pdf["MACD"] = ema12 - ema26
    pdf["MACD_signal"] = pdf["MACD"].ewm(span=9, adjust=False).mean()

    # Fill NaN
    pdf = pdf.ffill().bfill().fillna(0)
    return pdf


def _load_transform_state() -> dict[str, str]:
    """Load transform state from MinIO."""
    try:
        if storage.object_exists(BUCKET_PROCESSED, TRANSFORM_STATE_KEY):
            data = storage.download_json(BUCKET_PROCESSED, TRANSFORM_STATE_KEY)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
    except Exception as exc:
        logger.warning("Cannot read transform state from MinIO (%s), rebuilding state", exc)
    return {}


def _save_transform_state(state: dict[str, str]) -> None:
    """Save transform state to MinIO."""
    storage.upload_json(BUCKET_PROCESSED, TRANSFORM_STATE_KEY, state)
    logger.debug("Saved transform state to MinIO (%d symbols)", len(state))


def _bootstrap_transform_state_from_features(spark: SparkSession) -> dict[str, str]:
    if storage.object_exists(BUCKET_PROCESSED, TRANSFORM_STATE_KEY):
        return _load_transform_state()

    if not _parquet_has_data_files(FULL_FEATURES_PATH):
        _cleanup_empty_parquet_dir(FULL_FEATURES_PATH)
        return {}

    logger.info("Bootstrapping transform state from existing features.parquet")
    try:
        state_df = (
            spark.read.parquet(str(FULL_FEATURES_PATH))
            .groupBy("symbol")
            .max("open_time")
            .toPandas()
        )
    except Exception as exc:
        logger.warning("Cannot bootstrap state from features.parquet (%s)", exc)
        return {}

    state: dict[str, str] = {}
    if not state_df.empty:
        for _, row in state_df.iterrows():
            symbol = row["symbol"]
            ts_val = row["max(open_time)"]
            if pd.notna(symbol) and pd.notna(ts_val):
                state[str(symbol)] = pd.Timestamp(ts_val).isoformat()

    if state:
        _save_transform_state(state)
    return state


def _read_symbol_incremental(symbol: str, last_processed: pd.Timestamp | None) -> pd.DataFrame:
    """Read new rows for a symbol from its Parquet file on MinIO.

    Downloads from MinIO and filters for rows after last_processed,
    plus INDICATOR_CONTEXT_ROWS for warm-up context.
    """
    key = f"{symbol}.parquet"
    if not storage.object_exists(BUCKET_RAW, key):
        return pd.DataFrame()

    try:
        table = storage.download_parquet(BUCKET_RAW, key)
        pdf = table.to_pandas()
        pdf["open_time"] = pd.to_datetime(pdf["open_time"], utc=False)
        pdf = pdf.sort_values("open_time").drop_duplicates(
            subset=["open_time"], keep="last",
        )

        if last_processed is not None:
            newer_mask = pdf["open_time"] > last_processed
            if not newer_mask.any():
                return pd.DataFrame()

            first_new_idx = newer_mask.idxmax()
            start_idx = max(0, first_new_idx - INDICATOR_CONTEXT_ROWS)
            pdf = pdf.iloc[start_idx:].reset_index(drop=True)
    except Exception as exc:
        raise TransformError(
            f"Cannot read Parquet from MinIO for {symbol}: {exc}",
        ) from exc

    if pdf.empty:
        return pdf

    pdf["symbol"] = symbol
    pdf = pdf.dropna(subset=["open_time"])
    return pdf


def _transform_full_rebuild(spark: SparkSession, symbols: list[str]) -> str | None:
    logger.info("Full transform rebuild: reading all raw Parquet from MinIO")

    s3a_paths = [f"{_S3A_RAW}/{s}.parquet" for s in symbols
                 if storage.object_exists(BUCKET_RAW, f"{s}.parquet")]

    if not s3a_paths:
        raise TransformError(f"No klines Parquet files found in MinIO bucket {BUCKET_RAW}")

    df = spark.read.parquet(*s3a_paths)
    df = df.filter(col("open_time").isNotNull())

    processed_df = df.groupBy("symbol").applyInPandas(
        calculate_indicators_pandas,
        schema=OUTPUT_SCHEMA,
    )

    processed_df.write.mode("overwrite").partitionBy("symbol").parquet(S3A_FULL_FEATURES)
    processed_df.write.mode("overwrite").partitionBy("symbol").parquet(S3A_DELTA_FEATURES)

    max_state = (
        processed_df.groupBy("symbol")
        .max("open_time")
        .toPandas()
    )
    state: dict[str, str] = {}
    if not max_state.empty:
        for _, row in max_state.iterrows():
            symbol = row["symbol"]
            ts_val = row["max(open_time)"]
            if pd.notna(symbol) and pd.notna(ts_val):
                state[str(symbol)] = pd.Timestamp(ts_val).isoformat()
    _save_transform_state(state)

    logger.info("Transform complete (full rebuild)")
    return S3A_FULL_FEATURES


def transform_data(spark: SparkSession, symbols: list[str] | None = None) -> str | None:
    """Incremental transform: chi tinh indicator cho phan du lieu moi."""
    symbols = symbols or SYMBOLS
    logger.info("Incremental transform started for %d symbols", len(symbols))

    state = _bootstrap_transform_state_from_features(spark)
    if not state and _parquet_has_data_files(FULL_FEATURES_PATH):
        state = _load_transform_state()

    if not state and not _parquet_has_data_files(FULL_FEATURES_PATH):
        logger.info("No valid transform state/parquet found -> full rebuild")
        return _transform_full_rebuild(spark, symbols)

    # --- Parallel symbol processing (read + indicators) ---
    all_incremental: list[pd.DataFrame] = []
    next_state = dict(state)
    n_symbols = len(symbols)

    def _process_symbol(symbol: str) -> tuple[str, pd.DataFrame | None]:
        """Read + compute indicators for one symbol (thread-safe)."""
        raw_state = state.get(symbol)
        last_processed = pd.to_datetime(raw_state) if raw_state else None

        symbol_df = _read_symbol_incremental(symbol, last_processed)
        if symbol_df.empty:
            return symbol, None

        calculated = calculate_indicators_pandas(symbol_df)
        if last_processed is not None:
            calculated = calculated[calculated["open_time"] > last_processed]

        if calculated.empty:
            return symbol, None

        return symbol, calculated

    max_workers = min(TRANSFORM_MAX_WORKERS, n_symbols)
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_process_symbol, sym): sym for sym in symbols
        }
        for future in as_completed(future_map):
            completed += 1
            sym = future_map[future]
            try:
                symbol, calculated = future.result()
                if calculated is not None:
                    all_incremental.append(calculated)
                    next_state[symbol] = pd.Timestamp(calculated["open_time"].max()).isoformat()
                    logger.info(
                        "[Transform %d/%d] %s: +%s rows",
                        completed, n_symbols, symbol, f"{len(calculated):,}",
                    )
                else:
                    logger.debug(
                        "[Transform %d/%d] %s: no new data",
                        completed, n_symbols, sym,
                    )
            except Exception as exc:
                logger.error(
                    "[Transform %d/%d] %s: ERROR — %s",
                    completed, n_symbols, sym, exc,
                )

    if not all_incremental:
        logger.info("No new rows to transform")
        return None

    incremental_pdf = pd.concat(all_incremental, ignore_index=True)
    incremental_pdf = incremental_pdf[[f.name for f in OUTPUT_SCHEMA.fields]]

    incremental_sdf = spark.createDataFrame(incremental_pdf, schema=OUTPUT_SCHEMA)

    logger.info("Writing transformed delta rows to MinIO")
    incremental_sdf.write.mode("overwrite").partitionBy("symbol").parquet(S3A_DELTA_FEATURES)

    logger.info("Appending transformed rows to MinIO features")
    # Check if full features exist on MinIO
    existing_features = len(storage.list_objects(BUCKET_PROCESSED, "features.parquet/")) > 0
    full_mode = "append" if existing_features else "overwrite"
    incremental_sdf.write.mode(full_mode).partitionBy("symbol").parquet(S3A_FULL_FEATURES)

    _save_transform_state(next_state)

    logger.info("Transform complete: %s new rows", f"{len(incremental_pdf):,}")
    return S3A_DELTA_FEATURES


# --- CLI ---------------------------------------------------------------------
def _build_parser() -> "argparse.ArgumentParser":

    parser = argparse.ArgumentParser(
        description="Transform raw klines CSV → features Parquet",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYM",
        default=None,
        help="process only these symbols (default: all in SYMBOLS list)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="run verification query after transform (disabled by default for speed)",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="deprecated: kept for backward compatibility; verification is already off by default",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="force full rebuild of features.parquet from all raw CSV files",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    active_symbols = args.symbols or SYMBOLS

    logger.info("Transform started — symbols=%d", len(active_symbols))

    spark = get_spark_session("CryptoTransform")
    exit_code = 0
    try:
        if args.full_rebuild:
            out_file = _transform_full_rebuild(spark, symbols=active_symbols)
        else:
            out_file = transform_data(spark, symbols=active_symbols)

        should_verify = args.verify and not args.no_verify
        if out_file and should_verify:
            logger.info("--- Verifying result ---")
            res = spark.read.parquet(out_file)
            display_cols = ["open_time", "symbol", "close", "trades", "RSI", "MACD", "MACD_signal"]
            res.select(display_cols).show(5)
            logger.info("Total records: %s", f"{res.count():,}")

    except TransformError as exc:
        logger.error("Transform failed: %s", exc)
        exit_code = 1
    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
        exit_code = 1
    finally:
        spark.stop()
        logger.info("SparkSession stopped")

    raise SystemExit(exit_code)