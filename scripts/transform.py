# =============================================================================
# Transform Script - Xu ly du lieu voi Apache Spark
# =============================================================================

import sys
from pathlib import Path
import io
import json
import argparse
import shutil

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

from config.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from config.symbols import SYMBOLS
from utils.logger import get_logger
from utils.exceptions import TransformError
from utils.db_utils import get_spark_session
from utils.data_utils import get_last_timestamp

logger = get_logger(__name__)

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

# StructType.add() mutate in-place, phai tao ban copy rieng
OUTPUT_SCHEMA = StructType(list(INPUT_SCHEMA.fields) + [
    StructField("RSI", DoubleType(), True),
    StructField("MACD", DoubleType(), True),
    StructField("MACD_signal", DoubleType(), True),
])

FULL_FEATURES_PATH = PROCESSED_DATA_DIR / "features.parquet"
DELTA_FEATURES_PATH = PROCESSED_DATA_DIR / "features_delta.parquet"
TRANSFORM_STATE_PATH = PROCESSED_DATA_DIR / "transform_state.json"
INDICATOR_CONTEXT_ROWS = 120
MAX_TAIL_SCAN_BYTES = 8 * 1024 * 1024


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
    if not TRANSFORM_STATE_PATH.exists():
        return {}
    try:
        with open(TRANSFORM_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception as exc:
        logger.warning("Cannot read transform state (%s), rebuilding state", exc)
    return {}


def _save_transform_state(state: dict[str, str]) -> None:
    with open(TRANSFORM_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)


def _bootstrap_transform_state_from_features(spark: SparkSession) -> dict[str, str]:
    if TRANSFORM_STATE_PATH.exists():
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


def _tail_csv_rows(csv_path: Path, n_rows: int) -> pd.DataFrame:
    if n_rows <= 0:
        return pd.DataFrame()

    with open(csv_path, "rb") as f:
        header = f.readline().decode("utf-8").strip()
        f.seek(0, 2)
        file_size = f.tell()

        if file_size <= len(header) + 2:
            return pd.DataFrame(columns=header.split(","))

        step = 64 * 1024
        buffer = b""
        pos = file_size

        while pos > 0 and buffer.count(b"\n") <= (n_rows + 1):
            read_size = min(step, pos)
            pos -= read_size
            f.seek(pos)
            buffer = f.read(read_size) + buffer
            if len(buffer) >= MAX_TAIL_SCAN_BYTES:
                break

    tail_lines = buffer.decode("utf-8", errors="ignore").strip().splitlines()
    if len(tail_lines) > n_rows:
        tail_lines = tail_lines[-n_rows:]

    csv_text = "\n".join([header] + tail_lines)
    return pd.read_csv(
        io.StringIO(csv_text),
        parse_dates=["open_time", "close_time"],
    )


def _estimate_missing_rows(symbol: str, last_processed: pd.Timestamp | None) -> int:
    if last_processed is None:
        return 1000

    raw_last_ms = get_last_timestamp(symbol)
    if raw_last_ms is None:
        return 0

    last_raw = pd.Timestamp(raw_last_ms, unit="ms", tz="UTC")
    processed_utc = last_processed.tz_convert("UTC") if last_processed.tzinfo else last_processed.tz_localize("UTC")
    delta_minutes = int((last_raw - processed_utc).total_seconds() // 60)
    return max(delta_minutes + 10, 0)


def _read_symbol_incremental_csv(symbol: str, last_processed: pd.Timestamp | None) -> pd.DataFrame:
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"
    if not csv_path.exists():
        return pd.DataFrame()

    missing_rows = _estimate_missing_rows(symbol, last_processed)
    if missing_rows == 0:
        return pd.DataFrame()

    n_rows = max(missing_rows + INDICATOR_CONTEXT_ROWS, INDICATOR_CONTEXT_ROWS + 20)

    try:
        if n_rows > 20000:
            pdf = pd.read_csv(csv_path, parse_dates=["open_time", "close_time"])
        else:
            pdf = _tail_csv_rows(csv_path, n_rows)
    except Exception as exc:
        raise TransformError(f"Cannot read incremental CSV for {symbol}: {exc}") from exc

    if pdf.empty:
        return pdf

    pdf["symbol"] = symbol
    pdf = pdf.dropna(subset=["open_time"])
    pdf["open_time"] = pd.to_datetime(pdf["open_time"], utc=False)
    pdf = pdf.sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last")
    return pdf


def _transform_full_rebuild(spark: SparkSession, symbols: list[str]) -> str | None:
    logger.info("Full transform rebuild: reading all raw CSV files")

    csv_paths = [(RAW_DATA_DIR / f"{s}.csv") for s in symbols]
    existing = [str(p) for p in csv_paths if p.exists()]

    if not existing:
        raise TransformError(f"No klines CSV files found in {RAW_DATA_DIR}")

    df = spark.read.csv(
        existing,
        header=True,
        schema=INPUT_SCHEMA,
        timestampFormat="yyyy-MM-dd HH:mm:ss",
    )
    df = df.filter(col("open_time").isNotNull())

    processed_df = df.groupBy("symbol").applyInPandas(
        calculate_indicators_pandas,
        schema=OUTPUT_SCHEMA,
    )

    output_path = str(FULL_FEATURES_PATH)
    processed_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path)
    processed_df.write.mode("overwrite").partitionBy("symbol").parquet(str(DELTA_FEATURES_PATH))

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
    return output_path


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

    all_incremental: list[pd.DataFrame] = []
    next_state = dict(state)

    for symbol in symbols:
        raw_state = state.get(symbol)
        last_processed = pd.to_datetime(raw_state) if raw_state else None

        symbol_df = _read_symbol_incremental_csv(symbol, last_processed)
        if symbol_df.empty:
            continue

        calculated = calculate_indicators_pandas(symbol_df)
        if last_processed is not None:
            calculated = calculated[calculated["open_time"] > last_processed]

        if calculated.empty:
            continue

        all_incremental.append(calculated)
        next_state[symbol] = pd.Timestamp(calculated["open_time"].max()).isoformat()

    if not all_incremental:
        logger.info("No new rows to transform")
        return None

    incremental_pdf = pd.concat(all_incremental, ignore_index=True)
    incremental_pdf = incremental_pdf[[f.name for f in OUTPUT_SCHEMA.fields]]

    incremental_sdf = spark.createDataFrame(incremental_pdf, schema=OUTPUT_SCHEMA)

    logger.info("Writing transformed delta rows to %s", DELTA_FEATURES_PATH)
    incremental_sdf.write.mode("overwrite").partitionBy("symbol").parquet(str(DELTA_FEATURES_PATH))

    logger.info("Appending transformed rows to %s", FULL_FEATURES_PATH)
    full_mode = "append" if _parquet_has_data_files(FULL_FEATURES_PATH) else "overwrite"
    incremental_sdf.write.mode(full_mode).partitionBy("symbol").parquet(str(FULL_FEATURES_PATH))

    _save_transform_state(next_state)

    logger.info("Transform complete: %s new rows", f"{len(incremental_pdf):,}")
    return str(DELTA_FEATURES_PATH)


# =============================================================================
# CLI
# =============================================================================
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