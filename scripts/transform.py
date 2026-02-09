# =============================================================================
# Transform Script - Xu ly du lieu voi Apache Spark
# =============================================================================

import sys
from pathlib import Path

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


def transform_data(spark: SparkSession, symbols: list[str] | None = None) -> str | None:
    """Doc CSV tu raw, tinh indicators, luu Parquet."""
    symbols = symbols or SYMBOLS
    logger.info("Processing data from: %s", RAW_DATA_DIR)

    csv_paths = [(RAW_DATA_DIR / f"{s}.csv") for s in symbols]
    existing = [str(p) for p in csv_paths if p.exists()]

    if not existing:
        raise TransformError(f"No klines CSV files found in {RAW_DATA_DIR}")

    logger.info("Found %d/%d klines CSV files", len(existing), len(symbols))

    try:
        df = spark.read.csv(
            existing,
            header=True,
            schema=INPUT_SCHEMA,
            timestampFormat="yyyy-MM-dd HH:mm:ss",
        )
    except Exception as exc:
        raise TransformError(f"Cannot read klines CSV: {exc}") from exc

    df = df.filter(col("open_time").isNotNull())

    logger.info("Calculating RSI and MACD using Pandas UDF")

    processed_df = df.groupBy("symbol").applyInPandas(
        calculate_indicators_pandas,
        schema=OUTPUT_SCHEMA,
    )

    output_path = str(PROCESSED_DATA_DIR / "features.parquet")
    logger.info("Saving processed data to: %s", output_path)

    processed_df.write \
        .mode("overwrite") \
        .partitionBy("symbol") \
        .parquet(output_path)

    logger.info("Transform complete")
    return output_path


# =============================================================================
# CLI
# =============================================================================
def _build_parser() -> "argparse.ArgumentParser":
    import argparse

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
        "--no-verify",
        action="store_true",
        help="skip verification step after transform",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    active_symbols = args.symbols or SYMBOLS

    logger.info("Transform started — symbols=%d", len(active_symbols))

    spark = get_spark_session("CryptoTransform")
    try:
        out_file = transform_data(spark, symbols=active_symbols)

        if out_file and not args.no_verify:
            logger.info("--- Verifying result ---")
            res = spark.read.parquet(out_file)
            display_cols = ["open_time", "symbol", "close", "trades", "RSI", "MACD", "MACD_signal"]
            res.select(display_cols).show(5)
            logger.info("Total records: %s", f"{res.count():,}")

    except TransformError as exc:
        logger.error("Transform failed: %s", exc)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")