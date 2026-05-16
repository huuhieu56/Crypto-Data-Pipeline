"""Load script — write data to ClickHouse (ELT: load runs before transform).

Klines: raw OHLCV CSV from MinIO → ClickHouse klines (indicators NULL).
Ticker & order book: raw Parquet from MinIO → ClickHouse direct insert.

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

from config.config import MINIO_CONFIG, CLICKHOUSE_S3_ENDPOINT
from config.symbols import SYMBOLS, SYMBOL_REGISTRY
from utils.logger import get_logger
from utils.exceptions import LoadError
from utils.data_utils import validate_month_str
from utils.db_utils import (
    ch_insert_df,
    get_ch_client,
    get_table_watermarks,
)
from utils.storage import storage, discover_month_partitions

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]


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


def _insert_kline_csv_partition(
    symbol: str,
    month: str,
    watermark_ms: int,
) -> int:
    """Insert one raw kline CSV partition into ClickHouse.

    Returns number of rows inserted.
    """
    s3_access = MINIO_CONFIG["access_key"]
    s3_secret = MINIO_CONFIG["secret_key"]

    sql = (
        f"INSERT INTO crypto_db.klines "
        f"(symbol, timestamp, open, high, low, close, volume, quote_volume, trades, "
        f" rsi_14, macd, macd_signal) "
        f"SELECT "
        f"'{symbol}' AS symbol, "
        f"toDateTime(intDiv(open_time, 1000), 'UTC') AS timestamp, "
        f"open, high, low, close, volume, quote_volume, "
        f"toUInt32(trades) AS trades, "
        f"NULL AS rsi_14, NULL AS macd, NULL AS macd_signal "
        f"FROM s3("
        f"'{CLICKHOUSE_S3_ENDPOINT}/{BUCKET_RAW}/klines/{symbol}/{month}.csv', "
        f"'{s3_access}', '{s3_secret}', "
        f"'CSVWithNames', "
        f"'open_time Int64, open Float64, high Float64, low Float64, close Float64, "
        f" volume Float64, close_time Int64, quote_volume Float64, trades Int64, "
        f" taker_buy_base Float64, taker_buy_quote Float64'"
        f") "
        f"WHERE open_time > {watermark_ms}"
    )

    client = get_ch_client()
    client.query(sql)

    result = client.query(
        f"SELECT count() AS n FROM crypto_db.klines FINAL "
        f"WHERE symbol = '{symbol}' AND toYYYYMM(timestamp) = {month.replace('-', '')}"
    )
    return result.result_rows[0][0] if result.result_rows else 0


def _resolve_kline_months_to_load(
    symbol: str,
    month_str: str | None,
    watermark_ms: int,
) -> list[str]:
    """Return raw kline CSV month partitions that may contain new rows."""
    months = (
        [month_str]
        if month_str
        else discover_month_partitions(BUCKET_RAW, "klines", symbol, extension=".csv")
    )
    if watermark_ms <= 0:
        return months

    watermark_month = pd.to_datetime(watermark_ms, unit="ms", utc=True).strftime("%Y-%m")
    return [m for m in months if m >= watermark_month]


def load_klines(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Load raw OHLCV from MinIO CSV into ClickHouse klines (ELT: load before transform).

    Inserts raw candle data without indicators (rsi_14/macd/macd_signal = NULL).
    The transform step (scripts/transform.py) computes indicators afterwards.
    """
    symbols = symbols or SYMBOLS
    if month_str is not None:
        month_str = validate_month_str(month_str)
    wm_map = get_table_watermarks("klines", "timestamp", symbols)
    total_inserted = 0
    errors = 0

    logger.info("[Load] klines: %d symbols, raw CSV -> ClickHouse", len(symbols))

    for symbol in symbols:
        watermark_ms = wm_map.get(symbol, 0)
        months = _resolve_kline_months_to_load(symbol, month_str, watermark_ms)
        if not months:
            logger.debug("[Load] klines %s: no raw CSV partitions found", symbol)
            continue

        for month in months:
            key = f"klines/{symbol}/{month}.csv"
            if not storage.object_exists(BUCKET_RAW, key):
                logger.debug("[Load] klines %s/%s: no raw CSV, skipped", symbol, month)
                continue

            try:
                n = _insert_kline_csv_partition(symbol, month, watermark_ms)
                logger.info("[Load] klines %s/%s: %d raw rows loaded", symbol, month, n)
                total_inserted += n
            except Exception as exc:
                errors += 1
                logger.error("[Load] klines %s/%s: ERROR -- %s", symbol, month, exc)

    if total_inserted == 0 and errors > 0:
        raise LoadError(f"klines: all {errors} partition(s) failed, 0 loaded")
    logger.info("[Load] klines complete: %s raw rows across %d symbols, %d errors", f"{total_inserted:,}", len(symbols), errors)


def load_ticker(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Load transformed ticker Parquet from MinIO → ClickHouse."""
    _load_table(symbols, BUCKET_RAW, "ticker_24h", "snapshot_time", month_str=month_str)


def load_order_book(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """Load transformed order book Parquet from MinIO → ClickHouse."""
    _load_table(symbols, BUCKET_RAW, "order_book_snapshot", "timestamp", month_str=month_str)


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

    logger.info("=== Load pipeline complete ===")


if __name__ == "__main__":
    args = _build_parser().parse_args()
    main(
        only=set(args.only) if args.only else None,
        skip=set(args.skip),
        month_str=args.month,
    )
