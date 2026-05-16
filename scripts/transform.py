"""Transform script — compute indicators and derived columns.

- klines: ELT — compute RSI(14) + MACD(12,26,9) via ClickHouse SQL (after load)
- ticker_24h: ETL — merge raw ticker + book_ticker, rename, compute spread_pct → Parquet
- order_book: ETL — compute volumes/imbalance → Parquet

Usage:
    python scripts/transform.py
    python scripts/transform.py --symbols BTCUSDT ETHUSDT
    python scripts/transform.py --month 2026-05
    python scripts/transform.py --only ticker orderbook
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
from datetime import datetime, timezone

import pandas as pd

from config.config import (
    BINANCE_COLUMN_MAP,
    INDICATOR_CONTEXT_ROWS,
    MINIO_CONFIG,
)
from config.symbols import SYMBOLS
from utils.data_utils import validate_month_str
from utils.db_utils import get_ch_client
from utils.exceptions import TransformError
from utils.logger import get_logger
from utils.storage import append_to_partition, discover_month_partitions, storage

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]


def _get_indicator_watermarks(symbols: list[str]) -> dict[str, int]:
    """Return last timestamp per symbol that has computed indicators.

    Raw klines are inserted with NULL indicators. Older local databases may
    still have raw rows with all-zero indicators, so all-zero rows are treated
    as untransformed for recovery/backfill.
    """
    if not symbols:
        return {}

    sym_list = "', '".join(symbols)
    client = get_ch_client()
    result = client.query(
        f"""
        SELECT
            symbol,
            maxIf(
                toInt64(toUnixTimestamp(timestamp)) * 1000,
                isNotNull(rsi_14)
                AND isNotNull(macd)
                AND isNotNull(macd_signal)
                AND (
                    ifNull(rsi_14, 0) != 0
                    OR ifNull(macd, 0) != 0
                    OR ifNull(macd_signal, 0) != 0
                )
            ) AS max_ts_ms
        FROM klines FINAL
        WHERE symbol IN ('{sym_list}')
        GROUP BY symbol
        """
    )

    watermarks: dict[str, int] = {}
    for symbol, max_ts_ms in result.result_rows:
        if max_ts_ms:
            watermarks[symbol] = int(max_ts_ms)
    return watermarks


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
    wm_map = _get_indicator_watermarks(symbols)
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
                    f"SELECT count() AS n, max(timestamp) AS max_ts "
                    f"FROM crypto_db.klines FINAL "
                    f"WHERE symbol = '{symbol}' AND toYYYYMM(timestamp) = {month_int}"
                )
                n, max_ts = result.result_rows[0] if result.result_rows else (0, None)
                if max_ts is not None:
                    watermark_ts = pd.Timestamp(max_ts)
                    if watermark_ts.tzinfo is None:
                        watermark_ts = watermark_ts.tz_localize("UTC")
                    watermark_ms = int(watermark_ts.timestamp() * 1000)
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


# --- Ticker Transform (ETL) ---------------------------------------------------


def transform_ticker(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """ETL: merge raw ticker + book_ticker, rename, compute spread_pct → Parquet.

    Reads raw from MinIO (ticker_raw/, book_ticker_raw/), merges by symbol,
    renames columns via BINANCE_COLUMN_MAP, adds snapshot_time, computes
    spread_pct, and writes transformed Parquet to ticker_24h/ in MinIO.
    """
    if symbols is None:
        symbols = SYMBOLS
    if month_str is not None:
        month_str = validate_month_str(month_str)

    snapshot_time = pd.Timestamp.now(tz="UTC")
    target_cols = [
        "symbol", "snapshot_time", "price_change", "price_change_pct",
        "high_24h", "low_24h", "volume_24h", "quote_volume_24h",
        "trade_count", "bid_price", "ask_price", "spread_pct",
    ]
    total_transformed = 0
    errors = 0

    logger.info("[Transform] ticker_24h: %d symbols, ETL (MinIO → MinIO)", len(symbols))

    for symbol in symbols:
        months = (
            [month_str]
            if month_str
            else discover_month_partitions(BUCKET_RAW, "ticker_raw", symbol)
        )
        if not months:
            logger.debug("[Transform] ticker_24h %s: no raw partitions", symbol)
            continue

        for month in months:
            try:
                ticker_key = f"ticker_raw/{symbol}/{month}.parquet"
                book_key = f"book_ticker_raw/{symbol}/{month}.parquet"

                if not storage.object_exists(BUCKET_RAW, ticker_key):
                    logger.debug("[Transform] ticker_24h %s/%s: no raw ticker", symbol, month)
                    continue

                ticker_df = storage.download_parquet(BUCKET_RAW, ticker_key).to_pandas()
                if ticker_df.empty:
                    continue

                # Merge book_ticker if available
                if storage.object_exists(BUCKET_RAW, book_key):
                    book_df = storage.download_parquet(BUCKET_RAW, book_key).to_pandas()
                    if not book_df.empty:
                        merge_cols = ["symbol"] + [c for c in ["bidPrice", "askPrice"] if c in book_df.columns]
                        ticker_df = ticker_df.merge(book_df[merge_cols], on="symbol", how="left")

                # Keep only mapped columns + symbol
                keep = ["symbol"] + [c for c in BINANCE_COLUMN_MAP if c in ticker_df.columns]
                ticker_df = ticker_df[keep]

                # Rename camelCase → snake_case
                rename_map = {k: v for k, v in BINANCE_COLUMN_MAP.items() if k in ticker_df.columns}
                ticker_df = ticker_df.rename(columns=rename_map)

                # Add snapshot_time
                ticker_df["snapshot_time"] = snapshot_time

                # Compute spread_pct
                ticker_df["spread_pct"] = 0.0
                if "bid_price" in ticker_df.columns and "ask_price" in ticker_df.columns:
                    bp = pd.to_numeric(ticker_df["bid_price"], errors="coerce")
                    ap = pd.to_numeric(ticker_df["ask_price"], errors="coerce")
                    ticker_df["spread_pct"] = ((ap - bp) / ap * 100).fillna(0.0)

                # Ensure target columns exist
                for col in target_cols:
                    if col not in ticker_df.columns:
                        ticker_df[col] = 0 if col == "spread_pct" else None
                ticker_df = ticker_df[target_cols]

                append_to_partition(
                    BUCKET_RAW, "ticker_24h", symbol,
                    ticker_df, dedup_col="snapshot_time", month_str=month,
                )
                total_transformed += len(ticker_df)
                logger.info("[Transform] ticker_24h %s/%s: %d rows", symbol, month, len(ticker_df))

            except Exception as exc:
                errors += 1
                logger.error("[Transform] ticker_24h %s/%s: ERROR -- %s", symbol, month, exc)

    if total_transformed == 0 and errors > 0:
        raise TransformError(f"ticker_24h: all {errors} partition(s) failed")
    logger.info("[Transform] ticker_24h complete: %d rows, %d errors", total_transformed, errors)


# --- Order Book Transform (ETL) -----------------------------------------------


def transform_order_book(
    symbols: list[str] | None = None,
    month_str: str | None = None,
) -> None:
    """ETL: compute volumes/imbalance from raw bids/asks → Parquet.

    Reads raw from MinIO (order_book/), computes total_bid_volume,
    total_ask_volume, imbalance, and writes transformed Parquet to
    order_book_snapshot/ in MinIO.
    """
    if symbols is None:
        symbols = SYMBOLS
    if month_str is not None:
        month_str = validate_month_str(month_str)

    target_cols = ["symbol", "timestamp", "total_bid_volume", "total_ask_volume", "imbalance"]
    total_transformed = 0
    errors = 0

    logger.info("[Transform] order_book_snapshot: %d symbols, ETL (MinIO → MinIO)", len(symbols))

    for symbol in symbols:
        months = (
            [month_str]
            if month_str
            else discover_month_partitions(BUCKET_RAW, "order_book", symbol)
        )
        if not months:
            logger.debug("[Transform] order_book_snapshot %s: no raw partitions", symbol)
            continue

        for month in months:
            try:
                key = f"order_book/{symbol}/{month}.parquet"
                if not storage.object_exists(BUCKET_RAW, key):
                    continue

                df = storage.download_parquet(BUCKET_RAW, key).to_pandas()
                if df.empty:
                    continue

                # Compute volumes from raw bids/asks arrays
                bid_vols = []
                ask_vols = []
                for _, row in df.iterrows():
                    bids = row.get("bids", [])
                    asks = row.get("asks", [])
                    bid_vol = sum(float(b[1]) for b in bids if len(b) > 1)
                    ask_vol = sum(float(a[1]) for a in asks if len(a) > 1)
                    bid_vols.append(bid_vol)
                    ask_vols.append(ask_vol)

                df["total_bid_volume"] = bid_vols
                df["total_ask_volume"] = ask_vols
                total = df["total_bid_volume"] + df["total_ask_volume"]
                df["imbalance"] = (df["total_bid_volume"] / total).fillna(0.0)

                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df[target_cols].copy()

                append_to_partition(
                    BUCKET_RAW, "order_book_snapshot", symbol,
                    df, dedup_col="timestamp", month_str=month,
                )
                total_transformed += len(df)
                logger.info("[Transform] order_book_snapshot %s/%s: %d rows", symbol, month, len(df))

            except Exception as exc:
                errors += 1
                logger.error("[Transform] order_book_snapshot %s/%s: ERROR -- %s", symbol, month, exc)

    if total_transformed == 0 and errors > 0:
        raise TransformError(f"order_book_snapshot: all {errors} partition(s) failed")
    logger.info("[Transform] order_book_snapshot complete: %d rows, %d errors", total_transformed, errors)


# --- CLI ---------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Transform raw data -> indicators")
    p.add_argument(
        "--only", nargs="+",
        choices=["klines", "ticker", "orderbook"],
        help="Transform only these datasets",
    )
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
    only = set(args.only) if args.only else None

    if only is None or "klines" in only:
        transform_klines(
            symbols=args.symbols or None,
            month_str=args.month,
        )
    if only is None or "ticker" in only:
        transform_ticker(
            symbols=args.symbols or None,
            month_str=args.month,
        )
    if only is None or "orderbook" in only:
        transform_order_book(
            symbols=args.symbols or None,
            month_str=args.month,
        )


if __name__ == "__main__":
    main()
