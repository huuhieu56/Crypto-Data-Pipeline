"""Extract data from Binance.

Two modes:
  bulk  — Full historical download via Data Vision (monthly ZIP files)
  daily — Incremental update via REST API + Ticker 24h + Order Book

Bulk mode is used by pre_extract.py for first-time / backfill downloads.
Daily mode runs after pre_extract.py for ongoing incremental updates.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import pandas as pd
import pyarrow as pa
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.config import (
    ORDER_BOOK_LIMIT, MONTHS_BACK, MINIO_CONFIG, PARALLELISM, PARTITION_DATE_FORMAT,
)
from config.symbols import SYMBOLS, SYMBOLS_STATUS
from utils.logger import get_logger
from utils.exceptions import ExtractError
from utils.storage import storage
from utils.db_utils import get_last_timestamps
from utils.data_utils import partition_key, get_target_end, get_target_months
from utils.binance_utils import (
    get_ticker_24h,
    get_book_ticker,
    get_order_book,
    fetch_klines_paginated,
    download_klines_month,
    sleep_between_requests,
)

logger = get_logger(__name__)
KLINES_MAX_WORKERS = PARALLELISM["klines_max_workers"]
ORDERBOOK_MAX_WORKERS = PARALLELISM["orderbook_max_workers"]
BULK_DOWNLOAD_WORKERS = PARALLELISM["bulk_download_workers"]
BULK_SYMBOL_WORKERS = PARALLELISM["bulk_symbol_workers"]
BUCKET_RAW = MINIO_CONFIG["bucket_raw"]


# --- Partition I/O -----------------------------------------------------------

def _write_to_partition(symbol: str, new_df: pd.DataFrame) -> None:
    """Append rows to today's daily partition on MinIO.

    Daily partition is small (~1,440 rows max = ~100KB),
    so download+concat+upload is fast.
    """
    key = partition_key(symbol)
    new_table = pa.Table.from_pandas(new_df, preserve_index=False)

    if storage.object_exists(BUCKET_RAW, key):
        existing = storage.download_parquet(BUCKET_RAW, key)
        table = pa.concat_tables([existing, new_table])
    else:
        table = new_table

    storage.upload_parquet(BUCKET_RAW, key, table)


# --- Data Vision (parallel bulk download) ------------------------------------

def download_data_vision(
    symbol: str,
    months: list[tuple[int, int]],
) -> int | None:
    """Download klines from Data Vision with parallel month downloads.

    Writes daily partitions only: klines/{SYMBOL}/{YYYY-MM-DD}.parquet
    Returns total record count, or None if no data was downloaded.
    """
    sorted_months = sorted(months)
    if not sorted_months:
        return None

    downloaded: dict[tuple[int, int], pd.DataFrame] = {}
    max_workers = min(BULK_DOWNLOAD_WORKERS, len(sorted_months))

    def _download_one(ym: tuple[int, int]) -> tuple[tuple[int, int], pd.DataFrame | None]:
        y, m = ym
        return ym, download_klines_month(symbol, y, m)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_download_one, ym): ym for ym in sorted_months}
        for future in as_completed(future_map):
            ym = future_map[future]
            try:
                ym, df = future.result()
                if df is not None:
                    downloaded[ym] = df
                    logger.info(
                        "[%s] %d-%02d: %s rows",
                        symbol, ym[0], ym[1], f"{len(df):,}",
                    )
            except Exception as exc:
                logger.error(
                    "[%s] %d-%02d: ERROR %s",
                    symbol, ym[0], ym[1], exc,
                )

    if not downloaded:
        return None

    # Write daily partitions (consistent with incremental path)
    total = 0
    for ym in sorted_months:
        if ym not in downloaded:
            continue
        df = downloaded[ym]
        if df.empty:
            continue

        open_time = pd.to_datetime(df["open_time"], utc=True, errors="coerce")
        if open_time.isna().all():
            open_time = pd.to_datetime(df["open_time"], errors="coerce")

        daily_df = df.assign(_dt=open_time).dropna(subset=["_dt"])
        if daily_df.empty:
            continue

        for date_val, one_day in daily_df.groupby(daily_df["_dt"].dt.date):
            day_str = date_val.strftime(PARTITION_DATE_FORMAT)
            key = partition_key(symbol, day_str)
            day_out = one_day.drop(columns=["_dt"]).sort_values("open_time")
            table = pa.Table.from_pandas(day_out, preserve_index=False)
            storage.upload_parquet(BUCKET_RAW, key, table)
            total += len(day_out)

    logger.info(
        "[%s] Bulk complete: %d/%d months, %s records",
        symbol, len(downloaded), len(sorted_months), f"{total:,}",
    )
    return total


def extract_bulk(
    symbols: list[str] | None = None,
    months_back: int = MONTHS_BACK,
) -> dict[str, int]:
    """Force re-download all symbols from Data Vision (parallel)."""
    symbols = symbols or SYMBOLS
    target_months = get_target_months(months_back)
    results: dict[str, int] = {}

    logger.info(
        "=== Bulk Extract: %d symbols × %d months ===",
        len(symbols), months_back,
    )

    def _bulk_one(symbol: str) -> tuple[str, int | None]:
        return symbol, download_data_vision(symbol, target_months)

    max_workers = min(BULK_SYMBOL_WORKERS, len(symbols))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_bulk_one, sym): sym for sym in symbols}
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                sym, total = future.result()
                if total is not None:
                    results[sym] = total
                    logger.info("[%s] %s records", sym, f"{total:,}")
            except Exception as exc:
                logger.error("[%s] FAILED — %s", symbol, exc)

    # Snapshot data
    trading = [s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") == "TRADING"]
    extract_ticker_24h(trading)
    extract_order_book_snapshot(trading)

    return results


# --- REST API (incremental) --------------------------------------------------

def extract_recent_klines(
    symbols: list[str],
    end_times: dict[str, int] | None = None,
) -> dict[str, pd.DataFrame]:
    """Incremental klines update via REST API.

    Uses batch ClickHouse query for last timestamps (1 query for all symbols).
    Writes new rows to daily partitions on MinIO.
    """
    if not symbols:
        return {}

    end_times = end_times or {}
    results: dict[str, pd.DataFrame] = {}

    # 1 batch query for all symbols instead of N file downloads
    last_timestamps = get_last_timestamps(symbols)

    def _extract_one(symbol: str) -> tuple[str, pd.DataFrame | None]:
        last_ts = last_timestamps.get(symbol)
        if last_ts is None:
            return symbol, None

        if symbol in end_times:
            end_time = end_times[symbol]
        else:
            target_end = get_target_end(symbol)
            end_time = int(target_end.timestamp() * 1000)

        if last_ts >= end_time:
            return symbol, None

        new_df = fetch_klines_paginated(symbol, last_ts, end_time)
        if new_df is None or new_df.empty:
            return symbol, None

        _write_to_partition(symbol, new_df)
        return symbol, new_df

    max_workers = min(KLINES_MAX_WORKERS, len(symbols))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_extract_one, symbol): (idx, symbol)
            for idx, symbol in enumerate(symbols, 1)
        }
        for future in as_completed(future_map):
            idx, symbol = future_map[future]
            try:
                symbol, new_df = future.result()
                if new_df is not None:
                    results[symbol] = new_df
                    logger.info(
                        "[%d/%d] %s: +%d rows",
                        idx, len(symbols), symbol, len(new_df),
                    )
            except Exception as exc:
                logger.error("[%d/%d] %s: %s", idx, len(symbols), symbol, exc)

    return results


# --- Ticker 24h & Order Book -------------------------------------------------

def extract_ticker_24h(symbols: list[str]) -> pd.DataFrame | None:
    """Fetch ticker/24hr + bookTicker, append to CSV."""
    if not symbols:
        return None

    symbols_set = set(symbols)
    snapshot_time = datetime.now(timezone.utc)

    try:
        ticker_raw = get_ticker_24h()
    except Exception as exc:
        raise ExtractError(f"Failed to fetch ticker/24hr: {exc}") from exc

    ticker_df = pd.DataFrame(ticker_raw)
    ticker_df = ticker_df[ticker_df["symbol"].isin(symbols_set)].copy()
    ticker_df = ticker_df.rename(columns={
        "priceChange": "price_change",
        "priceChangePercent": "price_change_pct",
        "highPrice": "high_24h",
        "lowPrice": "low_24h",
        "volume": "volume_24h",
        "quoteVolume": "quote_volume_24h",
        "count": "trade_count",
    })
    ticker_df = ticker_df[[
        "symbol", "price_change", "price_change_pct",
        "high_24h", "low_24h", "volume_24h", "quote_volume_24h", "trade_count",
    ]]

    try:
        book_raw = get_book_ticker()
    except Exception as exc:
        raise ExtractError(f"Failed to fetch bookTicker: {exc}") from exc

    book_df = pd.DataFrame(book_raw)
    book_df = book_df[book_df["symbol"].isin(symbols_set)].copy()
    book_df = book_df.rename(columns={"bidPrice": "bid_price", "askPrice": "ask_price"})
    book_df = book_df[["symbol", "bid_price", "ask_price"]]

    merged = ticker_df.merge(book_df, on="symbol", how="left")

    float_cols = [
        "price_change", "price_change_pct", "high_24h", "low_24h",
        "volume_24h", "quote_volume_24h", "bid_price", "ask_price",
    ]
    for col in float_cols:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    merged["trade_count"] = pd.to_numeric(
        merged["trade_count"], errors="coerce",
    ).astype("Int64")
    merged["spread_pct"] = (
        (merged["ask_price"] - merged["bid_price"]) / merged["ask_price"] * 100
    )
    merged.insert(1, "snapshot_time", snapshot_time)

    merged = merged[[
        "symbol", "snapshot_time", "price_change", "price_change_pct",
        "high_24h", "low_24h", "volume_24h", "quote_volume_24h",
        "trade_count", "bid_price", "ask_price", "spread_pct",
    ]]

    storage.append_csv_df(BUCKET_RAW, "ticker_24h.csv", merged)
    logger.info("Saved ticker_24h (+%d records)", len(merged))
    return merged


def extract_order_book_snapshot(symbols: list[str]) -> pd.DataFrame | None:
    """Fetch order book depth and compute imbalance."""
    if not symbols:
        return None

    timestamp = datetime.now(timezone.utc)
    records = []

    def _extract_one(symbol: str) -> dict | None:
        try:
            data = get_order_book(symbol, limit=ORDER_BOOK_LIMIT)

            bid_vol = sum(
                float(b[1]) for b in data.get("bids", [])
                if len(b) > 1 and _is_float(b[1])
            )
            ask_vol = sum(
                float(a[1]) for a in data.get("asks", [])
                if len(a) > 1 and _is_float(a[1])
            )
            total = bid_vol + ask_vol

            return {
                "symbol": symbol,
                "timestamp": timestamp,
                "total_bid_volume": bid_vol,
                "total_ask_volume": ask_vol,
                "imbalance": bid_vol / total if total > 0 else 0.0,
            }
        except Exception as exc:
            logger.error("Order book failed for %s: %s", symbol, exc)
            return None

    max_workers = min(ORDERBOOK_MAX_WORKERS, len(symbols))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_extract_one, s): s for s in symbols}
        for future in as_completed(future_map):
            try:
                rec = future.result()
                if rec is not None:
                    records.append(rec)
            except Exception as exc:
                logger.error("Order book future failed: %s", exc)

    sleep_between_requests()

    if not records:
        logger.error("No order book data collected")
        return None

    df = pd.DataFrame(records)
    storage.append_csv_df(BUCKET_RAW, "order_book_snapshot.csv", df)
    logger.info("Saved order_book_snapshot (+%d records)", len(df))
    return df


def _is_float(s: str) -> bool:
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


# --- Entry Points ------------------------------------------------------------

def extract_daily(symbols: list[str] | None = None) -> None:
    """Daily extract: REST API klines + ticker + order book."""
    symbols = symbols or SYMBOLS
    trading = [s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") == "TRADING"]

    logger.info(
        "=== Daily Extract: %d symbols (%d TRADING) ===",
        len(symbols), len(trading),
    )

    recent = extract_recent_klines(symbols)
    if recent:
        total = sum(len(df) for df in recent.values())
        logger.info("REST API: %d symbols updated, %s new records", len(recent), f"{total:,}")

    extract_ticker_24h(trading)
    extract_order_book_snapshot(trading)
    logger.info("=== Daily Extract finished ===")


# --- CLI ---------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract data from Binance (bulk or daily)",
    )
    parser.add_argument(
        "--mode", choices=["daily", "bulk"], default="daily",
        help="daily = incremental REST API, bulk = full Data Vision",
    )
    parser.add_argument(
        "--symbols", nargs="+", metavar="SYM", default=None,
        help="override symbols list",
    )
    parser.add_argument(
        "--months", type=int, default=MONTHS_BACK,
        help=f"months of history for bulk mode (default: {MONTHS_BACK})",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    symbols = args.symbols or SYMBOLS

    logger.info("Extract started | mode=%s | symbols=%d", args.mode, len(symbols))

    if args.mode == "bulk":
        extract_bulk(symbols, months_back=args.months)
    else:
        extract_daily(symbols)


if __name__ == "__main__":
    main()
