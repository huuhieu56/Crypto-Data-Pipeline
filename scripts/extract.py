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
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.config import RAW_DATA_DIR, ORDER_BOOK_LIMIT, MONTHS_BACK
from config.symbols import SYMBOLS, SYMBOLS_STATUS
from utils.logger import get_logger
from utils.exceptions import ExtractError
from utils.binance_utils import (
    get_ticker_24h,
    get_book_ticker,
    get_order_book,
    fetch_klines_paginated,
    download_klines_month,
    sleep_between_requests,
)
from utils.data_utils import (
    get_last_timestamp,
    get_target_end,
    get_target_months,
    merge_and_save_klines,
)

logger = get_logger(__name__)
KLINES_MAX_WORKERS = 8
ORDERBOOK_MAX_WORKERS = 8


# ---------------------------------------------------------------------------
# Data Vision (bulk download)
# ---------------------------------------------------------------------------
def download_data_vision(
    symbol: str,
    months: list[tuple[int, int]],
) -> int | None:
    """Download klines from Data Vision month-by-month, append to CSV.

    Months are processed in chronological order so each write uses
    the fast append path (no re-read of entire CSV).  Peak memory =
    1 month (~44K rows) instead of all months concatenated (~1.58M).

    Returns total record count, or None if no data was downloaded.
    """
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"
    months_ok = 0
    total = 0

    for year, month in sorted(months):
        df = download_klines_month(symbol, year, month)
        if df is not None:
            total = merge_and_save_klines(csv_path, df)
            months_ok += 1
            logger.debug("%s %d-%02d: %d records", symbol, year, month, len(df))

    if months_ok == 0:
        return None

    logger.info(
        "%s: %d/%d months OK, %s total records",
        symbol, months_ok, len(months), f"{total:,}",
    )
    return total


def extract_bulk(
    symbols: list[str] | None = None,
    months_back: int = MONTHS_BACK,
) -> dict[str, int]:
    """Force re-download all symbols from Data Vision.

    Returns {symbol: record_count} instead of DataFrames to avoid
    holding ~78M rows in memory simultaneously.
    """
    symbols = symbols or SYMBOLS
    target_months = get_target_months(months_back)
    results: dict[str, int] = {}

    logger.info(
        "=== Bulk Extract: %d symbols, %d months ===",
        len(symbols), months_back,
    )

    for idx, symbol in enumerate(symbols, 1):
        logger.info("[%d/%d] Bulk downloading %s", idx, len(symbols), symbol)
        total = download_data_vision(symbol, target_months)
        if total is not None:
            results[symbol] = total
        else:
            logger.error("No Data Vision data for %s", symbol)

    grand_total = sum(results.values())
    logger.info(
        "=== Bulk Extract complete: %d/%d symbols, %s records ===",
        len(results), len(symbols), f"{grand_total:,}",
    )

    # Keep snapshot data in sync with klines extract
    trading = [
        s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") == "TRADING"
    ]
    extract_ticker_24h(trading)
    extract_order_book_snapshot(trading)
    logger.info(
        "Bulk snapshot complete — ticker/order_book for %d TRADING symbols",
        len(trading),
    )

    return results


# ---------------------------------------------------------------------------
# REST API (incremental)
# ---------------------------------------------------------------------------
def extract_recent_klines(
    symbols: list[str],
    end_times: dict[str, int] | None = None,
) -> dict[str, pd.DataFrame]:
    """Incremental klines update via REST API (target: now or break_date).

    Args:
        symbols: List of symbols to update.
        end_times: Optional per-symbol end-time overrides (ms).
                   If not given, each symbol uses get_target_end().
    """
    if not symbols:
        return {}

    end_times = end_times or {}
    results: dict[str, pd.DataFrame] = {}

    def _extract_one(symbol: str) -> tuple[str, pd.DataFrame | None, int | None]:
        last_ts = get_last_timestamp(symbol)
        if last_ts is None:
            return symbol, None, None

        if symbol in end_times:
            end_time = end_times[symbol]
        else:
            target_end = get_target_end(symbol)
            end_time = int(target_end.timestamp() * 1000)

        if last_ts >= end_time:
            return symbol, None, None

        new_df = fetch_klines_paginated(symbol, last_ts, end_time)
        if new_df is None or new_df.empty:
            return symbol, None, None

        csv_path = RAW_DATA_DIR / f"{symbol}.csv"
        total = merge_and_save_klines(csv_path, new_df)
        return symbol, new_df, total

    max_workers = min(KLINES_MAX_WORKERS, len(symbols))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_extract_one, symbol): (idx, symbol)
            for idx, symbol in enumerate(symbols, 1)
        }
        for future in as_completed(future_map):
            idx, symbol = future_map[future]
            try:
                symbol, new_df, total = future.result()
            except Exception as exc:
                logger.error("[%d/%d] %s: extract failed: %s", idx, len(symbols), symbol, exc)
                continue

            if new_df is None or total is None:
                logger.debug("[%d/%d] %s: no new klines", idx, len(symbols), symbol)
                continue

            logger.info(
                "[%d/%d] %s: +%d new (%s total)",
                idx, len(symbols), symbol, len(new_df), f"{total:,}",
            )
            results[symbol] = new_df

    return results


# ---------------------------------------------------------------------------
# Ticker 24h & Order Book
# ---------------------------------------------------------------------------
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

    output_path = RAW_DATA_DIR / "ticker_24h.csv"
    write_header = not output_path.exists()
    merged.to_csv(output_path, mode="a", header=write_header, index=False)
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

            bid_vol = 0.0
            for b in data.get("bids", []):
                try:
                    bid_vol += float(b[1])
                except (ValueError, IndexError):
                    pass

            ask_vol = 0.0
            for a in data.get("asks", []):
                try:
                    ask_vol += float(a[1])
                except (ValueError, IndexError):
                    pass

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
        future_map = {pool.submit(_extract_one, symbol): symbol for symbol in symbols}
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                rec = future.result()
                if rec is not None:
                    records.append(rec)
            except Exception as exc:
                logger.error("Order book future failed for %s: %s", symbol, exc)

    # Small sleep after concurrent burst to stay polite with API limits
    sleep_between_requests()

    if not records:
        logger.error("No order book data collected")
        return None

    df = pd.DataFrame(records)
    output_path = RAW_DATA_DIR / "order_book_snapshot.csv"
    write_header = not output_path.exists()
    df.to_csv(output_path, mode="a", header=write_header, index=False)
    logger.info("Saved order_book_snapshot (+%d records)", len(df))
    return df


# ---------------------------------------------------------------------------
# Entry Points
# ---------------------------------------------------------------------------
def extract_daily(
    symbols: list[str] | None = None,
) -> None:
    """Daily extract: REST API klines + ticker + order book."""
    symbols = symbols or SYMBOLS
    trading = [
        s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") == "TRADING"
    ]
    non_trading = [s for s in symbols if s not in set(trading)]

    logger.info(
        "=== Daily Extract: %d symbols (%d TRADING, %d BREAK) ===",
        len(symbols), len(trading), len(non_trading),
    )

    # REST API incremental klines (all symbols including BREAK)
    recent = extract_recent_klines(symbols)
    if recent:
        total = sum(len(df) for df in recent.values())
        logger.info(
            "REST API complete — %d/%d symbols updated, %s new records",
            len(recent), len(symbols), f"{total:,}",
        )

    # Ticker & order book for same TRADING symbols as extract scope
    extract_ticker_24h(trading)
    extract_order_book_snapshot(trading)
    logger.info("=== Daily Extract finished ===")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract data from Binance (bulk Data Vision or daily REST API)",
    )
    parser.add_argument(
        "--mode",
        choices=["daily", "bulk"],
        default="daily",
        help="daily = incremental REST API, bulk = full Data Vision (default: daily)",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYM",
        default=None,
        help="override symbols for klines + ticker + orderbook (e.g. --symbols BTCUSDT ETHUSDT)",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=MONTHS_BACK,
        help=f"months of history for bulk mode (default: {MONTHS_BACK})",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    symbols = args.symbols or SYMBOLS

    logger.info(
        "Extract started | mode=%s | symbols=%d",
        args.mode,
        len(symbols),
    )

    if args.mode == "bulk":
        extract_bulk(symbols, months_back=args.months)
    else:
        extract_daily(symbols)


if __name__ == "__main__":
    main()
