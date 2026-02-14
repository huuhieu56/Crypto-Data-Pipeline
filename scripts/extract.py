"""Daily incremental extract from Binance REST API.

Run after pre_extract.py. Handles:
  - REST API incremental klines update (gap < 30 days)
  - Ticker 24h + Book Ticker
  - Order Book snapshot
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import pandas as pd
from datetime import datetime, timezone

from config.config import RAW_DATA_DIR, ORDER_BOOK_LIMIT
from config.symbols import SYMBOLS, SYMBOLS_STATUS
from utils.logger import get_logger
from utils.exceptions import ExtractError
from utils.binance_utils import (
    get_ticker_24h,
    get_book_ticker,
    get_order_book,
    fetch_klines_paginated,
    sleep_between_requests,
)
from utils.data_utils import (
    get_last_timestamp,
    get_target_end,
    merge_and_save_klines,
)

logger = get_logger(__name__)


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

    for idx, symbol in enumerate(symbols, 1):
        last_ts = get_last_timestamp(symbol)
        if last_ts is None:
            logger.debug(
                "[%d/%d] %s — no existing data, skip",
                idx, len(symbols), symbol,
            )
            continue

        if symbol in end_times:
            end_time = end_times[symbol]
        else:
            target_end = get_target_end(symbol)
            end_time = int(target_end.timestamp() * 1000)

        if last_ts >= end_time:
            continue

        new_frames = fetch_klines_paginated(symbol, last_ts, end_time)
        if not new_frames:
            logger.info(
                "[%d/%d] %s: REST API returned 0 new records",
                idx, len(symbols), symbol,
            )
            continue

        new_df = pd.concat(new_frames, ignore_index=True)
        csv_path = RAW_DATA_DIR / f"{symbol}.csv"
        combined = merge_and_save_klines(csv_path, new_df)
        logger.info(
            "[%d/%d] %s: +%d new (%s total)",
            idx, len(symbols), symbol, len(new_df), f"{len(combined):,}",
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

    for symbol in symbols:
        try:
            data = get_order_book(symbol, limit=ORDER_BOOK_LIMIT)
            sleep_between_requests()

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

            records.append({
                "symbol": symbol,
                "timestamp": timestamp,
                "total_bid_volume": bid_vol,
                "total_ask_volume": ask_vol,
                "imbalance": bid_vol / total if total > 0 else 0.0,
            })
        except Exception as exc:
            logger.error("Order book failed for %s: %s", symbol, exc)

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
def extract_daily(symbols: list[str] | None = None) -> None:
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

    # Ticker & order book for TRADING symbols only
    extract_ticker_24h(trading)
    extract_order_book_snapshot(trading)
    logger.info("=== Daily Extract finished ===")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract data from Binance REST API (daily incremental)",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYM",
        default=None,
        help="override symbol list (e.g. --symbols BTCUSDT ETHUSDT)",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    symbols = args.symbols or SYMBOLS

    logger.info("Extract started | symbols=%d", len(symbols))
    extract_daily(symbols)


if __name__ == "__main__":
    main()
