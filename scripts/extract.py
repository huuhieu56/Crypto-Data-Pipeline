# =============================================================================
# Extract Script - Thu thap du lieu tu Binance
# =============================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import io
import zipfile
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from config.config import (
    RAW_DATA_DIR,
    BINANCE_DATA_VISION_URL,
    MONTHS_BACK,
    API_LIMIT,
    ORDER_BOOK_LIMIT,
)
from config.symbols import SYMBOLS
from utils.logger import get_logger
from utils.exceptions import ExtractError
from utils.binance_utils import (
    make_request_raw,
    get_klines,
    get_ticker_24h,
    get_book_ticker,
    get_order_book,
    sleep_between_requests,
)

logger = get_logger(__name__)

# Binance klines API tra ve 12 cot, cot cuoi ("ignore") bi bo
_KLINES_RAW_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]
_KLINES_COLUMNS = [c for c in _KLINES_RAW_COLUMNS if c != "ignore"]
_NUMERIC_COLUMNS = [
    "open", "high", "low", "close", "volume",
    "quote_volume", "taker_buy_base", "taker_buy_quote",
]


# =============================================================================
# Helpers
# =============================================================================
def _get_target_months(months_back: int) -> list[tuple[int, int]]:
    """Tra ve danh sach (year, month) de download tu Data Vision."""
    end_date = datetime.now() - relativedelta(months=1)
    return [
        ((end_date - relativedelta(months=i)).year,
         (end_date - relativedelta(months=i)).month)
        for i in range(months_back)
    ]


def _get_last_timestamp(symbol: str) -> int | None:
    """Tra ve open_time cuoi cung (ms) tu file CSV da luu."""
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, usecols=["open_time"])
        if df.empty:
            return None
        last = pd.to_datetime(df["open_time"].iloc[-1])
        return int(last.timestamp() * 1000)
    except Exception as exc:
        logger.error("Cannot read last timestamp from %s: %s", csv_path.name, exc)
        return None


def _parse_klines_df(raw_data: list[list], symbol: str) -> pd.DataFrame:
    """Chuyen raw klines (list of lists) thanh DataFrame chuan."""
    df = pd.DataFrame(raw_data, columns=_KLINES_RAW_COLUMNS)
    df = df.drop(columns=["ignore"])

    for col in _NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    df["symbol"] = symbol
    return df


# =============================================================================
# Extract Klines - Binance Data Vision (bulk, one-time)
# =============================================================================
def _download_month(symbol: str, year: int, month: int) -> pd.DataFrame | None:
    """Download 1 file ZIP klines tu Data Vision, parse thanh DataFrame."""
    url = BINANCE_DATA_VISION_URL.format(symbol=symbol, year=year, month=month)

    try:
        content = make_request_raw(url, timeout=60)
    except Exception as exc:
        logger.warning("Download failed %s %d-%02d: %s", symbol, year, month, exc)
        return None

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        with z.open(z.namelist()[0]) as f:
            df = pd.read_csv(f, header=None, usecols=range(11), names=_KLINES_COLUMNS)

    # Data Vision co the tra ve microseconds hoac milliseconds
    raw_ts = df["open_time"].astype("int64")
    divisor = 1000 if raw_ts.iloc[0] > 1e15 else 1

    df["open_time"] = pd.to_datetime(raw_ts // divisor, unit="ms")
    df["close_time"] = pd.to_datetime(
        df["close_time"].astype("int64") // divisor, unit="ms",
    )
    df["symbol"] = symbol
    return df


def extract_klines(
    symbols: list[str],
    months_back: int = MONTHS_BACK,
) -> dict[str, pd.DataFrame]:
    """Bulk download klines tu Data Vision cho nhieu symbols."""
    target_months = _get_target_months(months_back)
    results: dict[str, pd.DataFrame] = {}

    for idx, symbol in enumerate(symbols, 1):
        logger.info("[%d/%d] Downloading %s", idx, len(symbols), symbol)
        frames = []

        for year, month in target_months:
            df = _download_month(symbol, year, month)
            if df is not None:
                frames.append(df)

        if not frames:
            logger.error("No data for %s", symbol)
            continue

        combined = (
            pd.concat(frames, ignore_index=True)
            .sort_values("open_time")
            .reset_index(drop=True)
        )
        output = RAW_DATA_DIR / f"{symbol}.csv"
        combined.to_csv(output, index=False)
        logger.info("Saved %s (%s records)", output.name, f"{len(combined):,}")
        results[symbol] = combined

    return results


# =============================================================================
# Extract Recent Klines - REST API (daily incremental)
# =============================================================================
def _fetch_klines_paginated(
    symbol: str,
    start_time: int,
    end_time: int,
) -> list[pd.DataFrame]:
    """Paginate qua /klines API tu start_time den end_time."""
    frames = []
    cursor = start_time + 60_000

    while True:
        params = {"startTime": cursor, "endTime": end_time, "limit": API_LIMIT}

        try:
            data = get_klines(symbol, **params)
        except Exception as exc:
            logger.error("API error for %s: %s", symbol, exc)
            break

        if not data:
            break

        frames.append(_parse_klines_df(data, symbol))

        if len(data) < API_LIMIT:
            break

        cursor = int(data[-1][0]) + 60_000
        sleep_between_requests()

    return frames


def extract_recent_klines(symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Cap nhat klines moi tu REST API (incremental append)."""
    if not symbols:
        return {}

    results: dict[str, pd.DataFrame] = {}
    end_time = int(datetime.now().timestamp() * 1000)

    for idx, symbol in enumerate(symbols, 1):
        last_ts = _get_last_timestamp(symbol)
        if last_ts is None:
            logger.debug("[%d/%d] %s — no existing data, skip", idx, len(symbols), symbol)
            continue

        new_frames = _fetch_klines_paginated(symbol, last_ts, end_time)
        if not new_frames:
            continue

        new_df = pd.concat(new_frames, ignore_index=True)
        csv_path = RAW_DATA_DIR / f"{symbol}.csv"
        old_df = pd.read_csv(csv_path, parse_dates=["open_time", "close_time"])

        combined = (
            pd.concat([old_df, new_df], ignore_index=True)
            .drop_duplicates(subset=["open_time"], keep="last")
            .sort_values("open_time")
            .reset_index(drop=True)
        )
        combined.to_csv(csv_path, index=False)
        logger.info(
            "[%d/%d] %s: +%d new (%s total)",
            idx, len(symbols), symbol, len(new_df), f"{len(combined):,}",
        )
        results[symbol] = combined

    return results


# =============================================================================
# Extract Ticker 24h
# =============================================================================
def extract_ticker_24h(symbols: list[str]) -> pd.DataFrame | None:
    """Lay ticker/24hr + bookTicker, merge va luu CSV."""
    if not symbols:
        return None

    symbols_set = set(symbols)
    snapshot_time = datetime.utcnow()

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

    # Merge + cast types
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

    # Append vao CSV
    output_path = RAW_DATA_DIR / "ticker_24h.csv"
    if output_path.exists():
        old = pd.read_csv(output_path)
        merged = pd.concat([old, merged], ignore_index=True)

    merged.to_csv(output_path, index=False)
    logger.info("Saved ticker_24h (%s records)", f"{len(merged):,}")
    return merged


# =============================================================================
# Extract Order Book Snapshot
# =============================================================================
def extract_order_book_snapshot(symbols: list[str]) -> pd.DataFrame | None:
    """Lay order book depth, tinh imbalance cho moi symbol."""
    if not symbols:
        return None

    timestamp = datetime.utcnow()
    records = []

    for symbol in symbols:
        try:
            data = get_order_book(symbol, limit=ORDER_BOOK_LIMIT)
            bid_vol = sum(float(b[1]) for b in data.get("bids", []))
            ask_vol = sum(float(a[1]) for a in data.get("asks", []))
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


# =============================================================================
# Entry Points
# =============================================================================
def extract_bulk(
    symbols: list[str] | None = None,
    months_back: int = MONTHS_BACK,
) -> None:
    """One-time bulk download tu Binance Data Vision."""
    symbols = symbols or SYMBOLS
    logger.info("=== Bulk extraction: %d symbols, %d months ===", len(symbols), months_back)

    results = extract_klines(symbols, months_back=months_back)
    total = sum(len(df) for df in results.values())
    logger.info("Bulk complete: %d/%d symbols, %s records", len(results), len(symbols), f"{total:,}")


def extract_daily(symbols: list[str] | None = None) -> None:
    """Daily extraction: recent klines + ticker 24h + order book."""
    symbols = symbols or SYMBOLS
    logger.info("=== Daily extraction: %d symbols ===", len(symbols))

    recent = extract_recent_klines(symbols)
    if recent:
        total = sum(len(df) for df in recent.values())
        logger.info("Recent klines: %d symbols, %s new records", len(recent), f"{total:,}")

    extract_ticker_24h(symbols)
    extract_order_book_snapshot(symbols)
    logger.info("=== Daily extraction finished ===")


# =============================================================================
# CLI
# =============================================================================
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract data from Binance API & Data Vision",
    )
    parser.add_argument(
        "--mode",
        choices=["bulk", "daily", "full"],
        default="full",
        help="bulk = Data Vision only, daily = REST API only, full = both (default: full)",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYM",
        default=None,
        help="override symbol list (e.g. --symbols BTCUSDT ETHUSDT)",
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
        "Extract started — mode=%s, symbols=%d, months=%d",
        args.mode, len(symbols), args.months,
    )

    if args.mode in ("bulk", "full"):
        extract_bulk(symbols, months_back=args.months)
    if args.mode in ("daily", "full"):
        extract_daily(symbols)


if __name__ == "__main__":
    main()
