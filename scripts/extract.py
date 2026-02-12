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
from config.symbols import SYMBOLS, SYMBOLS_STATUS
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

# Nguong gap (ngay).  < threshold → REST API,  >= threshold → Data Vision + REST API
_GAP_THRESHOLD_DAYS = 30


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


def _get_months_between(
    start_dt: datetime,
    end_dt: datetime,
) -> list[tuple[int, int]]:
    """Tra ve cac thang DA KET THUC TRON VEN nam giua start_dt va end_dt.

    - Bo qua thang chua start_dt (dang do → REST API lap phan con lai).
    - Bo qua thang chua end_dt   (chua ket thuc → Data Vision chua co).
    - Chi gom nhung thang ma ngay dau + ngay cuoi deu nam trong gap.

    Vi du: start_dt = 15/01, end_dt = 15/03
      → Thang 1: bo (do dang)  → REST API se fill 15/01 → 31/01
      → Thang 2: tron ven      → tai tu Data Vision
      → Thang 3: chua het      → REST API
      ⇒ Ket qua: [(year, 2)]
    """
    months: list[tuple[int, int]] = []

    # Bat dau tu ngay 1 cua thang TIEP THEO sau start_dt
    cursor = start_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + relativedelta(months=1)

    while True:
        next_month_start = cursor + relativedelta(months=1)
        # Thang nay ket thuc tron ven chi khi ngay 1 thang sau <= end_dt
        if next_month_start > end_dt:
            break
        months.append((cursor.year, cursor.month))
        cursor = next_month_start

    return months


def _backfill_months(
    symbol: str,
    months: list[tuple[int, int]],
) -> int:
    """Download cac thang tu Data Vision va merge vao file CSV hien tai.

    Returns:
        So thang tai thanh cong.
    """
    frames: list[pd.DataFrame] = []
    for year, month in months:
        df = _download_month(symbol, year, month)
        if df is not None:
            frames.append(df)
            logger.info("  %s %d-%02d: %s records fetched", symbol, year, month, f"{len(df):,}")
        else:
            logger.warning("  %s %d-%02d: Data Vision file unavailable, will rely on REST API", symbol, year, month)

    if not frames:
        return 0

    new_data = pd.concat(frames, ignore_index=True)
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"

    if csv_path.exists():
        old_df = pd.read_csv(csv_path, parse_dates=["open_time", "close_time"])
        combined = pd.concat([old_df, new_data], ignore_index=True)
    else:
        combined = new_data

    combined = (
        combined
        .drop_duplicates(subset=["open_time"], keep="last")
        .sort_values("open_time")
        .reset_index(drop=True)
    )
    combined.to_csv(csv_path, index=False)
    logger.info(
        "Backfill complete for %s: %d/%d months succeeded, %s total records",
        symbol, len(frames), len(months), f"{len(combined):,}",
    )
    return len(frames)


# =============================================================================
# Extract Klines - Binance Data Vision (bulk, one-time)
# =============================================================================
def _download_month(symbol: str, year: int, month: int) -> pd.DataFrame | None:
    """Download 1 file ZIP klines tu Data Vision, parse thanh DataFrame."""
    url = BINANCE_DATA_VISION_URL.format(symbol=symbol, year=year, month=month)

    try:
        content = make_request_raw(url, timeout=60)
    except Exception as exc:
        logger.warning("Data Vision download failed for %s %d-%02d: %s", symbol, year, month, exc)
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
        logger.info("[%d/%d] Bulk downloading %s from Data Vision", idx, len(symbols), symbol)
        frames = []

        for year, month in target_months:
            df = _download_month(symbol, year, month)
            if df is not None:
                frames.append(df)

        if not frames:
            logger.error("No Data Vision data available for %s", symbol)
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
# Pre-Extract Orchestrator — Tu dong phuc hoi du lieu sau downtime
# =============================================================================
def _pre_extract(
    symbols: list[str],
    months_back: int = MONTHS_BACK,
) -> dict[str, str]:
    """Smart orchestrator: inspect CSV gap per symbol and decide recovery strategy.

    Strategy per symbol:
      - No CSV file       → Data Vision bulk + REST API (if TRADING)
      - gap < 30 days     → REST API only (if TRADING)
      - gap >= 30 days    → Data Vision for complete months + REST API remainder (if TRADING)

    Status handling:
      - Data Vision (historical zips): ALWAYS allowed regardless of status.
        Binance keeps historical files even after delisting.
      - REST API (realtime):           ONLY for TRADING symbols.
        BREAK symbols have no new candles, calling API would return 404.

    Returns:
        dict  symbol → action taken
    """
    now = datetime.utcnow()
    now_ms = int(now.timestamp() * 1000)

    break_set = {s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") != "TRADING"}

    bulk_symbols: list[str] = []                       # No CSV file
    api_symbols: list[str] = []                        # Short gap, TRADING only
    backfill_info: list[tuple[str, datetime]] = []     # Long gap

    # ── Step 1: Classify every symbol ────────────────────────────────
    for symbol in symbols:
        last_ts = _get_last_timestamp(symbol)
        is_trading = symbol not in break_set

        if last_ts is None:
            bulk_symbols.append(symbol)
            continue

        gap_days = (now_ms - last_ts) / 1000 / 86_400

        if gap_days < _GAP_THRESHOLD_DAYS:
            if is_trading:
                api_symbols.append(symbol)
            # BREAK + short gap → data is already up to date (coin is frozen)
        else:
            last_dt = datetime.utcfromtimestamp(last_ts / 1000)
            backfill_info.append((symbol, last_dt))

    results: dict[str, str] = {}

    # ── Step 2a: No existing file → bulk via Data Vision ─────────────
    if bulk_symbols:
        logger.info(
            "[Pre-Extract] %d symbol(s) have no CSV file — bulk downloading %d months from Data Vision",
            len(bulk_symbols), months_back,
        )
        extract_klines(bulk_symbols, months_back=months_back)
        for s in bulk_symbols:
            if s in break_set:
                results[s] = "bulk (Data Vision only, status=BREAK)"
                logger.info("[Pre-Extract] %s — bulk done (BREAK: skipping REST API)", s)
            else:
                api_symbols.append(s)
                results[s] = "bulk + api"

    # ── Step 2b: Long gap → Data Vision backfill + REST API ──────────
    for symbol, last_dt in backfill_info:
        gap_days = (now - last_dt).days
        is_trading = symbol not in break_set

        months_to_fill = _get_months_between(last_dt, now)
        filled = 0
        if months_to_fill:
            logger.info(
                "[Pre-Extract] %s — gap %d days, backfilling %d month(s) from Data Vision: %s",
                symbol, gap_days, len(months_to_fill),
                ", ".join(f"{y}-{m:02d}" for y, m in months_to_fill),
            )
            filled = _backfill_months(symbol, months_to_fill)
        else:
            logger.info(
                "[Pre-Extract] %s — gap %d days, no complete months to backfill",
                symbol, gap_days,
            )

        if is_trading:
            api_symbols.append(symbol)
            results[symbol] = f"backfill({filled}/{len(months_to_fill)} months) + api"
        else:
            results[symbol] = f"backfill({filled}/{len(months_to_fill)} months), status=BREAK"
            logger.info("[Pre-Extract] %s — BREAK: skipping REST API, historical data only", symbol)

    # ── Step 2c: REST API update (TRADING symbols only) ──────────────
    if api_symbols:
        logger.info(
            "[Pre-Extract] Updating %d TRADING symbol(s) via REST API",
            len(api_symbols),
        )
        recent = extract_recent_klines(api_symbols)
        if recent:
            total = sum(len(df) for df in recent.values())
            logger.info(
                "[Pre-Extract] REST API complete — %d symbols updated, %s new records",
                len(recent), f"{total:,}",
            )
        for s in api_symbols:
            if s not in results:
                results[s] = "api"

    # Mark remaining BREAK symbols with short/no gap
    for s in break_set:
        if s not in results:
            results[s] = "up-to-date (status=BREAK, no action needed)"

    return results


# =============================================================================
# Entry Points
# =============================================================================
def extract_bulk(
    symbols: list[str] | None = None,
    months_back: int = MONTHS_BACK,
) -> None:
    """Force re-download all historical data from Binance Data Vision."""
    symbols = symbols or SYMBOLS
    logger.info("=== Bulk Extract: %d symbols, %d months ===", len(symbols), months_back)

    results = extract_klines(symbols, months_back=months_back)
    total = sum(len(df) for df in results.values())
    logger.info("=== Bulk Extract complete: %d/%d symbols, %s records ===", len(results), len(symbols), f"{total:,}")


def extract_daily(symbols: list[str] | None = None) -> None:
    """Self-healing daily extraction: pre-extract recovery + ticker + order book.

    Pre-Extract analyzes each symbol's CSV to determine the optimal recovery:
      - No CSV file   → Data Vision bulk + REST API (TRADING) / Data Vision only (BREAK)
      - Gap < 30 days  → REST API (TRADING only)
      - Gap >= 30 days → Data Vision backfill + REST API (TRADING) / Data Vision only (BREAK)
    Then fetches ticker_24h and order_book_snapshot for TRADING symbols.
    """
    symbols = symbols or SYMBOLS
    trading = [s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") == "TRADING"]
    non_trading = [s for s in symbols if s not in set(trading)]

    logger.info(
        "=== Daily Extract (self-healing): %d symbols (%d TRADING, %d BREAK) ===",
        len(symbols), len(trading), len(non_trading),
    )

    actions = _pre_extract(symbols)

    logger.info("--- Pre-Extract summary ---")
    for sym, action in actions.items():
        logger.info("  %-14s → %s", sym, action)
    logger.info("---------------------------")

    # Ticker & order book only for TRADING symbols (BREAK has no live data)
    extract_ticker_24h(trading)
    extract_order_book_snapshot(trading)
    logger.info("=== Daily Extract finished ===")


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
        default="daily",
        help=(
            "daily = self-healing (auto bulk/backfill/API based on gap), "
            "bulk = force re-download all from Data Vision, "
            "full = bulk + daily (default: daily)"
        ),
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
        "Extract started | mode=%s | symbols=%d | months=%d",
        args.mode, len(symbols), args.months,
    )

    if args.mode in ("bulk", "full"):
        extract_bulk(symbols, months_back=args.months)
    if args.mode in ("daily", "full"):
        extract_daily(symbols)


if __name__ == "__main__":
    main()
