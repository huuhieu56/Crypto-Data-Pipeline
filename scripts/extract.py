"""Extract Script — Thu thap du lieu tu Binance API & Data Vision."""

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
from config.symbols import SYMBOLS, SYMBOLS_STATUS, BREAK_DATES
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

# Binance klines: 12 cot, bo cot "ignore"
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

# Gap < threshold → REST API only, >= threshold → Data Vision + REST API
_GAP_THRESHOLD_DAYS = 30


def _get_target_end(symbol: str) -> datetime:
    """TRADING → now, BREAK → break_date."""
    break_date_str = BREAK_DATES.get(symbol)
    if break_date_str and SYMBOLS_STATUS.get(symbol) != "TRADING":
        return datetime.strptime(break_date_str, "%Y-%m-%d")
    return datetime.utcnow()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_target_months(months_back: int) -> list[tuple[int, int]]:
    """Danh sach (year, month) de download tu Data Vision."""
    end_date = datetime.now() - relativedelta(months=1)
    return [
        ((end_date - relativedelta(months=i)).year,
         (end_date - relativedelta(months=i)).month)
        for i in range(months_back)
    ]


def _get_last_timestamp(symbol: str) -> int | None:
    """open_time cuoi cung (ms) tu CSV, None neu khong co."""
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
    """Raw klines → DataFrame chuan."""
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
    """Cac thang TRON VEN giua start_dt va end_dt (cho Data Vision)."""
    months: list[tuple[int, int]] = []
    cursor = start_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + relativedelta(months=1)

    while True:
        next_month_start = cursor + relativedelta(months=1)
        if next_month_start > end_dt:
            break
        months.append((cursor.year, cursor.month))
        cursor = next_month_start

    return months


def _backfill_months(
    symbol: str,
    months: list[tuple[int, int]],
) -> int:
    """Download cac thang tu Data Vision, merge vao CSV. Returns so thang OK."""
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


# ---------------------------------------------------------------------------
# Data Vision (bulk download)
# ---------------------------------------------------------------------------
def _download_month(symbol: str, year: int, month: int) -> pd.DataFrame | None:
    """Download 1 ZIP klines tu Data Vision → DataFrame."""
    url = BINANCE_DATA_VISION_URL.format(symbol=symbol, year=year, month=month)

    try:
        content = make_request_raw(url, timeout=60)
    except Exception as exc:
        logger.warning("Data Vision download failed for %s %d-%02d: %s", symbol, year, month, exc)
        return None

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        with z.open(z.namelist()[0]) as f:
            df = pd.read_csv(f, header=None, usecols=range(11), names=_KLINES_COLUMNS)

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
    """Bulk download klines tu Data Vision."""
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


# ---------------------------------------------------------------------------
# REST API (incremental)
# ---------------------------------------------------------------------------
def _fetch_klines_paginated(
    symbol: str,
    start_time: int,
    end_time: int,
) -> list[pd.DataFrame]:
    """Paginate /klines API tu start_time → end_time."""
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
    """Incremental append klines moi tu REST API."""
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
            logger.info(
                "[%d/%d] %s: REST API returned 0 new records (last_ts already near target)",
                idx, len(symbols), symbol,
            )
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
        results[symbol] = new_df

    return results


def _extract_recent_with_targets(
    symbols: list[str],
    end_times: dict[str, int],
) -> dict[str, pd.DataFrame]:
    """Nhu extract_recent_klines, nhung moi symbol co end_time rieng."""
    if not symbols:
        return {}

    results: dict[str, pd.DataFrame] = {}
    default_end = int(datetime.now().timestamp() * 1000)

    for idx, symbol in enumerate(symbols, 1):
        last_ts = _get_last_timestamp(symbol)
        if last_ts is None:
            logger.debug("[%d/%d] %s — no existing data, skip", idx, len(symbols), symbol)
            continue

        end_time = end_times.get(symbol, default_end)

        if last_ts >= end_time:
            continue

        new_frames = _fetch_klines_paginated(symbol, last_ts, end_time)
        if not new_frames:
            logger.info(
                "[%d/%d] %s: REST API returned 0 new records",
                idx, len(symbols), symbol,
            )
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
        results[symbol] = new_df

    return results


# ---------------------------------------------------------------------------
# Ticker 24h & Order Book
# ---------------------------------------------------------------------------
def extract_ticker_24h(symbols: list[str]) -> pd.DataFrame | None:
    """Lay ticker/24hr + bookTicker → CSV."""
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
    if output_path.exists():
        old = pd.read_csv(output_path)
        merged = pd.concat([old, merged], ignore_index=True)

    merged.to_csv(output_path, index=False)
    logger.info("Saved ticker_24h (%s records)", f"{len(merged):,}")
    return merged


def extract_order_book_snapshot(symbols: list[str]) -> pd.DataFrame | None:
    """Lay order book depth → imbalance."""
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


# ---------------------------------------------------------------------------
# Self-Healing Orchestrator
# ---------------------------------------------------------------------------
def _pre_extract(
    symbols: list[str],
    months_back: int = MONTHS_BACK,
) -> dict[str, str]:
    """Kiem tra gap tung symbol va tu dong phuc hoi (xem ProjectOverview §13)."""
    now = datetime.utcnow()
    now_ms = int(now.timestamp() * 1000)

    break_set = {s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") != "TRADING"}

    bulk_symbols: list[str] = []
    break_bulk: list[str] = []
    api_symbols: list[str] = []
    api_end_times: dict[str, int] = {}
    backfill_info: list[tuple[str, datetime, datetime]] = []
    results: dict[str, str] = {}

    # Step 1: Phan loai tung symbol
    for symbol in symbols:
        target_end = _get_target_end(symbol)
        target_end_ms = int(target_end.timestamp() * 1000)
        is_break = symbol in break_set

        last_ts = _get_last_timestamp(symbol)
        csv_path = RAW_DATA_DIR / f"{symbol}.csv"

        if last_ts is None:
            if is_break:
                if csv_path.exists():
                    results[symbol] = "done (BREAK, no data available)"
                else:
                    break_bulk.append(symbol)
            else:
                bulk_symbols.append(symbol)
            continue

        if last_ts >= target_end_ms:
            if is_break:
                results[symbol] = "done (BREAK, data complete up to break_date)"
            else:
                results[symbol] = "up-to-date"
            continue

        gap_days = (target_end_ms - last_ts) / 1000 / 86_400

        if gap_days < _GAP_THRESHOLD_DAYS:
            api_symbols.append(symbol)
            api_end_times[symbol] = target_end_ms
        else:
            last_dt = datetime.utcfromtimestamp(last_ts / 1000)
            backfill_info.append((symbol, last_dt, target_end))

    # Step 2a: Khong co CSV → bulk Data Vision
    if bulk_symbols:
        logger.info(
            "[Pre-Extract] %d symbol(s) have no CSV file — bulk downloading %d months from Data Vision",
            len(bulk_symbols), months_back,
        )
        extract_klines(bulk_symbols, months_back=months_back)
        for s in bulk_symbols:
            target_end = _get_target_end(s)
            target_end_ms = int(target_end.timestamp() * 1000)
            new_last_ts = _get_last_timestamp(s)

            if new_last_ts is not None and new_last_ts >= target_end_ms:
                results[s] = "bulk (Data Vision, data complete)"
                logger.info("[Pre-Extract] %s — bulk done, data already covers target", s)
            else:
                api_symbols.append(s)
                api_end_times[s] = target_end_ms
                results[s] = "bulk + api"

    # Step 2a-BREAK: Tao placeholder cho BREAK coins chua co CSV
    if break_bulk:
        logger.info(
            "[Pre-Extract] %d BREAK symbol(s) have no CSV — creating placeholder "
            "(use --mode bulk --symbols <SYM> if historical data is needed)",
            len(break_bulk),
        )
        for symbol in break_bulk:
            csv_path = RAW_DATA_DIR / f"{symbol}.csv"
            pd.DataFrame(columns=_KLINES_COLUMNS + ["symbol"]).to_csv(
                csv_path, index=False,
            )
            results[symbol] = "done (BREAK, placeholder — never tracked)"
            logger.info("  %s — placeholder created (break_date %s)", symbol, BREAK_DATES.get(symbol, "?"))

    # Step 2b: Gap dai → Data Vision backfill + REST API
    for symbol, last_dt, target_end in backfill_info:
        target_end_ms = int(target_end.timestamp() * 1000)
        gap_days = (target_end - last_dt).days

        months_to_fill = _get_months_between(last_dt, target_end)
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

        new_last_ts = _get_last_timestamp(symbol)
        if new_last_ts is not None and new_last_ts >= target_end_ms:
            tag = "BREAK" if symbol in break_set else "TRADING"
            results[symbol] = f"backfill({filled}/{len(months_to_fill)} months), data complete ({tag})"
            logger.info("[Pre-Extract] %s — backfill done, data already covers target", symbol)
        else:
            api_symbols.append(symbol)
            api_end_times[symbol] = target_end_ms
            results[symbol] = f"backfill({filled}/{len(months_to_fill)} months) + api"

    # Step 2c: REST API update
    if api_symbols:
        trading_api = [s for s in api_symbols if s not in break_set]
        break_api = [s for s in api_symbols if s in break_set]
        if trading_api:
            logger.info(
                "[Pre-Extract] Updating %d TRADING symbol(s) via REST API (target: now)",
                len(trading_api),
            )
        if break_api:
            logger.info(
                "[Pre-Extract] Updating %d BREAK symbol(s) via REST API (target: break_date)",
                len(break_api),
            )

        recent = _extract_recent_with_targets(api_symbols, api_end_times)
        if recent:
            total = sum(len(df) for df in recent.values())
            logger.info(
                "[Pre-Extract] REST API complete — %d/%d symbols updated, %s new records",
                len(recent), len(api_symbols), f"{total:,}",
            )
        else:
            logger.warning(
                "[Pre-Extract] REST API returned no new data for any of %d symbol(s)",
                len(api_symbols),
            )
        for s in api_symbols:
            if s not in results:
                tag = "api" if s not in break_set else f"api (up to break_date)"
                results[s] = tag

        for s in api_symbols:
            new_last_ts = _get_last_timestamp(s)
            target_end_ms = api_end_times.get(s, int(datetime.utcnow().timestamp() * 1000))
            if new_last_ts is not None and new_last_ts < target_end_ms:
                remaining_days = (target_end_ms - new_last_ts) / 1000 / 86_400
                tag = "BREAK" if s in break_set else "TRADING"
                logger.warning(
                    "[Pre-Extract] %s (%s) — gap of %.0f day(s) remains unresolved "
                    "(Data Vision + REST API both returned no data)",
                    s, tag, remaining_days,
                )

    return results


# ---------------------------------------------------------------------------
# Entry Points
# ---------------------------------------------------------------------------
def extract_bulk(
    symbols: list[str] | None = None,
    months_back: int = MONTHS_BACK,
) -> None:
    """Force re-download toan bo tu Data Vision."""
    symbols = symbols or SYMBOLS
    logger.info("=== Bulk Extract: %d symbols, %d months ===", len(symbols), months_back)

    results = extract_klines(symbols, months_back=months_back)
    total = sum(len(df) for df in results.values())
    logger.info("=== Bulk Extract complete: %d/%d symbols, %s records ===", len(results), len(symbols), f"{total:,}")


def extract_daily(symbols: list[str] | None = None) -> None:
    """Self-healing daily: pre-extract + ticker + order book."""
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

    extract_ticker_24h(trading)
    extract_order_book_snapshot(trading)
    logger.info("=== Daily Extract finished ===")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
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
