import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

from config.config import RAW_DATA_DIR, MONTHS_BACK
from config.symbols import SYMBOLS, SYMBOLS_STATUS, BREAK_DATES
from utils.logger import get_logger
from utils.binance_utils import (
    RAW_KLINES_COLUMNS,
    download_klines_month,
)
from utils.data_utils import get_last_timestamp, get_target_end, merge_and_save_klines

logger = get_logger(__name__)

# Gap < threshold → extract handle
# Gap >= threshold → pre_extract handle
_GAP_THRESHOLD_DAYS = 30
_GAP_WARNING_THRESHOLD_DAYS = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_target_months(months_back: int) -> list[tuple[int, int]]:
    """List of (year, month) tuples for Data Vision download."""
    end_date = datetime.now(timezone.utc) - relativedelta(months=1)
    return [
        ((end_date - relativedelta(months=i)).year,
         (end_date - relativedelta(months=i)).month)
        for i in range(months_back)
    ]


def _get_months_between(
    start_dt: datetime,
    end_dt: datetime,
) -> list[tuple[int, int]]:
    """Complete months between start_dt and end_dt."""
    months: list[tuple[int, int]] = []
    cursor = start_dt.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0,
    ) + relativedelta(months=1)

    while True:
        next_month_start = cursor + relativedelta(months=1)
        if next_month_start > end_dt:
            break
        months.append((cursor.year, cursor.month))
        cursor = next_month_start

    return months


# ---------------------------------------------------------------------------
# Bulk & Backfill
# ---------------------------------------------------------------------------
def _backfill_months(
    symbol: str,
    months: list[tuple[int, int]],
) -> int:
    """Download months from Data Vision, merge into CSV. Returns count of OK months."""
    frames: list[pd.DataFrame] = []
    for year, month in months:
        df = download_klines_month(symbol, year, month)
        if df is not None:
            frames.append(df)
            logger.info(
                "  %s %d-%02d: %s records fetched",
                symbol, year, month, f"{len(df):,}",
            )
        else:
            logger.warning(
                "  %s %d-%02d: Data Vision file unavailable, will rely on REST API",
                symbol, year, month,
            )

    if not frames:
        return 0

    new_data = pd.concat(frames, ignore_index=True)
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"
    combined = merge_and_save_klines(csv_path, new_data)
    logger.info(
        "Backfill complete for %s: %d/%d months succeeded, %s total records",
        symbol, len(frames), len(months), f"{len(combined):,}",
    )
    return len(frames)


def extract_klines(
    symbols: list[str],
    months_back: int = MONTHS_BACK,
) -> dict[str, pd.DataFrame]:
    """Bulk download klines from Data Vision."""
    target_months = _get_target_months(months_back)
    results: dict[str, pd.DataFrame] = {}

    for idx, symbol in enumerate(symbols, 1):
        logger.info(
            "[%d/%d] Bulk downloading %s from Data Vision",
            idx, len(symbols), symbol,
        )
        frames = []

        for year, month in target_months:
            df = download_klines_month(symbol, year, month)
            if df is not None:
                frames.append(df)

        if not frames:
            logger.error("No Data Vision data available for %s", symbol)
            continue

        new_data = pd.concat(frames, ignore_index=True)
        csv_path = RAW_DATA_DIR / f"{symbol}.csv"
        combined = merge_and_save_klines(csv_path, new_data)
        logger.info("Saved %s (%s records)", csv_path.name, f"{len(combined):,}")
        results[symbol] = combined

    return results


# ---------------------------------------------------------------------------
# Self-Healing Orchestrator
# ---------------------------------------------------------------------------
def pre_extract(
    symbols: list[str],
    months_back: int = MONTHS_BACK,
) -> dict[str, str]:
    """Detect gaps and auto-recover. See ProjectOverview §13."""
    # Lazy import: pre_extract is entry point, defer extract import to runtime.
    from scripts.extract import extract_recent_klines

    break_set = {
        s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") != "TRADING"
    }

    bulk_symbols: list[str] = []
    break_bulk: list[str] = []
    api_symbols: list[str] = []
    api_end_times: dict[str, int] = {}
    backfill_info: list[tuple[str, datetime, datetime]] = []
    results: dict[str, str] = {}

    for symbol in symbols:
        target_end = get_target_end(symbol)
        target_end_ms = int(target_end.timestamp() * 1000)
        is_break = symbol in break_set

        last_ts = get_last_timestamp(symbol)
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
            results[symbol] = "skip (gap < 30d, daily mode)"
        else:
            last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
            backfill_info.append((symbol, last_dt, target_end))

    # No CSV → bulk Data Vision
    if bulk_symbols:
        logger.info(
            "[Pre-Extract] %d symbol(s) have no CSV file "
            "— bulk downloading %d months from Data Vision",
            len(bulk_symbols), months_back,
        )
        extract_klines(bulk_symbols, months_back=months_back)
        for s in bulk_symbols:
            target_end = get_target_end(s)
            target_end_ms = int(target_end.timestamp() * 1000)
            new_last_ts = get_last_timestamp(s)

            if new_last_ts is not None and new_last_ts >= target_end_ms:
                results[s] = "bulk (Data Vision, data complete)"
                logger.info(
                    "[Pre-Extract] %s — bulk done, data already covers target",
                    s,
                )
            else:
                api_symbols.append(s)
                api_end_times[s] = target_end_ms
                results[s] = "bulk + api"

    # No CSV + BREAK: placeholder for BREAK coins without CSV
    if break_bulk:
        logger.info(
            "[Pre-Extract] %d BREAK symbol(s) have no CSV — creating placeholder "
            "(use --mode bulk --symbols <SYM> if historical data is needed)",
            len(break_bulk),
        )
        for symbol in break_bulk:
            csv_path = RAW_DATA_DIR / f"{symbol}.csv"
            pd.DataFrame(columns=list(RAW_KLINES_COLUMNS) + ["symbol"]).to_csv(
                csv_path, index=False,
            )
            results[symbol] = "done (BREAK, placeholder — never tracked)"
            logger.info(
                "  %s — placeholder created (break_date %s)",
                symbol, BREAK_DATES.get(symbol, "?"),
            )

    # Long gap → Data Vision backfill + REST API
    for symbol, last_dt, target_end in backfill_info:
        target_end_ms = int(target_end.timestamp() * 1000)
        gap_days = (target_end - last_dt).days

        months_to_fill = _get_months_between(last_dt, target_end)
        filled = 0
        if months_to_fill:
            logger.info(
                "[Pre-Extract] %s — gap %d days, backfilling %d month(s) "
                "from Data Vision: %s",
                symbol, gap_days, len(months_to_fill),
                ", ".join(f"{y}-{m:02d}" for y, m in months_to_fill),
            )
            filled = _backfill_months(symbol, months_to_fill)
        else:
            logger.info(
                "[Pre-Extract] %s — gap %d days, no complete months to backfill",
                symbol, gap_days,
            )

        new_last_ts = get_last_timestamp(symbol)
        if new_last_ts is not None and new_last_ts >= target_end_ms:
            tag = "BREAK" if symbol in break_set else "TRADING"
            results[symbol] = (
                f"backfill({filled}/{len(months_to_fill)} months), "
                f"data complete ({tag})"
            )
            logger.info(
                "[Pre-Extract] %s — backfill done, data already covers target",
                symbol,
            )
        else:
            api_symbols.append(symbol)
            api_end_times[symbol] = target_end_ms
            results[symbol] = (
                f"backfill({filled}/{len(months_to_fill)} months) + api"
            )

    # REST API update (for symbols after bulk/backfill missing data)
    if api_symbols:
        trading_api = [s for s in api_symbols if s not in break_set]
        break_api = [s for s in api_symbols if s in break_set]
        if trading_api:
            logger.info(
                "[Pre-Extract] Updating %d TRADING symbol(s) via REST API "
                "(target: now)",
                len(trading_api),
            )
        if break_api:
            logger.info(
                "[Pre-Extract] Updating %d BREAK symbol(s) via REST API "
                "(target: break_date)",
                len(break_api),
            )

        recent = extract_recent_klines(api_symbols, end_times=api_end_times)
        if recent:
            total = sum(len(df) for df in recent.values())
            logger.info(
                "[Pre-Extract] REST API complete — %d/%d symbols updated, "
                "%s new records",
                len(recent), len(api_symbols), f"{total:,}",
            )
        else:
            logger.warning(
                "[Pre-Extract] REST API returned no new data for any of "
                "%d symbol(s)",
                len(api_symbols),
            )
        for s in api_symbols:
            if s not in results:
                tag = (
                    "api" if s not in break_set
                    else "api (up to break_date)"
                )
                results[s] = tag

        for s in api_symbols:
            new_last_ts = get_last_timestamp(s)
            target_end_ms = api_end_times.get(
                s, int(datetime.now(timezone.utc).timestamp() * 1000),
            )
            if new_last_ts is not None and new_last_ts < target_end_ms:
                remaining_days = (target_end_ms - new_last_ts) / 1000 / 86_400
                if remaining_days >= _GAP_WARNING_THRESHOLD_DAYS:
                    tag = "BREAK" if s in break_set else "TRADING"
                    logger.warning(
                        "[Pre-Extract] %s (%s) — gap of %.0f day(s) remains "
                        "unresolved (Data Vision + REST API both returned "
                        "no data)",
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
    """Force re-download all from Data Vision."""
    symbols = symbols or SYMBOLS
    logger.info(
        "=== Bulk Extract: %d symbols, %d months ===",
        len(symbols), months_back,
    )

    results = extract_klines(symbols, months_back=months_back)
    total = sum(len(df) for df in results.values())
    logger.info(
        "=== Bulk Extract complete: %d/%d symbols, %s records ===",
        len(results), len(symbols), f"{total:,}",
    )


def run_pre_extract(symbols: list[str] | None = None) -> None:
    """Run self-healing pre-extract: detect gaps and recover."""
    symbols = symbols or SYMBOLS
    trading = [s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") == "TRADING"]
    non_trading = [s for s in symbols if s not in set(trading)]

    logger.info(
        "=== Pre-Extract (self-healing): %d symbols (%d TRADING, %d BREAK) ===",
        len(symbols), len(trading), len(non_trading),
    )

    actions = pre_extract(symbols)

    logger.info("--- Pre-Extract summary ---")
    for sym, action in actions.items():
        logger.info("  %-14s → %s", sym, action)
    logger.info("---------------------------")
    logger.info("=== Pre-Extract finished ===")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pre-extract: gap detection & Data Vision recovery",
    )
    parser.add_argument(
        "--mode",
        choices=["check", "bulk"],
        default="check",
        help=(
            "check = self-healing (auto bulk/backfill based on gap >= 30d), "
            "bulk  = force re-download all from Data Vision (default: check)"
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
        "Pre-Extract started | mode=%s | symbols=%d | months=%d",
        args.mode, len(symbols), args.months,
    )

    if args.mode == "bulk":
        extract_bulk(symbols, months_back=args.months)
    else:
        run_pre_extract(symbols)


if __name__ == "__main__":
    main()
