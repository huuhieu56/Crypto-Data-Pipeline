"""Self-healing pre-extract: detect data gaps and auto-recover.

Run before daily extract. Classifies each symbol's data state and
dispatches to the appropriate recovery strategy (Data Vision bulk,
backfill, or REST API).

See docs/ProjectOverview.md §13.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import pandas as pd
from datetime import datetime, timezone

from config.config import RAW_DATA_DIR, MONTHS_BACK, RAW_KLINES_COLUMNS
from config.symbols import SYMBOLS, SYMBOLS_STATUS, BREAK_DATES
from utils.logger import get_logger
from utils.data_utils import (
    get_last_timestamp,
    get_target_end,
    get_months_between,
    get_target_months,
)

logger = get_logger(__name__)

_GAP_THRESHOLD_DAYS = 30
_GAP_WARNING_DAYS = 1

# Action constants
_BULK = "bulk"
_BACKFILL = "backfill"
_API = "api"
_UP_TO_DATE = "up-to-date"
_DONE_BREAK = "done-break"
_PLACEHOLDER = "placeholder"


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------
def _classify(symbols: list[str]) -> dict[str, dict]:
    """Classify each symbol → action + metadata."""
    plans: dict[str, dict] = {}

    for symbol in symbols:
        target_end = get_target_end(symbol)
        target_end_ms = int(target_end.timestamp() * 1000)
        is_break = SYMBOLS_STATUS.get(symbol, "TRADING") != "TRADING"
        last_ts = get_last_timestamp(symbol)
        csv_exists = (RAW_DATA_DIR / f"{symbol}.csv").exists()

        if last_ts is None:
            if is_break:
                action = _DONE_BREAK if csv_exists else _PLACEHOLDER
            else:
                action = _BULK
        elif last_ts >= target_end_ms:
            action = _DONE_BREAK if is_break else _UP_TO_DATE
        else:
            gap_days = (target_end_ms - last_ts) / 1000 / 86_400
            action = _API if gap_days < _GAP_THRESHOLD_DAYS else _BACKFILL

        plans[symbol] = {
            "action": action,
            "target_end": target_end,
            "target_end_ms": target_end_ms,
            "last_ts": last_ts,
        }

    return plans


def _is_complete(symbol: str, target_end_ms: int) -> bool:
    """Check if symbol's data reaches target."""
    last_ts = get_last_timestamp(symbol)
    return last_ts is not None and last_ts >= target_end_ms


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def pre_extract(
    symbols: list[str],
    months_back: int = MONTHS_BACK,
) -> dict[str, str]:
    """Detect gaps and auto-recover. Returns {symbol: result_description}."""
    from scripts.extract import extract_recent_klines, download_data_vision

    plans = _classify(symbols)
    results: dict[str, str] = {}

    # --- Phase 1: no action needed ---
    for sym, p in plans.items():
        if p["action"] == _UP_TO_DATE:
            results[sym] = "up-to-date"
        elif p["action"] == _DONE_BREAK:
            results[sym] = "done (BREAK)"

    # --- Phase 2: BREAK placeholders ---
    placeholders = [s for s, p in plans.items() if p["action"] == _PLACEHOLDER]
    for sym in placeholders:
        pd.DataFrame(columns=list(RAW_KLINES_COLUMNS) + ["symbol"]).to_csv(
            RAW_DATA_DIR / f"{sym}.csv", index=False,
        )
        results[sym] = f"placeholder (break_date={BREAK_DATES.get(sym, '?')})"
        logger.info("  %s — placeholder created", sym)

    # --- Phase 3: bulk (no CSV, TRADING) ---
    bulk_syms = [s for s, p in plans.items() if p["action"] == _BULK]
    if bulk_syms:
        target_months = get_target_months(months_back)
        logger.info(
            "[Pre-Extract] %d symbol(s) — bulk download (%d months)",
            len(bulk_syms), len(target_months),
        )
        for sym in bulk_syms:
            download_data_vision(sym, target_months)
            if _is_complete(sym, plans[sym]["target_end_ms"]):
                results[sym] = "bulk (complete)"
            else:
                plans[sym]["action"] = _API
                results[sym] = "bulk + api"

    # --- Phase 4: backfill (gap >= 30 days) ---
    backfill_syms = [s for s, p in plans.items() if p["action"] == _BACKFILL]
    for sym in backfill_syms:
        last_ts = plans[sym]["last_ts"]  # cached from _classify()
        last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
        months = get_months_between(last_dt, plans[sym]["target_end"])
        if months:
            logger.info(
                "[Pre-Extract] %s — backfill %d month(s) from Data Vision",
                sym, len(months),
            )
            download_data_vision(sym, months)

        if _is_complete(sym, plans[sym]["target_end_ms"]):
            results[sym] = "backfill (complete)"
        else:
            plans[sym]["action"] = _API
            results[sym] = "backfill + api"

    # --- Phase 5: REST API (remaining gaps) ---
    api_syms = [s for s, p in plans.items() if p["action"] == _API]
    if api_syms:
        end_times = {s: plans[s]["target_end_ms"] for s in api_syms}
        logger.info("[Pre-Extract] %d symbol(s) via REST API", len(api_syms))

        recent = extract_recent_klines(api_syms, end_times=end_times)
        if recent:
            total = sum(len(df) for df in recent.values())
            logger.info(
                "[Pre-Extract] REST API: %d symbols, %s records",
                len(recent), f"{total:,}",
            )

        for sym in api_syms:
            if not _is_complete(sym, plans[sym]["target_end_ms"]):
                gap = (plans[sym]["target_end_ms"] - (get_last_timestamp(sym) or 0)) / 1000 / 86_400
                if gap >= _GAP_WARNING_DAYS:
                    logger.warning(
                        "[Pre-Extract] %s — %.0f day(s) gap remains", sym, gap,
                    )
            results.setdefault(sym, "api")

    return results


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def run_pre_extract(symbols: list[str] | None = None) -> None:
    """Run self-healing pre-extract."""
    symbols = symbols or SYMBOLS
    trading = [s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") == "TRADING"]

    logger.info(
        "=== Pre-Extract: %d symbols (%d TRADING, %d BREAK) ===",
        len(symbols), len(trading), len(symbols) - len(trading),
    )

    results = pre_extract(symbols)

    logger.info("--- Summary ---")
    for sym, action in results.items():
        logger.info("  %-14s → %s", sym, action)
    logger.info("=== Pre-Extract finished ===")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pre-extract: gap detection & auto-recovery",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYM",
        default=None,
        help="override symbol list",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    symbols = args.symbols or SYMBOLS
    logger.info("Pre-Extract started | symbols=%d", len(symbols))
    run_pre_extract(symbols)


if __name__ == "__main__":
    main()
