"""Extract data from Binance.

Two modes:
  bulk     - Full historical download via Data Vision (monthly ZIP files)
  minutely - Incremental update via REST API + Ticker 24h + Order Book

This file stays as the stable CLI/orchestrator entrypoint. Extract
implementations live under scripts.extract_modules.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import MONTHS_BACK
from config.symbols import SYMBOLS, SYMBOLS_STATUS
from scripts.extract_modules import (
    download_data_vision,
    extract_bulk,
    extract_crypto_news,
    extract_order_book_snapshot,
    extract_recent_klines,
    extract_ticker_24h,
)
from utils.logger import get_logger

logger = get_logger(__name__)

__all__ = [
    "download_data_vision",
    "extract_bulk",
    "extract_recent_klines",
    "extract_ticker_24h",
    "extract_order_book_snapshot",
    "extract_crypto_news",
    "extract_minutely",
]


def extract_minutely(symbols: list[str] | None = None) -> None:
    """Minutely extract: REST API klines + ticker + order book."""
    symbols = symbols or SYMBOLS
    trading = [s for s in symbols if SYMBOLS_STATUS.get(s, "TRADING") == "TRADING"]

    logger.info(
        "=== Minutely Extract: %d symbols (%d TRADING) ===",
        len(symbols), len(trading),
    )

    recent = extract_recent_klines(symbols)
    if recent:
        total = sum(len(df) for df in recent.values())
        logger.info("REST API: %d symbols updated, %s new records", len(recent), f"{total:,}")

    extract_ticker_24h(trading)
    extract_order_book_snapshot(trading)
    logger.info("=== Minutely Extract finished ===")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract data from Binance (bulk or minutely)",
    )
    parser.add_argument(
        "--mode", choices=["minutely", "bulk", "news"], default="minutely",
        help="minutely = incremental REST API, bulk = full Data Vision, news = GNews crypto news",
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
    elif args.mode == "news":
        extract_crypto_news()
    else:
        extract_minutely(symbols)


if __name__ == "__main__":
    main()
