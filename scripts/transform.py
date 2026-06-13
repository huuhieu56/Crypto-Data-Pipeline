"""Transform script — compute indicators and derived columns.

- klines: ETL — compute RSI(14) + MACD(12,26,9) in Python → Parquet
- ticker_24h: ETL — rename, compute spread_pct → Parquet
- order_book: ETL — compute OBI (±0.5% depth), spread, walls → Parquet

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
    OBI_DEPTH_PCT,
)
from config.symbols import SYMBOLS
from utils.data_utils import validate_month_str
from utils.db_utils import get_table_watermarks
from utils.exceptions import TransformError
from utils.logger import get_logger
from utils.storage import append_to_partition, discover_month_partitions, read_month_data, storage

logger = get_logger(__name__)

BUCKET_RAW = MINIO_CONFIG["bucket_raw"]
BUCKET_PROCESSED = MINIO_CONFIG["bucket_processed"]


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """RSI(period) — SMA-based rolling average of gains/losses."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return (100 - (100 / (1 + rs))).fillna(0.0)


def _compute_macd(close: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    """MACD(12,26,9) — returns (macd_line, signal_line, histogram)."""
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    return macd_line, signal_line, macd_line - signal_line


def _load_context_from_processed(symbol: str, before_month: str, n: int) -> pd.DataFrame | None:
    """Load last *n* rows from processed Parquet for indicator warm-up."""
    if n <= 0:
        return None
    ctx_parts = []
    for month in discover_month_partitions(BUCKET_PROCESSED, "klines", symbol):
        if month >= before_month:
            break
        key = f"klines/{symbol}/{month}.parquet"
        try:
            table = storage.download_parquet(BUCKET_PROCESSED, key)
            ctx_parts.append(table.to_pandas())
        except Exception:
            pass
    if not ctx_parts:
        return None
    ctx = pd.concat(ctx_parts, ignore_index=True).sort_values("open_time")
    return ctx.tail(n).reset_index(drop=True)


def _filter_month_rows(df: pd.DataFrame, month_str: str) -> pd.DataFrame:
    """Return only rows belonging to the given YYYY-MM partition."""
    month_start = pd.Timestamp(f"{month_str}-01", tz="UTC")
    month_end = month_start + pd.offsets.MonthBegin(1)
    ts = pd.to_datetime(df["open_time"], utc=True)
    mask = (ts >= month_start) & (ts < month_end)
    return df.loc[mask]


def transform_klines(
    symbols: list[str] | None = None,
    month_str: str | None = None,
    wm_months: dict[str, str] | None = None,
) -> None:
    """ETL: compute RSI(14) + MACD(12,26,9) on raw klines CSV → Parquet.

    Reads raw CSV from crypto-raw, loads indicator warm-up context from
    previously processed Parquet in crypto-processed, computes indicators
    in Python, and writes transformed Parquet to crypto-processed.
    No ClickHouse dependency.
    """
    symbols = symbols or SYMBOLS
    if month_str is not None:
        month_str = validate_month_str(month_str)

    target_cols = [
        "symbol", "open_time", "open", "high", "low", "close",
        "volume", "close_time", "quote_volume", "trade_count",
        "taker_buy_base", "taker_buy_quote",
        "rsi_14", "macd",
    ]
    total_processed = 0
    errors = 0
    is_backfill = month_str is not None

    logger.info("[Transform] klines: %d symbols, Python ETL compute", len(symbols))

    for symbol in symbols:
        wm_month = wm_months.get(symbol) if wm_months else None

        # List objects once per symbol — reuse for discover + read
        raw_keys = storage.list_objects(BUCKET_RAW, prefix=f"klines/{symbol}/")

        months = (
            [month_str]
            if month_str
            else discover_month_partitions(BUCKET_RAW, "klines", symbol, extension=".csv", keys=raw_keys)
        )
        if not months:
            logger.debug("[Transform] klines %s: no partitions found", symbol)
            continue

        for month in months:
            processed_key = f"klines/{symbol}/{month}.parquet"

            # Skip completed months (before watermark month)
            if not is_backfill and wm_month and month < wm_month:
                continue

            processed_exists = (
                not is_backfill
                and storage.object_exists(BUCKET_PROCESSED, processed_key)
            )

            try:
                # Incremental: if processed exists, only read new deltas
                watermark = None
                since_ms = None
                context = None
                processed_df = None
                if processed_exists:
                    processed_tbl = storage.download_parquet(BUCKET_PROCESSED, processed_key)
                    processed_df = processed_tbl.to_pandas()
                    del processed_tbl
                    processed_df["open_time"] = pd.to_datetime(processed_df["open_time"])
                    watermark = processed_df["open_time"].max()
                    context = processed_df.tail(INDICATOR_CONTEXT_ROWS)
                    since_ms = int(watermark.timestamp() * 1000)
                else:
                    context = _load_context_from_processed(symbol, month, INDICATOR_CONTEXT_ROWS)

                raw_df = read_month_data(BUCKET_RAW, "klines", symbol, month,
                                         extension=".csv", since_ms=since_ms, keys=raw_keys)

                if raw_df.empty:
                    continue

                raw_df["open_time"] = pd.to_datetime(raw_df["open_time"], unit="ms", utc=True)
                raw_df["close_time"] = pd.to_datetime(raw_df["close_time"], unit="ms", utc=True)

                if watermark is not None:
                    raw_df = raw_df[raw_df["open_time"] > watermark].copy()
                    if raw_df.empty:
                        continue

                combined = (
                    pd.concat([context, raw_df], ignore_index=True)
                    if context is not None and not context.empty
                    else raw_df
                ).sort_values("open_time").reset_index(drop=True)

                combined["rsi_14"] = _compute_rsi(combined["close"])
                macd_line, signal_line, _ = _compute_macd(combined["close"])
                combined["macd"] = macd_line

                if watermark is not None:
                    out_df = combined[combined["open_time"] > watermark].copy()
                else:
                    out_df = _filter_month_rows(combined, month)

                if out_df.empty:
                    continue

                out_df["symbol"] = symbol
                out_df = out_df[target_cols]

                append_to_partition(
                    BUCKET_PROCESSED, "klines", symbol,
                    out_df, dedup_col="open_time", month_str=month,
                    existing_df=processed_df,
                )
                total_processed += 1
                logger.info("[Transform] klines %s/%s: %d rows", symbol, month, len(out_df))
            except Exception as exc:
                errors += 1
                logger.error("[Transform] klines %s/%s: ERROR -- %s", symbol, month, exc)

    if total_processed == 0 and errors > 0:
        raise TransformError(f"klines: all {errors} partition(s) failed, 0 transformed")
    logger.info("[Transform] klines complete: %d partitions processed, %d errors", total_processed, errors)


# --- Ticker Transform (ETL) ---------------------------------------------------


def transform_ticker(
    symbols: list[str] | None = None,
    month_str: str | None = None,
    wm_months: dict[str, str] | None = None,
) -> None:
    """ETL: rename columns, compute spread_pct → Parquet.

    Reads raw from MinIO (ticker_raw/), renames columns via BINANCE_COLUMN_MAP,
    adds snapshot_time, computes spread_pct, and writes transformed Parquet
    to ticker_24h/ in MinIO.
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
    is_backfill = month_str is not None

    logger.info("[Transform] ticker_24h: %d symbols, ETL (MinIO → MinIO)", len(symbols))

    for symbol in symbols:
        wm_month = wm_months.get(symbol) if wm_months else None

        raw_keys = storage.list_objects(BUCKET_RAW, prefix=f"ticker_raw/{symbol}/")

        months = (
            [month_str]
            if month_str
            else discover_month_partitions(BUCKET_RAW, "ticker_raw", symbol, keys=raw_keys)
        )
        if not months:
            logger.debug("[Transform] ticker_24h %s: no raw partitions", symbol)
            continue

        for month in months:
            processed_key = f"ticker_24h/{symbol}/{month}.parquet"

            # Skip completed months (before watermark month)
            if not is_backfill and wm_month and month < wm_month:
                continue

            processed_exists = (
                not is_backfill
                and storage.object_exists(BUCKET_PROCESSED, processed_key)
            )

            try:
                # Incremental: if processed exists, compare row counts
                processed_df = None
                if processed_exists:
                    processed_tbl = storage.download_parquet(BUCKET_PROCESSED, processed_key)
                    processed_df = processed_tbl.to_pandas()
                    processed_count = len(processed_df)
                    del processed_tbl
                    ticker_df = read_month_data(BUCKET_RAW, "ticker_raw", symbol, month, keys=raw_keys)
                    if ticker_df.empty or len(ticker_df) <= processed_count:
                        continue
                    ticker_df = ticker_df.iloc[processed_count:].reset_index(drop=True)
                else:
                    ticker_df = read_month_data(BUCKET_RAW, "ticker_raw", symbol, month, keys=raw_keys)
                    if ticker_df.empty:
                        continue

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
                    BUCKET_PROCESSED, "ticker_24h", symbol,
                    ticker_df, dedup_col="snapshot_time", month_str=month,
                    existing_df=processed_df,
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
    wm_months: dict[str, str] | None = None,
) -> None:
    """ETL: compute OBI, spread, walls from raw bids/asks → Parquet.

    Reads raw from MinIO (order_book/), computes depth-filtered OBI
    (±0.5% around mid price), spread, bid/ask ratio, and liquidity walls,
    then writes transformed Parquet to order_book_snapshot/ in MinIO.
    """
    if symbols is None:
        symbols = SYMBOLS
    if month_str is not None:
        month_str = validate_month_str(month_str)

    target_cols = [
        "symbol", "timestamp",
        "best_bid", "best_ask", "mid_price", "spread_pct",
        "depth_bid_volume", "depth_ask_volume", "obi", "bid_ask_ratio",
        "nearest_bid_wall_price", "nearest_bid_wall_volume",
        "nearest_ask_wall_price", "nearest_ask_wall_volume",
    ]
    total_transformed = 0
    errors = 0
    is_backfill = month_str is not None

    logger.info("[Transform] order_book_snapshot: %d symbols, ETL (MinIO → MinIO)", len(symbols))

    for symbol in symbols:
        wm_month = wm_months.get(symbol) if wm_months else None

        raw_keys = storage.list_objects(BUCKET_RAW, prefix=f"order_book/{symbol}/")

        months = (
            [month_str]
            if month_str
            else discover_month_partitions(BUCKET_RAW, "order_book", symbol, keys=raw_keys)
        )
        if not months:
            logger.debug("[Transform] order_book_snapshot %s: no raw partitions", symbol)
            continue

        for month in months:
            processed_key = f"order_book_snapshot/{symbol}/{month}.parquet"

            # Skip completed months (before watermark month)
            if not is_backfill and wm_month and month < wm_month:
                continue

            processed_exists = (
                not is_backfill
                and storage.object_exists(BUCKET_PROCESSED, processed_key)
            )

            try:
                # Incremental: if processed exists, only read new deltas
                since_ms = None
                watermark = None
                if processed_exists:
                    processed_tbl = storage.download_parquet(BUCKET_PROCESSED, processed_key)
                    processed_df = processed_tbl.to_pandas()
                    del processed_tbl
                    processed_df["timestamp"] = pd.to_datetime(processed_df["timestamp"])
                    watermark = processed_df["timestamp"].max()
                    since_ms = int(watermark.timestamp() * 1000)

                df = read_month_data(BUCKET_RAW, "order_book", symbol, month,
                                     since_ms=since_ms, keys=raw_keys)
                if df.empty:
                    continue

                if watermark is not None:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                    df = df[df["timestamp"] > watermark].copy()
                    if df.empty:
                        continue

                # Compute liquidity pressure metrics row-by-row
                results = []
                for _, row in df.iterrows():
                    # Parquet round-trip qua PyArrow có thể convert lists → numpy arrays,
                    # dùng len() thay vì truthiness để tránh "ambiguous truth value" error
                    _raw_bids = row.get("bids", [])
                    _raw_asks = row.get("asks", [])
                    bids = list(_raw_bids) if hasattr(_raw_bids, '__len__') and len(_raw_bids) > 0 else []
                    asks = list(_raw_asks) if hasattr(_raw_asks, '__len__') and len(_raw_asks) > 0 else []

                    if len(bids) == 0 or len(asks) == 0:
                        results.append({
                            "symbol": row["symbol"],
                            "timestamp": row["timestamp"],
                            "best_bid": None, "best_ask": None,
                            "mid_price": None, "spread_pct": None,
                            "depth_bid_volume": None, "depth_ask_volume": None,
                            "obi": None, "bid_ask_ratio": None,
                            "nearest_bid_wall_price": None,
                            "nearest_bid_wall_volume": None,
                            "nearest_ask_wall_price": None,
                            "nearest_ask_wall_volume": None,
                        })
                        continue

                    best_bid = float(bids[0][0])
                    best_ask = float(asks[0][0])
                    mid_price = (best_bid + best_ask) / 2.0
                    spread_pct = (best_ask - best_bid) / mid_price * 100.0

                    # Filter to ±OBI_DEPTH_PCT around mid price
                    min_bid_price = mid_price * (1.0 - OBI_DEPTH_PCT)
                    max_ask_price = mid_price * (1.0 + OBI_DEPTH_PCT)

                    depth_bids = [
                        (float(b[0]), float(b[1]))
                        for b in bids if len(b) > 1
                        and min_bid_price <= float(b[0]) <= mid_price
                    ]
                    depth_asks = [
                        (float(a[0]), float(a[1]))
                        for a in asks if len(a) > 1
                        and mid_price <= float(a[0]) <= max_ask_price
                    ]

                    depth_bid_vol = sum(q for _, q in depth_bids)
                    depth_ask_vol = sum(q for _, q in depth_asks)
                    total_vol = depth_bid_vol + depth_ask_vol

                    obi = ((depth_bid_vol - depth_ask_vol) / total_vol) if total_vol > 0 else 0.0
                    bid_ask_ratio = (depth_bid_vol / depth_ask_vol) if depth_ask_vol > 0 else None

                    # Wall detection: max-quantity level >= 3x average
                    def _find_wall(levels):
                        if not levels:
                            return None, None
                        avg_qty = sum(q for _, q in levels) / len(levels)
                        best = max(levels, key=lambda x: x[1])
                        if best[1] >= avg_qty * 3:
                            return best[0], best[1]
                        return None, None

                    bid_wall_price, bid_wall_vol = _find_wall(depth_bids)
                    ask_wall_price, ask_wall_vol = _find_wall(depth_asks)

                    results.append({
                        "symbol": row["symbol"],
                        "timestamp": row["timestamp"],
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "mid_price": mid_price,
                        "spread_pct": spread_pct,
                        "depth_bid_volume": depth_bid_vol,
                        "depth_ask_volume": depth_ask_vol,
                        "obi": obi,
                        "bid_ask_ratio": bid_ask_ratio,
                        "nearest_bid_wall_price": bid_wall_price,
                        "nearest_bid_wall_volume": bid_wall_vol,
                        "nearest_ask_wall_price": ask_wall_price,
                        "nearest_ask_wall_volume": ask_wall_vol,
                    })

                df = pd.DataFrame(results)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df[target_cols].copy()

                append_to_partition(
                    BUCKET_PROCESSED, "order_book_snapshot", symbol,
                    df, dedup_col="timestamp", month_str=month,
                    existing_df=processed_df if processed_exists else None,
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
        choices=["klines", "ticker", "orderbook", "news"],
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
    symbols = args.symbols or None

    # Compute watermark months once — all tables share same ETL schedule
    wm_months: dict[str, str] | None = None
    if args.month is None:
        wm_map = get_table_watermarks("klines", "open_time", symbols or SYMBOLS)
        wm_months = {
            s: datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m")
            for s, ts in wm_map.items()
        }

    if only is None or "klines" in only:
        transform_klines(
            symbols=symbols,
            month_str=args.month,
            wm_months=wm_months,
        )
    if only is None or "ticker" in only:
        transform_ticker(
            symbols=symbols,
            month_str=args.month,
            wm_months=wm_months,
        )
    if only is None or "orderbook" in only:
        transform_order_book(
            symbols=symbols,
            month_str=args.month,
            wm_months=wm_months,
        )
    if only is None or "news" in only:
        from scripts.transform_modules.transform_news import transform_news
        transform_news(
            symbols=symbols,
            month_str=args.month,
        )


if __name__ == "__main__":
    main()
