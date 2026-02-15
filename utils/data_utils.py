# =============================================================================
# Data Utilities - Crypto Data Pipeline
# =============================================================================

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from dateutil.relativedelta import relativedelta

import pandas as pd

from config.config import RAW_DATA_DIR
from config.symbols import SYMBOLS_STATUS, BREAK_DATES
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers (used by pre_extract.py & extract.py)
# ---------------------------------------------------------------------------

def get_target_end(symbol: str) -> datetime:
    """TRADING → now (UTC), BREAK → break_date."""
    break_date_str = BREAK_DATES.get(symbol)
    if break_date_str and SYMBOLS_STATUS.get(symbol) != "TRADING":
        return datetime.strptime(break_date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc,
        )
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# CSV I/O helpers (optimized for large klines files)
# ---------------------------------------------------------------------------

def _tail_open_time(csv_path: Path) -> str | None:
    """Read the last row's open_time from a sorted CSV.  O(1) I/O."""
    try:
        with open(csv_path, "rb") as f:
            header = f.readline().decode().strip()
            cols = header.split(",")
            ot_idx = cols.index("open_time")

            f.seek(0, 2)
            size = f.tell()
            if size <= len(header) + 2:
                return None

            chunk = min(512, size)
            f.seek(-chunk, 2)
            tail = f.read().decode().strip()

        last_line = tail.rsplit("\n", 1)[-1]
        if not last_line or last_line == header:
            return None
        return last_line.split(",")[ot_idx]
    except Exception:
        return None


def _count_lines(csv_path: Path) -> int:
    """Count data rows in CSV (header excluded).  Fast buffered byte scan."""
    count = 0
    with open(csv_path, "rb") as f:
        while True:
            buf = f.read(1 << 20)  # 1 MB chunks
            if not buf:
                break
            count += buf.count(b"\n")
    return max(count - 1, 0)  # subtract header


def merge_and_save_klines(csv_path: Path, new_df: pd.DataFrame) -> int:
    """Merge *new_df* into existing CSV (dedup by open_time, sort, save).

    Optimization (common case — incremental / chronological append):
      If new data is strictly AFTER existing data, append-only without
      re-reading the entire CSV.  Falls back to full dedup only when
      overlap is detected (e.g. backfill re-download).

    Returns total record count (int) instead of DataFrame to avoid
    holding millions of rows in memory.
    """
    new_df = new_df.drop_duplicates(subset=["open_time"], keep="last")
    new_df = new_df.sort_values("open_time").reset_index(drop=True)

    if not csv_path.exists() or csv_path.stat().st_size < 50:
        new_df.to_csv(csv_path, index=False)
        return len(new_df)

    # --- fast path: append-only when no overlap ---
    last_existing = _tail_open_time(csv_path)
    if last_existing is not None:
        new_first = str(new_df["open_time"].iloc[0])
        if pd.Timestamp(new_first) > pd.Timestamp(last_existing):
            new_df.to_csv(csv_path, mode="a", header=False, index=False)
            logger.debug(
                "Fast append: +%d rows -> %s", len(new_df), csv_path.name,
            )
            return _count_lines(csv_path)

    # --- slow path: overlap -> full dedup ---
    logger.debug("Full merge (overlap): %s", csv_path.name)
    old_df = pd.read_csv(csv_path)
    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined = (
        combined
        .drop_duplicates(subset=["open_time"], keep="last")
        .sort_values("open_time")
        .reset_index(drop=True)
    )
    combined.to_csv(csv_path, index=False)
    return len(combined)


def get_last_timestamp(symbol: str) -> int | None:
    """Get last open_time (ms) from a symbol's sorted CSV.  O(1) I/O."""
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"
    if not csv_path.exists():
        return None
    raw = _tail_open_time(csv_path)
    if raw is None:
        return None
    try:
        ts = pd.to_datetime(raw, utc=True)
        return int(ts.timestamp() * 1000)
    except Exception as exc:
        logger.error(
            "Cannot parse timestamp '%s' from %s: %s", raw, symbol, exc,
        )
        return None


# ---------------------------------------------------------------------------
# Date / month utilities (Data Vision)
# ---------------------------------------------------------------------------

def get_target_months(months_back: int) -> list[tuple[int, int]]:
    """Last *months_back* completed months as (year, month) tuples."""
    end_date = datetime.now(timezone.utc) - relativedelta(months=1)
    return [
        ((end_date - relativedelta(months=i)).year,
         (end_date - relativedelta(months=i)).month)
        for i in range(months_back)
    ]


def get_months_between(
    start_dt: datetime,
    end_dt: datetime,
) -> list[tuple[int, int]]:
    """Complete months between *start_dt* and *end_dt*."""
    months: list[tuple[int, int]] = []
    cursor = start_dt.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0,
    ) + relativedelta(months=1)
    while True:
        next_month = cursor + relativedelta(months=1)
        if next_month > end_dt:
            break
        months.append((cursor.year, cursor.month))
        cursor = next_month
    return months


# TODO: normalize_data() — Min-Max normalization for features
# TODO: denormalize_data() — Reverse normalization for predictions
# TODO: create_sequences() — Build input/output sequences for LSTM
# TODO: validate_data() — Check missing values, duplicates
