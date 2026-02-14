# =============================================================================
# Data Utilities - Crypto Data Pipeline
# =============================================================================

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

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


def merge_and_save_klines(csv_path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    """Merge *new_df* into existing CSV (dedup by open_time, sort, save).

    If the CSV does not exist or is empty, *new_df* is used as-is.
    Returns the final combined DataFrame.
    """
    if csv_path.exists():
        try:
            old_df = pd.read_csv(csv_path, parse_dates=["open_time", "close_time"])
            if old_df.empty:
                combined = new_df
            else:
                combined = pd.concat([old_df, new_df], ignore_index=True)
        except Exception as exc:
            logger.warning(
                "Cannot read %s, overwriting: %s", csv_path.name, exc,
            )
            combined = new_df
    else:
        combined = new_df

    combined = (
        combined
        .drop_duplicates(subset=["open_time"], keep="last")
        .sort_values("open_time")
        .reset_index(drop=True)
    )
    combined.to_csv(csv_path, index=False)
    return combined


def get_last_timestamp(symbol: str) -> int | None:
    """Get last timestamp (in milliseconds) of a symbol's CSV data.

    Reads only the header and last line of the file for performance
    (CSVs are sorted by open_time).
    """
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"
    if not csv_path.exists():
        return None
    try:
        with open(csv_path, "rb") as f:
            # Read header to find open_time column index
            header_line = f.readline().decode().strip()
            columns = header_line.split(",")
            try:
                ot_idx = columns.index("open_time")
            except ValueError:
                logger.error("No 'open_time' column in %s", csv_path.name)
                return None

            # Seek to end and scan backwards for the last newline
            f.seek(0, 2)
            file_size = f.tell()
            if file_size <= len(header_line) + 1:
                # File has only the header (or is empty)
                return None

            # Read a small tail chunk (last 512 bytes is plenty for one row)
            chunk_size = min(512, file_size)
            f.seek(-chunk_size, 2)
            tail = f.read().decode()

        last_line = tail.strip().rsplit("\n", 1)[-1]
        if not last_line or last_line == header_line:
            return None

        fields = last_line.split(",")
        last = pd.to_datetime(fields[ot_idx], utc=True)
        return int(last.timestamp() * 1000)
    except Exception as exc:
        logger.error("Cannot read last timestamp from %s: %s", csv_path.name, exc)
        return None


# TODO: normalize_data() — Min-Max normalization for features
# TODO: denormalize_data() — Reverse normalization for predictions
# TODO: create_sequences() — Build input/output sequences for LSTM
# TODO: validate_data() — Check missing values, duplicates
