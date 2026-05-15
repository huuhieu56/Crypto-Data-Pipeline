"""Data utilities for the Crypto Data Pipeline.

Provides date/month helpers and partition key utilities.
"""

from __future__ import annotations

from datetime import datetime, timezone
import re

from dateutil.relativedelta import relativedelta
import pandas as pd

from config.config import PARTITION_MONTH_FORMAT
from config.symbols import SYMBOLS_STATUS, BREAK_DATES
from utils.logger import get_logger

logger = get_logger(__name__)

_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_EPOCH_MICROSECOND_THRESHOLD = 1_000_000_000_000_000


# --- Shared Helpers ----------------------------------------------------------

def get_target_end(symbol: str) -> datetime:
    """TRADING → now (UTC), BREAK → break_date."""
    break_date_str = BREAK_DATES.get(symbol)
    if break_date_str and SYMBOLS_STATUS.get(symbol) != "TRADING":
        return datetime.strptime(break_date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc,
        )
    return datetime.now(timezone.utc)


def validate_month_str(month_str: str) -> str:
    """Validate a monthly partition string in YYYY-MM format."""
    if not _MONTH_PATTERN.fullmatch(month_str):
        raise ValueError(f"Invalid month '{month_str}'. Expected YYYY-MM.")
    return month_str


def normalize_epoch_ms_columns(
    df: pd.DataFrame,
    columns: tuple[str, ...] = ("open_time", "close_time"),
) -> pd.DataFrame:
    """Return a copy with timestamp columns normalized to epoch milliseconds."""
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            continue
        col_data = out[col]
        if pd.api.types.is_datetime64_any_dtype(col_data):
            out[col] = col_data.astype("int64") // 1_000_000
        else:
            vals = pd.to_numeric(col_data, errors="raise").astype("int64")
            out[col] = vals.where(vals <= _EPOCH_MICROSECOND_THRESHOLD, vals // 1000)
    return out


# --- Partition Key Helpers ---------------------------------------------------

def _resolve_month(dt: datetime | str | None = None) -> str:
    """Resolve a datetime or string into a YYYY-MM month string."""
    if dt is None:
        return datetime.now(timezone.utc).strftime(PARTITION_MONTH_FORMAT)
    if isinstance(dt, str):
        return dt
    return dt.strftime(PARTITION_MONTH_FORMAT)


def minio_key(prefix: str, symbol: str, dt: datetime | str | None = None, extension: str = ".parquet") -> str:
    """MinIO key: {prefix}/{SYMBOL}/{YYYY-MM}{extension}"""
    return f"{prefix}/{symbol}/{_resolve_month(dt)}{extension}"


def partition_key(symbol: str, dt: datetime | str | None = None) -> str:
    """MinIO key for raw klines: klines/{SYMBOL}/{YYYY-MM}.csv"""
    return minio_key("klines", symbol, dt, extension=".csv")


# --- Date / Month Utilities (Data Vision) -----------------------------------

def get_target_months(months_back: int) -> list[tuple[int, int]]:
    """Last *months_back* completed months as (year, month) tuples."""
    end_date = datetime.now(timezone.utc) - relativedelta(months=1)
    return [
        ((end_date - relativedelta(months=i)).year,
         (end_date - relativedelta(months=i)).month)
        for i in range(months_back)
    ]
