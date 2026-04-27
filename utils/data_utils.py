"""Data utilities for the Crypto Data Pipeline.

Provides date/month helpers and partition key utilities.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dateutil.relativedelta import relativedelta

from config.config import PARTITION_MONTH_FORMAT
from config.symbols import SYMBOLS_STATUS, BREAK_DATES
from utils.logger import get_logger

logger = get_logger(__name__)


# --- Shared Helpers ----------------------------------------------------------

def get_target_end(symbol: str) -> datetime:
    """TRADING → now (UTC), BREAK → break_date."""
    break_date_str = BREAK_DATES.get(symbol)
    if break_date_str and SYMBOLS_STATUS.get(symbol) != "TRADING":
        return datetime.strptime(break_date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc,
        )
    return datetime.now(timezone.utc)


# --- Partition Key Helpers ---------------------------------------------------

def _resolve_month(dt: datetime | str | None = None) -> str:
    """Resolve a datetime or string into a YYYY-MM month string."""
    if dt is None:
        return datetime.now(timezone.utc).strftime(PARTITION_MONTH_FORMAT)
    if isinstance(dt, str):
        return dt
    return dt.strftime(PARTITION_MONTH_FORMAT)


def minio_key(prefix: str, symbol: str, dt: datetime | str | None = None) -> str:
    """MinIO key: {prefix}/{SYMBOL}/{YYYY-MM}.parquet"""
    return f"{prefix}/{symbol}/{_resolve_month(dt)}.parquet"


def partition_key(symbol: str, dt: datetime | str | None = None) -> str:
    """MinIO key for raw klines: klines/{SYMBOL}/{YYYY-MM}.parquet"""
    return minio_key("klines", symbol, dt)


def features_key(symbol: str, dt: datetime | str | None = None) -> str:
    """MinIO key for processed features: features/{SYMBOL}/{YYYY-MM}.parquet"""
    return minio_key("features", symbol, dt)


# --- Date / Month Utilities (Data Vision) -----------------------------------

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
