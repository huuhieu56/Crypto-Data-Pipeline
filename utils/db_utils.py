"""Database utilities for the Crypto Data Pipeline.

Provides ClickHouse client management, insert/query helpers.
All modules requiring database access MUST use these helpers.
"""

from __future__ import annotations

import threading

import pandas as pd
from clickhouse_connect.driver.client import Client
import clickhouse_connect

from config.config import CH_CONFIG
from utils.logger import get_logger
from utils.exceptions import DatabaseConnectionError, SchemaInitError

logger = get_logger(__name__)

# --- ClickHouse Client (thread-local) ----------------------------------------

_local = threading.local()


def get_ch_client() -> Client:
    """Return a thread-local ClickHouse client. Use for synchronous ETL code."""
    if not hasattr(_local, "client"):
        try:
            _local.client = clickhouse_connect.get_client(
                host=CH_CONFIG["host"],
                port=CH_CONFIG["port"],
                username=CH_CONFIG["user"],
                password=CH_CONFIG["password"],
                database=CH_CONFIG["database"],
            )
            _local.client.query("SELECT 1")
            logger.info(
                "ClickHouse connected: %s:%s/%s",
                CH_CONFIG["host"], CH_CONFIG["port"], CH_CONFIG["database"],
            )
        except Exception as exc:
            raise DatabaseConnectionError(
                f"Cannot connect to ClickHouse: {exc}"
            ) from exc
    return _local.client


def new_ch_client() -> Client:
    """Create a fresh ClickHouse client. Use for async/concurrent contexts."""
    return clickhouse_connect.get_client(
        host=CH_CONFIG["host"],
        port=CH_CONFIG["port"],
        username=CH_CONFIG["user"],
        password=CH_CONFIG["password"],
        database=CH_CONFIG["database"],
    )


def init_schema() -> None:
    """Execute sql/schema.sql to initialize database tables."""
    client = get_ch_client()
    schema_path = SQL_DIR / "schema.sql"

    if not schema_path.exists():
        logger.warning("Schema file not found: %s — skipping", schema_path)
        return

    logger.info("Initializing schema from %s", schema_path.name)
    try:
        sql = schema_path.read_text(encoding="utf-8")
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement and not statement.startswith("--"):
                client.command(statement)
        logger.info("Schema initialized successfully")
    except Exception as exc:
        raise SchemaInitError(f"Failed to init schema: {exc}") from exc


# --- Insert / Query Helpers --------------------------------------------------

def ch_insert_df(table: str, df: pd.DataFrame, max_retries: int = 3) -> int:
    """Insert a DataFrame into a ClickHouse table with retry. Returns row count."""
    if df.empty:
        return 0
    import time
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            client = get_ch_client()
            client.insert_df(table, df)
            return len(df)
        except Exception as exc:
            last_exc = exc
            logger.warning("Insert %s failed (%d/%d): %s", table, attempt, max_retries, exc)
            if attempt < max_retries:
                time.sleep(2 ** attempt)
    raise DatabaseConnectionError(f"Insert {table} failed after {max_retries} retries: {last_exc}")


def ch_query_df(query: str) -> pd.DataFrame:
    """Execute a SELECT query, return results as DataFrame."""
    client = get_ch_client()
    return client.query_df(query)


def ch_query_df_params(query: str, params: dict) -> pd.DataFrame:
    """Execute a parameterized SELECT query using a fresh client (async-safe)."""
    client = new_ch_client()
    try:
        return client.query_df(query, parameters=params)
    finally:
        client.close()


def ch_command_params(command: str, params: dict) -> None:
    """Execute a parameterized command (INSERT/DELETE/DDL) using a fresh client."""
    client = new_ch_client()
    try:
        client.command(command, parameters=params)
    finally:
        client.close()


# --- Batch Helpers -----------------------------------------------------------

def get_table_watermarks(
    table: str,
    ts_col: str,
    symbols: list[str],
) -> dict[str, int]:
    """Get max timestamp (ms) per symbol from any ClickHouse table.

    Generalizes watermark queries for klines, ticker_24h, and order_book_snapshot.
    Returns {symbol: last_timestamp_ms}.
    """
    if not symbols:
        return {}

    sym_list = "', '".join(symbols)
    df = ch_query_df(
        f"SELECT symbol, max({ts_col}) AS max_ts "
        f"FROM {table} "
        f"WHERE symbol IN ('{sym_list}') "
        f"GROUP BY symbol"
    )

    result: dict[str, int] = {}
    for _, row in df.iterrows():
        if pd.notna(row["max_ts"]):
            ts = pd.Timestamp(row["max_ts"])
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            result[row["symbol"]] = int(ts.timestamp() * 1000)

    return result


def get_last_timestamps(symbols: list[str]) -> dict[str, int]:
    """Get last loaded timestamp (ms) for each symbol from ClickHouse klines.

    Single batch query instead of N file downloads.
    Returns {symbol: last_timestamp_ms}.
    """
    return get_table_watermarks("klines", "timestamp", symbols)
