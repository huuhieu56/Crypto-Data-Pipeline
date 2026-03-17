"""Database utilities for the Crypto Data Pipeline.

Provides ClickHouse client management, insert/query helpers.
All modules requiring database access MUST use these helpers.
"""

from __future__ import annotations

import pandas as pd
from clickhouse_connect.driver.client import Client
import clickhouse_connect

from config.config import CH_CONFIG, SQL_DIR
from utils.logger import get_logger
from utils.exceptions import DatabaseConnectionError, SchemaInitError

logger = get_logger(__name__)

# --- ClickHouse Client (singleton) -------------------------------------------

_ch_client: Client | None = None


def get_ch_client() -> Client:
    """Return a singleton ClickHouse client."""
    global _ch_client
    if _ch_client is None:
        try:
            _ch_client = clickhouse_connect.get_client(
                host=CH_CONFIG["host"],
                port=CH_CONFIG["port"],
                username=CH_CONFIG["user"],
                password=CH_CONFIG["password"],
                database=CH_CONFIG["database"],
            )
            _ch_client.query("SELECT 1")
            logger.info(
                "ClickHouse connected: %s:%s/%s",
                CH_CONFIG["host"], CH_CONFIG["port"], CH_CONFIG["database"],
            )
        except Exception as exc:
            raise DatabaseConnectionError(
                f"Cannot connect to ClickHouse: {exc}"
            ) from exc
    return _ch_client


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

def ch_insert_df(table: str, df: pd.DataFrame) -> int:
    """Insert a DataFrame into a ClickHouse table. Returns row count."""
    if df.empty:
        return 0
    client = get_ch_client()
    client.insert_df(table, df)
    return len(df)


def ch_query_df(query: str) -> pd.DataFrame:
    """Execute a SELECT query, return results as DataFrame."""
    client = get_ch_client()
    return client.query_df(query)


def ch_command(sql: str) -> None:
    """Execute a non-SELECT command (DDL, ALTER, etc.)."""
    client = get_ch_client()
    client.command(sql)


# --- Batch Helpers -----------------------------------------------------------

def get_last_timestamps(symbols: list[str]) -> dict[str, int]:
    """Get last loaded timestamp (ms) for each symbol from ClickHouse.

    Single batch query instead of N file downloads.
    Returns {symbol: last_timestamp_ms}.
    """
    if not symbols:
        return {}

    sym_list = "', '".join(symbols)
    df = ch_query_df(
        f"SELECT symbol, max(timestamp) AS max_ts "
        f"FROM klines "
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
