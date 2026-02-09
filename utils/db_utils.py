# =============================================================================
# Database Utilities - Crypto Data Pipeline
# =============================================================================
# Cac function dung chung de thao tac voi PostgreSQL.
# Tat ca module can truy cap DB phai dung cac ham o day.
# =============================================================================

from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from pyspark.sql import SparkSession

from config.config import (
    DB_URL,
    JDBC_URL,
    JDBC_PROPERTIES,
    SQL_DIR,
    SPARK_CONFIG,
)
from utils.logger import get_logger
from utils.exceptions import DatabaseConnectionError, SchemaInitError

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# SQLAlchemy Engine (Pandas)
# ---------------------------------------------------------------------------

_engine: Engine | None = None


def get_engine() -> Engine:
    """Tra ve SQLAlchemy engine (singleton).

    Raises:
        DatabaseConnectionError: Khi khong ket noi duoc.
    """
    global _engine
    if _engine is None:
        try:
            _engine = create_engine(DB_URL, pool_pre_ping=True)
            # Kiem tra ket noi that su
            with _engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established: %s", DB_URL.split("@")[-1])
        except Exception as exc:
            raise DatabaseConnectionError(
                f"Cannot connect to PostgreSQL: {exc}"
            ) from exc
    return _engine


def init_schema(engine: Engine | None = None) -> None:
    """Chay file sql/schema.sql de khoi tao cac bang.

    Raises:
        SchemaInitError: Khi file khong ton tai hoac SQL loi.
    """
    engine = engine or get_engine()
    schema_path = SQL_DIR / "schema.sql"

    if not schema_path.exists():
        logger.warning("Schema file not found: %s — skipping", schema_path)
        return

    logger.info("Initializing database schema from %s", schema_path.name)
    try:
        sql = schema_path.read_text(encoding="utf-8")
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        logger.info("Schema initialized successfully")
    except Exception as exc:
        raise SchemaInitError(f"Failed to init schema: {exc}") from exc


# ---------------------------------------------------------------------------
# Upsert helper (dung lam method= cho pandas .to_sql)
# ---------------------------------------------------------------------------
# Map ten bang -> danh sach cot primary key de ON CONFLICT
_PK_MAP: dict[str, list[str]] = {
    "symbols": ["symbol"],
    "klines": ["symbol", "timestamp"],
    "ticker_24h": ["symbol", "snapshot_time"],
    "order_book_snapshot": ["symbol", "timestamp"],
    "predictions": ["symbol", "predicted_at", "step_index"],
}


def upsert_on_conflict_nothing(table, conn, keys, data_iter):
    """Pandas to_sql method: INSERT ... ON CONFLICT DO NOTHING."""
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    table_name = table.table.name
    index_elements = _PK_MAP.get(table_name)

    if not index_elements:
        # Fallback: lay PK tu metadata
        index_elements = [c.name for c in table.table.primary_key.columns]

    if not index_elements:
        raise ValueError(f"No primary key defined for table '{table_name}'")

    stmt = pg_insert(table.table).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=index_elements)
    conn.execute(stmt)


# ---------------------------------------------------------------------------
# Spark JDBC helpers
# ---------------------------------------------------------------------------

def get_spark_session(app_name: str = "CryptoPipeline") -> SparkSession:
    """Khoi tao SparkSession voi JDBC driver va Arrow.

    Cau hinh doc tu config.SPARK_CONFIG.
    """
    # Dam bao Spark worker dung dung Python interpreter (tranh Windows Store alias)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")
        .config("spark.jars.packages", SPARK_CONFIG["jdbc_package"])
        .config("spark.driver.memory", SPARK_CONFIG["driver_memory"])
        .config("spark.sql.execution.arrow.pyspark.enabled", SPARK_CONFIG["arrow_enabled"])
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .getOrCreate()
    )
    logger.info("SparkSession created: %s", app_name)
    return spark


def spark_write_jdbc(df, table: str, mode: str = "append") -> None:
    """Ghi Spark DataFrame vao PostgreSQL qua JDBC.

    Args:
        df: Spark DataFrame.
        table: Ten bang trong PostgreSQL.
        mode: 'append' hoac 'overwrite'.
    """
    logger.info("Writing to table '%s' via JDBC (mode=%s)", table, mode)
    df.write.jdbc(
        url=JDBC_URL,
        table=table,
        mode=mode,
        properties=JDBC_PROPERTIES,
    )
    logger.info("Write to table '%s' completed", table)
