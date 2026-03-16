"""Database utilities for the Crypto Data Pipeline.

Provides SQLAlchemy engine management, upsert helpers, and Spark JDBC
integration. All modules requiring database access MUST use these helpers.
"""

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
    MINIO_CONFIG,
)
from utils.logger import get_logger
from utils.exceptions import DatabaseConnectionError, SchemaInitError

logger = get_logger(__name__)

# --- SQLAlchemy Engine (Pandas) ----------------------------------------------

_engine: Engine | None = None


def get_engine() -> Engine:
    """Return a singleton SQLAlchemy engine.

    Raises:
        DatabaseConnectionError: If the connection cannot be established.
    """
    global _engine
    if _engine is None:
        try:
            _engine = create_engine(DB_URL, pool_pre_ping=True)
            with _engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established: %s", DB_URL.split("@")[-1])
        except Exception as exc:
            raise DatabaseConnectionError(
                f"Cannot connect to PostgreSQL: {exc}"
            ) from exc
    return _engine


def init_schema(engine: Engine | None = None) -> None:
    """Execute sql/schema.sql to initialize database tables.

    Raises:
        SchemaInitError: If the schema file is missing or SQL execution fails.
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


# --- Upsert Helper (pandas .to_sql method) -----------------------------------

# Table name -> primary key columns for ON CONFLICT
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
        # Fallback: extract PK from table metadata
        index_elements = [c.name for c in table.table.primary_key.columns]

    if not index_elements:
        raise ValueError(f"No primary key defined for table '{table_name}'")

    stmt = pg_insert(table.table).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=index_elements)
    conn.execute(stmt)


# --- Spark JDBC Helpers ------------------------------------------------------

def get_spark_session(app_name: str = "CryptoPipeline") -> SparkSession:
    """Create a SparkSession with JDBC, S3A (MinIO), and Arrow support.

    Configuration is read from config.SPARK_CONFIG and config.MINIO_CONFIG.
    """
    # Ensure Spark workers use the correct Python interpreter
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    packages = ",".join([
        SPARK_CONFIG["jdbc_package"],
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.driver.memory", SPARK_CONFIG["driver_memory"])
        .config("spark.sql.execution.arrow.pyspark.enabled", SPARK_CONFIG["arrow_enabled"])
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        # S3A / MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_CONFIG['endpoint']}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MINIO_CONFIG["secure"]).lower())
        .getOrCreate()
    )
    logger.info("SparkSession created: %s (S3A endpoint: %s)", app_name, MINIO_CONFIG["endpoint"])
    return spark


def spark_write_jdbc(df, table: str, mode: str = "append") -> None:
    """Write a Spark DataFrame to PostgreSQL via JDBC.

    Args:
        df: Spark DataFrame to write.
        table: Target table name in PostgreSQL.
        mode: Write mode — 'append' or 'overwrite'.
    """
    logger.info("Writing to table '%s' via JDBC (mode=%s)", table, mode)
    df.write.jdbc(
        url=JDBC_URL,
        table=table,
        mode=mode,
        properties=JDBC_PROPERTIES,
    )
    logger.info("Write to table '%s' completed", table)


def spark_upsert_jdbc(
    df,
    table: str,
    conflict_columns: list[str],
) -> None:
    """Upsert a Spark DataFrame into PostgreSQL via temp table.

    Steps:
      1. Write to _tmp_{table} (overwrite — no PK constraints)
      2. INSERT INTO {table} SELECT * FROM _tmp ON CONFLICT DO NOTHING
      3. DROP _tmp_{table}

    Safe for re-runs: duplicates are silently skipped.
    """
    temp_table = f"_tmp_{table}"

    # Step 1: write to temp table (fast, no PK constraint)
    logger.info("Writing to temp table '%s' via JDBC", temp_table)
    df.write.jdbc(
        url=JDBC_URL,
        table=temp_table,
        mode="overwrite",
        properties=JDBC_PROPERTIES,
    )

    # Step 2: upsert from temp -> target
    col_list = ", ".join(df.columns)
    conflict_list = ", ".join(conflict_columns)
    upsert_sql = (
        f"INSERT INTO {table} ({col_list}) "
        f"SELECT {col_list} FROM {temp_table} "
        f"ON CONFLICT ({conflict_list}) DO NOTHING"
    )

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(upsert_sql))
        inserted = result.rowcount
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        conn.commit()

    logger.info(
        "Upserted to '%s': %s new rows (duplicates skipped)",
        table, f"{inserted:,}",
    )
