"""PostgreSQL Loader.

Writes clean/processed data to the PostgreSQL data warehouse.
Uses Spark JDBC writer for efficient bulk loading.
"""
import logging
from pyspark.sql import DataFrame

from src.config import settings

logger = logging.getLogger(__name__)


def write_to_postgres(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite"
) -> None:
    """Write a Spark DataFrame to PostgreSQL.

    Args:
        df: Spark DataFrame to write.
        table_name: Target table name.
        mode: Write mode ('overwrite', 'append'). Default 'overwrite' for idempotency.

    Raises:
        Exception: If write fails.
    """
    logger.info(f"Writing DataFrame to PostgreSQL table: {table_name}")

    jdbc_properties = {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    df.write.jdbc(
        url=settings.POSTGRES_JDBC_URL,
        table=table_name,
        mode=mode,
        properties=jdbc_properties,
    )
    logger.info(f"Successfully wrote to {table_name}")
