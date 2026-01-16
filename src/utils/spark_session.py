"""Spark Session Factory.

Creates and manages PySpark sessions with optimized configurations
for a 16GB RAM host.
"""
import logging
from pyspark.sql import SparkSession

from src.config import settings

logger = logging.getLogger(__name__)

_spark_session: SparkSession = None


def get_spark_session() -> SparkSession:
    """Get or create a Spark session.

    Configures Spark for local execution with memory constraints.

    Returns:
        Configured SparkSession.
    """
    global _spark_session

    if _spark_session is None:
        logger.info("Creating new Spark session")
        _spark_session = (
            SparkSession.builder
            .appName(settings.SPARK_APP_NAME)
            .master(settings.SPARK_MASTER)
            # Memory tuning for 16GB host
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.shuffle.partitions", "8")
            # JDBC drivers
            .config("spark.jars.packages",
                    "org.postgresql:postgresql:42.6.0,"
                    "mysql:mysql-connector-java:8.0.33")
            .getOrCreate()
        )
        logger.info("Spark session created successfully")

    return _spark_session


def stop_spark_session() -> None:
    """Stop the current Spark session."""
    global _spark_session

    if _spark_session is not None:
        logger.info("Stopping Spark session")
        _spark_session.stop()
        _spark_session = None
