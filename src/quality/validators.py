"""Data Quality Validators.

Implements 'Fail Fast' principle: If data is garbage, crash early.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when data quality checks fail."""
    pass


def check_not_empty(df: DataFrame, name: str) -> None:
    """Check that DataFrame is not empty.

    Args:
        df: DataFrame to check.
        name: Name of the dataset for logging.

    Raises:
        DataQualityError: If DataFrame is empty.
    """
    count = df.count()
    if count == 0:
        raise DataQualityError(f"Dataset '{name}' is empty!")
    logger.info(f"Dataset '{name}' has {count} rows")


def check_no_nulls(df: DataFrame, columns: list, name: str) -> None:
    """Check that specified columns have no null values.

    Args:
        df: DataFrame to check.
        columns: List of column names to validate.
        name: Name of the dataset for logging.

    Raises:
        DataQualityError: If any nulls found.
    """
    for col_name in columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            raise DataQualityError(
                f"Dataset '{name}': Column '{col_name}' has {null_count} null values!"
            )
    logger.info(f"Dataset '{name}': No nulls in {columns}")


def check_no_negative_values(df: DataFrame, columns: list, name: str) -> None:
    """Check that numeric columns have no negative values.

    Args:
        df: DataFrame to check.
        columns: List of numeric column names to validate.
        name: Name of the dataset for logging.

    Raises:
        DataQualityError: If any negative values found.
    """
    for col_name in columns:
        negative_count = df.filter(F.col(col_name) < 0).count()
        if negative_count > 0:
            raise DataQualityError(
                f"Dataset '{name}': Column '{col_name}' has {negative_count} negative values!"
            )
    logger.info(f"Dataset '{name}': No negatives in {columns}")
