"""Data Cleaning Transformations.

Uses PySpark DataFrame API exclusively (no RDDs, no for-loops).
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def filter_adult_content(df: DataFrame) -> DataFrame:
    """Remove adult-only content from the dataset.

    Args:
        df: Input DataFrame with 'isAdult' column.

    Returns:
        Filtered DataFrame with adult content removed.
    """
    logger.info("Filtering adult content")
    return df.filter(F.col("isAdult") == 0)


def filter_missing_year(df: DataFrame, year_col: str = "startYear") -> DataFrame:
    """Remove rows with missing release year.

    Args:
        df: Input DataFrame.
        year_col: Column name containing the year.

    Returns:
        Filtered DataFrame.
    """
    logger.info(f"Filtering rows with missing '{year_col}'")
    return df.filter(F.col(year_col).isNotNull())


def standardize_data_types(df: DataFrame) -> DataFrame:
    """Standardize column data types.

    Converts string representations to appropriate numeric types.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with standardized types.
    """
    # TODO: Implement type casting based on schema requirements
    logger.info("Standardizing data types")
    return df
