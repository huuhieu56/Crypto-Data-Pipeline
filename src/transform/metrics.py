"""Business Metrics Calculation.

Computes derived metrics using vectorized Spark operations.
NO for-loops allowed.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def calculate_profit(df: DataFrame) -> DataFrame:
    """Calculate movie profit.

    Profit = Revenue - Budget

    Args:
        df: DataFrame with 'revenue' and 'budget' columns.

    Returns:
        DataFrame with 'profit' column added.
    """
    logger.info("Calculating profit")
    return df.withColumn(
        "profit",
        F.col("revenue") - F.col("budget")
    )


def calculate_roi(df: DataFrame) -> DataFrame:
    """Calculate Return on Investment (ROI).

    ROI = (Profit / Budget) * 100

    Args:
        df: DataFrame with 'profit' and 'budget' columns.

    Returns:
        DataFrame with 'roi' column added.
    """
    logger.info("Calculating ROI")
    return df.withColumn(
        "roi",
        F.when(
            F.col("budget") > 0,
            (F.col("profit") / F.col("budget")) * 100
        ).otherwise(None)
    )


def calculate_rating_diff(df: DataFrame) -> DataFrame:
    """Calculate rating difference between IMDb and internal scores.

    Rating_Diff = imdb_rating - internal_rating

    Args:
        df: DataFrame with 'imdb_rating' and 'internal_rating' columns.

    Returns:
        DataFrame with 'rating_diff' column added.
    """
    logger.info("Calculating rating difference")
    return df.withColumn(
        "rating_diff",
        F.col("imdb_rating") - F.col("internal_rating")
    )
