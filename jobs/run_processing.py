"""Processing Job Entry Point.

Orchestrates Spark ETL:
1. Read raw data from HDFS
2. Clean and transform
3. Calculate business metrics
4. Write to PostgreSQL
"""
import logging
import sys

from src.utils.logging_config import setup_logging
from src.utils.spark_session import get_spark_session, stop_spark_session
from src.config import settings
from src.transform.cleaning import filter_adult_content, filter_missing_year
from src.transform.integration import join_imdb_ratings
from src.transform.metrics import calculate_profit, calculate_roi
from src.quality.validators import check_not_empty
from src.load.postgres_loader import write_to_postgres

logger = logging.getLogger(__name__)


def run_processing() -> None:
    """Run Spark ETL processing job.

    Raises:
        Exception: If processing fails.
    """
    setup_logging()
    logger.info("Starting data processing pipeline")

    try:
        spark = get_spark_session()

        # Step 1: Read raw IMDb data from HDFS
        logger.info("=== Reading raw data from HDFS ===")
        imdb_basics_path = f"{settings.HDFS_BASE_URL}{settings.HDFS_RAW_IMDB_PATH}/title.basics.tsv.gz"
        imdb_ratings_path = f"{settings.HDFS_BASE_URL}{settings.HDFS_RAW_IMDB_PATH}/title.ratings.tsv.gz"

        basics_df = spark.read.csv(imdb_basics_path, sep="\t", header=True)
        ratings_df = spark.read.csv(imdb_ratings_path, sep="\t", header=True)

        # Step 2: Data Quality Checks
        logger.info("=== Running data quality checks ===")
        check_not_empty(basics_df, "imdb_basics")
        check_not_empty(ratings_df, "imdb_ratings")

        # Step 3: Clean
        logger.info("=== Cleaning data ===")
        basics_df = filter_adult_content(basics_df)
        basics_df = filter_missing_year(basics_df)

        # Step 4: Join & Transform
        logger.info("=== Joining datasets ===")
        combined_df = join_imdb_ratings(basics_df, ratings_df)

        # Step 5: Calculate metrics
        logger.info("=== Calculating metrics ===")
        # TODO: Add TMDB financial data for profit/ROI calculation

        # Step 6: Write to PostgreSQL
        logger.info("=== Writing to PostgreSQL ===")
        write_to_postgres(combined_df, "movies_analytics")

        logger.info("Processing pipeline completed successfully")

    except Exception as e:
        logger.error(f"Processing pipeline failed: {e}")
        raise
    finally:
        stop_spark_session()


if __name__ == "__main__":
    try:
        run_processing()
    except Exception:
        sys.exit(1)
