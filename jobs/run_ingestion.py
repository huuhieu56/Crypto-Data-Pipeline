"""Ingestion Job Entry Point.

Orchestrates all data extraction tasks:
- IMDb dataset download
- TMDB API extraction
- Internal MySQL export
"""
import logging
import sys

from src.utils.logging_config import setup_logging
from src.extract.imdb_extractor import download_imdb_datasets
from src.extract.tmdb_extractor import download_daily_id_export, fetch_movie_details
from src.extract.internal_extractor import extract_user_reviews

logger = logging.getLogger(__name__)


def run_ingestion() -> None:
    """Run all ingestion tasks.

    Raises:
        Exception: If any extraction fails.
    """
    setup_logging()
    logger.info("Starting data ingestion pipeline")

    try:
        # Step 1: IMDb datasets
        logger.info("=== Extracting IMDb datasets ===")
        download_imdb_datasets()

        # Step 2: TMDB API
        logger.info("=== Extracting TMDB data ===")
        # TODO: Implement date logic
        # movie_ids = download_daily_id_export("01_17_2026")
        # fetch_movie_details(movie_ids)

        # Step 3: Internal logs
        logger.info("=== Extracting internal logs ===")
        extract_user_reviews()

        logger.info("Ingestion pipeline completed successfully")

    except Exception as e:
        logger.error(f"Ingestion pipeline failed: {e}")
        raise


if __name__ == "__main__":
    try:
        run_ingestion()
    except Exception:
        sys.exit(1)
