"""TMDB API Extractor.

Uses the TMDB Daily ID Export strategy:
1. Download daily movie ID export file.
2. Filter IDs based on criteria.
3. Batch query the API with rate limiting.
4. Save results to HDFS as JSON.
"""
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def download_daily_id_export(date_str: str) -> List[int]:
    """Download TMDB daily movie ID export file.

    Args:
        date_str: Date in 'MM_DD_YYYY' format.

    Returns:
        List of valid movie IDs.

    Raises:
        requests.exceptions.RequestException: If download fails.
    """
    # TODO: Implement
    # URL format: https://files.tmdb.org/p/exports/movie_ids_MM_DD_YYYY.json.gz
    logger.info("TMDB daily export downloader not yet implemented")
    raise NotImplementedError("TMDB daily export needs implementation")


def fetch_movie_details(movie_ids: List[int]) -> List[Dict[str, Any]]:
    """Fetch detailed movie information from TMDB API.

    Handles rate limiting (50 req/s) and HTTP 429 errors with backoff.

    Args:
        movie_ids: List of TMDB movie IDs.

    Returns:
        List of movie detail dictionaries.
    """
    # TODO: Implement batch API calls with rate limiting
    logger.info("TMDB API fetcher not yet implemented")
    raise NotImplementedError("TMDB API fetcher needs implementation")
