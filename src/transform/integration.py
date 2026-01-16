"""Data Integration Transformations.

Handles JOIN operations between:
- IMDb (title.basics + title.ratings)
- TMDB (financial data)
- Internal Logs (user reviews)
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def join_imdb_ratings(basics_df: DataFrame, ratings_df: DataFrame) -> DataFrame:
    """Join IMDb basics with ratings.

    Args:
        basics_df: title.basics DataFrame.
        ratings_df: title.ratings DataFrame.

    Returns:
        Joined DataFrame.
    """
    logger.info("Joining IMDb basics with ratings on 'tconst'")
    return basics_df.join(ratings_df, on="tconst", how="left")


def join_with_tmdb(imdb_df: DataFrame, tmdb_df: DataFrame) -> DataFrame:
    """Join IMDb data with TMDB financial data.

    Uses Broadcast Join if TMDB data is small enough.

    Args:
        imdb_df: IMDb DataFrame.
        tmdb_df: TMDB DataFrame.

    Returns:
        Enriched DataFrame with financial info.
    """
    # TODO: Implement proper join key mapping (IMDb ID to TMDB ID)
    logger.info("Joining with TMDB data")
    return imdb_df


def join_with_internal_reviews(
    main_df: DataFrame, reviews_df: DataFrame
) -> DataFrame:
    """Join main dataset with internal user reviews.

    Args:
        main_df: Main DataFrame.
        reviews_df: Internal reviews DataFrame.

    Returns:
        Combined DataFrame.
    """
    # TODO: Implement join logic
    logger.info("Joining with internal reviews")
    return main_df
