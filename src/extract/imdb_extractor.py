"""IMDb Dataset Extractor.

Downloads compressed TSV files from IMDb Interfaces and uploads to HDFS.
Files are kept compressed to save storage.
"""
import logging
from typing import List

logger = logging.getLogger(__name__)

IMDB_DATASETS: List[str] = [
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz",
]
IMDB_BASE_URL: str = "https://datasets.imdbws.com/"


def download_imdb_datasets() -> None:
    """Download IMDb datasets and upload to HDFS.

    Downloads title.basics and title.ratings datasets.
    Uploads compressed files directly to HDFS without local decompression.

    Raises:
        requests.exceptions.RequestException: If download fails.
    """
    # TODO: Implement download logic
    # 1. Use requests.get with stream=True for large files
    # 2. Upload to HDFS using hdfs_loader
    logger.info("IMDb extractor not yet implemented")
    raise NotImplementedError("IMDb extractor needs implementation")
