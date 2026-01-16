"""HDFS Loader.

Handles uploading raw files to Hadoop HDFS.
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def upload_to_hdfs(
    local_path: str,
    hdfs_path: str,
    overwrite: bool = True
) -> None:
    """Upload a local file to HDFS.

    Args:
        local_path: Path to local file.
        hdfs_path: Destination path in HDFS.
        overwrite: If True, overwrite existing file (default: True for idempotency).

    Raises:
        Exception: If upload fails.
    """
    # TODO: Implement using hdfs command or pyarrow.hdfs
    logger.info(f"Uploading {local_path} to {hdfs_path}")
    raise NotImplementedError("HDFS loader needs implementation")


def create_hdfs_directory(hdfs_path: str) -> None:
    """Create a directory in HDFS if it doesn't exist.

    Args:
        hdfs_path: HDFS directory path.
    """
    # TODO: Implement
    logger.info(f"Creating HDFS directory: {hdfs_path}")
    raise NotImplementedError("HDFS directory creation needs implementation")
