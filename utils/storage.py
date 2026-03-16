"""MinIO object storage abstraction layer.

Provides a unified interface for reading/writing Parquet, CSV, and JSON
data to MinIO (S3-compatible). All data I/O should use the module-level
``storage`` singleton.

Usage::

    from utils.storage import storage
    storage.upload_parquet("crypto-raw", "BTCUSDT.parquet", table)
    table = storage.download_parquet("crypto-raw", "BTCUSDT.parquet")
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from config.config import MINIO_CONFIG
from utils.logger import get_logger

logger = get_logger(__name__)


# --- MinIO Storage Client (singleton) ----------------------------------------

class MinIOStorage:
    """Thin wrapper around Minio SDK for the crypto data pipeline."""

    def __init__(self) -> None:
        self._client: Minio | None = None

    @property
    def client(self) -> Minio:
        if self._client is None:
            self._client = Minio(
                endpoint=MINIO_CONFIG["endpoint"],
                access_key=MINIO_CONFIG["access_key"],
                secret_key=MINIO_CONFIG["secret_key"],
                secure=MINIO_CONFIG["secure"],
            )
            logger.info(
                "MinIO client connected: %s", MINIO_CONFIG["endpoint"],
            )
        return self._client

    # ---- Bucket operations ------------------------------------------------

    def ensure_buckets(self) -> None:
        """Create configured buckets if they don't exist."""
        for bucket in (MINIO_CONFIG["bucket_raw"], MINIO_CONFIG["bucket_processed"]):
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info("Created bucket: %s", bucket)

    # ---- Parquet I/O ------------------------------------------------------

    def upload_parquet(self, bucket: str, key: str, table: pa.Table) -> None:
        """Write a pyarrow Table as Parquet to MinIO."""
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        self.client.put_object(
            bucket, key, buf, length=buf.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        logger.debug("Uploaded %s/%s (%d rows)", bucket, key, table.num_rows)

    def download_parquet(self, bucket: str, key: str) -> pa.Table:
        """Read a Parquet file from MinIO into a pyarrow Table."""
        response = self.client.get_object(bucket, key)
        try:
            buf = io.BytesIO(response.read())
        finally:
            response.close()
            response.release_conn()
        return pq.read_table(buf)

    # ---- CSV I/O ----------------------------------------------------------

    def upload_csv_df(self, bucket: str, key: str, df: pd.DataFrame) -> None:
        """Write a pandas DataFrame as CSV to MinIO."""
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        self.client.put_object(
            bucket, key, buf, length=buf.getbuffer().nbytes,
            content_type="text/csv",
        )
        logger.debug("Uploaded CSV %s/%s (%d rows)", bucket, key, len(df))

    def download_csv_df(self, bucket: str, key: str) -> pd.DataFrame:
        """Read a CSV file from MinIO into a pandas DataFrame."""
        response = self.client.get_object(bucket, key)
        try:
            buf = io.BytesIO(response.read())
        finally:
            response.close()
            response.release_conn()
        return pd.read_csv(buf)

    def append_csv_df(self, bucket: str, key: str, df: pd.DataFrame) -> None:
        """Append rows to an existing CSV on MinIO (download → concat → upload)."""
        if self.object_exists(bucket, key):
            existing = self.download_csv_df(bucket, key)
            combined = pd.concat([existing, df], ignore_index=True)
        else:
            combined = df
        self.upload_csv_df(bucket, key, combined)

    # ---- JSON I/O ---------------------------------------------------------

    def upload_json(self, bucket: str, key: str, data: Any) -> None:
        """Write JSON data to MinIO."""
        buf = io.BytesIO(json.dumps(data, default=str).encode("utf-8"))
        buf.seek(0)
        self.client.put_object(
            bucket, key, buf, length=buf.getbuffer().nbytes,
            content_type="application/json",
        )

    def download_json(self, bucket: str, key: str) -> Any:
        """Read a JSON file from MinIO."""
        response = self.client.get_object(bucket, key)
        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()

    # ---- Generic file I/O -------------------------------------------------

    def upload_file(self, bucket: str, key: str, local_path: Path) -> None:
        """Upload a local file to MinIO."""
        self.client.fput_object(bucket, key, str(local_path))
        logger.debug("Uploaded file %s -> %s/%s", local_path, bucket, key)

    def download_file(self, bucket: str, key: str, local_path: Path) -> None:
        """Download a file from MinIO to local path."""
        self.client.fget_object(bucket, key, str(local_path))
        logger.debug("Downloaded %s/%s -> %s", bucket, key, local_path)

    # ---- Utility ----------------------------------------------------------

    def object_exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists in a bucket."""
        try:
            self.client.stat_object(bucket, key)
            return True
        except S3Error:
            return False

    def list_objects(
        self, bucket: str, prefix: str = "", recursive: bool = True,
    ) -> list[str]:
        """List object keys in a bucket with optional prefix filter."""
        return [
            obj.object_name
            for obj in self.client.list_objects(
                bucket, prefix=prefix, recursive=recursive,
            )
        ]

    def remove_object(self, bucket: str, key: str) -> None:
        """Delete an object from a bucket."""
        self.client.remove_object(bucket, key)
        logger.debug("Removed %s/%s", bucket, key)


# --- Module-level Singleton --------------------------------------------------
storage = MinIOStorage()
