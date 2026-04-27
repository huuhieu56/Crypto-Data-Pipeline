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
from typing import Any

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


# --- Partition I/O (module-level functions using singleton) -------------------

import pandas as pd
from datetime import datetime, timezone
from config.config import PARTITION_MONTH_FORMAT


def _normalize_timestamp_unit(table: pa.Table, unit: str = "us") -> pa.Table:
    """Normalize timestamp columns to a consistent Arrow unit for safe concat."""
    fields = []
    for field in table.schema:
        if pa.types.is_timestamp(field.type):
            fields.append(pa.field(field.name, pa.timestamp(unit, tz=field.type.tz), nullable=field.nullable))
        else:
            fields.append(field)
    target_schema = pa.schema(fields)
    return table.cast(target_schema, safe=False)


def append_to_partition(
    bucket: str,
    prefix: str,
    symbol: str,
    new_df: pd.DataFrame,
    dedup_col: str,
    month_str: str | None = None,
) -> None:
    """Append rows to a monthly partition on MinIO with dedup.

    Unified write pattern for all data types:
        download partition → concat → dedup(dedup_col) → upload.

    File is capped at ~44,640 rows/month (1 row/min × 31 days)
    and resets automatically when the month changes.
    """
    month_str = month_str or datetime.now(timezone.utc).strftime(PARTITION_MONTH_FORMAT)
    key = f"{prefix}/{symbol}/{month_str}.parquet"

    # Normalize timestamps to microseconds for stable Arrow concat
    for c in new_df.columns:
        if pd.api.types.is_datetime64_any_dtype(new_df[c]):
            new_df[c] = new_df[c].dt.as_unit("us")

    new_table = pa.Table.from_pandas(new_df, preserve_index=False)
    new_table = _normalize_timestamp_unit(new_table, unit="us")

    if storage.object_exists(bucket, key):
        existing = storage.download_parquet(bucket, key)
        existing = _normalize_timestamp_unit(existing, unit="us")
        table = pa.concat_tables([existing, new_table])

        # Dedup: keep last (newest) value for each timestamp
        pdf = table.to_pandas()
        pdf = pdf.drop_duplicates(subset=[dedup_col], keep="last")
        pdf = pdf.sort_values(dedup_col).reset_index(drop=True)
        table = pa.Table.from_pandas(pdf, preserve_index=False)
        del pdf
    else:
        table = new_table

    storage.upload_parquet(bucket, key, table)
