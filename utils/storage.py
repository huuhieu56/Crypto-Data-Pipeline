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
    existing_df: pd.DataFrame | None = None,
) -> None:
    """Append rows to a monthly partition on MinIO with dedup.

    Unified write pattern for all data types:
        download partition → concat → dedup(dedup_col) → upload.

    Args:
        existing_df: If provided, skip download and merge with this DataFrame.
                     Avoids redundant download when transform already loaded it.
    """
    month_str = month_str or datetime.now(timezone.utc).strftime(PARTITION_MONTH_FORMAT)
    key = f"{prefix}/{symbol}/{month_str}.parquet"

    # Normalize timestamps to microseconds for stable Arrow concat
    for c in new_df.columns:
        if pd.api.types.is_datetime64_any_dtype(new_df[c]):
            new_df[c] = new_df[c].dt.as_unit("us")

    new_table = pa.Table.from_pandas(new_df, preserve_index=False)
    new_table = _normalize_timestamp_unit(new_table, unit="us")

    if existing_df is not None:
        # Use pre-loaded existing data (avoids redundant download)
        for c in existing_df.columns:
            if pd.api.types.is_datetime64_any_dtype(existing_df[c]):
                existing_df[c] = existing_df[c].dt.as_unit("us")
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        if dedup_col is not None:
            combined = combined.drop_duplicates(subset=[dedup_col], keep="last")
            combined = combined.sort_values(dedup_col).reset_index(drop=True)
        table = pa.Table.from_pandas(combined, preserve_index=False)
        table = _normalize_timestamp_unit(table, unit="us")
        del combined
    elif storage.object_exists(bucket, key):
        existing = storage.download_parquet(bucket, key)
        existing = _normalize_timestamp_unit(existing, unit="us")

        # Use pandas concat to handle schema evolution (e.g. new columns)
        existing_pdf = existing.to_pandas()
        new_pdf = new_df
        combined = pd.concat([existing_pdf, new_pdf], ignore_index=True)
        del existing_pdf

        if dedup_col is not None:
            combined = combined.drop_duplicates(subset=[dedup_col], keep="last")
            combined = combined.sort_values(dedup_col).reset_index(drop=True)

        table = pa.Table.from_pandas(combined, preserve_index=False)
        table = _normalize_timestamp_unit(table, unit="us")
        del combined
    else:
        table = new_table

    storage.upload_parquet(bucket, key, table)


def write_delta(
    bucket: str,
    prefix: str,
    symbol: str,
    new_df: pd.DataFrame,
    month_str: str | None = None,
) -> None:
    """Append new rows as a delta Parquet file. No read, no merge.

    Each call creates a new file: {prefix}/{symbol}/{month}_delta_{ts_ms}.parquet
    Dedup is deferred to Transform when all deltas are concatenated.
    """
    month_str = month_str or datetime.now(timezone.utc).strftime(PARTITION_MONTH_FORMAT)
    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    key = f"{prefix}/{symbol}/{month_str}_delta_{ts_ms}.parquet"
    table = pa.Table.from_pandas(new_df, preserve_index=False)
    storage.upload_parquet(bucket, key, table)
    logger.debug("Wrote delta %s/%s (%d rows)", bucket, key, len(new_df))


def read_month_data(
    bucket: str,
    prefix: str,
    symbol: str,
    month_str: str,
    extension: str = ".parquet",
    since_ms: int | None = None,
    keys: list[str] | None = None,
) -> pd.DataFrame:
    """Read base monthly file + delta files, return concatenated DataFrame.

    Args:
        extension: Base file extension (".csv" for legacy klines, ".parquet" otherwise).
        since_ms: If provided, skip base file (already processed) and only read
                  delta files created after this epoch ms. Dramatically reduces I/O
                  on incremental runs.
        keys: Pre-fetched object keys (avoids redundant list_objects call).
    """
    frames: list[pd.DataFrame] = []

    if since_ms is None:
        # Full read: base file + all deltas (first run / backfill)
        base_key = f"{prefix}/{symbol}/{month_str}{extension}"
        if storage.object_exists(bucket, base_key):
            if extension == ".csv":
                response = storage.client.get_object(bucket, base_key)
                try:
                    frames.append(pd.read_csv(io.BytesIO(response.read())))
                finally:
                    response.close()
                    response.release_conn()
            else:
                frames.append(storage.download_parquet(bucket, base_key).to_pandas())

    # Delta files: prefix/symbol/month_delta_*.parquet
    if keys is None:
        keys = storage.list_objects(bucket, prefix=f"{prefix}/{symbol}/")
    delta_prefix = f"{month_str}_delta_"
    for k in sorted(keys):
        fname = k.rsplit("/", 1)[-1]
        if fname.startswith(delta_prefix) and fname.endswith(".parquet"):
            # Extract timestamp from filename: {month}_delta_{ts_ms}.parquet
            if since_ms is not None:
                try:
                    delta_ts = int(fname[len(delta_prefix):-len(".parquet")])
                    if delta_ts <= since_ms:
                        continue
                except ValueError:
                    continue
            frames.append(storage.download_parquet(bucket, k).to_pandas())

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def discover_month_partitions(
    bucket: str,
    prefix: str,
    symbol: str,
    extension: str = ".parquet",
    keys: list[str] | None = None,
) -> list[str]:
    """Find all month partitions for a symbol in MinIO.

    Args:
        bucket: MinIO bucket name.
        prefix: Object prefix (e.g. "klines", "ticker_24h").
        symbol: Trading pair symbol.
        extension: File extension to filter (default ".parquet").
        keys: Pre-fetched object keys (avoids redundant list_objects call).
    """
    if keys is None:
        keys = storage.list_objects(bucket, prefix=f"{prefix}/{symbol}/")
    months = []
    for k in keys:
        fname = k.rsplit("/", 1)[-1]
        if fname.endswith(extension) and "_delta_" not in fname:
            month = fname.replace(extension, "")
            months.append(month)
    return sorted(months)
