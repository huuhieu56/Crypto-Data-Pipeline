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


def append_to_partition_csv(
    bucket: str,
    prefix: str,
    symbol: str,
    new_df: pd.DataFrame,
    dedup_col: str,
) -> None:
    """Append rows to CSV monthly partitions with dedup, grouped by open_time month.

    Unlike append_to_partition (which writes to a single month), this function
    groups new_df rows by the YYYY-MM derived from open_time and appends each
    group to the correct {prefix}/{SYMBOL}/{YYYY-MM}.csv file.

    Handles microsecond timestamps from Data Vision by normalizing to ms.
    Converts datetime columns to epoch ms before writing for consistent CSV format.
    """
    import io

    # Ensure open_time is numeric epoch ms
    ts_col = new_df[dedup_col]
    if pd.api.types.is_datetime64_any_dtype(ts_col):
        ts_series = ts_col.astype("int64") // 1_000_000  # ns -> ms
    else:
        ts_series = ts_col.astype("int64")
        ts_series = ts_series.where(ts_series <= 1000000000000000, ts_series // 1000)

    # Derive YYYY-MM from epoch ms
    month_series = pd.to_datetime(ts_series, unit="ms", utc=True).dt.strftime(PARTITION_MONTH_FORMAT)

    # Normalize timestamp columns to epoch ms for consistent CSV output
    out_df = new_df.copy()
    for col in ("open_time", "close_time"):
        if col in out_df.columns:
            col_data = out_df[col]
            if pd.api.types.is_datetime64_any_dtype(col_data):
                out_df[col] = col_data.astype("int64") // 1_000_000
            else:
                vals = col_data.astype("int64")
                out_df[col] = vals.where(vals <= 1000000000000000, vals // 1000)

    for month_str, group_df in out_df.groupby(month_series):
        key = f"{prefix}/{symbol}/{month_str}.csv"
        group_df = group_df.copy()

        if storage.object_exists(bucket, key):
            response = storage.client.get_object(bucket, key)
            try:
                existing_df = pd.read_csv(io.BytesIO(response.read()))
            finally:
                response.close()
                response.release_conn()

            combined = pd.concat([existing_df, group_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=[dedup_col], keep="last")
            combined = combined.sort_values(dedup_col).reset_index(drop=True)
        else:
            combined = group_df

        csv_buf = io.BytesIO()
        combined.to_csv(csv_buf, index=False)
        csv_buf.seek(0)
        storage.client.put_object(
            bucket, key, csv_buf, csv_buf.getbuffer().nbytes,
            content_type="text/csv",
        )
        logger.debug("Appended %d rows to %s/%s", len(group_df), bucket, key)


def discover_month_partitions(bucket: str, prefix: str, symbol: str, extension: str = ".parquet") -> list[str]:
    """Find all month partitions for a symbol in MinIO.

    Args:
        bucket: MinIO bucket name.
        prefix: Object prefix (e.g. "klines", "ticker_24h").
        symbol: Trading pair symbol.
        extension: File extension to filter (default ".parquet").
    """
    keys = storage.list_objects(bucket, prefix=f"{prefix}/{symbol}/")
    months = []
    for k in keys:
        if k.endswith(extension):
            month = k.rsplit("/", 1)[-1].replace(extension, "")
            months.append(month)
    return sorted(months)
