# =============================================================================
# MinIO Utilities - Crypto Data Pipeline
# =============================================================================

from __future__ import annotations

from io import BytesIO
from urllib.parse import urlparse

import pandas as pd
from minio import Minio
from minio.error import S3Error

from config.config import MINIO_CONFIG
from utils.logger import get_logger

logger = get_logger(__name__)


def _normalize_open_time(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure open_time is consistently datetime-like for safe sort/merge."""
    if df is None or df.empty or "open_time" not in df.columns:
        return df

    out = df.copy()
    out["open_time"] = pd.to_datetime(out["open_time"], utc=True, errors="coerce")
    out = out.dropna(subset=["open_time"])
    return out


def _parse_endpoint(endpoint: str) -> tuple[str, bool]:
    """Normalize endpoint and infer secure flag when scheme is present."""
    raw = (endpoint or "").strip()
    if not raw:
        return "localhost:9000", False

    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        if not parsed.netloc:
            raise ValueError(f"Invalid MinIO endpoint: {endpoint}")
        return parsed.netloc, parsed.scheme == "https"

    return raw, bool(MINIO_CONFIG.get("secure", False))


def is_minio_enabled() -> bool:
    return bool(MINIO_CONFIG.get("enabled", False))


def get_minio_client() -> Minio:
    endpoint, secure_from_endpoint = _parse_endpoint(MINIO_CONFIG.get("endpoint", ""))
    secure = secure_from_endpoint if "http" in str(MINIO_CONFIG.get("endpoint", "")) else bool(MINIO_CONFIG.get("secure", False))

    return Minio(
        endpoint,
        access_key=MINIO_CONFIG.get("access_key", "minioadmin"),
        secret_key=MINIO_CONFIG.get("secret_key", "minioadmin"),
        secure=secure,
    )


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    if client.bucket_exists(bucket_name):
        return
    client.make_bucket(bucket_name)
    logger.info("Created MinIO bucket: %s", bucket_name)


def ensure_raw_bucket() -> str:
    bucket = MINIO_CONFIG.get("bucket_raw", "crypto-raw")
    client = get_minio_client()
    ensure_bucket_exists(client, bucket)
    return bucket


def read_csv_from_object(
    client: Minio,
    bucket_name: str,
    object_name: str,
) -> pd.DataFrame | None:
    """Return DataFrame from CSV object; None if object does not exist."""
    response = None
    try:
        response = client.get_object(bucket_name, object_name)
        payload = response.read()
        if not payload:
            return None
        return pd.read_csv(BytesIO(payload))
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
            return None
        raise
    finally:
        if response is not None:
            response.close()
            response.release_conn()


def write_csv_to_object(
    client: Minio,
    bucket_name: str,
    object_name: str,
    df: pd.DataFrame,
) -> None:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    stream = BytesIO(csv_bytes)
    client.put_object(
        bucket_name,
        object_name,
        data=stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )


def merge_klines_to_object(
    client: Minio,
    bucket_name: str,
    object_name: str,
    new_df: pd.DataFrame,
) -> int:
    """Merge klines by open_time directly in MinIO object and return total rows."""
    if new_df is None or new_df.empty:
        old_df = read_csv_from_object(client, bucket_name, object_name)
        return 0 if old_df is None else len(old_df)

    incoming = _normalize_open_time(new_df)
    if incoming is None or incoming.empty:
        old_df = read_csv_from_object(client, bucket_name, object_name)
        return 0 if old_df is None else len(old_df)

    incoming = (
        incoming.drop_duplicates(subset=["open_time"], keep="last")
        .sort_values("open_time")
        .reset_index(drop=True)
    )

    old_df = read_csv_from_object(client, bucket_name, object_name)
    old_df = _normalize_open_time(old_df)
    if old_df is None or old_df.empty:
        merged = incoming
    else:
        merged = pd.concat([old_df, incoming], ignore_index=True)
        merged = (
            merged.drop_duplicates(subset=["open_time"], keep="last")
            .sort_values("open_time")
            .reset_index(drop=True)
        )

    write_csv_to_object(client, bucket_name, object_name, merged)
    return len(merged)


def get_last_open_time_ms(
    client: Minio,
    bucket_name: str,
    object_name: str,
) -> int | None:
    df = read_csv_from_object(client, bucket_name, object_name)
    if df is None or df.empty or "open_time" not in df.columns:
        return None

    try:
        last_raw = df["open_time"].iloc[-1]
        ts = pd.to_datetime(last_raw, utc=True)
        return int(ts.timestamp() * 1000)
    except Exception as exc:
        logger.error("Cannot parse open_time from %s/%s: %s", bucket_name, object_name, exc)
        return None
