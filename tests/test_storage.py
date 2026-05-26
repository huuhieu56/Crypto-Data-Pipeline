"""Unit tests for MinIO storage partition helpers."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import storage as storage_module


def test_discover_month_partitions_base_only(monkeypatch):
    monkeypatch.setattr(
        storage_module.storage,
        "list_objects",
        MagicMock(return_value=[
            "klines/BTCUSDT/2026-04.csv",
            "klines/BTCUSDT/2026-05.csv",
        ]),
    )

    months = storage_module.discover_month_partitions(
        "crypto-raw",
        "klines",
        "BTCUSDT",
        extension=".csv",
    )

    assert months == ["2026-04", "2026-05"]


def test_discover_month_partitions_delta_only_old_and_new_formats(monkeypatch):
    monkeypatch.setattr(
        storage_module.storage,
        "list_objects",
        MagicMock(return_value=[
            "klines/BTCUSDT/2026-04_delta_1779531739000.parquet",
            "klines/BTCUSDT/2026-05_delta_1779531739000_deadbeef.parquet",
        ]),
    )

    months = storage_module.discover_month_partitions(
        "crypto-raw",
        "klines",
        "BTCUSDT",
        extension=".csv",
    )

    assert months == ["2026-04", "2026-05"]


def test_discover_month_partitions_mixed_base_delta_dedupes(monkeypatch):
    monkeypatch.setattr(
        storage_module.storage,
        "list_objects",
        MagicMock(return_value=[
            "ticker_raw/BTCUSDT/2026-05.parquet",
            "ticker_raw/BTCUSDT/2026-05_delta_1779531739000.parquet",
            "ticker_raw/BTCUSDT/2026-05_delta_1779531740000_deadbeef.parquet",
            "ticker_raw/BTCUSDT/2026-06_delta_1779531740000_deadbeef.parquet",
        ]),
    )

    months = storage_module.discover_month_partitions(
        "crypto-raw",
        "ticker_raw",
        "BTCUSDT",
    )

    assert months == ["2026-05", "2026-06"]


def test_discover_month_partitions_ignores_malformed_names(monkeypatch):
    monkeypatch.setattr(
        storage_module.storage,
        "list_objects",
        MagicMock(return_value=[
            "klines/BTCUSDT/_SUCCESS",
            "klines/BTCUSDT/latest.parquet",
            "klines/BTCUSDT/2026-13.parquet",
            "klines/BTCUSDT/not-a-month_delta_1779531739000.parquet",
            "klines/BTCUSDT/2026-05_delta_1779531739000.parquet",
        ]),
    )

    months = storage_module.discover_month_partitions(
        "crypto-raw",
        "klines",
        "BTCUSDT",
    )

    assert months == ["2026-05"]


def test_write_delta_explicit_month_writes_single_partition(monkeypatch):
    mock_upload = MagicMock()
    monkeypatch.setattr(storage_module.storage, "upload_parquet", mock_upload)

    storage_module.write_delta(
        "crypto-raw",
        "ticker_raw",
        "BTCUSDT",
        pd.DataFrame({"symbol": ["BTCUSDT"]}),
        month_str="2026-05",
    )

    assert mock_upload.call_count == 1
    assert mock_upload.call_args[0][1].startswith("ticker_raw/BTCUSDT/2026-05_delta_")


def test_write_delta_groups_klines_by_open_time_epoch_ms(monkeypatch):
    mock_upload = MagicMock()
    monkeypatch.setattr(storage_module.storage, "upload_parquet", mock_upload)
    df = pd.DataFrame({
        "open_time": [
            1777507200000,  # 2026-04-30 00:00:00 UTC
            1777593600000,  # 2026-05-01 00:00:00 UTC
        ],
        "close": [1.0, 2.0],
    })

    storage_module.write_delta("crypto-raw", "klines", "BTCUSDT", df)

    keys = [call.args[1] for call in mock_upload.call_args_list]
    assert len(keys) == 2
    assert keys[0].startswith("klines/BTCUSDT/2026-04_delta_")
    assert keys[1].startswith("klines/BTCUSDT/2026-05_delta_")


def test_write_delta_groups_order_book_by_timestamp(monkeypatch):
    mock_upload = MagicMock()
    monkeypatch.setattr(storage_module.storage, "upload_parquet", mock_upload)
    df = pd.DataFrame({
        "timestamp": [
            datetime(2026, 4, 30, tzinfo=timezone.utc),
            datetime(2026, 5, 1, tzinfo=timezone.utc),
        ],
        "bids": [[], []],
    })

    storage_module.write_delta("crypto-raw", "order_book", "BTCUSDT", df)

    keys = [call.args[1] for call in mock_upload.call_args_list]
    assert len(keys) == 2
    assert keys[0].startswith("order_book/BTCUSDT/2026-04_delta_")
    assert keys[1].startswith("order_book/BTCUSDT/2026-05_delta_")


def test_write_delta_groups_news_by_extracted_at(monkeypatch):
    mock_upload = MagicMock()
    monkeypatch.setattr(storage_module.storage, "upload_parquet", mock_upload)
    df = pd.DataFrame({
        "article_id": ["a1"],
        "extracted_at": [pd.Timestamp("2026-05-26T10:00:00Z")],
    })

    storage_module.write_delta("crypto-raw", "crypto_news", "gnews", df)

    assert mock_upload.call_count == 1
    assert mock_upload.call_args[0][1].startswith("crypto_news/gnews/2026-05_delta_")


def test_write_delta_falls_back_to_current_month_without_timestamp_column(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 5, 26, 10, 0, tzinfo=tz)

    mock_upload = MagicMock()
    monkeypatch.setattr(storage_module, "datetime", FixedDateTime)
    monkeypatch.setattr(storage_module.storage, "upload_parquet", mock_upload)

    storage_module.write_delta(
        "crypto-raw",
        "ticker_raw",
        "BTCUSDT",
        pd.DataFrame({"symbol": ["BTCUSDT"]}),
    )

    assert mock_upload.call_count == 1
    assert mock_upload.call_args[0][1].startswith("ticker_raw/BTCUSDT/2026-05_delta_")


def test_write_delta_raises_for_null_detected_timestamp(monkeypatch):
    mock_upload = MagicMock()
    monkeypatch.setattr(storage_module.storage, "upload_parquet", mock_upload)
    df = pd.DataFrame({"open_time": [None], "close": [1.0]})

    with pytest.raises(ValueError, match="Cannot derive month"):
        storage_module.write_delta("crypto-raw", "klines", "BTCUSDT", df)

    mock_upload.assert_not_called()
