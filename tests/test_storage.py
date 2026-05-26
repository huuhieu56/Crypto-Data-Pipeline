"""Unit tests for MinIO storage partition helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

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
