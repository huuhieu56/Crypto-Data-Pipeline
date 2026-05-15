from unittest.mock import MagicMock

import pandas as pd

from scripts import load as load_module


def _mock_parquet_table(df: pd.DataFrame) -> MagicMock:
    table = MagicMock()
    table.to_pandas.return_value = df.copy()
    return table


def test_load_table_keeps_minio_partition_after_success(monkeypatch):
    df = pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "timestamp": [pd.Timestamp("2026-05-15 08:45:00")],
        "open": [100.0],
    })
    storage = MagicMock()
    storage.download_parquet.return_value = _mock_parquet_table(df)

    monkeypatch.setattr(load_module, "storage", storage)
    monkeypatch.setattr(load_module, "get_table_watermarks", lambda table, ts_col, symbols: {})
    monkeypatch.setattr(
        load_module,
        "discover_month_partitions",
        lambda bucket, prefix, symbol: ["2026-05"],
    )
    insert = MagicMock(return_value=1)
    monkeypatch.setattr(load_module, "ch_insert_df", insert)

    load_module._load_table(
        ["BTCUSDT"],
        "crypto-processed",
        "features",
        "klines",
        "timestamp",
        ["symbol", "timestamp", "open"],
    )

    storage.download_parquet.assert_called_once_with(
        "crypto-processed",
        "features/BTCUSDT/2026-05.parquet",
    )
    insert.assert_called_once()
    storage.remove_object.assert_not_called()


def test_load_klines_uses_processed_features_path(monkeypatch):
    load_table = MagicMock()
    monkeypatch.setattr(load_module, "_load_table", load_table)

    load_module.load_klines(symbols=["BTCUSDT"], month_str="2026-05")

    args, kwargs = load_table.call_args
    assert kwargs == {}
    assert args[:5] == (
        ["BTCUSDT"],
        load_module.BUCKET_PROCESSED,
        "features",
        "klines",
        "timestamp",
    )


def test_load_ticker_uses_raw_ticker_path(monkeypatch):
    load_table = MagicMock()
    monkeypatch.setattr(load_module, "_load_table", load_table)

    load_module.load_ticker(symbols=["BTCUSDT"], month_str="2026-05")

    args, kwargs = load_table.call_args
    assert kwargs == {}
    assert args[:5] == (
        ["BTCUSDT"],
        load_module.BUCKET_RAW,
        "ticker_24h",
        "ticker_24h",
        "snapshot_time",
    )


def test_load_order_book_uses_raw_order_book_path(monkeypatch):
    load_table = MagicMock()
    monkeypatch.setattr(load_module, "_load_table", load_table)

    load_module.load_order_book(symbols=["BTCUSDT"], month_str="2026-05")

    args, kwargs = load_table.call_args
    assert args[:5] == (
        ["BTCUSDT"],
        load_module.BUCKET_RAW,
        "order_book",
        "order_book_snapshot",
        "timestamp",
    )
    assert kwargs == {"month_str": "2026-05"}
