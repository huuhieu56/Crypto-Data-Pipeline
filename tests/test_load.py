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
        "crypto-raw",
        "features",
        "klines",
        "timestamp",
        ["symbol", "timestamp", "open"],
    )

    storage.download_parquet.assert_called_once_with(
        "crypto-raw",
        "features/BTCUSDT/2026-05.parquet",
    )
    insert.assert_called_once()
    storage.remove_object.assert_not_called()


def test_load_klines_uses_clickhouse_sql_transform(monkeypatch):
    """load_klines should execute SQL transform, not read processed Parquet."""
    from unittest.mock import ANY

    # Mock dependencies
    monkeypatch.setattr(load_module, "get_table_watermarks", lambda table, ts_col, symbols: {"BTCUSDT": 1000})
    monkeypatch.setattr(
        load_module,
        "discover_month_partitions",
        lambda bucket, prefix, symbol, extension: ["2026-05"],
    )
    storage = MagicMock()
    storage.object_exists.return_value = True
    monkeypatch.setattr(load_module, "storage", storage)

    mock_result = MagicMock()
    mock_result.result_rows = [[100]]
    mock_client = MagicMock()
    mock_client.query.return_value = mock_result
    monkeypatch.setattr(load_module, "get_ch_client", lambda: mock_client)

    monkeypatch.setattr(load_module.sys.modules["scripts.load"], "CLICKHOUSE_S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setattr(load_module, "INDICATOR_CONTEXT_ROWS", 120)

    load_module.load_klines(symbols=["BTCUSDT"], month_str="2026-05")

    # Verify SQL was executed, not Parquet-based _load_table
    mock_client.query.assert_called()
    sql_call = mock_client.query.call_args_list[0][0][0]
    assert "INSERT INTO crypto_db.klines" in sql_call
    assert "s3(" in sql_call
    assert "exponentialMovingAverage" in sql_call


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
