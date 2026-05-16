from unittest.mock import MagicMock

import pandas as pd
import pytest

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


def test_load_klines_inserts_raw_csv_to_klines(monkeypatch):
    """load_klines should load raw OHLCV CSV into klines, no indicator computation."""
    from unittest.mock import ANY

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

    load_module.load_klines(symbols=["BTCUSDT"], month_str="2026-05")

    mock_client.query.assert_called()
    sql_call = mock_client.query.call_args_list[0][0][0]
    assert "INSERT INTO crypto_db.klines" in sql_call
    assert "s3(" in sql_call
    assert f"/{load_module.BUCKET_RAW}/klines/BTCUSDT/2026-05.csv" in sql_call
    assert "CSVWithNames" in sql_call
    assert "toDateTime(intDiv(open_time, 1000), 'UTC') AS timestamp" in sql_call
    assert "WHERE open_time > 1000" in sql_call
    assert "if(open_time >" not in sql_call

    # Raw OHLCV load should leave indicators NULL until transform computes them.
    assert "rsi_14" in sql_call
    assert "macd" in sql_call
    assert "macd_signal" in sql_call
    assert "NULL AS rsi_14" in sql_call
    assert "NULL AS macd" in sql_call
    assert "NULL AS macd_signal" in sql_call

    # Should NOT contain indicator computation (that's transform's job)
    assert "exponentialMovingAverage" not in sql_call


def test_load_klines_rejects_invalid_month():
    with pytest.raises(ValueError, match="Invalid month"):
        load_module.load_klines(
            symbols=["BTCUSDT"],
            month_str="2026-05'; DROP TABLE crypto_db.klines; --",
        )


def test_resolve_kline_months_to_load_uses_explicit_month(monkeypatch):
    discover = MagicMock()
    monkeypatch.setattr(load_module, "discover_month_partitions", discover)

    months = load_module._resolve_kline_months_to_load(
        "BTCUSDT",
        "2026-05",
        watermark_ms=0,
    )

    assert months == ["2026-05"]
    discover.assert_not_called()


def test_resolve_kline_months_to_load_filters_before_watermark_month(monkeypatch):
    monkeypatch.setattr(
        load_module,
        "discover_month_partitions",
        lambda bucket, prefix, symbol, extension: ["2026-03", "2026-04", "2026-05"],
    )

    months = load_module._resolve_kline_months_to_load(
        "BTCUSDT",
        None,
        watermark_ms=1776211200000,
    )

    assert months == ["2026-04", "2026-05"]


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
