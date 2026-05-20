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
        "open_time": [pd.Timestamp("2026-05-15 08:45:00")],
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
        "klines",
        "open_time",
    )

    storage.download_parquet.assert_called_once_with(
        "crypto-raw",
        "klines/BTCUSDT/2026-05.parquet",
    )
    insert.assert_called_once()
    storage.remove_object.assert_not_called()


def test_load_klines_delegates_to_load_table_with_processed_bucket(monkeypatch):
    """load_klines should read transformed Parquet from crypto-processed."""
    transformed_df = pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "open_time": [pd.Timestamp("2026-05-15 08:45:00")],
        "open": [100.0],
        "high": [105.0],
        "low": [99.0],
        "close": [103.0],
        "volume": [10.0],
        "quote_volume": [1030.0],
        "trade_count": [50],
        "rsi_14": [65.0],
        "macd": [0.5],
        "macd_signal": [0.3],
    })
    storage = MagicMock()
    storage.download_parquet.return_value = _mock_parquet_table(transformed_df)
    monkeypatch.setattr(load_module, "storage", storage)
    monkeypatch.setattr(load_module, "get_table_watermarks", lambda table, ts_col, symbols: {})
    monkeypatch.setattr(
        load_module, "discover_month_partitions",
        lambda bucket, prefix, symbol: ["2026-05"],
    )
    insert = MagicMock(return_value=1)
    monkeypatch.setattr(load_module, "ch_insert_df", insert)

    load_module.load_klines(symbols=["BTCUSDT"], month_str="2026-05")

    storage.download_parquet.assert_called_once_with(
        "crypto-processed",
        "klines/BTCUSDT/2026-05.parquet",
    )
    insert.assert_called_once()
    call_args = insert.call_args
    assert call_args[0][0] == "klines"
    df = call_args[0][1]
    assert "rsi_14" in df.columns
    assert "macd" in df.columns
    assert "macd_signal" in df.columns
    assert "trade_count" in df.columns


def test_load_klines_rejects_invalid_month():
    with pytest.raises(ValueError, match="Invalid month"):
        load_module.load_klines(
            symbols=["BTCUSDT"],
            month_str="2026-05'; DROP TABLE crypto_db.klines; --",
        )


def test_load_ticker_inserts_transformed_parquet(monkeypatch):
    """load_ticker delegates to _load_table with correct prefix and columns."""
    transformed_df = pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "snapshot_time": [pd.Timestamp("2026-05-15 08:45:00", tz="UTC")],
        "price_change": [1500.0],
        "spread_pct": [0.0473],
    })

    storage = MagicMock()
    storage.download_parquet.return_value = _mock_parquet_table(transformed_df)
    monkeypatch.setattr(load_module, "storage", storage)
    monkeypatch.setattr(load_module, "get_table_watermarks", lambda table, ts_col, symbols: {})
    monkeypatch.setattr(
        load_module, "discover_month_partitions",
        lambda bucket, prefix, symbol: ["2026-05"],
    )
    insert = MagicMock(return_value=1)
    monkeypatch.setattr(load_module, "ch_insert_df", insert)

    load_module.load_ticker(symbols=["BTCUSDT"], month_str="2026-05")

    storage.download_parquet.assert_called_once_with(
        "crypto-processed",
        "ticker_24h/BTCUSDT/2026-05.parquet",
    )
    insert.assert_called_once()
    call_args = insert.call_args
    assert call_args[0][0] == "ticker_24h"
    df = call_args[0][1]
    assert "price_change" in df.columns
    assert "snapshot_time" in df.columns
    assert "spread_pct" in df.columns


def test_load_order_book_inserts_transformed_parquet(monkeypatch):
    """load_order_book delegates to _load_table with correct prefix and columns."""
    transformed_df = pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "timestamp": [pd.Timestamp("2026-05-15 08:45:00")],
        "best_bid": [42290.0],
        "best_ask": [42310.0],
        "mid_price": [42300.0],
        "spread_pct": [0.0473],
        "depth_bid_volume": [3.8],
        "depth_ask_volume": [4.3],
        "obi": [-0.0617],
        "bid_ask_ratio": [0.8837],
        "nearest_bid_wall_price": [None],
        "nearest_bid_wall_volume": [None],
        "nearest_ask_wall_price": [None],
        "nearest_ask_wall_volume": [None],
    })

    storage = MagicMock()
    storage.download_parquet.return_value = _mock_parquet_table(transformed_df)
    monkeypatch.setattr(load_module, "storage", storage)
    monkeypatch.setattr(load_module, "get_table_watermarks", lambda table, ts_col, symbols: {})
    monkeypatch.setattr(
        load_module, "discover_month_partitions",
        lambda bucket, prefix, symbol: ["2026-05"],
    )
    insert = MagicMock(return_value=1)
    monkeypatch.setattr(load_module, "ch_insert_df", insert)

    load_module.load_order_book(symbols=["BTCUSDT"], month_str="2026-05")

    storage.download_parquet.assert_called_once_with(
        "crypto-processed",
        "order_book_snapshot/BTCUSDT/2026-05.parquet",
    )
    insert.assert_called_once()
    call_args = insert.call_args
    assert call_args[0][0] == "order_book_snapshot"
    df = call_args[0][1]
    assert "obi" in df.columns
    assert "depth_bid_volume" in df.columns
    assert "depth_ask_volume" in df.columns
    assert "spread_pct" in df.columns
