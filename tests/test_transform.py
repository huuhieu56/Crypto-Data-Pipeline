from unittest.mock import MagicMock

import pandas as pd
import pytest

from scripts import transform as transform_module


def test_get_indicator_watermarks_ignores_raw_zero_rows(monkeypatch):
    client = MagicMock()
    client.query.return_value.result_rows = [
        ("BTCUSDT", 1715731200000),
        ("ETHUSDT", 0),
    ]
    monkeypatch.setattr(transform_module, "get_ch_client", lambda: client)

    result = transform_module._get_indicator_watermarks(["BTCUSDT", "ETHUSDT"])

    assert result == {"BTCUSDT": 1715731200000}
    sql = client.query.call_args[0][0]
    assert "maxIf" in sql
    assert "isNotNull(rsi_14)" in sql
    assert "ifNull(rsi_14, 0) != 0" in sql


# --- transform_ticker (ETL) ---------------------------------------------------


def test_transform_ticker_merges_renames_computes_spread(monkeypatch):
    """transform_ticker merges raw ticker + book_ticker, renames, computes spread_pct."""
    ticker_raw = pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "priceChange": ["1500.00"],
        "priceChangePercent": ["3.50"],
        "highPrice": ["44000.00"],
        "lowPrice": ["41000.00"],
        "volume": ["25000.50"],
        "quoteVolume": ["1050000000.00"],
        "count": [1200000],
    })
    book_raw = pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "bidPrice": ["42290.00"],
        "askPrice": ["42310.00"],
    })

    mock_storage = MagicMock()
    mock_storage.object_exists.return_value = True

    def _download(bucket, key):
        mock_table = MagicMock()
        if key.startswith("ticker_raw/"):
            mock_table.to_pandas.return_value = ticker_raw.copy()
        else:
            mock_table.to_pandas.return_value = book_raw.copy()
        return mock_table

    mock_storage.download_parquet.side_effect = _download
    monkeypatch.setattr(transform_module, "storage", mock_storage)
    monkeypatch.setattr(
        transform_module, "discover_month_partitions",
        lambda bucket, prefix, symbol: ["2026-05"],
    )
    mock_append = MagicMock()
    monkeypatch.setattr(transform_module, "append_to_partition", mock_append)

    transform_module.transform_ticker(symbols=["BTCUSDT"], month_str="2026-05")

    mock_append.assert_called_once()
    call_args = mock_append.call_args
    assert call_args[0][1] == "ticker_24h"  # prefix
    assert call_args[0][2] == "BTCUSDT"     # symbol
    df = call_args[0][3]
    assert "price_change" in df.columns
    assert "bid_price" in df.columns
    assert "ask_price" in df.columns
    assert "snapshot_time" in df.columns
    assert "spread_pct" in df.columns
    assert "priceChange" not in df.columns
    # spread_pct = (42310 - 42290) / 42310 * 100 ≈ 0.0473
    assert df["spread_pct"].iloc[0] == pytest.approx(0.04727, rel=1e-3)


def test_transform_ticker_skips_empty_partitions(monkeypatch):
    """transform_ticker skips empty partitions without error."""
    mock_storage = MagicMock()
    mock_storage.object_exists.return_value = True
    empty_table = MagicMock()
    empty_table.to_pandas.return_value = pd.DataFrame()
    mock_storage.download_parquet.return_value = empty_table
    monkeypatch.setattr(transform_module, "storage", mock_storage)
    monkeypatch.setattr(
        transform_module, "discover_month_partitions",
        lambda bucket, prefix, symbol: ["2026-05"],
    )
    mock_append = MagicMock()
    monkeypatch.setattr(transform_module, "append_to_partition", mock_append)

    transform_module.transform_ticker(symbols=["BTCUSDT"], month_str="2026-05")

    mock_append.assert_not_called()


def test_transform_ticker_empty_symbols_does_nothing(monkeypatch):
    """transform_ticker with empty symbols list does nothing."""
    mock_append = MagicMock()
    monkeypatch.setattr(transform_module, "append_to_partition", mock_append)

    transform_module.transform_ticker(symbols=[], month_str="2026-05")

    mock_append.assert_not_called()


# --- transform_order_book (ETL) -----------------------------------------------


def test_transform_order_book_computes_volumes_and_imbalance(monkeypatch):
    """transform_order_book computes total_bid_volume, total_ask_volume, imbalance."""
    raw_df = pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "timestamp": [pd.Timestamp("2026-05-15 08:45:00")],
        "bids": [[["42290.00", "1.5"], ["42285.00", "2.3"]]],
        "asks": [[["42310.00", "1.2"], ["42315.00", "3.1"]]],
    })

    mock_storage = MagicMock()
    mock_storage.object_exists.return_value = True
    mock_table = MagicMock()
    mock_table.to_pandas.return_value = raw_df.copy()
    mock_storage.download_parquet.return_value = mock_table
    monkeypatch.setattr(transform_module, "storage", mock_storage)
    monkeypatch.setattr(
        transform_module, "discover_month_partitions",
        lambda bucket, prefix, symbol: ["2026-05"],
    )
    mock_append = MagicMock()
    monkeypatch.setattr(transform_module, "append_to_partition", mock_append)

    transform_module.transform_order_book(symbols=["BTCUSDT"], month_str="2026-05")

    mock_append.assert_called_once()
    call_args = mock_append.call_args
    assert call_args[0][1] == "order_book_snapshot"  # prefix
    assert call_args[0][2] == "BTCUSDT"              # symbol
    df = call_args[0][3]
    assert "total_bid_volume" in df.columns
    assert "total_ask_volume" in df.columns
    assert "imbalance" in df.columns
    assert "bids" not in df.columns
    assert "asks" not in df.columns
    assert df["total_bid_volume"].iloc[0] == pytest.approx(3.8)
    assert df["total_ask_volume"].iloc[0] == pytest.approx(4.3)


def test_transform_order_book_skips_missing_partition(monkeypatch):
    """transform_order_book skips when raw partition doesn't exist."""
    mock_storage = MagicMock()
    mock_storage.object_exists.return_value = False
    monkeypatch.setattr(transform_module, "storage", mock_storage)
    monkeypatch.setattr(
        transform_module, "discover_month_partitions",
        lambda bucket, prefix, symbol: ["2026-05"],
    )
    mock_append = MagicMock()
    monkeypatch.setattr(transform_module, "append_to_partition", mock_append)

    transform_module.transform_order_book(symbols=["BTCUSDT"], month_str="2026-05")

    mock_append.assert_not_called()
