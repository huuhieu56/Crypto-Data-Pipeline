from unittest.mock import MagicMock

import pandas as pd
import pytest

from scripts import transform as transform_module


def test_compute_rsi_returns_series():
    import pandas as pd
    close = pd.Series([100, 102, 101, 105, 103, 107, 106, 110, 108, 112,
                       111, 115, 113, 117, 116, 120.0])
    rsi = transform_module._compute_rsi(close, period=14)
    assert len(rsi) == len(close)
    assert rsi.iloc[0] == 0.0  # first value is NaN→0
    assert 0 <= rsi.iloc[-1] <= 100


def test_compute_macd_returns_three_series():
    import pandas as pd
    close = pd.Series(range(100, 150), dtype=float)
    macd_line, signal, hist = transform_module._compute_macd(close)
    assert len(macd_line) == len(close)
    assert len(signal) == len(close)
    assert len(hist) == len(close)


def test_transform_klines_computes_indicators_and_writes_processed(monkeypatch):
    """transform_klines reads raw CSV, computes RSI/MACD, writes Parquet to processed."""
    import pandas as pd

    raw_csv = (
        "open_time,open,high,low,close,volume,close_time,quote_volume,trade_count,"
        "taker_buy_base,taker_buy_quote\n"
    )
    base_ts = 1715731200000  # 2024-05-15 08:00:00 UTC
    for i in range(20):
        ts = base_ts + i * 60000
        raw_csv += f"{ts},100,105,99,{100 + i},10,{ts + 59999},{(100 + i) * 10},50,5,50\n"

    mock_storage = MagicMock()
    resp = MagicMock()
    resp.read.return_value = raw_csv.encode()
    mock_storage.client.get_object.return_value = resp
    monkeypatch.setattr(transform_module, "storage", mock_storage)
    monkeypatch.setattr(
        transform_module, "_load_context_from_processed",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        transform_module, "discover_month_partitions",
        lambda bucket, prefix, symbol, extension: ["2024-05"],
    )
    mock_append = MagicMock()
    monkeypatch.setattr(transform_module, "append_to_partition", mock_append)

    transform_module.transform_klines(symbols=["BTCUSDT"], month_str="2024-05")

    mock_append.assert_called_once()
    call_args = mock_append.call_args
    assert call_args[0][0] == "crypto-processed"
    assert call_args[0][1] == "klines"
    assert call_args[0][2] == "BTCUSDT"
    df = call_args[0][3]
    assert "rsi_14" in df.columns
    assert "macd" in df.columns
    assert "macd_signal" in df.columns
    assert "open_time" in df.columns
    assert "trade_count" in df.columns
    assert len(df) == 20


def test_transform_klines_skips_empty_csv(monkeypatch):
    """transform_klines skips empty CSV partitions."""
    import pandas as pd

    mock_storage = MagicMock()
    resp = MagicMock()
    resp.read.return_value = b"open_time,open,high,low,close,volume,close_time,quote_volume,trade_count,taker_buy_base,taker_buy_quote\n"
    mock_storage.client.get_object.return_value = resp
    monkeypatch.setattr(transform_module, "storage", mock_storage)
    monkeypatch.setattr(transform_module, "_load_context_from_processed", lambda *a, **k: None)
    monkeypatch.setattr(
        transform_module, "discover_month_partitions",
        lambda bucket, prefix, symbol, extension: ["2024-05"],
    )
    mock_append = MagicMock()
    monkeypatch.setattr(transform_module, "append_to_partition", mock_append)

    transform_module.transform_klines(symbols=["BTCUSDT"], month_str="2024-05")

    mock_append.assert_not_called()


# --- transform_ticker (ETL) ---------------------------------------------------


def test_transform_ticker_renames_computes_spread(monkeypatch):
    """transform_ticker renames columns, computes spread_pct."""
    ticker_raw = pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "priceChange": ["1500.00"],
        "priceChangePercent": ["3.50"],
        "highPrice": ["44000.00"],
        "lowPrice": ["41000.00"],
        "volume": ["25000.50"],
        "quoteVolume": ["1050000000.00"],
        "count": [1200000],
        "bidPrice": ["42290.00"],
        "askPrice": ["42310.00"],
    })

    mock_storage = MagicMock()
    mock_storage.object_exists.return_value = True
    mock_table = MagicMock()
    mock_table.to_pandas.return_value = ticker_raw.copy()
    mock_storage.download_parquet.return_value = mock_table
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
    assert call_args[0][0] == "crypto-processed"  # bucket
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
    assert call_args[0][0] == "crypto-processed"     # bucket
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
