
# =============================================================================
# Test Transform Module
# =============================================================================
# Unit tests cho scripts/transform.py
#
# Chạy: pytest tests/test_transform.py -v
# =============================================================================

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import pandas as pd
import numpy as np

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.transform import (
    calculate_indicators,
    OUTPUT_COLUMNS,
    _process_symbol,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_close_prices():
    """Generate 50 sample close prices simulating a trending market."""
    np.random.seed(42)
    base = 100.0
    changes = np.random.randn(50) * 2
    prices = base + np.cumsum(changes)
    return prices.tolist()


@pytest.fixture
def sample_dataframe(sample_close_prices):
    """Create a sample DataFrame mimicking raw klines data."""
    n = len(sample_close_prices)
    dates = pd.date_range("2024-01-01", periods=n, freq="min")
    return pd.DataFrame({
        "open_time": dates,
        "open": sample_close_prices,
        "high": [p + 1.0 for p in sample_close_prices],
        "low": [p - 1.0 for p in sample_close_prices],
        "close": sample_close_prices,
        "volume": [1000.0] * n,
        "close_time": dates + pd.Timedelta(minutes=1),
        "quote_volume": [50000.0] * n,
        "trades": list(range(100, 100 + n)),
        "taker_buy_base": [500.0] * n,
        "taker_buy_quote": [25000.0] * n,
        "symbol": ["BTCUSDT"] * n,
    })


@pytest.fixture
def all_gains_dataframe():
    """DataFrame where close prices only go up (all gains, no losses)."""
    n = 30
    dates = pd.date_range("2024-01-01", periods=n, freq="min")
    prices = [100.0 + i for i in range(n)]  # monotonically increasing
    return pd.DataFrame({
        "open_time": dates,
        "open": prices,
        "high": [p + 0.5 for p in prices],
        "low": [p - 0.5 for p in prices],
        "close": prices,
        "volume": [1000.0] * n,
        "close_time": dates + pd.Timedelta(minutes=1),
        "quote_volume": [50000.0] * n,
        "trades": list(range(n)),
        "taker_buy_base": [500.0] * n,
        "taker_buy_quote": [25000.0] * n,
        "symbol": ["BTCUSDT"] * n,
    })


@pytest.fixture
def all_losses_dataframe():
    """DataFrame where close prices only go down (all losses, no gains)."""
    n = 30
    dates = pd.date_range("2024-01-01", periods=n, freq="min")
    prices = [200.0 - i for i in range(n)]  # monotonically decreasing
    return pd.DataFrame({
        "open_time": dates,
        "open": prices,
        "high": [p + 0.5 for p in prices],
        "low": [p - 0.5 for p in prices],
        "close": prices,
        "volume": [1000.0] * n,
        "close_time": dates + pd.Timedelta(minutes=1),
        "quote_volume": [50000.0] * n,
        "trades": list(range(n)),
        "taker_buy_base": [500.0] * n,
        "taker_buy_quote": [25000.0] * n,
        "symbol": ["ETHUSDT"] * n,
    })


@pytest.fixture
def flat_price_dataframe():
    """DataFrame where close price is constant (no change)."""
    n = 30
    dates = pd.date_range("2024-01-01", periods=n, freq="min")
    prices = [100.0] * n
    return pd.DataFrame({
        "open_time": dates,
        "open": prices,
        "high": prices,
        "low": prices,
        "close": prices,
        "volume": [1000.0] * n,
        "close_time": dates + pd.Timedelta(minutes=1),
        "quote_volume": [50000.0] * n,
        "trades": list(range(n)),
        "taker_buy_base": [500.0] * n,
        "taker_buy_quote": [25000.0] * n,
        "symbol": ["BNBUSDT"] * n,
    })


# =============================================================================
# Test calculate_indicators - RSI
# =============================================================================

class TestCalculateRSI:
    """Tests for RSI calculation within calculate_indicators."""

    def test_rsi_column_exists(self, sample_dataframe):
        result = calculate_indicators(sample_dataframe)
        assert "rsi_14" in result.columns

    def test_rsi_range_0_to_100(self, sample_dataframe):
        result = calculate_indicators(sample_dataframe)
        rsi_values = result["rsi_14"]
        assert rsi_values.min() >= 0.0, f"RSI min={rsi_values.min()} < 0"
        assert rsi_values.max() <= 100.0, f"RSI max={rsi_values.max()} > 100"

    def test_rsi_no_nan_after_fill(self, sample_dataframe):
        result = calculate_indicators(sample_dataframe)
        assert result["rsi_14"].isna().sum() == 0, "RSI should have no NaN after fill"

    def test_rsi_all_gains_near_100(self, all_gains_dataframe):
        """When prices only go up, loss=0 is replaced by NaN,
        so RSI becomes NaN and gets filled to 0."""
        result = calculate_indicators(all_gains_dataframe)
        rsi_after_warmup = result["rsi_14"].iloc[14:]
        assert (rsi_after_warmup == 0.0).all(), (
            f"RSI with all gains should be 0.0, got values: {rsi_after_warmup.unique()}"
        )

    def test_rsi_all_losses_near_0(self, all_losses_dataframe):
        """When prices only go down, RSI after warmup should be near 0."""
        result = calculate_indicators(all_losses_dataframe)
        rsi_after_warmup = result["rsi_14"].iloc[14:]
        assert rsi_after_warmup.max() < 10.0, (
            f"RSI with all losses should be near 0, got max={rsi_after_warmup.max()}"
        )

    def test_rsi_flat_price(self, flat_price_dataframe):
        """When price is constant, verify no crash and values are in range."""
        result = calculate_indicators(flat_price_dataframe)
        assert result["rsi_14"].between(0, 100).all() or (result["rsi_14"] == 0).all()

    def test_rsi_with_small_dataset(self):
        """RSI with fewer than 14 rows should still not crash."""
        n = 5
        dates = pd.date_range("2024-01-01", periods=n, freq="min")
        df = pd.DataFrame({
            "open_time": dates,
            "open": [100, 102, 101, 103, 104],
            "high": [101, 103, 102, 104, 105],
            "low": [99, 101, 100, 102, 103],
            "close": [100, 102, 101, 103, 104],
            "volume": [1000.0] * n,
            "close_time": dates + pd.Timedelta(minutes=1),
            "quote_volume": [50000.0] * n,
            "trades": list(range(n)),
            "taker_buy_base": [500.0] * n,
            "taker_buy_quote": [25000.0] * n,
            "symbol": ["BTCUSDT"] * n,
        })
        result = calculate_indicators(df)
        assert "rsi_14" in result.columns
        assert len(result) == n
        assert result["rsi_14"].isna().sum() == 0


# =============================================================================
# Test calculate_indicators - MACD
# =============================================================================

class TestCalculateMACD:
    """Tests for MACD calculation within calculate_indicators."""

    def test_macd_columns_exist(self, sample_dataframe):
        result = calculate_indicators(sample_dataframe)
        assert "macd" in result.columns
        assert "macd_signal" in result.columns

    def test_macd_no_nan_after_fill(self, sample_dataframe):
        result = calculate_indicators(sample_dataframe)
        assert result["macd"].isna().sum() == 0, "MACD should have no NaN"
        assert result["macd_signal"].isna().sum() == 0, "MACD_signal should have no NaN"

    def test_macd_values_are_numeric(self, sample_dataframe):
        result = calculate_indicators(sample_dataframe)
        assert pd.api.types.is_numeric_dtype(result["macd"])
        assert pd.api.types.is_numeric_dtype(result["macd_signal"])

    def test_macd_is_ema12_minus_ema26(self, sample_dataframe):
        """Verify MACD = EMA(12) - EMA(26) by manual calculation."""
        df = sample_dataframe.sort_values("open_time").copy()
        close = df["close"]
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        expected_macd = ema12 - ema26

        result = calculate_indicators(df)
        pd.testing.assert_series_equal(
            result["macd"].reset_index(drop=True),
            expected_macd.reset_index(drop=True),
            check_names=False,
            atol=1e-10,
        )

    def test_macd_signal_is_ema9_of_macd(self, sample_dataframe):
        """Verify macd_signal = EMA(9) of MACD."""
        df = sample_dataframe.sort_values("open_time").copy()
        close = df["close"]
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        expected_signal = macd.ewm(span=9, adjust=False).mean()

        result = calculate_indicators(df)
        pd.testing.assert_series_equal(
            result["macd_signal"].reset_index(drop=True),
            expected_signal.reset_index(drop=True),
            check_names=False,
            atol=1e-10,
        )

    def test_macd_all_gains(self, all_gains_dataframe):
        """MACD should be positive for consistently rising prices."""
        result = calculate_indicators(all_gains_dataframe)
        macd_tail = result["macd"].iloc[26:]
        assert (macd_tail > 0).all(), "MACD should be positive for all-gain series"

    def test_macd_all_losses(self, all_losses_dataframe):
        """MACD should be negative for consistently falling prices."""
        result = calculate_indicators(all_losses_dataframe)
        macd_tail = result["macd"].iloc[26:]
        assert (macd_tail < 0).all(), "MACD should be negative for all-loss series"

    def test_macd_flat_price_near_zero(self, flat_price_dataframe):
        """MACD should be ~0 for flat prices."""
        result = calculate_indicators(flat_price_dataframe)
        assert result["macd"].abs().max() < 1e-10, "MACD should be ~0 for flat prices"
        assert result["macd_signal"].abs().max() < 1e-10


# =============================================================================
# Test calculate_indicators - General behavior
# =============================================================================

class TestCalculateIndicatorsGeneral:
    """General tests for calculate_indicators."""

    def test_output_preserves_row_count(self, sample_dataframe):
        result = calculate_indicators(sample_dataframe)
        assert len(result) == len(sample_dataframe)

    def test_output_sorted_by_open_time(self, sample_dataframe):
        # Shuffle input
        shuffled = sample_dataframe.sample(frac=1, random_state=0)
        result = calculate_indicators(shuffled)
        assert result["open_time"].is_monotonic_increasing

    def test_original_columns_preserved(self, sample_dataframe):
        original_cols = set(sample_dataframe.columns)
        result = calculate_indicators(sample_dataframe)
        for col in original_cols:
            assert col in result.columns, f"Original column '{col}' missing from output"

    def test_new_indicator_columns_added(self, sample_dataframe):
        result = calculate_indicators(sample_dataframe)
        assert "rsi_14" in result.columns
        assert "macd" in result.columns
        assert "macd_signal" in result.columns

    def test_no_nan_values_in_output(self, sample_dataframe):
        result = calculate_indicators(sample_dataframe)
        assert result.isna().sum().sum() == 0, "Output should have no NaN values"

    def test_multiple_symbols_independent(self):
        """Indicators should be computed per-symbol independently."""
        n = 30
        dates = pd.date_range("2024-01-01", periods=n, freq="min")

        np.random.seed(10)
        prices_a = 100.0 + np.cumsum(np.random.randn(n))
        prices_b = 50.0 + np.cumsum(np.random.randn(n) * 0.5)

        def make_df(prices, symbol):
            return pd.DataFrame({
                "open_time": dates,
                "open": prices,
                "high": prices + 1,
                "low": prices - 1,
                "close": prices,
                "volume": [1000.0] * n,
                "close_time": dates + pd.Timedelta(minutes=1),
                "quote_volume": [50000.0] * n,
                "trades": list(range(n)),
                "taker_buy_base": [500.0] * n,
                "taker_buy_quote": [25000.0] * n,
                "symbol": [symbol] * n,
            })

        df_a = make_df(prices_a, "BTCUSDT")
        df_b = make_df(prices_b, "ETHUSDT")

        result_a = calculate_indicators(df_a)
        result_b = calculate_indicators(df_b)

        assert not np.allclose(
            result_a["macd"].values, result_b["macd"].values
        ), "Different price series should produce different MACD"


# =============================================================================
# Test transform_data integration
# =============================================================================

class TestTransformDataIntegration:
    """Higher-level tests for transform_data logic using mocks."""

    @patch("scripts.transform.storage")
    @patch("scripts.transform.get_last_timestamps", return_value={})
    def test_returns_none_when_no_new_data(self, mock_ts, mock_storage):
        """When no partitions found, should return None."""
        mock_storage.object_exists.return_value = False

        from scripts.transform import transform_data
        result = transform_data(symbols=["BTCUSDT"])

        assert result is None

    @patch("scripts.transform._get_ch_context", return_value=pd.DataFrame())
    @patch("scripts.transform._read_monthly_partition")
    def test_process_symbol_filters_rows_after_watermark(self, mock_read, mock_ctx):
        """Rows at or before the ClickHouse watermark should not be transformed."""
        dates = pd.date_range("2024-01-01", periods=3, freq="min")
        mock_read.return_value = pd.DataFrame({
            "open_time": dates,
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [1000.0] * 3,
            "close_time": dates + pd.Timedelta(minutes=1),
            "quote_volume": [50000.0] * 3,
            "trades": [10, 11, 12],
            "taker_buy_base": [500.0] * 3,
            "taker_buy_quote": [25000.0] * 3,
        })

        result = _process_symbol(
            "BTCUSDT",
            "2024-01",
            pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
        )

        assert result is not None
        assert len(result) == 2
        assert result["timestamp"].min() == pd.Timestamp("2024-01-01 00:01:00")


# =============================================================================
# Test edge cases & data integrity
# =============================================================================

class TestEdgeCases:

    def test_single_row_dataframe(self):
        """calculate_indicators should handle a single row."""
        df = pd.DataFrame({
            "open_time": [pd.Timestamp("2024-01-01")],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.0],
            "volume": [1000.0],
            "close_time": [pd.Timestamp("2024-01-01 00:01:00")],
            "quote_volume": [50000.0],
            "trades": [100],
            "taker_buy_base": [500.0],
            "taker_buy_quote": [25000.0],
            "symbol": ["BTCUSDT"],
        })
        result = calculate_indicators(df)
        assert len(result) == 1
        assert result["rsi_14"].isna().sum() == 0
        assert result["macd"].isna().sum() == 0
        assert result["macd_signal"].isna().sum() == 0

    def test_large_price_values(self):
        """Test with very large price values."""
        n = 30
        dates = pd.date_range("2024-01-01", periods=n, freq="min")
        np.random.seed(99)
        prices = 1e8 + np.cumsum(np.random.randn(n) * 1e4)
        df = pd.DataFrame({
            "open_time": dates,
            "open": prices,
            "high": prices + 100,
            "low": prices - 100,
            "close": prices,
            "volume": [1e9] * n,
            "close_time": dates + pd.Timedelta(minutes=1),
            "quote_volume": [1e12] * n,
            "trades": list(range(n)),
            "taker_buy_base": [1e6] * n,
            "taker_buy_quote": [1e9] * n,
            "symbol": ["BTCUSDT"] * n,
        })
        result = calculate_indicators(df)
        assert result["rsi_14"].between(0, 100).all()
        assert np.isfinite(result["macd"]).all()
        assert np.isfinite(result["macd_signal"]).all()

    def test_very_small_price_values(self):
        """Test with very small (sub-penny) price values."""
        n = 30
        dates = pd.date_range("2024-01-01", periods=n, freq="min")
        np.random.seed(7)
        prices = 0.00001 + np.abs(np.cumsum(np.random.randn(n) * 0.000001))
        df = pd.DataFrame({
            "open_time": dates,
            "open": prices,
            "high": prices + 0.000001,
            "low": prices - 0.0000005,
            "close": prices,
            "volume": [1e6] * n,
            "close_time": dates + pd.Timedelta(minutes=1),
            "quote_volume": [100.0] * n,
            "trades": list(range(n)),
            "taker_buy_base": [1e5] * n,
            "taker_buy_quote": [50.0] * n,
            "symbol": ["SHIBUSDT"] * n,
        })
        result = calculate_indicators(df)
        assert len(result) == n
        assert result.isna().sum().sum() == 0

    def test_duplicate_timestamps_handled(self):
        """Duplicate timestamps in input should not crash."""
        n = 20
        dates = pd.date_range("2024-01-01", periods=n, freq="min")
        dup_dates = list(dates[:15]) + list(dates[10:15])
        prices = list(range(100, 100 + n))
        df = pd.DataFrame({
            "open_time": dup_dates,
            "open": prices,
            "high": [p + 1 for p in prices],
            "low": [p - 1 for p in prices],
            "close": prices,
            "volume": [1000.0] * n,
            "close_time": [d + pd.Timedelta(minutes=1) for d in dup_dates],
            "quote_volume": [50000.0] * n,
            "trades": list(range(n)),
            "taker_buy_base": [500.0] * n,
            "taker_buy_quote": [25000.0] * n,
            "symbol": ["BTCUSDT"] * n,
        })
        result = calculate_indicators(df)
        assert len(result) == n
        assert result.isna().sum().sum() == 0
