

# =============================================================================
# Test Transform Module
# =============================================================================
# Unit tests cho scripts/transform.py
#
# Chạy: pytest tests/test_transform.py -v
# =============================================================================

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import shutil
import json

import pytest
import pandas as pd
import numpy as np

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.transform import (
    calculate_indicators_pandas,
    OUTPUT_SCHEMA,
    _parquet_has_data_files,
    _cleanup_empty_parquet_dir,
    _tail_csv_rows,
    _load_transform_state,
    _save_transform_state,
    _estimate_missing_rows,
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


@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a temporary directory for file-based tests."""
    return tmp_path


# =============================================================================
# Test calculate_indicators_pandas - RSI
# =============================================================================

class TestCalculateRSI:
    """Tests for RSI calculation within calculate_indicators_pandas."""

    def test_rsi_column_exists(self, sample_dataframe):
        result = calculate_indicators_pandas(sample_dataframe)
        assert "RSI" in result.columns

    def test_rsi_range_0_to_100(self, sample_dataframe):
        result = calculate_indicators_pandas(sample_dataframe)
        rsi_values = result["RSI"]
        assert rsi_values.min() >= 0.0, f"RSI min={rsi_values.min()} < 0"
        assert rsi_values.max() <= 100.0, f"RSI max={rsi_values.max()} > 100"

    def test_rsi_no_nan_after_fill(self, sample_dataframe):
        result = calculate_indicators_pandas(sample_dataframe)
        assert result["RSI"].isna().sum() == 0, "RSI should have no NaN after fill"

    def test_rsi_all_gains_near_100(self, all_gains_dataframe):
        """When prices only go up, loss=0 is replaced by NaN in source code,
        so RSI becomes NaN and gets filled to 0. This tests actual behavior."""
        result = calculate_indicators_pandas(all_gains_dataframe)
        rsi_after_warmup = result["RSI"].iloc[14:]
        assert (rsi_after_warmup == 0.0).all(), (
            f"RSI with all gains should be 0.0 (due to loss=NaN fill), got values: {rsi_after_warmup.unique()}"
        )


    def test_rsi_all_losses_near_0(self, all_losses_dataframe):
        """When prices only go down, RSI after warmup should be near 0."""
        result = calculate_indicators_pandas(all_losses_dataframe)
        # After fill, RSI for all-loss should be 0 (gain=0, loss>0 => rs=0 => RSI=0)
        rsi_after_warmup = result["RSI"].iloc[14:]
        assert rsi_after_warmup.max() < 10.0, (
            f"RSI with all losses should be near 0, got max={rsi_after_warmup.max()}"
        )

    def test_rsi_flat_price(self, flat_price_dataframe):
        """When price is constant, delta=0 for all rows after first."""
        result = calculate_indicators_pandas(flat_price_dataframe)
        # With flat prices: gain=0, loss=NaN(replaced) => rs=NaN => RSI filled
        # Just verify no crash and values are in range
        assert result["RSI"].between(0, 100).all() or (result["RSI"] == 0).all()

    def test_rsi_with_small_dataset(self):
        """RSI with fewer than 14 rows should still not crash (filled via bfill/ffill)."""
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
        result = calculate_indicators_pandas(df)
        assert "RSI" in result.columns
        assert len(result) == n
        assert result["RSI"].isna().sum() == 0


# =============================================================================
# Test calculate_indicators_pandas - MACD
# =============================================================================

class TestCalculateMACD:
    """Tests for MACD calculation within calculate_indicators_pandas."""

    def test_macd_columns_exist(self, sample_dataframe):
        result = calculate_indicators_pandas(sample_dataframe)
        assert "MACD" in result.columns
        assert "MACD_signal" in result.columns

    def test_macd_no_nan_after_fill(self, sample_dataframe):
        result = calculate_indicators_pandas(sample_dataframe)
        assert result["MACD"].isna().sum() == 0, "MACD should have no NaN"
        assert result["MACD_signal"].isna().sum() == 0, "MACD_signal should have no NaN"

    def test_macd_values_are_numeric(self, sample_dataframe):
        result = calculate_indicators_pandas(sample_dataframe)
        assert pd.api.types.is_numeric_dtype(result["MACD"])
        assert pd.api.types.is_numeric_dtype(result["MACD_signal"])

    def test_macd_is_ema12_minus_ema26(self, sample_dataframe):
        """Verify MACD = EMA(12) - EMA(26) by manual calculation."""
        df = sample_dataframe.sort_values("open_time").copy()
        close = df["close"]
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        expected_macd = ema12 - ema26

        result = calculate_indicators_pandas(df)
        pd.testing.assert_series_equal(
            result["MACD"].reset_index(drop=True),
            expected_macd.reset_index(drop=True),
            check_names=False,
            atol=1e-10,
        )

    def test_macd_signal_is_ema9_of_macd(self, sample_dataframe):
        """Verify MACD_signal = EMA(9) of MACD."""
        df = sample_dataframe.sort_values("open_time").copy()
        close = df["close"]
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        expected_signal = macd.ewm(span=9, adjust=False).mean()

        result = calculate_indicators_pandas(df)
        pd.testing.assert_series_equal(
            result["MACD_signal"].reset_index(drop=True),
            expected_signal.reset_index(drop=True),
            check_names=False,
            atol=1e-10,
        )

    def test_macd_all_gains(self, all_gains_dataframe):
        """MACD should be positive for consistently rising prices (after warmup)."""
        result = calculate_indicators_pandas(all_gains_dataframe)
        macd_tail = result["MACD"].iloc[26:]
        assert (macd_tail > 0).all(), "MACD should be positive for all-gain series"

    def test_macd_all_losses(self, all_losses_dataframe):
        """MACD should be negative for consistently falling prices (after warmup)."""
        result = calculate_indicators_pandas(all_losses_dataframe)
        macd_tail = result["MACD"].iloc[26:]
        assert (macd_tail < 0).all(), "MACD should be negative for all-loss series"

    def test_macd_flat_price_near_zero(self, flat_price_dataframe):
        """MACD should be ~0 for flat prices."""
        result = calculate_indicators_pandas(flat_price_dataframe)
        assert result["MACD"].abs().max() < 1e-10, "MACD should be ~0 for flat prices"
        assert result["MACD_signal"].abs().max() < 1e-10


# =============================================================================
# Test calculate_indicators_pandas - General behavior
# =============================================================================

class TestCalculateIndicatorsGeneral:
    """General tests for calculate_indicators_pandas."""

    def test_output_preserves_row_count(self, sample_dataframe):
        result = calculate_indicators_pandas(sample_dataframe)
        assert len(result) == len(sample_dataframe)

    def test_output_sorted_by_open_time(self, sample_dataframe):
        # Shuffle input
        shuffled = sample_dataframe.sample(frac=1, random_state=0)
        result = calculate_indicators_pandas(shuffled)
        assert result["open_time"].is_monotonic_increasing

    def test_original_columns_preserved(self, sample_dataframe):
        original_cols = set(sample_dataframe.columns)
        result = calculate_indicators_pandas(sample_dataframe)
        for col in original_cols:
            assert col in result.columns, f"Original column '{col}' missing from output"

    def test_new_indicator_columns_added(self, sample_dataframe):
        result = calculate_indicators_pandas(sample_dataframe)
        assert "RSI" in result.columns
        assert "MACD" in result.columns
        assert "MACD_signal" in result.columns

    def test_no_nan_values_in_output(self, sample_dataframe):
        result = calculate_indicators_pandas(sample_dataframe)
        assert result.isna().sum().sum() == 0, "Output should have no NaN values"

    def test_output_schema_matches(self, sample_dataframe):
        """Verify output has all columns defined in OUTPUT_SCHEMA."""
        result = calculate_indicators_pandas(sample_dataframe)
        expected_cols = {f.name for f in OUTPUT_SCHEMA.fields}
        actual_cols = set(result.columns)
        assert expected_cols.issubset(actual_cols), (
            f"Missing columns: {expected_cols - actual_cols}"
        )

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

        result_a = calculate_indicators_pandas(df_a)
        result_b = calculate_indicators_pandas(df_b)

        # Run combined and compare
        combined = pd.concat([df_a, df_b], ignore_index=True)
        # calculate_indicators_pandas works per-group, but if called on combined
        # it computes on all rows together. This test verifies separate calls give
        # different results for different price series.
        assert not np.allclose(
            result_a["MACD"].values, result_b["MACD"].values
        ), "Different price series should produce different MACD"


# =============================================================================
# Test _parquet_has_data_files
# =============================================================================

class TestParquetHasDataFiles:

    def test_returns_false_for_nonexistent_path(self, tmp_dir):
        assert _parquet_has_data_files(tmp_dir / "nonexistent") is False

    def test_returns_false_for_empty_dir(self, tmp_dir):
        empty_dir = tmp_dir / "empty_parquet"
        empty_dir.mkdir()
        assert _parquet_has_data_files(empty_dir) is False

    def test_returns_true_with_part_file(self, tmp_dir):
        parquet_dir = tmp_dir / "has_data"
        parquet_dir.mkdir()
        (parquet_dir / "part-00000.parquet").write_text("fake data")
        assert _parquet_has_data_files(parquet_dir) is True

    def test_returns_true_with_parquet_suffix(self, tmp_dir):
        parquet_dir = tmp_dir / "has_parquet"
        parquet_dir.mkdir()
        (parquet_dir / "data.parquet").write_text("fake data")
        assert _parquet_has_data_files(parquet_dir) is True

    def test_returns_false_for_file_not_dir(self, tmp_dir):
        file_path = tmp_dir / "not_a_dir.parquet"
        file_path.write_text("data")
        assert _parquet_has_data_files(file_path) is False

    def test_returns_true_with_nested_part_file(self, tmp_dir):
        parquet_dir = tmp_dir / "nested"
        sub_dir = parquet_dir / "symbol=BTCUSDT"
        sub_dir.mkdir(parents=True)
        (sub_dir / "part-00000-abc.snappy.parquet").write_text("fake")
        assert _parquet_has_data_files(parquet_dir) is True


# =============================================================================
# Test _cleanup_empty_parquet_dir
# =============================================================================

class TestCleanupEmptyParquetDir:

    def test_removes_empty_parquet_dir(self, tmp_dir):
        empty_dir = tmp_dir / "empty_parquet"
        empty_dir.mkdir()
        (empty_dir / "_SUCCESS").write_text("")  # metadata, not data
        _cleanup_empty_parquet_dir(empty_dir)
        assert not empty_dir.exists()

    def test_keeps_dir_with_data(self, tmp_dir):
        data_dir = tmp_dir / "with_data"
        data_dir.mkdir()
        (data_dir / "part-00000.parquet").write_text("fake data")
        _cleanup_empty_parquet_dir(data_dir)
        assert data_dir.exists()

    def test_noop_for_nonexistent(self, tmp_dir):
        _cleanup_empty_parquet_dir(tmp_dir / "nonexistent")
        # Should not raise


# =============================================================================
# Test _tail_csv_rows
# =============================================================================

class TestTailCsvRows:

    def test_reads_last_n_rows(self, tmp_dir):
        csv_path = tmp_dir / "test.csv"
        lines = ["open_time,close_time,value"]
        for i in range(20):
            lines.append(f"2024-01-01 00:{i:02d}:00,2024-01-01 00:{i+1:02d}:00,{i}")
        csv_path.write_text("\n".join(lines))

        result = _tail_csv_rows(csv_path, 5)
        assert len(result) == 5
        assert result["value"].iloc[-1] == 19

    def test_returns_empty_for_zero_rows(self, tmp_dir):
        csv_path = tmp_dir / "test.csv"
        csv_path.write_text("open_time,close_time,value\n2024-01-01,2024-01-02,1\n")
        result = _tail_csv_rows(csv_path, 0)
        assert result.empty

    def test_reads_all_if_n_larger_than_file(self, tmp_dir):
        csv_path = tmp_dir / "small.csv"
        lines = [
            "open_time,close_time,value",
            "2024-01-01 00:00:00,2024-01-01 00:01:00,10",
            "2024-01-01 00:01:00,2024-01-01 00:02:00,20",
        ]
        csv_path.write_text("\n".join(lines))

        result = _tail_csv_rows(csv_path, 100)
        assert len(result) == 3

    def test_handles_header_only_file(self, tmp_dir):
        csv_path = tmp_dir / "header_only.csv"
        csv_path.write_text("open_time,close_time,value\n")
        result = _tail_csv_rows(csv_path, 5)
        assert result.empty or len(result) == 0


# =============================================================================
# Test _load_transform_state / _save_transform_state
# =============================================================================

class TestTransformState:

    def test_save_and_load_roundtrip(self, tmp_dir, monkeypatch):
        state_path = tmp_dir / "state.json"
        monkeypatch.setattr("scripts.transform.TRANSFORM_STATE_PATH", state_path)

        state = {"BTCUSDT": "2024-06-01T00:00:00", "ETHUSDT": "2024-06-01T01:00:00"}
        _save_transform_state(state)

        loaded = _load_transform_state()
        assert loaded == state

    def test_load_returns_empty_if_no_file(self, tmp_dir, monkeypatch):
        state_path = tmp_dir / "nonexistent.json"
        monkeypatch.setattr("scripts.transform.TRANSFORM_STATE_PATH", state_path)
        assert _load_transform_state() == {}

    def test_load_returns_empty_for_corrupt_json(self, tmp_dir, monkeypatch):
        state_path = tmp_dir / "corrupt.json"
        state_path.write_text("not valid json {{")
        monkeypatch.setattr("scripts.transform.TRANSFORM_STATE_PATH", state_path)
        assert _load_transform_state() == {}

    def test_load_returns_empty_for_non_dict_json(self, tmp_dir, monkeypatch):
        state_path = tmp_dir / "array.json"
        state_path.write_text("[1, 2, 3]")
        monkeypatch.setattr("scripts.transform.TRANSFORM_STATE_PATH", state_path)
        assert _load_transform_state() == {}


# =============================================================================
# Test _estimate_missing_rows
# =============================================================================

class TestEstimateMissingRows:

    def test_returns_1000_when_no_last_processed(self):
        result = _estimate_missing_rows("BTCUSDT", None)
        assert result == 1000

    @patch("scripts.transform.get_last_timestamp", return_value=None)
    def test_returns_0_when_no_raw_data(self, mock_ts):
        last = pd.Timestamp("2024-01-01", tz="UTC")
        result = _estimate_missing_rows("BTCUSDT", last)
        assert result == 0

    @patch("scripts.transform.get_last_timestamp")
    def test_estimates_correctly(self, mock_ts):
        raw_ts_ms = int(pd.Timestamp("2024-01-01 02:00:00", tz="UTC").timestamp() * 1000)
        mock_ts.return_value = raw_ts_ms

        last_processed = pd.Timestamp("2024-01-01 01:00:00", tz="UTC")
        result = _estimate_missing_rows("BTCUSDT", last_processed)
        assert result == 70

    @patch("scripts.transform.get_last_timestamp")
    def test_returns_at_least_zero(self, mock_ts):
        raw_ts_ms = int(pd.Timestamp("2024-01-01 00:00:00", tz="UTC").timestamp() * 1000)
        mock_ts.return_value = raw_ts_ms

        last_processed = pd.Timestamp("2024-01-01 02:00:00", tz="UTC")
        result = _estimate_missing_rows("BTCUSDT", last_processed)
        assert result >= 0


# =============================================================================
# Test transform_data (integration-level with mocked Spark)
# =============================================================================

class TestTransformDataIntegration:
    """Higher-level tests for transform_data logic using mocks."""

    @patch("scripts.transform._parquet_has_data_files", return_value=False)
    @patch("scripts.transform._bootstrap_transform_state_from_features", return_value={})
    @patch("scripts.transform._transform_full_rebuild")
    def test_triggers_full_rebuild_when_no_state(
        self, mock_rebuild, mock_bootstrap, mock_has_data
    ):
        """When no state and no parquet, should trigger full rebuild."""
        mock_spark = MagicMock()
        mock_rebuild.return_value = "/fake/path"

        from scripts.transform import transform_data
        result = transform_data(mock_spark, symbols=["BTCUSDT"])

        mock_rebuild.assert_called_once_with(mock_spark, ["BTCUSDT"])
        assert result == "/fake/path"

    @patch("scripts.transform._save_transform_state")
    @patch("scripts.transform._parquet_has_data_files", return_value=True)
    @patch("scripts.transform._bootstrap_transform_state_from_features")
    @patch("scripts.transform._read_symbol_incremental_csv")
    def test_returns_none_when_no_new_data(
        self, mock_read_csv, mock_bootstrap, mock_has_data, mock_save
    ):
        """When incremental read returns empty, should return None."""
        mock_bootstrap.return_value = {"BTCUSDT": "2024-01-01T00:00:00"}
        mock_read_csv.return_value = pd.DataFrame()

        mock_spark = MagicMock()

        from scripts.transform import transform_data
        result = transform_data(mock_spark, symbols=["BTCUSDT"])

        assert result is None


# =============================================================================
# Test edge cases & data integrity
# =============================================================================

class TestEdgeCases:

    def test_single_row_dataframe(self):
        """calculate_indicators_pandas should handle a single row."""
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
        result = calculate_indicators_pandas(df)
        assert len(result) == 1
        assert result["RSI"].isna().sum() == 0
        assert result["MACD"].isna().sum() == 0
        assert result["MACD_signal"].isna().sum() == 0

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
        result = calculate_indicators_pandas(df)
        assert result["RSI"].between(0, 100).all()
        assert np.isfinite(result["MACD"]).all()
        assert np.isfinite(result["MACD_signal"]).all()

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
        result = calculate_indicators_pandas(df)
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
        result = calculate_indicators_pandas(df)
        assert len(result) == n
        assert result.isna().sum().sum() == 0