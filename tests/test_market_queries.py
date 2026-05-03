# =============================================================================
# Test Market Queries Module
# =============================================================================
# Unit tests cho services/chat_api/market_queries.py
#
# Chạy: pytest tests/test_market_queries.py -v
# =============================================================================

# ---------------------------------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

# Đảm bảo project root + service dir nằm trên sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "chat_api"))

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from services.chat_api.market_queries import (
    _esc,
    _imbalance_tag,
    _safe_float,
    format_candles,
    format_orderbook,
    format_ticker_trend,
    fetch_candles,
    fetch_ticker_trend,
    fetch_latest_ticker,
    fetch_orderbook_data,
)


# ---------------------------------------------------------------------------
# 2. MOCK DATA
# ---------------------------------------------------------------------------

SAMPLE_CANDLE_DATA = {
    "ts": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 01:00"]),
    "open": [42000.0, 42300.0],
    "high": [42500.0, 42400.0],
    "low": [41800.0, 42100.0],
    "close": [42300.0, 42200.0],
    "volume": [100.5, 80.2],
    "rsi_14": [55.0, 48.3],
    "macd": [120.5, -50.2],
    "macd_signal": [100.0, -30.0],
}

SAMPLE_TICKER_TREND_DATA = {
    "ts": pd.to_datetime(["2024-01-01", "2024-01-02"]),
    "avg_price_change_pct": [2.5, -1.3],
    "avg_volume_24h": [5000000.0, 4500000.0],
    "avg_spread_pct": [0.0012, 0.0015],
    "avg_trade_count": [150000.0, 130000.0],
}


# ---------------------------------------------------------------------------
# 3. FIXTURES
# ---------------------------------------------------------------------------

@pytest.fixture
def candle_df():
    return pd.DataFrame(SAMPLE_CANDLE_DATA).copy()


@pytest.fixture
def ticker_trend_df():
    return pd.DataFrame(SAMPLE_TICKER_TREND_DATA).copy()


@pytest.fixture
def medium_config():
    """Timeframe config tương tự TIMEFRAME_CONFIG['medium']."""
    return {
        "candle_group_by": "toStartOfInterval(timestamp, INTERVAL 4 HOUR)",
        "candle_lookback_days": 90,
        "candle_limit": 540,
        "candle_ts_format": "%Y-%m-%d %H:00",
        "ticker_group_by": "toDate(snapshot_time)",
        "ticker_lookback_days": 30,
        "ob_group_by": "toDate(timestamp)",
        "ob_lookback_days": 30,
        "ob_mode": "trend",
    }


# ============================================================================
# 4. TEST CASES
# ============================================================================


# ============================================================================
# 4.1 _esc()
# ============================================================================
class TestEsc:
    """Tests cho SQL escape helper."""

    def test_escapes_single_quotes(self):
        assert _esc("it's") == "it''s"

    def test_no_quotes_unchanged(self):
        assert _esc("BTCUSDT") == "BTCUSDT"

    def test_multiple_quotes(self):
        assert _esc("a'b'c") == "a''b''c"

    def test_empty_string(self):
        assert _esc("") == ""


# ============================================================================
# 4.2 _safe_float()
# ============================================================================
class TestSafeFloat:
    """Tests cho _safe_float helper."""

    def test_dict_row_valid(self):
        assert _safe_float({"col": 42.5}, "col") == 42.5

    def test_dict_row_missing_key(self):
        assert _safe_float({"other": 1.0}, "col", 99.0) == 99.0

    def test_series_row_valid(self):
        row = pd.Series({"val": 3.14})
        assert _safe_float(row, "val") == pytest.approx(3.14)

    def test_series_row_missing_col(self):
        row = pd.Series({"other": 1.0})
        assert _safe_float(row, "val", 7.0) == 7.0

    def test_nan_returns_default(self):
        row = pd.Series({"val": float("nan")})
        assert _safe_float(row, "val", 0.0) == 0.0

    def test_none_in_dict_returns_default(self):
        assert _safe_float({"col": None}, "col", 5.0) == 5.0

    def test_integer_value_returned_as_float(self):
        assert _safe_float({"col": 10}, "col") == 10.0
        assert isinstance(_safe_float({"col": 10}, "col"), float)


# ============================================================================
# 4.3 _imbalance_tag()
# ============================================================================
class TestImbalanceTag:
    """Tests cho _imbalance_tag helper."""

    def test_strong_buy(self):
        assert _imbalance_tag(0.7) == "strong buy pressure"

    def test_strong_sell(self):
        assert _imbalance_tag(0.3) == "strong sell pressure"

    def test_balanced_middle(self):
        assert _imbalance_tag(0.5) == "balanced"

    def test_boundary_0_6_is_balanced(self):
        """0.6 chính xác → balanced (condition là > 0.6)."""
        assert _imbalance_tag(0.6) == "balanced"

    def test_boundary_0_4_is_balanced(self):
        """0.4 chính xác → balanced (condition là < 0.4)."""
        assert _imbalance_tag(0.4) == "balanced"

    def test_just_above_0_6(self):
        assert _imbalance_tag(0.601) == "strong buy pressure"

    def test_just_below_0_4(self):
        assert _imbalance_tag(0.399) == "strong sell pressure"


# ============================================================================
# 4.4 format_candles()
# ============================================================================
class TestFormatCandles:
    """Tests cho format_candles formatter."""

    def test_happy_path(self, candle_df):
        result = format_candles(candle_df, "%Y-%m-%d %H:00")

        lines = result.strip().split("\n")
        assert len(lines) == 2
        # Kiểm tra dòng đầu chứa các fields
        assert "O:42000.0000" in lines[0]
        assert "H:42500.0000" in lines[0]
        assert "C:42300.0000" in lines[0]
        assert "RSI:55.0" in lines[0]

    def test_empty_dataframe(self):
        result = format_candles(pd.DataFrame(), "%Y-%m-%d")
        assert result == "(No candle data available)"

    def test_daily_format(self, candle_df):
        result = format_candles(candle_df, "%Y-%m-%d")
        assert "2024-01-01" in result
        # Không chứa giờ
        assert "00:00" not in result.split("O:")[0].strip()


# ============================================================================
# 4.5 format_ticker_trend()
# ============================================================================
class TestFormatTickerTrend:
    """Tests cho format_ticker_trend formatter."""

    def test_happy_path(self, ticker_trend_df):
        result = format_ticker_trend(ticker_trend_df)

        lines = result.strip().split("\n")
        assert len(lines) == 2
        assert "2024-01-01" in lines[0]
        assert "Vol:" in lines[0]
        assert "Spread:" in lines[0]
        assert "PriceChg:" in lines[0]

    def test_empty_dataframe(self):
        result = format_ticker_trend(pd.DataFrame())
        assert result == "(No ticker trend data available)"


# ============================================================================
# 4.6 format_orderbook()
# ============================================================================
class TestFormatOrderbook:
    """Tests cho format_orderbook formatter."""

    def test_latest_only_mode(self):
        data = {"mode": "latest_only", "latest_imbalance": 0.65}
        result = format_orderbook(data)

        assert "0.650" in result
        assert "strong buy pressure" in result

    def test_summary_30d_mode(self):
        data = {
            "mode": "summary_30d",
            "avg_imbalance": 0.52,
            "min_imbalance": 0.35,
            "max_imbalance": 0.70,
            "latest_imbalance": 0.55,
        }
        result = format_orderbook(data)

        assert "30-day order book summary" in result
        assert "0.520" in result
        assert "0.350" in result
        assert "0.700" in result

    def test_trend_mode_with_data(self):
        trend_df = pd.DataFrame({
            "ts": pd.to_datetime(["2024-01-01"]),
            "avg_imbalance": [0.48],
            "avg_bid_vol": [1000.0],
            "avg_ask_vol": [1100.0],
        })
        data = {
            "mode": "trend",
            "trend_df": trend_df,
            "latest_imbalance": 0.48,
        }
        result = format_orderbook(data)

        assert "Current imbalance: 0.480" in result
        assert "Order book trend:" in result
        assert "Bid:" in result

    def test_trend_mode_empty_df(self):
        data = {
            "mode": "trend",
            "trend_df": pd.DataFrame(),
            "latest_imbalance": 0.5,
        }
        result = format_orderbook(data)

        assert "Current imbalance: 0.500" in result
        # Không có dòng trend
        assert "Order book trend:" not in result


# ============================================================================
# 4.7 fetch_candles() — SQL construction
# ============================================================================
class TestFetchCandles:
    """Tests cho fetch_candles — mock ch_query_df_params."""

    @patch("services.chat_api.market_queries.ch_query_df_params")
    def test_query_contains_config(self, mock_query, medium_config):
        mock_query.return_value = pd.DataFrame()

        fetch_candles("BTCUSDT", medium_config)

        sql = mock_query.call_args[0][0]
        params = mock_query.call_args[0][1]
        assert "INTERVAL 90 DAY" in sql
        assert "LIMIT 540" in sql
        assert "toStartOfInterval(timestamp, INTERVAL 4 HOUR)" in sql
        # Symbol is now in params, not in the SQL string
        assert params["symbol"] == "BTCUSDT"

    @patch("services.chat_api.market_queries.ch_query_df_params")
    def test_symbol_passed_as_parameter(self, mock_query, medium_config):
        mock_query.return_value = pd.DataFrame()

        fetch_candles("BTC'USDT", medium_config)

        params = mock_query.call_args[0][1]
        # Symbol is passed as parameter — no escaping needed
        assert params["symbol"] == "BTC'USDT"

    @patch("services.chat_api.market_queries.ch_query_df_params")
    def test_returns_dataframe_from_query(self, mock_query, candle_df, medium_config):
        mock_query.return_value = candle_df

        result = fetch_candles("BTCUSDT", medium_config)

        assert len(result) == 2


# ============================================================================
# 4.8 fetch_ticker_trend()
# ============================================================================
class TestFetchTickerTrend:
    """Tests cho fetch_ticker_trend — mock ch_query_df_params."""

    @patch("services.chat_api.market_queries.ch_query_df_params")
    def test_query_uses_config_lookback(self, mock_query, medium_config):
        mock_query.return_value = pd.DataFrame()

        fetch_ticker_trend("ETHUSDT", medium_config)

        sql = mock_query.call_args[0][0]
        params = mock_query.call_args[0][1]
        assert "INTERVAL 30 DAY" in sql
        assert "toDate(snapshot_time)" in sql
        assert params["symbol"] == "ETHUSDT"


# ============================================================================
# 4.9 fetch_latest_ticker()
# ============================================================================
class TestFetchLatestTicker:
    """Tests cho fetch_latest_ticker."""

    @patch("services.chat_api.market_queries.ch_query_df_params")
    def test_happy_path(self, mock_query):
        mock_query.return_value = pd.DataFrame({
            "price_change_pct": [3.5],
            "volume_24h": [5000000.0],
            "spread_pct": [0.0012],
        })

        result = fetch_latest_ticker("BTCUSDT")

        assert result["price_change_pct"] == 3.5
        assert result["volume_24h"] == 5000000.0
        assert result["spread_pct"] == 0.0012

    @patch("services.chat_api.market_queries.ch_query_df_params")
    def test_empty_result_returns_defaults(self, mock_query):
        mock_query.return_value = pd.DataFrame()

        result = fetch_latest_ticker("BTCUSDT")

        assert result == {"price_change_pct": 0.0, "volume_24h": 0.0, "spread_pct": 0.0}


# ============================================================================
# 4.10 fetch_orderbook_data() — mode routing
# ============================================================================
class TestFetchOrderbookData:
    """Tests cho fetch_orderbook_data — kiểm tra routing theo ob_mode."""

    @patch("services.chat_api.market_queries._fetch_ob_latest")
    def test_latest_only_mode(self, mock_latest):
        mock_latest.return_value = {"mode": "latest_only", "latest_imbalance": 0.5}
        config = {"ob_mode": "latest_only"}

        result = fetch_orderbook_data("BTCUSDT", config)

        mock_latest.assert_called_once_with("BTCUSDT")
        assert result["mode"] == "latest_only"

    @patch("services.chat_api.market_queries._fetch_ob_summary")
    def test_summary_30d_mode(self, mock_summary):
        mock_summary.return_value = {"mode": "summary_30d"}
        config = {"ob_mode": "summary_30d"}

        result = fetch_orderbook_data("BTCUSDT", config)

        mock_summary.assert_called_once_with("BTCUSDT")
        assert result["mode"] == "summary_30d"

    @patch("services.chat_api.market_queries._fetch_ob_trend")
    def test_trend_mode(self, mock_trend):
        mock_trend.return_value = {"mode": "trend"}
        config = {"ob_mode": "trend", "ob_group_by": "toDate(timestamp)", "ob_lookback_days": 30}

        result = fetch_orderbook_data("BTCUSDT", config)

        mock_trend.assert_called_once_with("BTCUSDT", config)
        assert result["mode"] == "trend"

    @patch("services.chat_api.market_queries._fetch_ob_trend")
    def test_default_mode_is_trend(self, mock_trend):
        """Config không có ob_mode → default 'trend'."""
        mock_trend.return_value = {"mode": "trend"}
        config = {"ob_group_by": "toDate(timestamp)", "ob_lookback_days": 30}

        fetch_orderbook_data("BTCUSDT", config)

        mock_trend.assert_called_once()
