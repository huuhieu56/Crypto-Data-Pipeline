# =============================================================================
# Test Extract Module
# =============================================================================
# Unit tests cho scripts/extract.py
#
# Chạy: pytest tests/test_extract.py -v
# =============================================================================

# ---------------------------------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

# Đảm bảo project root nằm trên sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import MagicMock

from scripts.extract import (
    download_data_vision,
    extract_bulk,
    extract_recent_klines,
    extract_ticker_24h,
    extract_order_book_snapshot,
    extract_minutely,
)
from config.config import MONTHS_BACK
from utils.exceptions import ExtractError


# ---------------------------------------------------------------------------
# 2. MOCK DATA TEMPLATES (module-level constants)
# ---------------------------------------------------------------------------
# Lưu ý: DataFrames là mutable → dùng fixtures trả về .copy() thay vì
# truy cập trực tiếp. Các list/dict dưới đây chỉ đọc, không bị mutate.
# ---------------------------------------------------------------------------

_KLINES_DF_DATA = {
    "open_time": [1704067200000, 1704067260000],
    "open": [42000.0, 42300.0],
    "high": [42500.0, 42400.0],
    "low": [41800.0, 42100.0],
    "close": [42300.0, 42200.0],
    "volume": [100.5, 80.2],
    "close_time": [1704067259999, 1704067319999],
    "quote_volume": [4230000.0, 3384400.0],
    "trade_count": [500, 300],
    "taker_buy_base": [60.3, 40.1],
    "taker_buy_quote": [2538000.0, 1692200.0],
    "symbol": ["BTCUSDT", "BTCUSDT"],
}

# --- Ticker 24h mock data (raw API response — includes bid/ask) ---
SAMPLE_TICKER_24H_RAW = [
    {
        "symbol": "BTCUSDT",
        "priceChange": "1500.00",
        "priceChangePercent": "3.50",
        "highPrice": "44000.00",
        "lowPrice": "41000.00",
        "volume": "25000.50",
        "quoteVolume": "1050000000.00",
        "count": 1200000,
        "bidPrice": "42290.00",
        "askPrice": "42310.00",
    },
    {
        "symbol": "ETHUSDT",
        "priceChange": "80.00",
        "priceChangePercent": "2.80",
        "highPrice": "2900.00",
        "lowPrice": "2750.00",
        "volume": "150000.20",
        "quoteVolume": "420000000.00",
        "count": 800000,
        "bidPrice": "2849.50",
        "askPrice": "2850.50",
    },
    # Coin không nằm trong danh sách test → phải bị lọc bỏ
    {
        "symbol": "DOGEUSDT",
        "priceChange": "0.005",
        "priceChangePercent": "1.20",
        "highPrice": "0.42",
        "lowPrice": "0.40",
        "volume": "5000000.00",
        "quoteVolume": "2050000.00",
        "count": 300000,
        "bidPrice": "0.4100",
        "askPrice": "0.4105",
    },
]

# --- Order book mock data ---
SAMPLE_ORDER_BOOK = {
    "bids": [
        ["42290.00", "1.5"],
        ["42285.00", "2.3"],
        ["42280.00", "0.8"],
    ],
    "asks": [
        ["42310.00", "1.2"],
        ["42315.00", "3.1"],
        ["42320.00", "0.5"],
    ],
}

# Danh sách symbols dùng trong test
TEST_SYMBOLS = ["BTCUSDT", "ETHUSDT"]


# ---------------------------------------------------------------------------
# 3. FIXTURES
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_klines_df():
    """Bản sao mới của klines DataFrame cho mỗi test — tránh mutation."""
    return pd.DataFrame(_KLINES_DF_DATA).copy()


@pytest.fixture
def tmp_raw_dir(tmp_path):
    """Tạo thư mục raw tạm thời."""
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    return raw


@pytest.fixture
def sample_parquet(tmp_raw_dir, sample_klines_df):
    """Tạo Parquet mẫu có sẵn dữ liệu cho BTCUSDT."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    parquet_path = tmp_raw_dir / "BTCUSDT.parquet"
    table = pa.Table.from_pandas(sample_klines_df, preserve_index=False)
    pq.write_table(table, parquet_path)
    return parquet_path


# ============================================================================
# 4. TEST CASES
# ============================================================================


# ============================================================================
# 4.1 download_data_vision()
# ============================================================================
class TestDownloadDataVision:
    """Tests cho download_data_vision() — bulk download từ Data Vision."""

    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch, sample_klines_df):
        """Setup chung: mock download_klines_month & append_to_partition_csv."""
        self.sample_df = sample_klines_df
        self.mock_download = MagicMock(return_value=sample_klines_df)
        self.mock_append = MagicMock()
        monkeypatch.setattr(
            "scripts.extract_modules.extract_klines.download_klines_month", self.mock_download,
        )
        monkeypatch.setattr(
            "scripts.extract_modules.extract_klines.append_to_partition_csv", self.mock_append,
        )

    # --- Happy Path ---
    def test_happy_path_downloads_all_months(self):
        """API trả về data cho tất cả các tháng → trả về tổng record count."""
        months = [(2024, 1), (2024, 2), (2024, 3)]
        result = download_data_vision("BTCUSDT", months)

        assert result is not None
        assert result > 0
        assert self.mock_download.call_count == 3
        assert self.mock_append.call_count == 3

    def test_months_processed_in_chronological_order(self):
        """Months phải được download đầy đủ theo thứ tự thời gian."""
        # Truyền months không đúng thứ tự
        months = [(2024, 3), (2024, 1), (2024, 2)]
        download_data_vision("BTCUSDT", months)

        # Kiểm tra tất cả months đều được download
        calls = self.mock_download.call_args_list
        called_months = {c.args for c in calls}
        assert called_months == {
            ("BTCUSDT", 2024, 1),
            ("BTCUSDT", 2024, 2),
            ("BTCUSDT", 2024, 3),
        }

    # --- Sad Path ---
    def test_all_months_fail_returns_none(self):
        """Tất cả tháng đều thất bại → trả về None."""
        self.mock_download.return_value = None

        result = download_data_vision("BTCUSDT", [(2024, 1), (2024, 2)])

        assert result is None

    # --- Edge Case ---
    def test_partial_months_success(self):
        """Một số tháng thành công, một số thất bại → vẫn trả về tổng."""
        self.mock_download.side_effect = [self.sample_df, None, self.sample_df]

        result = download_data_vision("BTCUSDT", [(2024, 1), (2024, 2), (2024, 3)])

        assert result is not None
        assert result > 0
        assert self.mock_append.call_count == 2  # chỉ 2 tháng OK

    def test_one_month_fails_others_succeed(self):
        """Một tháng raise exception → các tháng khác vẫn được xử lý."""
        self.mock_download.side_effect = [
            self.sample_df,
            RuntimeError("boom"),
            self.sample_df,
        ]

        result = download_data_vision("BTCUSDT", [(2024, 1), (2024, 2), (2024, 3)])

        assert result == len(self.sample_df) * 2
        assert self.mock_download.call_count == 3
        assert self.mock_append.call_count == 2

    def test_empty_months_list_returns_none(self):
        """Danh sách months rỗng → trả về None."""
        result = download_data_vision("BTCUSDT", [])
        assert result is None
        self.mock_download.assert_not_called()


# ============================================================================
# 4.2 extract_bulk()
# ============================================================================
class TestExtractBulk:
    """Tests cho extract_bulk() — bulk download toàn bộ symbols."""

    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch):
        """Setup chung: mock download_data_vision, get_target_months, ticker & orderbook."""
        self.mock_dv = MagicMock(return_value=5000)
        self.mock_months = MagicMock(return_value=[(2024, 1)])
        monkeypatch.setattr(
            "scripts.extract_modules.extract_klines.download_data_vision", self.mock_dv,
        )
        monkeypatch.setattr(
            "scripts.extract_modules.extract_klines.get_target_months", self.mock_months,
        )
        # extract_bulk calls extract_ticker_24h and extract_order_book_snapshot
        monkeypatch.setattr(
            "scripts.extract.extract_ticker_24h", MagicMock(),
        )
        monkeypatch.setattr(
            "scripts.extract.extract_order_book_snapshot", MagicMock(),
        )

    # --- Happy Path ---
    def test_happy_path_returns_dict_of_counts(self):
        """Tất cả symbols download thành công → dict {symbol: count}."""
        result = extract_bulk(symbols=TEST_SYMBOLS, months_back=1)

        assert result == {"BTCUSDT": 5000, "ETHUSDT": 5000}
        assert self.mock_dv.call_count == 2

    # --- Sad Path ---
    def test_all_symbols_fail_returns_empty(self):
        """Tất cả symbols thất bại → dict rỗng."""
        self.mock_dv.return_value = None

        result = extract_bulk(symbols=TEST_SYMBOLS, months_back=1)

        assert result == {}

    # --- Edge Case ---
    def test_partial_symbols_success(self):
        """Một số symbols thành công → chỉ chứa symbols thành công."""
        self.mock_dv.side_effect = [10000, None]

        result = extract_bulk(symbols=TEST_SYMBOLS, months_back=1)

        assert result == {"BTCUSDT": 10000}
        assert "ETHUSDT" not in result

    def test_one_symbol_fails_in_bulk_others_succeed(self):
        """Một symbol raise exception → symbol khác vẫn có kết quả."""
        self.mock_dv.side_effect = [10000, RuntimeError("boom")]

        result = extract_bulk(symbols=TEST_SYMBOLS, months_back=1)

        assert result == {"BTCUSDT": 10000}
        assert self.mock_dv.call_count == 2


# ============================================================================
# 4.3 extract_recent_klines()
# ============================================================================
class TestExtractRecentKlines:
    """Tests cho extract_recent_klines() — incremental REST API update."""

    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch, sample_klines_df):
        """Setup chung: mock 4 dependencies dùng trong extract_recent_klines."""
        self.sample_df = sample_klines_df
        self.mock_last_ts = MagicMock(return_value={"BTCUSDT": 1704067200000})
        self.mock_target_end = MagicMock(
            return_value=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        self.mock_fetch = MagicMock(return_value=sample_klines_df)
        self.mock_write = MagicMock()
        self.mock_bulk = MagicMock(return_value={"BTCUSDT": 5000})

        monkeypatch.setattr(
            "scripts.extract_modules.extract_klines.get_last_timestamps", self.mock_last_ts,
        )
        monkeypatch.setattr(
            "scripts.extract_modules.extract_klines.get_target_end", self.mock_target_end,
        )
        monkeypatch.setattr(
            "scripts.extract_modules.extract_klines.fetch_klines_paginated", self.mock_fetch,
        )
        monkeypatch.setattr(
            "scripts.extract_modules.extract_klines.append_to_partition_csv", self.mock_write,
        )
        monkeypatch.setattr(
            "scripts.extract_modules.extract_klines.extract_bulk", self.mock_bulk,
        )

    # --- Happy Path ---
    def test_happy_path_returns_new_data(self):
        """Có data mới từ API → trả về dict {symbol: DataFrame}."""
        result = extract_recent_klines(["BTCUSDT"])

        assert "BTCUSDT" in result
        assert len(result["BTCUSDT"]) == 2
        self.mock_fetch.assert_called_once()

    # --- Sad Path ---
    def test_no_existing_data_bootstraps_then_skips_if_still_missing(self):
        """Symbol chưa có dữ liệu → bootstrap bằng extract_bulk, rồi skip nếu vẫn thiếu watermark."""
        self.mock_last_ts.side_effect = [{}, {}]

        result = extract_recent_klines(["BTCUSDT"])

        assert result == {}
        self.mock_bulk.assert_called_once_with(
            ["BTCUSDT"],
            months_back=MONTHS_BACK,
            log_context="BOOTSTRAP",
        )
        self.mock_fetch.assert_not_called()

    def test_api_returns_empty_dataframe(self):
        """API trả về DataFrame rỗng → không có kết quả."""
        self.mock_fetch.return_value = pd.DataFrame()

        result = extract_recent_klines(["BTCUSDT"])

        assert result == {}

    def test_api_returns_none(self):
        """API trả về None → không có kết quả."""
        self.mock_fetch.return_value = None

        result = extract_recent_klines(["BTCUSDT"])

        assert result == {}

    # --- Edge Cases ---
    def test_empty_symbols_list_returns_empty(self):
        """Truyền list rỗng → trả về dict rỗng, không gọi API."""
        result = extract_recent_klines([])
        assert result == {}

    def test_already_up_to_date_skips(self):
        """last_ts >= end_time → data đã up-to-date, skip."""
        self.mock_last_ts.return_value = {"BTCUSDT": 1704153600000}  # 2024-01-02 UTC
        self.mock_target_end.return_value = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = extract_recent_klines(["BTCUSDT"])

        assert result == {}

    def test_target_end_drives_fetch_end_time(self):
        """Trading symbols fetch only through the latest fully closed 1m kline."""
        target_end_ms = 1704153540000
        result = extract_recent_klines(["BTCUSDT"])

        assert "BTCUSDT" in result
        self.mock_target_end.assert_called_once_with("BTCUSDT")
        self.mock_fetch.assert_called_once_with("BTCUSDT", 1704067200000, target_end_ms)

    def test_current_open_kline_is_not_fetched(self):
        """If the latest closed kline is loaded, do not fetch the open kline."""
        self.mock_last_ts.return_value = {"BTCUSDT": 1704153600000}
        self.mock_target_end.return_value = datetime(2024, 1, 2, 0, 1, 30, tzinfo=timezone.utc)

        result = extract_recent_klines(["BTCUSDT"])

        assert result == {}
        self.mock_fetch.assert_not_called()

    def test_one_symbol_raises_in_recent_klines_others_succeed(self):
        """Một symbol raise exception → symbols còn lại vẫn được xử lý."""
        self.mock_last_ts.return_value = {
            "BTCUSDT": 1704067200000,
            "ETHUSDT": 1704067200000,
        }

        def _fetch(symbol, start_time, end_time):
            if symbol == "ETHUSDT":
                raise RuntimeError("boom")
            return self.sample_df

        self.mock_fetch.side_effect = _fetch

        result = extract_recent_klines(TEST_SYMBOLS)

        assert list(result) == ["BTCUSDT"]
        assert self.mock_fetch.call_count == 2
        assert self.mock_write.call_count == 1


# ============================================================================
# 4.4 extract_ticker_24h()
# ============================================================================
class TestExtractTicker24h:
    """Tests cho extract_ticker_24h() — fetch ticker/24hr (includes bid/ask)."""

    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch, tmp_path):
        """Setup chung: mock get_ticker_24h, append_to_partition."""
        self.tmp_path = tmp_path

        self.mock_ticker = MagicMock(return_value=SAMPLE_TICKER_24H_RAW)
        self.mock_append = MagicMock()
        monkeypatch.setattr(
            "scripts.extract_modules.extract_ticker.get_ticker_24h", self.mock_ticker,
        )
        monkeypatch.setattr(
            "scripts.extract_modules.extract_ticker.append_to_partition", self.mock_append,
        )

    # --- Happy Path ---
    def test_happy_path_returns_raw_dataframe(self):
        """API trả về data chuẩn → ticker_df raw (camelCase) với bid/ask."""
        ticker_df = extract_ticker_24h(TEST_SYMBOLS)

        assert ticker_df is not None
        assert len(ticker_df) == 2  # chỉ BTCUSDT và ETHUSDT
        assert set(ticker_df["symbol"].tolist()) == {"BTCUSDT", "ETHUSDT"}

        # Raw columns từ Binance (camelCase), chưa rename
        assert "symbol" in ticker_df.columns
        assert "priceChange" in ticker_df.columns
        assert "bidPrice" in ticker_df.columns
        assert "askPrice" in ticker_df.columns

    def test_filters_only_requested_symbols(self):
        """Chỉ giữ symbols trong danh sách, loại bỏ coins khác."""
        ticker_df = extract_ticker_24h(["BTCUSDT"])

        assert len(ticker_df) == 1
        assert ticker_df.iloc[0]["symbol"] == "BTCUSDT"

    # --- Sad Path ---
    def test_ticker_api_error_raises_extract_error(self):
        """get_ticker_24h() raise Exception → ExtractError."""
        self.mock_ticker.side_effect = ConnectionError("Network unreachable")

        with pytest.raises(ExtractError, match="Failed to fetch ticker/24hr"):
            extract_ticker_24h(TEST_SYMBOLS)

    # --- Edge Cases ---
    def test_empty_symbols_returns_none(self):
        """Truyền list rỗng → trả về None, không gọi API."""
        result = extract_ticker_24h([])
        assert result is None

    def test_api_returns_empty_data_raises_key_error(self):
        """API trả về list rỗng → pd.DataFrame([]) không có cột 'symbol' → KeyError.

        Ghi chú: Đây là hành vi thực tế của production code — không có guard
        cho trường hợp API trả về mảng trống hoàn toàn.
        """
        self.mock_ticker.return_value = []

        with pytest.raises(KeyError):
            extract_ticker_24h(TEST_SYMBOLS)

    def test_per_symbol_partition_writes(self):
        """append_to_partition called once per symbol (ticker_raw only)."""
        extract_ticker_24h(TEST_SYMBOLS)

        # 2 symbols × 1 data type (ticker_raw) = 2 calls
        assert self.mock_append.call_count == 2
        prefixes = {call.args[1] for call in self.mock_append.call_args_list}
        assert prefixes == {"ticker_raw"}


# ============================================================================
# 4.5 extract_order_book_snapshot()
# ============================================================================
class TestExtractOrderBookSnapshot:
    """Tests cho extract_order_book_snapshot() — depth & imbalance."""

    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch, tmp_path):
        """Setup chung: mock get_order_book, sleep, append_to_partition."""
        self.tmp_path = tmp_path
        monkeypatch.setattr(
            "scripts.extract_modules.extract_order_book.sleep_between_requests", lambda: None,
        )
        self.mock_append = MagicMock()
        monkeypatch.setattr(
            "scripts.extract_modules.extract_order_book.append_to_partition", self.mock_append,
        )

        self.mock_ob = MagicMock(return_value=SAMPLE_ORDER_BOOK)
        monkeypatch.setattr(
            "scripts.extract_modules.extract_order_book.get_order_book", self.mock_ob,
        )

    # --- Happy Path ---
    def test_happy_path_returns_raw_dataframe(self):
        """API trả về order book chuẩn → DataFrame raw với bids/asks arrays."""
        result = extract_order_book_snapshot(["BTCUSDT"])

        assert result is not None
        assert len(result) == 1
        row = result.iloc[0]
        assert row["symbol"] == "BTCUSDT"
        assert len(row["bids"]) == 3
        assert len(row["asks"]) == 3

    def test_output_columns_correct(self):
        """Kiểm tra schema cột output — raw bids/asks, chưa compute."""
        result = extract_order_book_snapshot(["BTCUSDT"])

        expected_cols = {"symbol", "timestamp", "bids", "asks"}
        assert set(result.columns) == expected_cols

    def test_multiple_symbols(self):
        """Nhiều symbols → mỗi symbol 1 row."""
        result = extract_order_book_snapshot(TEST_SYMBOLS)

        assert result is not None
        assert len(result) == 2
        assert set(result["symbol"].tolist()) == {"BTCUSDT", "ETHUSDT"}

    # --- Sad Path ---
    def test_all_symbols_fail_returns_none(self):
        """Tất cả API calls thất bại → trả về None."""
        self.mock_ob.side_effect = ConnectionError("Network down")

        result = extract_order_book_snapshot(["BTCUSDT"])

        assert result is None

    def test_partial_failure_still_returns_data(self):
        """Một symbol thất bại, một thành công → trả về data của symbol thành công."""
        self.mock_ob.side_effect = [
            SAMPLE_ORDER_BOOK,
            ConnectionError("Timeout"),
        ]

        result = extract_order_book_snapshot(TEST_SYMBOLS)

        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["symbol"] == "BTCUSDT"

    # --- Edge Cases ---
    def test_empty_symbols_returns_none(self):
        """Truyền list rỗng → None."""
        result = extract_order_book_snapshot([])
        assert result is None

    def test_empty_order_book_stored_as_empty_lists(self):
        """Order book không có bids/asks → raw arrays rỗng."""
        self.mock_ob.return_value = {"bids": [], "asks": []}

        result = extract_order_book_snapshot(["BTCUSDT"])

        assert result is not None
        row = result.iloc[0]
        assert row["bids"] == []
        assert row["asks"] == []

    def test_raw_bids_asks_stored_as_lists(self):
        """Bids/asks được lưu nguyên dạng list, không compute."""
        self.mock_ob.return_value = {
            "bids": [["42290.00", "1.5"], ["42285.00", "2.3"]],
            "asks": [["42310.00", "1.2"]],
        }

        result = extract_order_book_snapshot(["BTCUSDT"])

        assert result is not None
        row = result.iloc[0]
        assert len(row["bids"]) == 2
        assert len(row["asks"]) == 1
        assert row["bids"][0] == ["42290.00", "1.5"]

    def test_per_symbol_partition_writes(self):
        """append_to_partition is called once per symbol (per-symbol Parquet)."""
        extract_order_book_snapshot(["BTCUSDT"])

        # 1 symbol → 1 call to append_to_partition
        assert self.mock_append.call_count == 1
        args = self.mock_append.call_args[0]
        assert args[1] == "order_book"  # prefix
        assert args[2] == "BTCUSDT"     # symbol

    def test_get_order_book_called_with_limit(self, monkeypatch):
        """get_order_book phải được gọi với limit=ORDER_BOOK_LIMIT."""
        monkeypatch.setattr("scripts.extract_modules.extract_order_book.ORDER_BOOK_LIMIT", 50)

        extract_order_book_snapshot(["BTCUSDT"])

        self.mock_ob.assert_called_once_with("BTCUSDT", limit=50)


# ============================================================================
# 4.6 extract_minutely()
# ============================================================================
class TestExtractMinutely:
    """Tests cho extract_minutely() — orchestrator cho minutely extract."""

    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch, sample_klines_df):
        """Setup chung: mock 3 sub-functions."""
        self.mock_klines = MagicMock(return_value={"BTCUSDT": sample_klines_df})
        self.mock_ticker = MagicMock()
        self.mock_ob = MagicMock()
        monkeypatch.setattr(
            "scripts.extract.extract_recent_klines", self.mock_klines,
        )
        monkeypatch.setattr(
            "scripts.extract.extract_ticker_24h", self.mock_ticker,
        )
        monkeypatch.setattr(
            "scripts.extract.extract_order_book_snapshot", self.mock_ob,
        )

    # --- Happy Path ---
    def test_happy_path_calls_all_steps(self, monkeypatch):
        """Chạy đủ 3 bước: klines, ticker, order book."""
        monkeypatch.setattr(
            "scripts.extract.SYMBOLS_STATUS",
            {"BTCUSDT": "TRADING", "ETHUSDT": "TRADING"},
        )

        extract_minutely(symbols=TEST_SYMBOLS)

        self.mock_klines.assert_called_once_with(TEST_SYMBOLS)
        self.mock_ticker.assert_called_once()
        self.mock_ob.assert_called_once()

    def test_break_symbols_excluded_from_ticker_and_orderbook(self, monkeypatch):
        """BREAK symbols không được gọi ticker/order book, nhưng vẫn gọi klines."""
        monkeypatch.setattr(
            "scripts.extract.SYMBOLS_STATUS",
            {"BTCUSDT": "TRADING", "CROUSDT": "BREAK"},
        )
        self.mock_klines.return_value = {}

        extract_minutely(symbols=["BTCUSDT", "CROUSDT"])

        # klines gọi cho TẤT CẢ symbols
        self.mock_klines.assert_called_once_with(["BTCUSDT", "CROUSDT"])
        # ticker & order book chỉ gọi cho TRADING symbols
        self.mock_ticker.assert_called_once_with(["BTCUSDT"])
        self.mock_ob.assert_called_once_with(["BTCUSDT"])

    def test_empty_klines_still_calls_ticker_and_orderbook(self, monkeypatch):
        """Klines API trả về dict rỗng → vẫn tiếp tục gọi ticker & order book."""
        monkeypatch.setattr(
            "scripts.extract.SYMBOLS_STATUS",
            {"BTCUSDT": "TRADING"},
        )
        self.mock_klines.return_value = {}

        extract_minutely(symbols=["BTCUSDT"])

        self.mock_klines.assert_called_once_with(["BTCUSDT"])
        self.mock_ticker.assert_called_once_with(["BTCUSDT"])
        self.mock_ob.assert_called_once_with(["BTCUSDT"])

    # --- Edge Case ---
    def test_unknown_status_defaults_to_trading(self, monkeypatch):
        """Symbol không có trong SYMBOLS_STATUS → mặc định TRADING."""
        monkeypatch.setattr("scripts.extract.SYMBOLS_STATUS", {})
        self.mock_klines.return_value = {}

        extract_minutely(symbols=["NEWCOIN"])

        # NEWCOIN phải nằm trong trading list
        self.mock_ticker.assert_called_once_with(["NEWCOIN"])
        self.mock_ob.assert_called_once_with(["NEWCOIN"])
