# =============================================================================
# Test Load Crypto News
# =============================================================================
# Unit tests cho scripts/load_modules/load_news.py
#
# Chạy: pytest tests/test_load_news.py -v
# =============================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import pandas as pd
import pyarrow as pa
from unittest.mock import MagicMock, patch

from scripts.load_modules.load_news import (
    load_news,
    _get_watermark,
)
from utils.exceptions import LoadError


# ---------------------------------------------------------------------------
# MOCK DATA
# ---------------------------------------------------------------------------

SAMPLE_PROCESSED_DF = pd.DataFrame({
    "article_id": ["abc123", "def456"],
    "title": ["Bitcoin Surges", "Ethereum Upgrade"],
    "description": [
        "Bitcoin has broken through the $100K barrier.",
        "Ethereum upgrade brings scalability.",
    ],
    "content": [
        "Full article about bitcoin...",
        "Ethereum network details...",
    ],
    "url": [
        "https://example.com/bitcoin",
        "https://example.com/eth",
    ],
    "image_url": ["https://example.com/img1.jpg", ""],
    "source_name": ["CoinDesk", "CryptoSlate"],
    "source_url": ["https://coindesk.com", "https://cryptoslate.com"],
    "published_at": pd.to_datetime([
        "2025-09-30T19:38:25Z",
        "2025-09-30T18:00:00Z",
    ]),
    "search_query": ["bitcoin", "ethereum"],
    "extracted_at": pd.to_datetime([
        "2025-09-30T20:00:00Z",
        "2025-09-30T20:00:00Z",
    ]),
})


# ---------------------------------------------------------------------------
# TEST CASES
# ---------------------------------------------------------------------------


# ===========================================================================
# 1. _get_watermark()
# ===========================================================================
class TestGetWatermark:
    """Tests cho _get_watermark() — lấy max published_at từ ClickHouse."""

    @patch("scripts.load_modules.load_news.ch_query_df")
    def test_returns_timestamp(self, mock_query):
        """Table có data → trả về Timestamp."""
        mock_query.return_value = pd.DataFrame({
            "max_ts": [pd.Timestamp("2025-09-30T19:00:00Z")]
        })

        result = _get_watermark()
        assert result is not None
        assert result.year == 2025
        assert result.month == 9

    @patch("scripts.load_modules.load_news.ch_query_df")
    def test_empty_table_returns_none(self, mock_query):
        """Table rỗng → trả về None."""
        mock_query.return_value = pd.DataFrame({"max_ts": [None]})

        result = _get_watermark()
        assert result is None

    @patch("scripts.load_modules.load_news.ch_query_df")
    def test_query_error_returns_none(self, mock_query):
        """Query lỗi → trả về None (không crash)."""
        mock_query.side_effect = Exception("ClickHouse down")

        result = _get_watermark()
        assert result is None


# ===========================================================================
# 2. load_news()
# ===========================================================================
class TestLoadNews:
    """Tests cho load_news() — orchestrator chính."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        """Mock ClickHouse và MinIO calls."""
        self.mock_ch_insert = MagicMock(return_value=2)
        self.mock_ch_query = MagicMock(return_value=pd.DataFrame({"max_ts": [None]}))
        self.mock_discover = MagicMock(return_value=["2025-09"])
        self.mock_exists = MagicMock(return_value=True)
        self.mock_download = MagicMock()

        self._patches = [
            patch("scripts.load_modules.load_news.ch_insert_df", self.mock_ch_insert),
            patch("scripts.load_modules.load_news.ch_query_df", self.mock_ch_query),
            patch("scripts.load_modules.load_news.discover_month_partitions", self.mock_discover),
            patch("scripts.load_modules.load_news.storage"),
        ]
        for p in self._patches:
            p.start()

        import scripts.load_modules.load_news as mod
        mod.storage.object_exists = self.mock_exists
        mod.storage.download_parquet.return_value = pa.Table.from_pandas(SAMPLE_PROCESSED_DF)

        yield

        for p in self._patches:
            p.stop()

    def test_happy_path_inserts(self):
        """Chạy thành công → ch_insert_df được gọi."""
        load_news()
        self.mock_ch_insert.assert_called_once_with("crypto_news", pytest.approx(SAMPLE_PROCESSED_DF, nan_ok=True))

    def test_inserts_correct_table(self):
        """Insert vào đúng table crypto_news."""
        load_news()
        call_args = self.mock_ch_insert.call_args[0]
        assert call_args[0] == "crypto_news"

    def test_filters_by_watermark(self):
        """Có watermark → chỉ insert rows mới hơn."""
        self.mock_ch_query.return_value = pd.DataFrame({
            "max_ts": [pd.Timestamp("2025-09-30T19:00:00Z")]
        })

        load_news()

        # Chỉ 1 row mới hơn watermark (19:38:25 > 19:00:00)
        call_args = self.mock_ch_insert.call_args[0]
        df = call_args[1]
        assert len(df) == 1
        assert df.iloc[0]["article_id"] == "abc123"

    def test_all_filtered_by_watermark(self):
        """Tất cả rows cũ hơn watermark → không insert."""
        self.mock_ch_query.return_value = pd.DataFrame({
            "max_ts": [pd.Timestamp("2025-10-01T00:00:00Z")]
        })

        load_news()
        self.mock_ch_insert.assert_not_called()

    def test_empty_partition_skipped(self):
        """Partition rỗng → skip."""
        import scripts.load_modules.load_news as mod

        empty_df = pd.DataFrame(columns=SAMPLE_PROCESSED_DF.columns)
        mod.storage.download_parquet.return_value = pa.Table.from_pandas(empty_df)

        load_news()
        self.mock_ch_insert.assert_not_called()

    def test_no_partitions_found(self):
        """Không có partition → return sớm."""
        self.mock_discover.return_value = []
        load_news()
        self.mock_ch_insert.assert_not_called()

    def test_month_str_filter(self):
        """month_str → chỉ xử lý tháng đó."""
        load_news(month_str="2025-09")
        self.mock_discover.assert_not_called()
        self.mock_exists.assert_called_once()

    def test_all_partitions_fail_raises(self):
        """Tất cả partitions đều lỗi → LoadError."""
        import scripts.load_modules.load_news as mod

        mod.storage.object_exists = MagicMock(side_effect=Exception("MinIO down"))

        with pytest.raises(LoadError):
            load_news(month_str="2025-09")
