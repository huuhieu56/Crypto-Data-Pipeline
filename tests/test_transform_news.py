# =============================================================================
# Test Transform Crypto News
# =============================================================================
# Unit tests cho scripts/transform_modules/transform_news.py
#
# Chạy: pytest tests/test_transform_news.py -v
# =============================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import pandas as pd
import pyarrow as pa
from unittest.mock import MagicMock, patch

from scripts.transform_modules.transform_news import (
    clean_text,
    extract_symbols,
    transform_news,
)
from utils.exceptions import TransformError


# ---------------------------------------------------------------------------
# MOCK DATA
# ---------------------------------------------------------------------------

SAMPLE_RAW_DF = pd.DataFrame({
    "article_id": ["abc123", "def456", "ghi789"],
    "title": [
        "Bitcoin Surges Past $100K",
        "<p>Ethereum Upgrade Live</p>",
        "Random News About Weather",
    ],
    "description": [
        "Bitcoin has broken through the $100,000 barrier as institutions invest.",
        "Ethereum's latest upgrade brings <b>scalability</b> improvements.",
        "Today's weather is sunny with no rain expected in the region.",
    ],
    "content": [
        "Full article about bitcoin and crypto market growth...",
        "Ethereum network upgrade details and impact on defi...",
        "Weather forecast for the week ahead.",
    ],
    "url": [
        "https://example.com/bitcoin-surges",
        "https://example.com/eth-upgrade",
        "https://example.com/weather",
    ],
    "image_url": [
        "https://example.com/img1.jpg",
        "https://example.com/img2.jpg",
        "",
    ],
    "source_name": ["CoinDesk", "CryptoSlate", "WeatherNews"],
    "source_url": [
        "https://coindesk.com",
        "https://cryptoslate.com",
        "https://weathernews.com",
    ],
    "published_at": pd.to_datetime([
        "2025-09-30T19:38:25Z",
        "2025-09-30T18:00:00Z",
        "2025-09-30T17:00:00Z",
    ]),
    "search_query": ["bitcoin", "ethereum", "weather"],
    "extracted_at": pd.to_datetime([
        "2025-09-30T20:00:00Z",
        "2025-09-30T20:00:00Z",
        "2025-09-30T20:00:00Z",
    ]),
})


# ---------------------------------------------------------------------------
# TEST CASES
# ---------------------------------------------------------------------------


# ===========================================================================
# 1. clean_text()
# ===========================================================================
class TestCleanText:
    """Tests cho clean_text() — loại bỏ HTML tags, chuẩn hóa whitespace."""

    def test_removes_html_tags(self):
        assert clean_text("<p>Hello</p>") == "Hello"
        assert clean_text("<b>bold</b> and <i>italic</i>") == "bold and italic"

    def test_normalizes_whitespace(self):
        assert clean_text("  hello   world  ") == "hello world"
        assert clean_text("line1\n\nline2") == "line1 line2"

    def test_empty_string(self):
        assert clean_text("") == ""

    def test_none_returns_empty(self):
        assert clean_text(None) == ""

    def test_non_string_returns_empty(self):
        assert clean_text(123) == ""
        assert clean_text([]) == ""

    def test_no_html_unchanged(self):
        assert clean_text("plain text") == "plain text"


# ===========================================================================
# 2. extract_symbols()
# ===========================================================================
class TestExtractSymbols:
    """Tests cho extract_symbols() — nhận diện crypto symbols từ text."""

    def test_detects_bitcoin(self):
        assert "BTCUSDT" in extract_symbols("Bitcoin price surges today")

    def test_detects_ethereum(self):
        assert "ETHUSDT" in extract_symbols("Ethereum network upgrade")

    def test_detects_multiple_symbols(self):
        symbols = extract_symbols("Bitcoin and Ethereum both up today")
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols

    def test_case_insensitive(self):
        assert "BTCUSDT" in extract_symbols("BITCOIN rally continues")
        assert "BTCUSDT" in extract_symbols("btc hits new high")

    def test_no_match_returns_empty(self):
        assert extract_symbols("Weather forecast for today") == []

    def test_empty_string(self):
        assert extract_symbols("") == []

    def test_none_returns_empty(self):
        assert extract_symbols(None) == []

    def test_returns_sorted(self):
        symbols = extract_symbols("Solana and Bitcoin")
        assert symbols == sorted(symbols)

    def test_detects_abbreviations(self):
        assert "SOLUSDT" in extract_symbols("SOL price up 10%")
        assert "ADAUSDT" in extract_symbols("ADA staking rewards")
        assert "DOGEUSDT" in extract_symbols("DOGE to the moon")

    def test_word_boundary(self):
        """Không match partial words — 'adoption' không phải 'ada'."""
        symbols = extract_symbols("Crypto adoption is growing")
        assert "ADAUSDT" not in symbols


# ===========================================================================
# 3. transform_news()
# ===========================================================================
class TestTransformNews:
    """Tests cho transform_news() — orchestrator chính."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        """Mock MinIO storage calls."""
        self.mock_discover = MagicMock(return_value=["2025-09"])
        self.mock_download = MagicMock()
        self.mock_exists = MagicMock(return_value=True)
        self.mock_append = MagicMock()

        self._patches = [
            patch("scripts.transform_modules.transform_news.discover_month_partitions", self.mock_discover),
            patch("scripts.transform_modules.transform_news.storage"),
            patch("scripts.transform_modules.transform_news.append_to_partition", self.mock_append),
        ]
        for p in self._patches:
            p.start()

        # Setup storage mock
        import scripts.transform_modules.transform_news as mod
        mod.storage.object_exists = self.mock_exists
        mod.storage.download_parquet.return_value = pa.Table.from_pandas(SAMPLE_RAW_DF)

        yield

        for p in self._patches:
            p.stop()

    def test_happy_path_processes_partition(self):
        """Chạy thành công → append_to_partition được gọi."""
        transform_news()
        self.mock_append.assert_called_once()

    def test_writes_to_correct_path(self):
        """Ghi đúng bucket, prefix, symbol."""
        transform_news()

        call_args = self.mock_append.call_args[0]
        assert call_args[1] == "crypto_news"  # prefix
        assert call_args[2] == "gnews"        # symbol

    def test_dedup_col_is_article_id(self):
        """Dedup dùng article_id."""
        transform_news()
        call_kwargs = self.mock_append.call_args
        assert call_kwargs[1]["dedup_col"] == "article_id"

    def test_html_cleaned_in_output(self):
        """HTML tags bị loại bỏ trong title, description, content."""
        import scripts.transform_modules.transform_news as mod

        # Tạo data có HTML
        html_df = SAMPLE_RAW_DF.copy()
        html_df["title"] = ["<p>Bitcoin</p>", "<b>Ethereum</b>", "<i>Weather</i>"]
        mod.storage.download_parquet.return_value = pa.Table.from_pandas(html_df)

        transform_news()

        out_df = self.mock_append.call_args[0][3]
        assert "<p>" not in out_df["title"].iloc[0]
        assert "<b>" not in out_df["title"].iloc[1]

    def test_target_columns_correct(self):
        """Output có đúng các cột theo ClickHouse schema."""
        transform_news()

        out_df = self.mock_append.call_args[0][3]
        expected_cols = [
            "article_id", "title", "description", "content",
            "url", "image_url", "source_name", "source_url",
            "published_at", "search_query", "extracted_at",
        ]
        assert list(out_df.columns) == expected_cols

    def test_no_symbols_column_in_output(self):
        """Cột symbols (dùng để filter) không có trong output."""
        transform_news()

        out_df = self.mock_append.call_args[0][3]
        assert "symbols" not in out_df.columns

    def test_symbols_filter(self):
        """Filter theo symbols → chỉ giữ articles mention symbols đó."""
        transform_news(symbols=["BTCUSDT"])

        out_df = self.mock_append.call_args[0][3]
        # Chỉ article đầu tiên mention bitcoin
        assert len(out_df) == 1
        assert "Bitcoin" in out_df.iloc[0]["title"]

    def test_symbols_filter_no_match(self):
        """Filter symbols không match → không ghi, không crash."""
        transform_news(symbols=["NONEXIST"])

        self.mock_append.assert_not_called()

    def test_dedup_removes_duplicates(self):
        """Duplicate article_id bị loại bỏ."""
        import scripts.transform_modules.transform_news as mod

        dup_df = pd.concat([SAMPLE_RAW_DF, SAMPLE_RAW_DF.iloc[[0]]], ignore_index=True)
        mod.storage.download_parquet.return_value = pa.Table.from_pandas(dup_df)

        transform_news()

        out_df = self.mock_append.call_args[0][3]
        assert len(out_df) == 3  # 3 unique, 1 dup removed

    def test_empty_partition_skipped(self):
        """Partition rỗng → skip, không crash."""
        import scripts.transform_modules.transform_news as mod

        empty_df = pd.DataFrame(columns=SAMPLE_RAW_DF.columns)
        mod.storage.download_parquet.return_value = pa.Table.from_pandas(empty_df)

        transform_news()
        self.mock_append.assert_not_called()

    def test_no_partitions_found(self):
        """Không có partition → return sớm, không crash."""
        self.mock_discover.return_value = []
        transform_news()
        self.mock_append.assert_not_called()

    def test_month_str_filter(self):
        """month_str → chỉ xử lý tháng đó."""
        transform_news(month_str="2025-09")
        self.mock_discover.assert_not_called()
        self.mock_exists.assert_called_once()

    def test_all_partitions_fail_raises(self):
        """Tất cả partitions đều lỗi → TransformError."""
        import scripts.transform_modules.transform_news as mod

        mod.storage.object_exists = MagicMock(side_effect=Exception("MinIO down"))

        with pytest.raises(TransformError):
            transform_news(month_str="2025-09")
