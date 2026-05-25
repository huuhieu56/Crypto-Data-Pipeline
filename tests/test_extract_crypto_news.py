# =============================================================================
# Test Extract Crypto News (GNews API)
# =============================================================================
# Unit tests cho scripts/extract_modules/extract_crypto_news.py
#
# Chạy: pytest tests/test_extract_crypto_news.py -v
# =============================================================================

# ---------------------------------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from scripts.extract_modules.extract_crypto_news import (
    extract_crypto_news,
    _fetch_articles,
    _parse_articles,
    _make_article_id,
)
from utils.exceptions import ExtractError


# ---------------------------------------------------------------------------
# 2. MOCK DATA
# ---------------------------------------------------------------------------

SAMPLE_GNEWS_RESPONSE = {
    "totalArticles": 2,
    "articles": [
        {
            "title": "Bitcoin Surges Past $100K as Institutional Demand Grows",
            "description": "Bitcoin has broken through the $100,000 barrier...",
            "content": "Bitcoin has broken through the $100,000 barrier as major institutions continue to invest heavily in cryptocurrency. Analysts predict further growth...",
            "url": "https://example.com/bitcoin-surges",
            "image": "https://example.com/images/bitcoin.jpg",
            "publishedAt": "2025-09-30T19:38:25Z",
            "source": {
                "name": "CoinDesk",
                "url": "https://coindesk.com",
            },
        },
        {
            "title": "Binance Launches New Trading Features for 2025",
            "description": "Binance has announced a suite of new trading tools...",
            "content": "Binance, the world's largest cryptocurrency exchange, has announced new features including advanced charting and AI-powered analysis...",
            "url": "https://example.com/binance-features",
            "image": "https://example.com/images/binance.jpg",
            "publishedAt": "2025-09-30T18:00:00Z",
            "source": {
                "name": "CryptoSlate",
                "url": "https://cryptoslate.com",
            },
        },
    ],
}

SAMPLE_EMPTY_RESPONSE = {"totalArticles": 0, "articles": []}


# ---------------------------------------------------------------------------
# 3. TEST CASES
# ---------------------------------------------------------------------------


# ===========================================================================
# 3.1 _make_article_id()
# ===========================================================================
class TestMakeArticleId:
    """Tests cho _make_article_id() — tạo dedup key từ URL."""

    def test_same_url_produces_same_id(self):
        """Cùng URL → cùng article_id (deterministic)."""
        a1 = _make_article_id({"url": "https://example.com/article-1"})
        a2 = _make_article_id({"url": "https://example.com/article-1"})
        assert a1 == a2

    def test_different_url_produces_different_id(self):
        """URL khác → article_id khác."""
        a1 = _make_article_id({"url": "https://example.com/article-1"})
        a2 = _make_article_id({"url": "https://example.com/article-2"})
        assert a1 != a2

    def test_missing_url_returns_hash_of_empty_string(self):
        """Không có URL → hash chuỗi rỗng (không crash)."""
        result = _make_article_id({})
        assert isinstance(result, str)
        assert len(result) == 32  # MD5 hex digest


# ===========================================================================
# 3.2 _parse_articles()
# ===========================================================================
class TestParseArticles:
    """Tests cho _parse_articles() — parse JSON thành records."""

    def test_happy_path_parses_all_fields(self):
        """Parse đúng tất cả fields từ GNews response."""
        articles = SAMPLE_GNEWS_RESPONSE["articles"]
        records = _parse_articles(articles, "bitcoin")

        assert len(records) == 2

        rec = records[0]
        assert rec["title"] == "Bitcoin Surges Past $100K as Institutional Demand Grows"
        assert rec["source_name"] == "CoinDesk"
        assert rec["search_query"] == "bitcoin"
        assert isinstance(rec["published_at"], datetime)
        assert isinstance(rec["extracted_at"], datetime)
        assert isinstance(rec["article_id"], str)

    def test_published_at_parsed_correctly(self):
        """publishedAt ISO 8601 → datetime UTC."""
        articles = SAMPLE_GNEWS_RESPONSE["articles"]
        records = _parse_articles(articles, "test")

        dt = records[0]["published_at"]
        assert dt.year == 2025
        assert dt.month == 9
        assert dt.day == 30
        assert dt.tzinfo is not None

    def test_truncates_long_fields(self):
        """Fields dài quá limit → bị truncate."""
        long_article = {
            "title": "X" * 1000,
            "description": "Y" * 2000,
            "content": "Z" * 5000,
            "url": "https://example.com/" + "a" * 1000,
            "image": "https://example.com/" + "b" * 1000,
            "publishedAt": "2025-01-01T00:00:00Z",
            "source": {"name": "N" * 500, "url": "https://example.com/" + "c" * 1000},
        }
        records = _parse_articles([long_article], "test")

        assert len(records[0]["title"]) == 500
        assert len(records[0]["description"]) == 1000
        assert len(records[0]["content"]) == 5000
        assert len(records[0]["url"]) == 500

    def test_missing_fields_default_to_empty_string(self):
        """Fields thiếu hoặc None → chuỗi rỗng, không crash."""
        minimal_article = {
            "publishedAt": "2025-01-01T00:00:00Z",
        }
        records = _parse_articles([minimal_article], "test")

        assert len(records) == 1
        assert records[0]["title"] == ""
        assert records[0]["description"] == ""
        assert records[0]["source_name"] == ""

    def test_invalid_published_at_defaults_to_now(self):
        """publishedAt không hợp lệ → fallback về thời gian hiện tại."""
        article = {
            "title": "Test",
            "publishedAt": "invalid-date",
            "source": {},
        }
        records = _parse_articles([article], "test")

        # Không crash, published_at vẫn là datetime
        assert isinstance(records[0]["published_at"], datetime)

    def test_empty_articles_list(self):
        """Danh sách articles rỗng → records rỗng."""
        records = _parse_articles([], "test")
        assert records == []


# ===========================================================================
# 3.3 _fetch_articles()
# ===========================================================================
class TestFetchArticles:
    """Tests cho _fetch_articles() — gọi GNews API."""

    @pytest.fixture(autouse=True)
    def _mod(self):
        self._mod = sys.modules["scripts.extract_modules.extract_crypto_news"]
        yield

    def _patch(self, **attrs):
        """Context manager to patch module attributes via sys.modules."""
        import contextlib

        @contextlib.contextmanager
        def _ctx():
            originals = {}
            for k, v in attrs.items():
                originals[k] = getattr(self._mod, k)
                setattr(self._mod, k, v)
            try:
                yield
            finally:
                for k, v in originals.items():
                    setattr(self._mod, k, v)

        return _ctx()

    def test_happy_path_returns_articles(self):
        """API trả về 200 → list of articles."""
        mock_get = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = SAMPLE_GNEWS_RESPONSE
        mock_get.return_value = mock_resp

        with self._patch(http_get_with_retry=mock_get, GNEWS_API_KEY="test-key"):
            articles = _fetch_articles("bitcoin", max_articles=10)

        assert len(articles) == 2
        mock_get.assert_called_once()

        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["q"] == "bitcoin"
        assert params["max"] == 10
        assert params["apikey"] == "test-key"
        assert params["lang"] == "en"

    def test_no_api_key_raises_extract_error(self):
        """API key trống → ExtractError."""
        with self._patch(GNEWS_API_KEY=""):
            with pytest.raises(ExtractError, match="GNews API key not configured"):
                _fetch_articles("bitcoin")

    def test_api_http_error_raises_extract_error(self):
        """API trả về HTTP error → ExtractError."""
        import requests as req

        mock_get = MagicMock(side_effect=req.HTTPError("403 Forbidden"))

        with self._patch(http_get_with_retry=mock_get, GNEWS_API_KEY="test-key"):
            with pytest.raises(ExtractError, match="GNews API request failed"):
                _fetch_articles("bitcoin")

    def test_network_timeout_raises_extract_error(self):
        """Network timeout → ExtractError."""
        import requests as req

        mock_get = MagicMock()
        mock_get.side_effect = req.Timeout("Connection timed out")

        with self._patch(http_get_with_retry=mock_get, GNEWS_API_KEY="test-key"):
            with pytest.raises(ExtractError, match="GNews API request failed"):
                _fetch_articles("bitcoin")

    def test_empty_response_returns_empty_list(self):
        """API trả về 0 articles → list rỗng."""
        mock_get = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_EMPTY_RESPONSE
        mock_get.return_value = mock_resp

        with self._patch(http_get_with_retry=mock_get, GNEWS_API_KEY="test-key"):
            articles = _fetch_articles("obscure-query")
        assert articles == []


# ===========================================================================
# 3.4 extract_crypto_news()
# ===========================================================================
class TestExtractCryptoNews:
    """Tests cho extract_crypto_news() — orchestrator chính."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        """Setup chung: mock append_to_partition via sys.modules."""
        self._mod = sys.modules["scripts.extract_modules.extract_crypto_news"]

        self.mock_append = MagicMock()
        self._orig_append = self._mod.append_to_partition
        self._mod.append_to_partition = self.mock_append
        yield
        self._mod.append_to_partition = self._orig_append

    def _patch_fetch(self, mock_fetch):
        """Temporarily replace _fetch_articles on the module."""
        orig = self._mod._fetch_articles
        self._mod._fetch_articles = mock_fetch
        return orig

    # --- Happy Path ---
    def test_happy_path_returns_counts(self):
        """Chạy thành công → trả về dict counts."""
        mock_fetch = MagicMock(return_value=SAMPLE_GNEWS_RESPONSE["articles"])
        orig = self._patch_fetch(mock_fetch)
        try:
            result = extract_crypto_news(queries=["bitcoin"])
        finally:
            self._mod._fetch_articles = orig

        assert result == {"articles": 2, "queries": 1}
        mock_fetch.assert_called_once_with("bitcoin", max_articles=10)
        self.mock_append.assert_called_once()

    def test_writes_to_correct_minio_path(self):
        """Ghi đúng bucket, prefix, partition key."""
        mock_fetch = MagicMock(return_value=SAMPLE_GNEWS_RESPONSE["articles"])
        orig = self._patch_fetch(mock_fetch)
        try:
            extract_crypto_news(queries=["bitcoin"])
        finally:
            self._mod._fetch_articles = orig

        call_args = self.mock_append.call_args[0]
        assert call_args[1] == "crypto_news"  # prefix
        assert call_args[2] == "gnews"        # partition key

    def test_dedup_col_is_article_id(self):
        """Dedup dùng article_id."""
        mock_fetch = MagicMock(return_value=SAMPLE_GNEWS_RESPONSE["articles"])
        orig = self._patch_fetch(mock_fetch)
        try:
            extract_crypto_news(queries=["bitcoin"])
        finally:
            self._mod._fetch_articles = orig

        call_kwargs = self.mock_append.call_args
        assert call_kwargs[1]["dedup_col"] == "article_id"

    def test_multiple_queries(self):
        """Nhiều queries → fetch nhiều lần."""
        mock_fetch = MagicMock(return_value=SAMPLE_GNEWS_RESPONSE["articles"])
        orig = self._patch_fetch(mock_fetch)
        try:
            result = extract_crypto_news(queries=["bitcoin", "ethereum"])
        finally:
            self._mod._fetch_articles = orig

        assert result == {"articles": 4, "queries": 2}
        assert mock_fetch.call_count == 2
        assert self.mock_append.call_count == 2

    # --- Sad Path ---
    def test_no_articles_found(self):
        """API trả về 0 articles → không ghi MinIO."""
        mock_fetch = MagicMock(return_value=[])
        orig = self._patch_fetch(mock_fetch)
        try:
            result = extract_crypto_news(queries=["obscure"])
        finally:
            self._mod._fetch_articles = orig

        assert result == {"articles": 0, "queries": 1}
        self.mock_append.assert_not_called()

    def test_api_key_missing_raises(self):
        """API key thiếu → ExtractError propagated."""
        mock_fetch = MagicMock(side_effect=ExtractError("GNews API key not configured"))
        orig = self._patch_fetch(mock_fetch)
        try:
            with pytest.raises(ExtractError):
                extract_crypto_news(queries=["bitcoin"])
        finally:
            self._mod._fetch_articles = orig

    def test_one_query_fails_others_continue(self):
        """Một query lỗi (non-ExtractError) → queries khác vẫn chạy."""
        mock_fetch = MagicMock(side_effect=[
            RuntimeError("Network error"),
            SAMPLE_GNEWS_RESPONSE["articles"],
        ])
        orig = self._patch_fetch(mock_fetch)
        try:
            result = extract_crypto_news(queries=["bad-query", "bitcoin"])
        finally:
            self._mod._fetch_articles = orig

        assert result == {"articles": 2, "queries": 2}
        assert mock_fetch.call_count == 2
        self.mock_append.assert_called_once()

    # --- Edge Cases ---
    def test_default_queries_from_config(self):
        """Không truyền queries → dùng GNEWS_SEARCH_QUERIES từ config."""
        mock_fetch = MagicMock(return_value=SAMPLE_GNEWS_RESPONSE["articles"])
        orig = self._patch_fetch(mock_fetch)
        orig_queries = self._mod.GNEWS_SEARCH_QUERIES
        self._mod.GNEWS_SEARCH_QUERIES = ["cryptocurrency OR bitcoin"]
        try:
            extract_crypto_news()

            mock_fetch.assert_called_once_with(
                "cryptocurrency OR bitcoin", max_articles=10,
            )
        finally:
            self._mod.GNEWS_SEARCH_QUERIES = orig_queries
            self._mod._fetch_articles = orig

    def test_dataframe_columns_correct(self):
        """DataFrame ghi vào MinIO có đúng 11 cột."""
        mock_fetch = MagicMock(return_value=SAMPLE_GNEWS_RESPONSE["articles"])
        orig = self._patch_fetch(mock_fetch)
        try:
            extract_crypto_news(queries=["bitcoin"])
        finally:
            self._mod._fetch_articles = orig

        call_args = self.mock_append.call_args[0]
        df = call_args[3]  # 4th positional arg = DataFrame

        expected_cols = {
            "article_id", "title", "description", "content",
            "url", "image_url", "source_name", "source_url",
            "published_at", "search_query", "extracted_at",
        }
        assert set(df.columns) == expected_cols
        assert len(df) == 2

