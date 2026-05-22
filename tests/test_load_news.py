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
from unittest.mock import MagicMock, patch

from scripts.load_modules.load_news import load_news


# ---------------------------------------------------------------------------
# TEST CASES
# ---------------------------------------------------------------------------


class TestLoadNews:
    """Tests cho load_news() — thin wrapper around _load_table."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        # Use sys.modules to get the actual module (not the re-exported function)
        self._mod = sys.modules["scripts.load_modules.load_news"]
        self.orig_load_table = self._mod._load_table
        yield
        self._mod._load_table = self.orig_load_table

    def test_calls_load_table_with_correct_args(self):
        """load_news() gọi _load_table với đúng args."""
        mock_load_table = MagicMock()
        self._mod._load_table = mock_load_table

        load_news()
        mock_load_table.assert_called_once_with(
            None, "crypto-processed", "crypto_news", "published_at",
            month_str=None, per_symbol=False, sub_prefix="gnews",
        )

    def test_passes_month_str(self):
        """month_str được truyền đúng."""
        mock_load_table = MagicMock()
        self._mod._load_table = mock_load_table

        load_news(month_str="2025-09")
        mock_load_table.assert_called_once_with(
            None, "crypto-processed", "crypto_news", "published_at",
            month_str="2025-09", per_symbol=False, sub_prefix="gnews",
        )

    def test_propagates_load_error(self):
        """LoadError từ _load_table được propagate."""
        from utils.exceptions import LoadError
        mock_load_table = MagicMock(side_effect=LoadError("all partitions failed"))
        self._mod._load_table = mock_load_table

        with pytest.raises(LoadError, match="all partitions failed"):
            load_news(month_str="2025-09")
