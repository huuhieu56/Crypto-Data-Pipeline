# =============================================================================
# Test Chat API (FastAPI Endpoints)
# =============================================================================
# Unit tests cho services/chat_api/main.py
#
# Chạy: pytest tests/test_chat_api.py -v
# =============================================================================

# ---------------------------------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "chat_api"))

from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# 2. FIXTURES
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    """TestClient cho FastAPI app -- mock compiled_graph trước khi import."""
    # Mock graph trước khi import main để tránh LangGraph import chain
    mock_graph = MagicMock()
    mock_graph.ainvoke = AsyncMock(return_value={
        "reply": "BTC looks bullish.",
        "context_summary": {"symbol": "BTCUSDT", "tools_used": [], "timeframes": []},
    })

    with patch.dict("sys.modules", {
        "graph": MagicMock(compiled_graph=mock_graph),
        "market_queries": MagicMock(),
        "nodes": MagicMock(),
    }):
        with patch("services.chat_api.main.compiled_graph", mock_graph):
            from services.chat_api.main import app
            from fastapi.testclient import TestClient
            yield TestClient(app), mock_graph


# ============================================================================
# 3. TEST CASES
# ============================================================================


# ============================================================================
# 3.1 Health Endpoint
# ============================================================================
class TestHealthEndpoint:
    """Tests cho GET /health."""

    def test_returns_ok(self, client):
        tc, _ = client
        resp = tc.get("/health")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "base_url" in data


# ============================================================================
# 3.2 Chat Endpoint
# ============================================================================
class TestChatEndpoint:
    """Tests cho POST /api/chat."""

    def test_happy_path(self, client):
        tc, mock_graph = client

        resp = tc.post("/api/chat", json={
            "session_id": "test-session",
            "symbol": "BTCUSDT",
            "message": "Analyze BTC",
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["reply"] == "BTC looks bullish."
        assert "context_summary" in data

    def test_empty_message_returns_400(self, client):
        tc, _ = client

        resp = tc.post("/api/chat", json={
            "session_id": "test",
            "symbol": "BTCUSDT",
            "message": "   ",
        })

        assert resp.status_code == 400

    def test_empty_session_id_returns_400(self, client):
        tc, _ = client

        resp = tc.post("/api/chat", json={
            "session_id": "  ",
            "symbol": "BTCUSDT",
            "message": "hello",
        })

        assert resp.status_code == 400

    def test_empty_symbol_returns_400(self, client):
        tc, _ = client

        resp = tc.post("/api/chat", json={
            "session_id": "test",
            "symbol": "  ",
            "message": "hello",
        })

        assert resp.status_code == 400

    def test_graph_failure_returns_502(self, client):
        tc, mock_graph = client
        mock_graph.ainvoke = AsyncMock(side_effect=RuntimeError("LLM timeout"))

        resp = tc.post("/api/chat", json={
            "session_id": "test",
            "symbol": "BTCUSDT",
            "message": "hello",
        })

        assert resp.status_code == 502
        assert "failed" in resp.json()["detail"].lower()

    def test_symbol_uppercased(self, client):
        tc, mock_graph = client

        tc.post("/api/chat", json={
            "session_id": "test",
            "symbol": "btcusdt",
            "message": "hi",
        })

        call_args = mock_graph.ainvoke.call_args[0][0]
        assert call_args["symbol"] == "BTCUSDT"

    def test_missing_required_field_returns_422(self, client):
        """Pydantic validation: thiếu field → 422."""
        tc, _ = client

        resp = tc.post("/api/chat", json={
            "symbol": "BTCUSDT",
            "message": "hi",
            # thiếu session_id
        })

        assert resp.status_code == 422


# ============================================================================
# 3.3 History Endpoints
# ============================================================================
class TestHistoryEndpoints:
    """Tests cho GET/DELETE /api/chat/history/{session_id}."""

    @patch("services.chat_api.main.ch_query_df_params")
    def test_get_history_returns_messages(self, mock_query, client):
        tc, _ = client
        mock_query.return_value = pd.DataFrame({
            "role": ["user", "assistant"],
            "content": ["Hi", "Hello!"],
        })

        resp = tc.get("/api/chat/history/test-session")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["messages"]) == 2
        assert data["messages"][0]["role"] == "user"
        assert data["messages"][1]["content"] == "Hello!"

    @patch("services.chat_api.main.ch_query_df_params")
    def test_get_history_empty_session(self, mock_query, client):
        tc, _ = client
        mock_query.return_value = pd.DataFrame()

        resp = tc.get("/api/chat/history/empty-session")

        assert resp.status_code == 200
        assert resp.json()["messages"] == []

    @patch("services.chat_api.main.ch_query_df_params")
    def test_get_history_db_error_returns_500(self, mock_query, client):
        tc, _ = client
        mock_query.side_effect = Exception("Connection refused")

        resp = tc.get("/api/chat/history/err-session")

        assert resp.status_code == 500

    @patch("services.chat_api.main.new_ch_client")
    def test_delete_history(self, mock_new_client, client):
        tc, _ = client
        mock_client = MagicMock()
        mock_new_client.return_value = mock_client

        resp = tc.delete("/api/chat/history/test-session")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "deleted"
        assert data["session_id"] == "test-session"
        mock_client.command.assert_called_once()

    @patch("services.chat_api.main.new_ch_client")
    def test_delete_history_db_error_returns_500(self, mock_new_client, client):
        tc, _ = client
        mock_new_client.side_effect = Exception("DB unreachable")

        resp = tc.delete("/api/chat/history/err-session")

        assert resp.status_code == 500
