# =============================================================================
# Test Nodes Module
# =============================================================================
# Unit tests cho services/chat_api/nodes.py
#
# Chạy: pytest tests/test_nodes.py -v
# =============================================================================

# ---------------------------------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "chat_api"))

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage


# ============================================================================
# 2. TEST CASES
# ============================================================================


# ============================================================================
# 2.1 _get_llm() — LLM factory
# ============================================================================
class TestGetLlm:
    """Tests cho _get_llm singleton factory."""

    def setup_method(self):
        """Reset singleton trước mỗi test."""
        import services.chat_api.nodes as nodes_mod
        nodes_mod._llm_instance = None

    @patch("services.chat_api.nodes.LLM_BASE_URL", "")
    @patch("services.chat_api.nodes.LLM_PROVIDER", "gemini")
    @patch("services.chat_api.nodes.LLM_API_KEY", "test-key")
    @patch("services.chat_api.nodes.LLM_MODEL", "gemini-2.5-flash-lite")
    def test_gemini_provider(self):
        with patch("services.chat_api.nodes.ChatGoogleGenerativeAI", create=True) as MockGemini:
            # Phải patch import trong module
            import services.chat_api.nodes as nodes_mod
            mock_cls = MagicMock()
            with patch.dict("sys.modules", {"langchain_google_genai": MagicMock(ChatGoogleGenerativeAI=mock_cls)}):
                nodes_mod._llm_instance = None
                from services.chat_api.nodes import _get_llm
                nodes_mod._llm_instance = None
                llm = _get_llm()
                assert llm is not None

    @patch("services.chat_api.nodes.LLM_BASE_URL", "https://api.deepseek.com/v1")
    @patch("services.chat_api.nodes.LLM_API_KEY", "sk-test")
    @patch("services.chat_api.nodes.LLM_MODEL", "deepseek-chat")
    def test_deepseek_url_uses_chat_deepseek(self):
        """Khi LLM_BASE_URL chứa deepseek → dùng ChatDeepSeek."""
        import services.chat_api.nodes as nodes_mod
        mock_cls = MagicMock()
        with patch.dict("sys.modules", {"langchain_deepseek": MagicMock(ChatDeepSeek=mock_cls)}):
            nodes_mod._llm_instance = None
            from services.chat_api.nodes import _get_llm
            llm = _get_llm()
            assert llm is not None
            mock_cls.assert_called_once()

    @patch("services.chat_api.nodes.LLM_BASE_URL", "https://api.groq.com/openai/v1")
    @patch("services.chat_api.nodes.LLM_API_KEY", "gsk-test")
    @patch("services.chat_api.nodes.LLM_MODEL", "llama-3.3-70b")
    def test_non_deepseek_url_uses_openai_compatible(self):
        """Khi LLM_BASE_URL không phải deepseek → dùng ChatOpenAI(base_url=...)."""
        import services.chat_api.nodes as nodes_mod
        mock_cls = MagicMock()
        with patch.dict("sys.modules", {"langchain_openai": MagicMock(ChatOpenAI=mock_cls)}):
            nodes_mod._llm_instance = None
            from services.chat_api.nodes import _get_llm
            llm = _get_llm()
            assert llm is not None
            mock_cls.assert_called_once()
            call_kwargs = mock_cls.call_args[1]
            assert call_kwargs["base_url"] == "https://api.groq.com/openai/v1"

    @patch("services.chat_api.nodes.LLM_BASE_URL", "")
    @patch("services.chat_api.nodes.LLM_PROVIDER", "openai")
    @patch("services.chat_api.nodes.LLM_API_KEY", "sk-test")
    @patch("services.chat_api.nodes.LLM_MODEL", "gpt-4o")
    def test_default_falls_back_to_openai(self):
        """Không có base_url + provider != gemini → ChatOpenAI default."""
        import services.chat_api.nodes as nodes_mod
        mock_cls = MagicMock()
        with patch.dict("sys.modules", {"langchain_openai": MagicMock(ChatOpenAI=mock_cls)}):
            nodes_mod._llm_instance = None
            from services.chat_api.nodes import _get_llm
            llm = _get_llm()
            assert llm is not None
            mock_cls.assert_called_once()

    @patch("services.chat_api.nodes.LLM_PROVIDER", "gemini")
    def test_singleton_returns_same_instance(self):
        """Gọi _get_llm 2 lần → trả về cùng một instance."""
        from services.chat_api.nodes import _get_llm
        import services.chat_api.nodes as nodes_mod

        fake_llm = MagicMock()
        nodes_mod._llm_instance = fake_llm

        result = _get_llm()
        assert result is fake_llm


# ============================================================================
# 2.2 should_continue() — router
# ============================================================================
class TestShouldContinue:
    """Tests cho should_continue router."""

    def test_routes_to_tools_when_tool_calls_present(self):
        from services.chat_api.nodes import should_continue

        msg = AIMessage(content="", tool_calls=[{"name": "get_price_candles", "args": {}, "id": "1"}])
        state = {"messages": [msg]}

        assert should_continue(state) == "tools"

    def test_routes_to_save_history_when_no_tool_calls(self):
        from services.chat_api.nodes import should_continue

        msg = AIMessage(content="BTC is trending up.")
        state = {"messages": [msg]}

        assert should_continue(state) == "save_history"

    def test_routes_to_save_history_when_empty_tool_calls(self):
        from services.chat_api.nodes import should_continue

        msg = AIMessage(content="done", tool_calls=[])
        state = {"messages": [msg]}

        assert should_continue(state) == "save_history"

    def test_routes_to_save_history_for_human_message(self):
        """HumanMessage không có tool_calls → save_history."""
        from services.chat_api.nodes import should_continue

        msg = HumanMessage(content="hi")
        state = {"messages": [msg]}

        assert should_continue(state) == "save_history"


# ============================================================================
# 2.3 load_history() — node
# ============================================================================
class TestLoadHistory:
    """Tests cho load_history node -- mock ClickHouse."""

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.ch_query_df_params")
    async def test_builds_messages_with_history(self, mock_query):
        from services.chat_api.nodes import load_history

        mock_query.return_value = pd.DataFrame({
            "role": ["user", "assistant"],
            "content": ["Hello", "Hi! How can I help?"],
        })

        state = {
            "session_id": "test-session",
            "symbol": "BTCUSDT",
            "user_message": "What's the price?",
        }

        result = await load_history(state)

        messages = result["messages"]
        # SystemMessage + 2 history + 1 new user message
        assert len(messages) == 4
        assert isinstance(messages[0], SystemMessage)
        assert isinstance(messages[1], HumanMessage)
        assert messages[1].content == "Hello"
        assert isinstance(messages[2], AIMessage)
        assert messages[2].content == "Hi! How can I help?"
        assert isinstance(messages[3], HumanMessage)
        assert messages[3].content == "What's the price?"

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.ch_query_df_params")
    async def test_empty_history(self, mock_query):
        from services.chat_api.nodes import load_history

        mock_query.return_value = pd.DataFrame()

        state = {
            "session_id": "new-session",
            "symbol": "ETHUSDT",
            "user_message": "Tell me about ETH",
        }

        result = await load_history(state)

        messages = result["messages"]
        # SystemMessage + 1 user message (no history)
        assert len(messages) == 2
        assert isinstance(messages[0], SystemMessage)
        assert "ETHUSDT" in messages[0].content

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.ch_query_df_params")
    async def test_db_failure_falls_back_gracefully(self, mock_query):
        from services.chat_api.nodes import load_history

        mock_query.side_effect = Exception("Connection refused")

        state = {
            "session_id": "test",
            "symbol": "BTCUSDT",
            "user_message": "Hi",
        }

        result = await load_history(state)

        messages = result["messages"]
        # Vẫn có SystemMessage + user message dù DB fail
        assert len(messages) == 2

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.ch_query_df_params")
    async def test_system_prompt_contains_symbol(self, mock_query):
        from services.chat_api.nodes import load_history

        mock_query.return_value = pd.DataFrame()

        state = {
            "session_id": "s1",
            "symbol": "SOLUSDT",
            "user_message": "hi",
        }

        result = await load_history(state)

        assert "SOLUSDT" in result["messages"][0].content


# ============================================================================
# 2.4 save_history() — node
# ============================================================================
class TestSaveHistory:
    """Tests cho save_history node — mock ClickHouse insert."""

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.new_ch_client")
    async def test_extracts_reply_and_saves(self, mock_new_client):
        from services.chat_api.nodes import save_history
        mock_client = MagicMock()
        mock_new_client.return_value = mock_client

        state = {
            "session_id": "sess-1",
            "symbol": "BTCUSDT",
            "user_message": "Analyze BTC",
            "messages": [
                SystemMessage(content="system"),
                HumanMessage(content="Analyze BTC"),
                AIMessage(content="BTC is bullish."),
            ],
        }

        result = await save_history(state)

        assert result["reply"] == "BTC is bullish."
        assert result["context_summary"]["symbol"] == "BTCUSDT"
        # Saved 2 rows (user + assistant)
        mock_client.insert_df.assert_called_once()
        saved_df = mock_client.insert_df.call_args[0][1]
        assert len(saved_df) == 2
        assert saved_df.iloc[0]["role"] == "user"
        assert saved_df.iloc[1]["role"] == "assistant"

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.new_ch_client")
    async def test_fallback_reply_on_empty_messages(self, mock_new_client):
        from services.chat_api.nodes import save_history
        mock_client = MagicMock()
        mock_new_client.return_value = mock_client

        state = {
            "session_id": "sess-2",
            "symbol": "BTCUSDT",
            "user_message": "Hi",
            "messages": [
                SystemMessage(content="system"),
                HumanMessage(content="Hi"),
            ],
        }

        result = await save_history(state)

        assert "could not generate" in result["reply"].lower()

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.new_ch_client")
    async def test_context_summary_tracks_tool_calls(self, mock_new_client):
        from services.chat_api.nodes import save_history
        mock_client = MagicMock()
        mock_new_client.return_value = mock_client

        ai_msg = AIMessage(content="", tool_calls=[
            {"name": "get_price_candles", "args": {"symbol": "BTCUSDT", "timeframe": "short"}, "id": "1"},
            {"name": "get_orderbook_pressure", "args": {"symbol": "BTCUSDT", "timeframe": "medium"}, "id": "2"},
        ])

        state = {
            "session_id": "sess-3",
            "symbol": "BTCUSDT",
            "user_message": "Analyze",
            "messages": [
                ai_msg,
                AIMessage(content="Analysis complete."),
            ],
        }

        result = await save_history(state)

        ctx = result["context_summary"]
        assert "get_price_candles" in ctx["tools_used"]
        assert "get_orderbook_pressure" in ctx["tools_used"]
        assert set(ctx["timeframes"]) == {"short", "medium"}

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.new_ch_client")
    async def test_db_failure_does_not_crash(self, mock_new_client):
        from services.chat_api.nodes import save_history

        mock_new_client.side_effect = Exception("DB down")

        state = {
            "session_id": "sess-4",
            "symbol": "BTCUSDT",
            "user_message": "test",
            "messages": [AIMessage(content="reply")],
        }

        # Không raise exception
        result = await save_history(state)
        assert result["reply"] == "reply"


# ============================================================================
# 2.5 agent_node() — node
# ============================================================================
class TestAgentNode:
    """Tests cho agent_node — mock LLM."""

    @pytest.mark.asyncio
    async def test_invokes_llm_with_tools(self):
        from services.chat_api.nodes import agent_node
        import services.chat_api.nodes as nodes_mod

        mock_response = AIMessage(content="response")
        mock_llm = MagicMock()
        mock_llm_with_tools = MagicMock()
        mock_llm_with_tools.ainvoke = AsyncMock(return_value=mock_response)
        mock_llm.bind_tools.return_value = mock_llm_with_tools

        with patch.object(nodes_mod, "_get_llm", return_value=mock_llm):
            state = {"messages": [HumanMessage(content="hi")]}
            result = await agent_node(state)

        assert result["messages"] == [mock_response]
        mock_llm.bind_tools.assert_called_once()
        mock_llm_with_tools.ainvoke.assert_called_once()


# ============================================================================
# 2.6 Tool definitions
# ============================================================================
class TestToolDefinitions:
    """Tests cho tool functions — mock market_queries."""

    @patch("services.chat_api.nodes.mq")
    def test_get_price_candles_happy_path(self, mock_mq):
        from services.chat_api.nodes import get_price_candles

        candle_df = pd.DataFrame({
            "ts": pd.to_datetime(["2024-01-01"]),
            "open": [42000.0], "high": [42500.0],
            "low": [41800.0], "close": [42300.0],
            "volume": [100.0], "rsi_14": [55.0],
            "macd": [120.0], "macd_signal": [100.0],
        })
        mock_mq.fetch_candles.return_value = candle_df
        mock_mq.format_candles.return_value = "formatted candles"
        mock_mq._safe_float.return_value = 55.0

        result = get_price_candles.invoke({"symbol": "BTCUSDT", "timeframe": "medium"})

        assert "formatted candles" in result
        assert "Summary" in result
        mock_mq.fetch_candles.assert_called_once()

    @patch("services.chat_api.nodes.mq")
    def test_get_price_candles_empty_data(self, mock_mq):
        from services.chat_api.nodes import get_price_candles

        mock_mq.fetch_candles.return_value = pd.DataFrame()

        result = get_price_candles.invoke({"symbol": "BTCUSDT", "timeframe": "short"})

        assert "No candle data" in result

    @patch("services.chat_api.nodes.mq")
    def test_get_volume_and_liquidity(self, mock_mq):
        from services.chat_api.nodes import get_volume_and_liquidity

        mock_mq.fetch_ticker_trend.return_value = pd.DataFrame()
        mock_mq.format_ticker_trend.return_value = "ticker trend"
        mock_mq.fetch_latest_ticker.return_value = {
            "price_change_pct": 2.5,
            "volume_24h": 5000000.0,
            "spread_pct": 0.001,
        }

        result = get_volume_and_liquidity.invoke({"symbol": "BTCUSDT", "timeframe": "medium"})

        assert "ticker trend" in result
        assert "Latest 24h snapshot" in result
        assert "+2.50%" in result

    @patch("services.chat_api.nodes.mq")
    def test_get_orderbook_pressure(self, mock_mq):
        from services.chat_api.nodes import get_orderbook_pressure

        mock_mq.fetch_orderbook_data.return_value = {"mode": "trend"}
        mock_mq.format_orderbook.return_value = "orderbook formatted"

        result = get_orderbook_pressure.invoke({"symbol": "BTCUSDT", "timeframe": "short"})

        assert result == "orderbook formatted"
        mock_mq.fetch_orderbook_data.assert_called_once()

    def test_tools_list_has_five_entries(self):
        from services.chat_api.nodes import TOOLS

        assert len(TOOLS) == 5
        names = [t.name for t in TOOLS]
        assert "get_price_candles" in names
        assert "get_volume_and_liquidity" in names
        assert "get_orderbook_pressure" in names
        assert "get_funding_rate" in names
        assert "get_open_interest" in names
