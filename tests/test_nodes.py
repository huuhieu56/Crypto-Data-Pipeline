# =============================================================================
# Test Multi-Agent System
# =============================================================================
# Unit tests cho services/chat_api/ (nodes, agents, supervisor)
#
# Chạy: pytest tests/test_nodes.py -v
# =============================================================================

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
# LLM Factory
# ============================================================================
class TestGetLlm:

    def setup_method(self):
        import services.chat_api.nodes as nodes_mod
        nodes_mod._llm_instance = None
        nodes_mod._llm_bound_cache = {}

    @patch("services.chat_api.nodes.LLM_BASE_URL", "https://api.example.com/v1")
    @patch("services.chat_api.nodes.LLM_API_KEY", "test-key")
    @patch("services.chat_api.nodes.LLM_MODEL", "test-model")
    def test_creates_chatopenai_with_base_url(self):
        import services.chat_api.nodes as nodes_mod
        mock_cls = MagicMock()
        with patch.dict("sys.modules", {"langchain_openai": MagicMock(ChatOpenAI=mock_cls)}):
            nodes_mod._llm_instance = None
            from services.chat_api.nodes import _get_llm
            llm = _get_llm()
            assert llm is not None
            mock_cls.assert_called_once()

    def test_singleton_returns_same_instance(self):
        from services.chat_api.nodes import _get_llm
        import services.chat_api.nodes as nodes_mod
        fake_llm = MagicMock()
        nodes_mod._llm_instance = fake_llm
        result = _get_llm()
        assert result is fake_llm

    def test_bind_tools_cached(self):
        from services.chat_api.nodes import _get_llm_with_tools
        import services.chat_api.nodes as nodes_mod

        fake_llm = MagicMock()
        fake_bound = MagicMock()
        fake_llm.bind_tools.return_value = fake_bound
        nodes_mod._llm_instance = fake_llm

        t1 = MagicMock(); t1.name = "tool_a"
        t2 = MagicMock(); t2.name = "tool_b"
        tools = [t1, t2]
        result1 = _get_llm_with_tools(tools)
        result2 = _get_llm_with_tools(tools)

        assert result1 is result2
        fake_llm.bind_tools.assert_called_once()


# ============================================================================
# load_history
# ============================================================================
class TestLoadHistory:

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.ch_query_df_params")
    async def test_builds_messages_with_history(self, mock_query):
        from services.chat_api.nodes import load_history
        mock_query.return_value = pd.DataFrame({
            "role": ["user", "assistant"],
            "content": ["Hello", "Hi!"],
        })
        state = {"session_id": "s1", "symbol": "BTCUSDT", "user_message": "Price?"}
        result = await load_history(state)
        messages = result["messages"]
        assert len(messages) == 4
        assert isinstance(messages[0], SystemMessage)
        assert isinstance(messages[3], HumanMessage)
        assert messages[3].content == "Price?"

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.ch_query_df_params")
    async def test_empty_history(self, mock_query):
        from services.chat_api.nodes import load_history
        mock_query.return_value = pd.DataFrame()
        state = {"session_id": "s1", "symbol": "BTCUSDT", "user_message": "Hi"}
        result = await load_history(state)
        assert len(result["messages"]) == 2

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.ch_query_df_params")
    async def test_db_failure_graceful(self, mock_query):
        from services.chat_api.nodes import load_history
        mock_query.side_effect = Exception("DB down")
        state = {"session_id": "s1", "symbol": "BTCUSDT", "user_message": "Hi"}
        result = await load_history(state)
        assert len(result["messages"]) == 2


# ============================================================================
# save_history
# ============================================================================
class TestSaveHistory:

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.new_ch_client")
    async def test_extracts_reply_and_saves(self, mock_new_client):
        from services.chat_api.nodes import save_history
        mock_client = MagicMock()
        mock_new_client.return_value = mock_client

        state = {
            "session_id": "s1", "symbol": "BTCUSDT", "user_message": "Analyze",
            "messages": [AIMessage(content="BTC is bullish.")],
            "supervisor_decision": ["technical"], "supervisor_reason": "test",
            "_start_time": 0,
        }
        result = await save_history(state)
        assert result["reply"] == "BTC is bullish."
        mock_client.insert_df.assert_called()

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.new_ch_client")
    async def test_direct_reply_takes_priority(self, mock_new_client):
        from services.chat_api.nodes import save_history
        mock_client = MagicMock()
        mock_new_client.return_value = mock_client

        state = {
            "session_id": "s1", "symbol": "BTCUSDT", "user_message": "Hi",
            "messages": [AIMessage(content="ignored")],
            "direct_reply": "Hello from supervisor!",
            "supervisor_decision": [], "supervisor_reason": "greeting",
            "_start_time": 0,
        }
        result = await save_history(state)
        assert result["reply"] == "Hello from supervisor!"

    @pytest.mark.asyncio
    @patch("services.chat_api.nodes.new_ch_client")
    async def test_saves_agent_trace(self, mock_new_client):
        from services.chat_api.nodes import save_history
        mock_client = MagicMock()
        mock_new_client.return_value = mock_client

        state = {
            "session_id": "s1", "symbol": "BTCUSDT", "user_message": "test",
            "messages": [AIMessage(content="reply")],
            "supervisor_decision": ["technical", "news"],
            "supervisor_reason": "comprehensive",
            "_start_time": 0,
        }
        await save_history(state)
        # Should call insert_df twice: chat_history + agent_trace
        assert mock_client.insert_df.call_count == 2


# ============================================================================
# Technical Agent Tools
# ============================================================================
class TestTechnicalTools:

    @patch("technical_agent.mq")
    def test_get_price_candles(self, mock_mq):
        from technical_agent import get_price_candles
        candle_df = pd.DataFrame({
            "ts": pd.to_datetime(["2024-01-01"]),
            "open": [42000.0], "high": [42500.0],
            "low": [41800.0], "close": [42300.0],
            "volume": [100.0], "rsi_14": [55.0], "macd": [120.0],
        })
        mock_mq.fetch_candles.return_value = candle_df
        mock_mq.format_candles.return_value = "formatted"
        mock_mq._safe_float.return_value = 55.0
        mock_mq.compute_signals.return_value = {}
        mock_mq.format_signals.return_value = ""

        result = get_price_candles.invoke({"symbol": "BTCUSDT", "timeframe": "medium"})
        assert "formatted" in result
        assert "Summary" in result

    @patch("technical_agent.mq")
    def test_get_price_candles_empty(self, mock_mq):
        from technical_agent import get_price_candles
        mock_mq.fetch_candles.return_value = pd.DataFrame()
        result = get_price_candles.invoke({"symbol": "BTCUSDT", "timeframe": "short"})
        assert "No candle data" in result

    @patch("technical_agent.mq")
    def test_get_volume_and_liquidity(self, mock_mq):
        from technical_agent import get_volume_and_liquidity
        mock_mq.fetch_ticker_trend.return_value = pd.DataFrame()
        mock_mq.format_ticker_trend.return_value = "trend"
        mock_mq.fetch_latest_ticker.return_value = {
            "price_change_pct": 2.5, "volume_24h": 5000000.0, "spread_pct": 0.001,
        }
        result = get_volume_and_liquidity.invoke({"symbol": "BTCUSDT", "timeframe": "medium"})
        assert "trend" in result
        assert "+2.50%" in result

    @patch("technical_agent.mq")
    def test_get_orderbook_pressure(self, mock_mq):
        from technical_agent import get_orderbook_pressure
        mock_mq.fetch_orderbook_data.return_value = {"mode": "trend"}
        mock_mq.format_orderbook.return_value = "ob formatted"
        result = get_orderbook_pressure.invoke({"symbol": "BTCUSDT", "timeframe": "short"})
        assert result == "ob formatted"


# ============================================================================
# News Agent Tools
# ============================================================================
class TestNewsTools:

    @patch("news_agent.mq")
    def test_get_crypto_news(self, mock_mq):
        from news_agent import get_crypto_news
        mock_mq.fetch_crypto_news.return_value = pd.DataFrame()
        mock_mq.format_news.return_value = "news formatted"
        result = get_crypto_news.invoke({"symbol": "BTCUSDT", "timeframe": "medium"})
        assert result == "news formatted"


# ============================================================================
# Supervisor Routing
# ============================================================================
class TestSupervisor:

    def test_rule_based_technical(self):
        from supervisor import _rule_based_route
        assert _rule_based_route("Phân tích RSI BTC") == ["technical"]
        assert _rule_based_route("What is the MACD?") == ["technical"]
        assert _rule_based_route("Show me the order book") == ["technical"]

    def test_rule_based_news(self):
        from supervisor import _rule_based_route
        assert _rule_based_route("Tin tức BTC gần nhất") == ["news"]
        assert _rule_based_route("Any news about ETH?") == ["news"]

    def test_rule_based_comprehensive(self):
        from supervisor import _rule_based_route
        assert "technical" in _rule_based_route("Should I buy BTC?")
        assert "news" in _rule_based_route("Phân tích rủi ro đầu tư ETH")
        assert "deep" in _rule_based_route("Comprehensive analysis of SOL")

    def test_rule_based_greeting(self):
        from supervisor import _rule_based_route
        assert _rule_based_route("Xin chào") == []
        assert _rule_based_route("Hello") == []

    def test_rule_based_ambiguous_returns_none(self):
        from supervisor import _rule_based_route
        assert _rule_based_route("Tell me about blockchain") is None

    def test_route_after_supervisor_with_agents(self):
        from supervisor import route_after_supervisor
        state = {"supervisor_decision": ["technical", "news"]}
        assert route_after_supervisor(state) == "technical"

    def test_route_after_supervisor_empty(self):
        from supervisor import route_after_supervisor
        state = {"supervisor_decision": []}
        assert route_after_supervisor(state) == "direct_response"

    def test_route_after_technical_to_news(self):
        from supervisor import route_after_technical
        msg = AIMessage(content="done")
        state = {"messages": [msg], "supervisor_decision": ["technical", "news"]}
        assert route_after_technical(state) == "news"

    def test_route_after_technical_to_deep(self):
        from supervisor import route_after_technical
        msg = AIMessage(content="done")
        state = {"messages": [msg], "supervisor_decision": ["technical", "deep"]}
        assert route_after_technical(state) == "deep"

    def test_route_after_technical_to_save(self):
        from supervisor import route_after_technical
        msg = AIMessage(content="done")
        state = {"messages": [msg], "supervisor_decision": ["technical"]}
        assert route_after_technical(state) == "save_memory"

    def test_route_after_technical_with_tool_calls(self):
        from supervisor import route_after_technical
        msg = AIMessage(content="", tool_calls=[{"name": "x", "args": {}, "id": "1"}])
        state = {"messages": [msg], "supervisor_decision": ["technical"]}
        assert route_after_technical(state) == "tools"

    def test_route_after_news_to_deep(self):
        from supervisor import route_after_news
        msg = AIMessage(content="done")
        state = {"messages": [msg], "supervisor_decision": ["technical", "news", "deep"]}
        assert route_after_news(state) == "deep"

    def test_route_after_news_to_save(self):
        from supervisor import route_after_news
        msg = AIMessage(content="done")
        state = {"messages": [msg], "supervisor_decision": ["news"]}
        assert route_after_news(state) == "save_memory"

    def test_route_after_tools(self):
        from supervisor import route_after_tools
        assert route_after_tools({"_current_agent": "technical"}) == "technical"
        assert route_after_tools({"_current_agent": "news"}) == "news"


# ============================================================================
# TOOLS list
# ============================================================================
class TestToolsList:

    def test_technical_tools_count(self):
        from technical_agent import TECHNICAL_TOOLS
        assert len(TECHNICAL_TOOLS) == 3

    def test_news_tools_count(self):
        from news_agent import NEWS_TOOLS
        assert len(NEWS_TOOLS) == 1

    def test_all_tool_names(self):
        from technical_agent import TECHNICAL_TOOLS
        from news_agent import NEWS_TOOLS
        names = {t.name for t in TECHNICAL_TOOLS + NEWS_TOOLS}
        assert names == {"get_price_candles", "get_volume_and_liquidity", "get_orderbook_pressure", "get_crypto_news"}
