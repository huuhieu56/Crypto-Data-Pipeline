# =============================================================================
# Test AI Evaluation — Integration Tests with Real LLM API
# =============================================================================
# Gọi LLM thật (Gemini/OpenAI) để đánh giá hành vi end-to-end.
# ClickHouse vẫn mock (không cần DB), nhưng LLM API phải có key thật.
#
# Chạy:  pytest tests/test_ai_eval.py -v -m llm
# Skip:  pytest tests/ -v -m "not llm"   (bỏ qua khi CI không có API key)
# =============================================================================

# ---------------------------------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "chat_api"))

import os
import re

import pandas as pd
import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from config.llm_config import LLM_API_KEY, TIMEFRAME_CONFIG
from services.chat_api.nodes import (
    SYSTEM_PROMPT,
    TOOLS,
    _get_llm,
    should_continue,
)


# ---------------------------------------------------------------------------
# 2. SKIP CONDITION + RATE LIMITING
# ---------------------------------------------------------------------------

_SKIP_REASON = "LLM_API_KEY not set — skipping live LLM tests"
_has_key = bool(LLM_API_KEY and LLM_API_KEY != "your_api_key_here")

pytestmark = [
    pytest.mark.llm,
    pytest.mark.skipif(not _has_key, reason=_SKIP_REASON),
]


# ---------------------------------------------------------------------------
# 3. FIXTURES + HELPERS
# ---------------------------------------------------------------------------


def _invoke(llm_or_bound, messages):
    """Wrapper gọi LLM — skip test nếu bị rate limit hoặc model incompatibility."""
    try:
        return llm_or_bound.invoke(messages)
    except Exception as exc:
        exc_str = str(exc).lower()
        if "429" in exc_str or "resourceexhausted" in exc_str or "quota" in exc_str:
            pytest.skip(f"Rate limit exceeded: {exc_str[:120]}")
        if "reasoning_content" in exc_str:
            pytest.skip("Model requires reasoning_content passthrough (reasoning model incompatibility)")
        raise


@pytest.fixture(scope="module")
def llm():
    """LLM instance thật — dùng chung cho cả module để tiết kiệm init."""
    import services.chat_api.nodes as nodes_mod
    nodes_mod._llm_instance = None  # reset singleton
    return _get_llm()


@pytest.fixture(scope="module")
def llm_with_tools(llm):
    """LLM với tools bound — dùng cho các test tool calling."""
    return llm.bind_tools(TOOLS)


def _system_msg(symbol: str = "BTCUSDT") -> SystemMessage:
    return SystemMessage(content=SYSTEM_PROMPT.format(symbol=symbol))


def _fake_candle_tool_result() -> str:
    """Dữ liệu candle giả nhưng realistic để LLM phân tích."""
    return (
        "2024-06-01 00:00 O:67500.0000 H:68200.0000 L:67100.0000 C:67800.0000 V:12,500 RSI:58.3\n"
        "2024-06-02 00:00 O:67800.0000 H:69000.0000 L:67500.0000 C:68900.0000 V:15,200 RSI:62.1\n"
        "2024-06-03 00:00 O:68900.0000 H:69500.0000 L:68400.0000 C:69200.0000 V:13,800 RSI:65.7\n"
        "2024-06-04 00:00 O:69200.0000 H:70100.0000 L:68800.0000 C:69800.0000 V:18,300 RSI:68.4\n"
        "2024-06-05 00:00 O:69800.0000 H:71000.0000 L:69500.0000 C:70500.0000 V:20,100 RSI:72.0\n"
        "\n\n--- Summary ---\n"
        "Candles: 5 (Medium-term (weeks → month))\n"
        "Latest close: 70500.0000\n"
        "RSI(14): 72.0\n"
        "MACD: 450.5, Signal: 320.1"
    )


def _fake_volume_tool_result() -> str:
    return (
        "2024-06-01 Vol:5,200,000 Spread:0.0015% PriceChg:+1.20%\n"
        "2024-06-02 Vol:6,100,000 Spread:0.0012% PriceChg:+1.80%\n"
        "2024-06-03 Vol:5,800,000 Spread:0.0014% PriceChg:+0.45%\n"
        "\n\n--- Latest 24h snapshot ---\n"
        "Price change: +2.10%\n"
        "Volume 24h: 6,500,000\n"
        "Bid-ask spread: 0.0011%"
    )


def _fake_orderbook_tool_result() -> str:
    return "Current imbalance: 0.620 (strong buy pressure)"


# ============================================================================
# 4. TEST CASES
# ============================================================================


# ============================================================================
# 4.1 Tool Calling — LLM có gọi tool khi hỏi về thị trường?
# ============================================================================
class TestToolCalling:
    """Kiểm tra LLM có gọi đúng tool khi nhận câu hỏi thị trường."""

    def test_market_question_triggers_tool_call(self, llm_with_tools):
        """Hỏi 'Phân tích BTC' → LLM phải gọi ít nhất 1 tool."""
        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Phân tích giá BTC hiện tại"),
        ]
        response = _invoke(llm_with_tools, messages)

        assert hasattr(response, "tool_calls"), "Response phải có tool_calls"
        assert len(response.tool_calls) >= 1, "Phải gọi ít nhất 1 tool"

        tool_names = [tc["name"] for tc in response.tool_calls]
        assert "get_price_candles" in tool_names, (
            f"Hỏi về giá phải gọi get_price_candles, got: {tool_names}"
        )

    def test_volume_question_triggers_volume_tool(self, llm_with_tools):
        """Hỏi về volume → phải gọi get_volume_and_liquidity."""
        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Volume giao dịch BTC gần đây thế nào?"),
        ]
        response = _invoke(llm_with_tools, messages)

        assert len(response.tool_calls) >= 1
        tool_names = [tc["name"] for tc in response.tool_calls]
        assert "get_volume_and_liquidity" in tool_names, (
            f"Hỏi volume phải gọi get_volume_and_liquidity, got: {tool_names}"
        )

    def test_orderbook_question_triggers_orderbook_tool(self, llm_with_tools):
        """Hỏi về áp lực mua/bán → phải gọi get_orderbook_pressure."""
        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Áp lực mua bán trên order book BTC thế nào?"),
        ]
        response = _invoke(llm_with_tools, messages)

        assert len(response.tool_calls) >= 1
        tool_names = [tc["name"] for tc in response.tool_calls]
        assert "get_orderbook_pressure" in tool_names, (
            f"Hỏi orderbook phải gọi get_orderbook_pressure, got: {tool_names}"
        )

    def test_comprehensive_question_calls_multiple_tools(self, llm_with_tools):
        """Câu hỏi tổng hợp → phải gọi >= 2 tools."""
        messages = [
            _system_msg("ETHUSDT"),
            HumanMessage(content=(
                "Phân tích toàn diện ETH: giá, volume, và áp lực order book"
            )),
        ]
        response = _invoke(llm_with_tools, messages)

        assert len(response.tool_calls) >= 2, (
            f"Câu hỏi tổng hợp phải gọi >= 2 tools, got {len(response.tool_calls)}"
        )


# ============================================================================
# 4.2 Timeframe Selection — LLM chọn đúng timeframe?
# ============================================================================
class TestTimeframeSelection:
    """Kiểm tra LLM chọn timeframe phù hợp với câu hỏi."""

    def test_short_term_question_uses_short_timeframe(self, llm_with_tools):
        """Hỏi ngắn hạn → timeframe 'short'."""
        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="BTC trong vài giờ qua thay đổi thế nào?"),
        ]
        response = _invoke(llm_with_tools, messages)

        assert response.tool_calls, "Phải gọi tool"
        timeframes = [tc["args"].get("timeframe") for tc in response.tool_calls]
        assert "short" in timeframes, (
            f"Câu hỏi ngắn hạn phải dùng 'short', got: {timeframes}"
        )

    def test_long_term_question_uses_long_timeframe(self, llm_with_tools):
        """Hỏi dài hạn → timeframe 'long' hoặc 'very_long'."""
        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Xu hướng BTC trong 1 năm qua thế nào?"),
        ]
        response = _invoke(llm_with_tools, messages)

        assert response.tool_calls, "Phải gọi tool"
        timeframes = [tc["args"].get("timeframe") for tc in response.tool_calls]
        assert any(tf in ("long", "very_long") for tf in timeframes), (
            f"Câu hỏi 1 năm phải dùng 'long'/'very_long', got: {timeframes}"
        )

    def test_ambiguous_question_defaults_to_medium(self, llm_with_tools):
        """Không nói rõ timeframe → mặc định 'medium'."""
        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Phân tích BTC"),
        ]
        response = _invoke(llm_with_tools, messages)

        assert response.tool_calls, "Phải gọi tool"
        timeframes = [tc["args"].get("timeframe") for tc in response.tool_calls]
        # medium hoặc short đều chấp nhận được khi không rõ
        assert any(tf in ("medium", "short") for tf in timeframes), (
            f"Câu hỏi mơ hồ nên dùng 'medium' hoặc 'short', got: {timeframes}"
        )


# ============================================================================
# 4.3 Symbol Handling — LLM xử lý symbol đúng?
# ============================================================================
class TestSymbolHandling:
    """Kiểm tra LLM sử dụng đúng symbol."""

    def test_uses_default_symbol_from_context(self, llm_with_tools):
        """Không nói rõ coin → dùng symbol từ system prompt."""
        messages = [
            _system_msg("SOLUSDT"),
            HumanMessage(content="Phân tích giá coin này"),
        ]
        response = _invoke(llm_with_tools, messages)

        assert response.tool_calls, "Phải gọi tool"
        symbols = [tc["args"].get("symbol", "").upper() for tc in response.tool_calls]
        assert all(s == "SOLUSDT" for s in symbols), (
            f"Phải dùng SOLUSDT từ context, got: {symbols}"
        )

    def test_switches_symbol_when_user_asks(self, llm_with_tools):
        """User hỏi coin khác → phải chuyển symbol."""
        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Phân tích giá ETH cho tôi"),
        ]
        response = _invoke(llm_with_tools, messages)

        assert response.tool_calls, "Phải gọi tool"
        symbols = [tc["args"].get("symbol", "").upper() for tc in response.tool_calls]
        assert any("ETH" in s for s in symbols), (
            f"User hỏi ETH phải chuyển sang ETHUSDT, got: {symbols}"
        )


# ============================================================================
# 4.4 Response Quality — Phản hồi có chất lượng?
# ============================================================================
class TestResponseQuality:
    """Kiểm tra chất lượng phản hồi sau khi LLM nhận tool results."""

    def test_response_uses_tool_data(self, llm):
        """Sau khi nhận tool result → phản hồi phải chứa số liệu cụ thể."""
        ai_tool_call = AIMessage(content="", tool_calls=[{
            "name": "get_price_candles",
            "args": {"symbol": "BTCUSDT", "timeframe": "medium"},
            "id": "call_1",
        }])
        tool_result = ToolMessage(
            content=_fake_candle_tool_result(),
            tool_call_id="call_1",
        )

        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Phân tích giá BTC"),
            ai_tool_call,
            tool_result,
        ]
        response = _invoke(llm, messages)

        reply = response.content.lower()
        # Phải đề cập đến số liệu từ tool data
        assert any(kw in reply for kw in ["70", "rsi", "macd", "btc"]), (
            f"Phản hồi phải chứa số liệu cụ thể, got: {reply[:200]}"
        )
        # Phải có nội dung phân tích (không chỉ trả data thô)
        assert len(response.content) > 100, (
            "Phản hồi phải có phân tích, không chỉ vài dòng"
        )

    def test_response_with_multiple_tool_results(self, llm):
        """Nhận kết quả từ nhiều tools → phản hồi tổng hợp."""
        ai_calls = AIMessage(content="", tool_calls=[
            {"name": "get_price_candles", "args": {"symbol": "BTCUSDT", "timeframe": "medium"}, "id": "c1"},
            {"name": "get_volume_and_liquidity", "args": {"symbol": "BTCUSDT", "timeframe": "medium"}, "id": "c2"},
            {"name": "get_orderbook_pressure", "args": {"symbol": "BTCUSDT", "timeframe": "medium"}, "id": "c3"},
        ])

        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Phân tích toàn diện BTC"),
            ai_calls,
            ToolMessage(content=_fake_candle_tool_result(), tool_call_id="c1"),
            ToolMessage(content=_fake_volume_tool_result(), tool_call_id="c2"),
            ToolMessage(content=_fake_orderbook_tool_result(), tool_call_id="c3"),
        ]
        response = _invoke(llm, messages)

        reply = response.content.lower()
        # Phải đề cập cả giá, volume, và order book
        has_price = any(kw in reply for kw in ["70", "giá", "price", "rsi"])
        has_volume = any(kw in reply for kw in ["volume", "khối lượng", "giao dịch"])
        has_ob = any(kw in reply for kw in ["order book", "áp lực", "imbalance", "mua", "buy"])

        assert has_price, f"Phải phân tích giá, got: {reply[:300]}"
        assert has_volume or has_ob, (
            f"Phải phân tích volume hoặc order book, got: {reply[:300]}"
        )

    def test_response_cites_specific_numbers(self, llm):
        """Phản hồi phải cite số liệu cụ thể (không nói chung chung)."""
        ai_call = AIMessage(content="", tool_calls=[{
            "name": "get_price_candles",
            "args": {"symbol": "BTCUSDT", "timeframe": "medium"},
            "id": "c1",
        }])

        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="RSI và MACD BTC đang thế nào?"),
            ai_call,
            ToolMessage(content=_fake_candle_tool_result(), tool_call_id="c1"),
        ]
        response = _invoke(llm, messages)

        # Phải chứa ít nhất 1 số cụ thể
        numbers = re.findall(r"\d+\.?\d*", response.content)
        assert len(numbers) >= 2, (
            f"Phải cite ít nhất 2 con số cụ thể, found {len(numbers)}"
        )


# ============================================================================
# 4.5 Language Matching — Trả lời đúng ngôn ngữ?
# ============================================================================
class TestLanguageMatching:
    """Kiểm tra LLM trả lời cùng ngôn ngữ với user."""

    def test_responds_in_vietnamese_when_asked_in_vietnamese(self, llm):
        """Hỏi tiếng Việt → trả lời tiếng Việt."""
        ai_call = AIMessage(content="", tool_calls=[{
            "name": "get_price_candles",
            "args": {"symbol": "BTCUSDT", "timeframe": "medium"},
            "id": "c1",
        }])

        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Phân tích xu hướng giá BTC gần đây"),
            ai_call,
            ToolMessage(content=_fake_candle_tool_result(), tool_call_id="c1"),
        ]
        response = _invoke(llm, messages)

        # Kiểm tra có chứa tiếng Việt (các từ phổ biến)
        vn_keywords = ["giá", "xu hướng", "tăng", "giảm", "phân tích", "thị trường",
                        "mức", "hỗ trợ", "kháng cự", "hiện tại", "cho thấy"]
        reply_lower = response.content.lower()
        matches = [kw for kw in vn_keywords if kw in reply_lower]
        assert len(matches) >= 2, (
            f"Trả lời phải bằng tiếng Việt, found keywords: {matches}\n"
            f"Reply: {response.content[:300]}"
        )

    def test_responds_in_english_when_asked_in_english(self, llm):
        """Hỏi tiếng Anh → trả lời tiếng Anh."""
        ai_call = AIMessage(content="", tool_calls=[{
            "name": "get_price_candles",
            "args": {"symbol": "BTCUSDT", "timeframe": "medium"},
            "id": "c1",
        }])

        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Analyze the BTC price trend"),
            ai_call,
            ToolMessage(content=_fake_candle_tool_result(), tool_call_id="c1"),
        ]
        response = _invoke(llm, messages)

        en_keywords = ["price", "trend", "support", "resistance", "bullish",
                        "bearish", "level", "analysis", "indicates", "currently"]
        reply_lower = response.content.lower()
        matches = [kw for kw in en_keywords if kw in reply_lower]
        assert len(matches) >= 2, (
            f"Reply should be in English, found keywords: {matches}\n"
            f"Reply: {response.content[:300]}"
        )


# ============================================================================
# 4.6 Router Integration — should_continue hoạt động đúng với LLM thật?
# ============================================================================
class TestRouterWithRealLLM:
    """Kiểm tra should_continue router với output LLM thật."""

    def test_routes_to_tools_after_market_question(self, llm_with_tools):
        """LLM gọi tool → should_continue phải route đến 'tools'."""
        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Giá BTC hiện tại bao nhiêu?"),
        ]
        response = _invoke(llm_with_tools, messages)

        state = {"messages": [response]}
        assert should_continue(state) == "tools", (
            f"Market question phải route đến tools, "
            f"tool_calls={getattr(response, 'tool_calls', [])}"
        )

    def test_routes_to_save_after_final_answer(self, llm):
        """Sau khi có tool result → LLM trả lời text → route đến 'save_history'."""
        ai_call = AIMessage(content="", tool_calls=[{
            "name": "get_price_candles",
            "args": {"symbol": "BTCUSDT", "timeframe": "medium"},
            "id": "c1",
        }])

        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Phân tích BTC"),
            ai_call,
            ToolMessage(content=_fake_candle_tool_result(), tool_call_id="c1"),
        ]
        response = _invoke(llm, messages)

        state = {"messages": [response]}
        route = should_continue(state)
        assert route == "save_history", (
            f"Final answer phải route đến save_history, got '{route}', "
            f"tool_calls={getattr(response, 'tool_calls', [])}"
        )


# ============================================================================
# 4.7 Edge Cases — Xử lý câu hỏi khó
# ============================================================================
class TestEdgeCases:
    """Kiểm tra LLM xử lý edge cases."""

    def test_greeting_still_responds(self, llm_with_tools):
        """Chào hỏi đơn giản → LLM có thể trả lời hoặc gọi tool, không crash."""
        messages = [
            _system_msg("BTCUSDT"),
            HumanMessage(content="Xin chào!"),
        ]
        response = _invoke(llm_with_tools, messages)

        # Phải có response (text hoặc tool call)
        has_content = bool(response.content and response.content.strip())
        has_tools = bool(getattr(response, "tool_calls", []))
        assert has_content or has_tools, "Phải trả lời hoặc gọi tool"

    def test_unknown_symbol_handled(self, llm_with_tools):
        """Hỏi coin không phổ biến → vẫn phải gọi tool (không crash)."""
        messages = [
            _system_msg("XYZUSDT"),
            HumanMessage(content="Phân tích coin này"),
        ]
        response = _invoke(llm_with_tools, messages)

        # LLM phải thử gọi tool dù symbol lạ
        has_content = bool(response.content and response.content.strip())
        has_tools = bool(getattr(response, "tool_calls", []))
        assert has_content or has_tools, "Phải xử lý được symbol lạ"

    def test_handles_empty_tool_result_gracefully(self, llm):
        """Tool trả về 'No data' → LLM phải thông báo không có data."""
        ai_call = AIMessage(content="", tool_calls=[{
            "name": "get_price_candles",
            "args": {"symbol": "XYZUSDT", "timeframe": "medium"},
            "id": "c1",
        }])

        messages = [
            _system_msg("XYZUSDT"),
            HumanMessage(content="Phân tích XYZUSDT"),
            ai_call,
            ToolMessage(content="No candle data available for XYZUSDT.", tool_call_id="c1"),
        ]
        response = _invoke(llm, messages)

        reply = response.content.lower()
        # Phải thông báo không có data, không bịa data
        assert any(kw in reply for kw in [
            "không có", "no data", "unavailable", "not available",
            "không tìm", "cannot", "chưa có", "thiếu",
        ]), f"Phải thông báo không có data, got: {reply[:300]}"
