"""Supervisor — routes user queries to specialized agents.

Rule-based routing for clear patterns, LLM fallback for ambiguous cases.
"""

from __future__ import annotations

import json
import re
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Rule-based routing patterns
# ---------------------------------------------------------------------------

TECHNICAL_KEYWORDS = re.compile(
    r"(?i)(rsi|macd|volume|khối\s*lượng|orderbook|order\s*book|obi|"
    r"candle|ohlc|nến|support|resistance|hỗ\s*trợ|kháng\s*cự|"
    r"indicator|chỉ\s*báo|technical|chart|price\s*action|"
    r"momentum|divergence|overbought|quá\s*mua|oversold|quá\s*bán|"
    r"trend|xu\s*hướng|breakout|phá\s*vỡ|"
    r"moving\s*average|bollinger|fibonacci|elliott|wave|pattern|"
    r"ichimoku|stochastic|atr|adx|vwap|pivot|fib|"
    r"spread|liquidity|thanh\s*khoản|bid|ask|wall)"
)

NEWS_KEYWORDS = re.compile(
    r"(?i)(news|tin\s*tức|sentiment|tâm\s*lý|catalyst|regulation|"
    r"pháp\s*luật|partnership|hợp\s*tác|listing|delisting|"
    r"hack|security|bảo\s*mật|sec|cftc|etf|approval|chấp\s*thuận|"
    r"adoption|institutional|whale|cá\s*voi|on[\-\s]?chain|"
    r"announcement|công\s*bố|rumor|tin\s*đồn|fud|fomo|"
    r"sự\s*kiện|event|buổi\s*nói|conference|AMA)"
)

COMPREHENSIVE_KEYWORDS = re.compile(
    r"(?i)(should\s*i\s*(buy|sell)|nên\s*(mua|bán|giữ)\s*không|"
    r"(muốn|want\s*to)\s*(buy|mua|sell|bán|invest|đầu\s*tư)|"
    r"(phân\s*tích|rủi\s*ro|risk)\s*(mua|bán|đầu\s*tư|invest)|"
    r"comprehensive|toàn\s*diện|forecast|dự\s*đoán|prediction|"
    r"hold\s*long|giữ\s*lâu|target\s*price|price\s*goal)"
)

GREETING_KEYWORDS = re.compile(
    r"(?i)^(xin\s*chào|hello|hi|hey|chào|hế\s*lo|good\s*(morning|afternoon|evening)|"
    r"what\'s\s*up|howdy|yo|👋|cảm\s*on|thank|help|giúp|làm\s*quen)$"
)


# ---------------------------------------------------------------------------
# Routing logic
# ---------------------------------------------------------------------------

def _rule_based_route(user_msg: str) -> list[str] | None:
    """Try rule-based routing. Returns None if ambiguous."""
    msg = user_msg.strip()

    # Greeting → no agents
    if GREETING_KEYWORDS.match(msg):
        return []

    has_tech = bool(TECHNICAL_KEYWORDS.search(msg))
    has_news = bool(NEWS_KEYWORDS.search(msg))
    has_comp = bool(COMPREHENSIVE_KEYWORDS.search(msg))

    # Clear technical question
    if has_tech and not has_news and not has_comp:
        return ["technical"]

    # Clear news question
    if has_news and not has_tech and not has_comp:
        return ["news"]

    # Comprehensive: both technical + news + deep
    if has_comp or (has_tech and has_news):
        return ["technical", "news", "deep"]

    # Ambiguous — let LLM decide
    return None


# ---------------------------------------------------------------------------
# LLM-based routing (fallback)
# ---------------------------------------------------------------------------

ROUTING_PROMPT = """You are a routing supervisor for a crypto analysis system.
Analyze the user's question and decide which specialized agents to call.

Available agents:
- technical: Price action, RSI, MACD, volume, order book, support/resistance
- news: Crypto news, sentiment, catalysts, regulatory events, partnerships
- deep: Cross-validation, synthesis, final recommendations (used after other agents)

Respond with ONLY a JSON object:
{{"agents": ["technical"], "reason": "brief reason"}}

Rules:
- For pure technical questions (RSI, MACD, price, volume): ["technical"]
- For news/sentiment questions: ["news"]
- For buy/sell/risk/comprehensive questions: ["technical", "news", "deep"]
- For simple greetings or non-market questions: [] (no agents needed)
- Always include a brief reason"""


async def _llm_based_route(llm, user_msg: str, symbol: str) -> tuple[list[str], str]:
    """Use LLM to decide routing for ambiguous queries."""
    try:
        routing_messages = [
            SystemMessage(content=ROUTING_PROMPT),
            HumanMessage(content=f"User question: {user_msg}\nSymbol: {symbol}"),
        ]
        response = await llm.ainvoke(routing_messages)
        routing_text = response.content.strip()

        if "{" in routing_text:
            json_str = routing_text[routing_text.index("{"):routing_text.rindex("}") + 1]
            decision = json.loads(json_str)
            return decision.get("agents", ["technical"]), decision.get("reason", "")
    except Exception as exc:
        logger.warning("LLM routing failed: %s", exc)

    return ["technical", "news"], "fallback"


# ---------------------------------------------------------------------------
# Supervisor node
# ---------------------------------------------------------------------------

async def supervisor_node(state: dict[str, Any]) -> dict[str, Any]:
    """Supervisor: decide which agents to call."""
    from nodes import _get_llm

    llm = _get_llm()

    # Get user message
    user_msg = ""
    for msg in reversed(state["messages"]):
        if isinstance(msg, HumanMessage):
            user_msg = msg.content
            break

    symbol = state.get("symbol", "BTCUSDT")

    # Try rule-based routing first
    agents = _rule_based_route(user_msg)
    reason = "rule-based"

    # Fallback to LLM for ambiguous queries
    if agents is None:
        agents, reason = await _llm_based_route(llm, user_msg, symbol)

    logger.info("Supervisor routing: agents=%s reason=%s", agents, reason)

    # For non-market questions, generate direct reply
    direct_reply = None
    if not agents:
        direct_reply = await _generate_direct_reply(llm, user_msg)

    return {
        "supervisor_decision": agents,
        "supervisor_reason": reason,
        "direct_reply": direct_reply,
        "_start_time": state.get("_start_time"),
    }


async def _generate_direct_reply(llm, user_msg: str) -> str:
    """Generate a friendly reply for non-market questions."""
    try:
        messages = [
            SystemMessage(content=(
                "You are a friendly crypto market assistant. "
                "The user said something that doesn't require market analysis. "
                "Respond briefly and warmly, and suggest they ask about crypto markets. "
                "Answer in the same language the user uses."
            )),
            HumanMessage(content=user_msg),
        ]
        response = await llm.ainvoke(messages)
        return response.content
    except Exception as exc:
        logger.error("Direct reply failed: %s", exc)
        return "Hello! I'm your crypto market assistant. Ask me about prices, technical analysis, or news!"


# ---------------------------------------------------------------------------
# Routing functions (used by graph.py)
# ---------------------------------------------------------------------------

def route_after_supervisor(state: dict[str, Any]) -> str:
    """Route from supervisor to first agent or direct response."""
    agents = state.get("supervisor_decision", [])
    if not agents:
        return "direct_response"
    return agents[0]


def route_after_technical(state: dict[str, Any]) -> str:
    """After technical agent: tools → more agents → deep → save."""
    messages = state.get("messages", [])
    last = messages[-1] if messages else None

    if hasattr(last, "tool_calls") and last.tool_calls:
        return "tools"

    agents = state.get("supervisor_decision", [])
    if "news" in agents:
        return "news"

    if "deep" in agents:
        return "deep"

    return "save_memory"


def route_after_news(state: dict[str, Any]) -> str:
    """After news agent: tools → deep → save."""
    messages = state.get("messages", [])
    last = messages[-1] if messages else None

    if hasattr(last, "tool_calls") and last.tool_calls:
        return "tools"

    agents = state.get("supervisor_decision", [])
    if "deep" in agents:
        return "deep"

    return "save_memory"


def route_after_tools(state: dict[str, Any]) -> str:
    """After tools execute: return to the agent that called them."""
    current = state.get("_current_agent", "technical")
    return current
