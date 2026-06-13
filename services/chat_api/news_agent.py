"""News & Sentiment Agent — crypto news analysis.

Specializes in news sentiment, catalysts, and risk assessment.
"""

from __future__ import annotations

from typing import Any

from langchain_core.messages import SystemMessage
from langchain_core.tools import tool

from config.llm_config import TIMEFRAME_CONFIG
from utils.logger import get_logger
import market_queries as mq

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@tool
def get_crypto_news(symbol: str, timeframe: str) -> str:
    """Fetch recent crypto news articles mentioning a symbol.

    Use this to assess market sentiment, identify catalysts (partnerships,
    regulations, hacks, listings), and evaluate news-driven risk.

    Args:
        symbol: Crypto trading pair (e.g. BTCUSDT, ETHUSDT).
        timeframe: One of 'short', 'medium', 'long', 'very_long'.
    """
    config = TIMEFRAME_CONFIG.get(timeframe, TIMEFRAME_CONFIG["medium"])
    df = mq.fetch_crypto_news(symbol, config)
    return mq.format_news(df)


NEWS_TOOLS = [get_crypto_news]


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

NEWS_SYSTEM_PROMPT = (
    "You are a specialized NEWS & SENTIMENT crypto agent.\n"
    "You focus ONLY on news analysis, market sentiment, and catalysts.\n\n"
    "YOUR EXPERTISE:\n"
    "- News sentiment: positive/negative/neutral tone analysis\n"
    "- Catalysts: partnerships, listings, regulatory news, hacks\n"
    "- Risk assessment: red flags, regulatory actions, security issues\n"
    "- Market narrative: what story is the market telling?\n\n"
    "RULES:\n"
    "- Call get_crypto_news to fetch fresh news before answering.\n"
    "- Identify the most impactful news items and explain why they matter.\n"
    "- Assess overall sentiment: bullish, bearish, or neutral.\n"
    "- Flag any high-risk events (hacks, delistings, regulatory actions).\n"
    "- Do NOT provide technical analysis — that's for the technical agent.\n"
    "- Answer in the same language the user uses.\n"
)


# ---------------------------------------------------------------------------
# Agent node
# ---------------------------------------------------------------------------

async def news_agent_node(state: dict[str, Any]) -> dict[str, Any]:
    """News & sentiment analysis agent."""
    from nodes import _get_llm_with_tools

    try:
        llm_with_tools = _get_llm_with_tools(NEWS_TOOLS)

        messages = [SystemMessage(content=NEWS_SYSTEM_PROMPT)]
        for msg in state["messages"]:
            if not isinstance(msg, SystemMessage):
                messages.append(msg)

        response = await llm_with_tools.ainvoke(messages)
        return {"messages": [response]}
    except Exception as exc:
        logger.error("News agent failed: %s", exc)
        from langchain_core.messages import AIMessage
        return {"messages": [AIMessage(content=f"News analysis unavailable: {exc}")]}
