"""LangGraph nodes and tools for the crypto chat assistant.

Uses function calling: the LLM decides which market data tools to invoke
and with what timeframe, rather than a rigid pre-classification pipeline.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import pandas as pd
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool

from config.llm_config import (
    CHAT_MAX_HISTORY,
    LLM_API_KEY,
    LLM_BASE_URL,
    LLM_MODEL,
    MAX_TOKENS,
    TEMPERATURE,
    TIMEFRAME_CONFIG,
)
from utils.db_utils import new_ch_client, ch_query_df_params
from utils.logger import get_logger

import market_queries as mq

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# LLM factory (lazy singleton)
# ---------------------------------------------------------------------------

_llm_instance = None


def _get_llm():
    """Create the LangChain ChatModel (OpenAI-compatible API)."""
    global _llm_instance
    if _llm_instance is not None:
        return _llm_instance

    from langchain_openai import ChatOpenAI

    _llm_instance = ChatOpenAI(
        model=LLM_MODEL,
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL,
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
    )
    logger.info("LLM initialized: model=%s, base_url=%s", LLM_MODEL, LLM_BASE_URL)
    return _llm_instance


# ---------------------------------------------------------------------------
# Tool definitions — LLM calls these via function calling
# ---------------------------------------------------------------------------

@tool
def get_price_candles(symbol: str, timeframe: str) -> str:
    """Fetch OHLCV price candles with technical indicators (RSI-14, MACD).

    Use this to analyze price trends, support/resistance levels, and
    technical patterns.

    Args:
        symbol: Crypto trading pair (e.g. BTCUSDT, ETHUSDT).
        timeframe: One of 'short' (hourly candles, 20-day lookback),
                   'medium' (4-hour candles, 90-day lookback),
                   'long' (daily candles, 2-year lookback),
                   'very_long' (daily candles, 3-year lookback).
    """
    config = TIMEFRAME_CONFIG.get(timeframe, TIMEFRAME_CONFIG["medium"])
    df = mq.fetch_candles(symbol, config)
    if df.empty:
        return f"No candle data available for {symbol}."

    result = mq.format_candles(df, config["candle_ts_format"])

    # Append latest technical indicators summary
    latest = df.iloc[-1]
    rsi = mq._safe_float(latest, "rsi_14", None)
    macd_val = mq._safe_float(latest, "macd", None)
    signal_val = mq._safe_float(latest, "macd_signal", None)
    price = float(latest["close"])

    result += (
        f"\n\n--- Summary ---"
        f"\nCandles: {len(df)} ({config['label']})"
        f"\nLatest close: {price:.4f}"
        f"\nRSI(14): {rsi}"
        f"\nMACD: {macd_val}, Signal: {signal_val}"
    )

    # Append pre-interpreted signals
    signals = mq.compute_signals(df)
    signal_text = mq.format_signals(signals)
    if signal_text:
        result += f"\n\n{signal_text}"

    return result


@tool
def get_volume_and_liquidity(symbol: str, timeframe: str) -> str:
    """Fetch volume trends and liquidity metrics (spread, trade count).

    Use this to analyze trading activity, liquidity conditions, and
    volume patterns over time.

    Args:
        symbol: Crypto trading pair (e.g. BTCUSDT, ETHUSDT).
        timeframe: One of 'short', 'medium', 'long', 'very_long'.
    """
    config = TIMEFRAME_CONFIG.get(timeframe, TIMEFRAME_CONFIG["medium"])
    trend_df = mq.fetch_ticker_trend(symbol, config)
    latest = mq.fetch_latest_ticker(symbol)

    result = mq.format_ticker_trend(trend_df)
    result += (
        f"\n\n--- Latest 24h snapshot ---"
        f"\nPrice change: {latest['price_change_pct']:+.2f}%"
        f"\nVolume 24h: {latest['volume_24h']:,.0f}"
        f"\nBid-ask spread: {latest['spread_pct']:.4f}%"
    )
    return result


@tool
def get_orderbook_pressure(symbol: str, timeframe: str) -> str:
    """Fetch order book buy/sell pressure and imbalance data.

    Use this to assess current market sentiment and accumulation or
    distribution patterns.  Most useful for short-term analysis.

    Args:
        symbol: Crypto trading pair (e.g. BTCUSDT, ETHUSDT).
        timeframe: One of 'short', 'medium', 'long', 'very_long'.
    """
    config = TIMEFRAME_CONFIG.get(timeframe, TIMEFRAME_CONFIG["medium"])
    data = mq.fetch_orderbook_data(symbol, config)
    return mq.format_orderbook(data)


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


# List used by the graph to bind tools to the LLM.
TOOLS = [get_price_candles, get_volume_and_liquidity, get_orderbook_pressure, get_crypto_news]


# ---------------------------------------------------------------------------
# System prompt template
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are a professional crypto market analyst assistant.\n"
    "You have access to tools that query the market database.\n\n"
    "DETECTING THE SYMBOL:\n"
    "Determine which crypto trading pair (e.g. BTCUSDT, ETHUSDT) the user "
    "is asking about from their message. If they mention a coin name or "
    "ticker (BTC, ETH, SOL, ARB...), convert it to the pair format "
    "by appending USDT. If no specific coin is mentioned, use {symbol} "
    "as the default.\n\n"
    "WORKFLOW:\n"
    "1. Analyze the user's question to determine what data you need.\n"
    "2. Call the appropriate tools with the right symbol AND timeframe:\n"
    "   - 'short' for hours to ~3 days (intraday / scalp / swing)\n"
    "   - 'medium' for 1 week to 1 month (default if unclear)\n"
    "   - 'long' for a few months to 1 year\n"
    "   - 'very_long' for 1 to 3+ years\n"
    "3. Use the returned data to provide your analysis.\n\n"
    "AVAILABLE TOOLS:\n"
    "- get_price_candles: OHLCV with RSI/MACD and computed signals\n"
    "- get_volume_and_liquidity: Volume trends, spread, trade count\n"
    "- get_orderbook_pressure: Buy/sell pressure and imbalance\n"
    "- get_crypto_news: Recent news articles for sentiment and catalysts\n\n"
    "ANALYSIS FRAMEWORK:\n"
    "For comprehensive analysis, combine ALL four data sources:\n"
    "1. Price Action: trend direction, support/resistance, RSI/MACD signals\n"
    "2. Volume & Liquidity: volume trends, spread, trade activity\n"
    "3. Order Book: buy/sell pressure, wall levels, OBI\n"
    "4. News Sentiment: recent catalysts, regulatory news, partnerships\n\n"
    "RISK ASSESSMENT:\n"
    "When the user asks about buying, selling, or risk, ALWAYS check news\n"
    "first for catalysts or red flags, then combine with technical data.\n"
    "Risk factors to consider:\n"
    "- Volatility: RSI extremes, large price swings\n"
    "- News risk: negative news, regulatory actions, hacks\n"
    "- Liquidity risk: wide spreads, thin order book\n"
    "- Momentum divergence: price vs volume vs order book disagreement\n\n"
    "RULES:\n"
    "- ALWAYS call at least one tool before answering market questions.\n"
    "- For buy/sell/risk questions, call get_crypto_news AND get_price_candles.\n"
    "- Base analysis ONLY on data returned by your tools.\n"
    "- Be concise but thorough; cite specific numbers and dates.\n"
    "- When uncertain, say so clearly.\n"
    "- Answer in the same language the user uses.\n"
)


# ---------------------------------------------------------------------------
# Node: load_history — build initial messages from DB + user input
# ---------------------------------------------------------------------------

async def load_history(state: dict[str, Any]) -> dict[str, Any]:
    """Load chat history from ClickHouse and build the messages list."""
    session_id = state["session_id"]
    symbol = state["symbol"]
    user_message = state["user_message"]
    limit = CHAT_MAX_HISTORY * 2

    # Load history from DB
    history: list[dict] = []
    try:
        q = (
            "SELECT role, content "
            "FROM chat_history "
            "WHERE session_id = {session_id:String} "
            "ORDER BY timestamp ASC"
        )
        df = ch_query_df_params(q, {"session_id": session_id})
        if not df.empty:
            for _, row in df.iterrows():
                history.append({"role": row["role"], "content": row["content"]})
            history = history[-limit:]
    except Exception as exc:
        logger.warning("Failed to load chat history: %s", exc)

    # Build messages
    messages = [SystemMessage(content=SYSTEM_PROMPT.format(symbol=symbol))]
    for msg in history:
        if msg["role"] == "user":
            messages.append(HumanMessage(content=msg["content"]))
        else:
            messages.append(AIMessage(content=msg["content"]))
    messages.append(HumanMessage(content=user_message))

    return {"messages": messages}


# ---------------------------------------------------------------------------
# Node: agent — call LLM with tools bound
# ---------------------------------------------------------------------------

async def agent_node(state: dict[str, Any]) -> dict[str, Any]:
    """Invoke the LLM with tool-calling enabled.

    For DeepSeek reasoning models: converts AIMessages to include
    reasoning_content in the format the API expects.
    """
    llm = _get_llm()
    llm_with_tools = llm.bind_tools(TOOLS)

    response = await llm_with_tools.ainvoke(state["messages"])
    return {"messages": [response]}


# ---------------------------------------------------------------------------
# Router: should the agent loop continue or finish?
# ---------------------------------------------------------------------------

def should_continue(state: dict[str, Any]) -> str:
    """Route to 'tools' if the LLM made tool calls, else to 'save_history'."""
    last = state["messages"][-1]
    if hasattr(last, "tool_calls") and last.tool_calls:
        return "tools"
    return "save_history"


# ---------------------------------------------------------------------------
# Node: save_history — persist conversation and build context summary
# ---------------------------------------------------------------------------

async def save_history(state: dict[str, Any]) -> dict[str, Any]:
    """Extract the final reply, save to ClickHouse, build context summary."""
    session_id = state["session_id"]
    symbol = state["symbol"]
    user_message = state["user_message"]

    # Extract reply from last AI message
    # DeepSeek reasoning models may put analysis in reasoning_content
    reply = ""
    for msg in reversed(state["messages"]):
        if isinstance(msg, AIMessage):
            if msg.content:
                reply = msg.content.strip()
                break
            # Fallback: use reasoning_content if content is empty
            rc = msg.additional_kwargs.get("reasoning_content", "")
            if rc:
                reply = rc.strip()
                break

    if not reply:
        reply = "Sorry, I could not generate a response. Please try again."

    # Build context summary from tool calls
    tool_calls_made = []
    for msg in state["messages"]:
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            for tc in msg.tool_calls:
                tool_calls_made.append({
                    "name": tc["name"],
                    "args": tc["args"],
                })

    timeframes_used = list({
        tc["args"].get("timeframe", "medium")
        for tc in tool_calls_made
        if "timeframe" in tc.get("args", {})
    })

    context_summary = {
        "symbol": symbol,
        "tools_used": [tc["name"] for tc in tool_calls_made],
        "timeframes": timeframes_used,
        "provider": LLM_BASE_URL or LLM_PROVIDER,
    }

    # Save user message + assistant reply to ClickHouse
    timeframe_str = ",".join(timeframes_used) if timeframes_used else None
    rows = [
        {
            "session_id": session_id,
            "message_id": str(uuid4()),
            "symbol": symbol,
            "role": "user",
            "content": user_message,
            "timeframe": timeframe_str,
            "context_summary": "",
        },
        {
            "session_id": session_id,
            "message_id": str(uuid4()),
            "symbol": symbol,
            "role": "assistant",
            "content": reply,
            "timeframe": timeframe_str,
            "context_summary": json.dumps(context_summary),
        },
    ]

    try:
        df = pd.DataFrame(rows)
        client = new_ch_client()
        try:
            client.insert_df("chat_history", df)
        finally:
            client.close()
        logger.debug("Saved %d messages for session %s", len(rows), session_id)
    except Exception as exc:
        logger.error("Failed to save chat history: %s", exc)

    return {"reply": reply, "context_summary": context_summary}
