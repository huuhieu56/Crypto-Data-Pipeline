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
    LLM_PROVIDER,
    MAX_RETRIES,
    MAX_TOKENS,
    RETRY_DELAY,
    TEMPERATURE,
    TIMEFRAME_CONFIG,
)
from utils.db_utils import new_ch_client
from utils.logger import get_logger

import market_queries as mq

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# LLM factory (lazy singleton)
# ---------------------------------------------------------------------------

_llm_instance = None


def _get_llm():
    """Create the LangChain ChatModel based on configured provider.

    Priority: LLM_BASE_URL (any OpenAI-compatible) > LLM_PROVIDER.
    """
    global _llm_instance
    if _llm_instance is not None:
        return _llm_instance

    if LLM_BASE_URL and "deepseek" in LLM_BASE_URL.lower():
        # DeepSeek reasoning models — patched to preserve reasoning_content
        # in multi-turn tool-calling loops (upstream strips it)
        from langchain_deepseek import ChatDeepSeek as _BaseChatDeepSeek

        class _PatchedDeepSeek(_BaseChatDeepSeek):
            def _get_request_payload(self, input_, *, stop=None, **kwargs):
                payload = super()._get_request_payload(input_, stop=stop, **kwargs)
                # Re-inject reasoning_content stripped by ChatOpenAI serializer
                input_msgs = self._convert_input(input_).to_messages()
                for lc_msg, api_msg in zip(input_msgs, payload.get("messages", [])):
                    if api_msg.get("role") == "assistant":
                        rc = getattr(lc_msg, "additional_kwargs", {}).get("reasoning_content")
                        if rc:
                            api_msg["reasoning_content"] = rc
                return payload

        _llm_instance = _PatchedDeepSeek(
            model=LLM_MODEL,
            api_key=LLM_API_KEY,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
        )
    elif LLM_BASE_URL:
        # Any other OpenAI-compatible provider (Groq, Mistral, etc.)
        from langchain_openai import ChatOpenAI

        _llm_instance = ChatOpenAI(
            model=LLM_MODEL,
            api_key=LLM_API_KEY,
            base_url=LLM_BASE_URL,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
        )
    elif LLM_PROVIDER == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI

        _llm_instance = ChatGoogleGenerativeAI(
            model=LLM_MODEL,
            google_api_key=LLM_API_KEY,
            temperature=TEMPERATURE,
            max_output_tokens=MAX_TOKENS,
        )
    else:
        # Default: OpenAI native (no base_url needed)
        from langchain_openai import ChatOpenAI

        _llm_instance = ChatOpenAI(
            model=LLM_MODEL,
            api_key=LLM_API_KEY,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
        )

    provider = LLM_BASE_URL or LLM_PROVIDER
    logger.info("LLM initialized: provider=%s, model=%s", provider, LLM_MODEL)
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


# List used by the graph to bind tools to the LLM.
TOOLS = [get_price_candles, get_volume_and_liquidity, get_orderbook_pressure]


# ---------------------------------------------------------------------------
# System prompt template
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are a professional crypto market analyst assistant.\n"
    "You have access to tools that query the market database.\n\n"
    "WORKFLOW:\n"
    "1. Analyze the user's question to determine what data you need.\n"
    "2. Call the appropriate tools with the right timeframe:\n"
    "   - 'short' for hours to ~3 days (intraday / scalp / swing)\n"
    "   - 'medium' for 1 week to 1 month (default if unclear)\n"
    "   - 'long' for a few months to 1 year\n"
    "   - 'very_long' for 1 to 3+ years\n"
    "3. Use the returned data to provide your analysis.\n\n"
    "The user is currently viewing {symbol}. "
    "Use this symbol unless they ask about a different coin.\n\n"
    "RULES:\n"
    "- ALWAYS call at least one tool before answering market questions.\n"
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
            f"WHERE session_id = '{mq._esc(session_id)}' "
            "ORDER BY timestamp ASC"
        )
        client = new_ch_client()
        try:
            df = client.query_df(q)
        finally:
            client.close()
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
