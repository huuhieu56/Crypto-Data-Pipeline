"""Technical Analysis Agent — RSI, MACD, Volume, Orderbook.

Specializes in price action, indicators, and market microstructure.
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

    latest = df.iloc[-1]
    rsi = mq._safe_float(latest, "rsi_14", None)
    macd_val = mq._safe_float(latest, "macd", None)
    price = float(latest["close"])

    result += (
        f"\n\n--- Summary ---"
        f"\nCandles: {len(df)} ({config['label']})"
        f"\nLatest close: {price:.4f}"
        f"\nRSI(14): {rsi}"
        f"\nMACD: {macd_val}"
    )

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


TECHNICAL_TOOLS = [get_price_candles, get_volume_and_liquidity, get_orderbook_pressure]


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

TECHNICAL_SYSTEM_PROMPT = (
    "You are a specialized TECHNICAL ANALYSIS crypto agent.\n"
    "You focus ONLY on price action, indicators, volume, and order book data.\n\n"
    "YOUR EXPERTISE:\n"
    "- RSI(14): overbought/oversold, divergences, trend direction\n"
    "- MACD: crossovers, momentum, histogram analysis\n"
    "- Volume: trends, spikes, divergences from price\n"
    "- Order Book: OBI, bid/ask pressure, wall levels\n"
    "- Price Structure: support/resistance, higher highs/lower lows\n\n"
    "RULES:\n"
    "- Call tools to get fresh data before answering.\n"
    "- Choose the right timeframe based on the user's question.\n"
    "- Be precise with numbers: cite RSI values, MACD levels, price levels.\n"
    "- Do NOT speculate on news or sentiment — that's for the news agent.\n"
    "- Answer in the same language the user uses.\n"
)


# ---------------------------------------------------------------------------
# Agent node
# ---------------------------------------------------------------------------

async def technical_agent_node(state: dict[str, Any]) -> dict[str, Any]:
    """Technical analysis agent — calls price/volume/orderbook tools."""
    from nodes import _get_llm_with_tools

    try:
        llm_with_tools = _get_llm_with_tools(TECHNICAL_TOOLS)

        # Replace system prompt (don't prepend duplicate)
        messages = [SystemMessage(content=TECHNICAL_SYSTEM_PROMPT)]
        # Skip the original system message from load_history
        for msg in state["messages"]:
            if not isinstance(msg, SystemMessage):
                messages.append(msg)

        response = await llm_with_tools.ainvoke(messages)
        return {"messages": [response]}
    except Exception as exc:
        logger.error("Technical agent failed: %s", exc)
        from langchain_core.messages import AIMessage
        return {"messages": [AIMessage(content=f"Technical analysis unavailable: {exc}")]}
