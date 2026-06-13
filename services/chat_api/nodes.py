"""Shared infrastructure for multi-agent system.

Provides:
- LLM factory (shared by all agents, with bind_tools caching)
- load_history node (ClickHouse)
- save_history node (ClickHouse + agent_trace)
"""

from __future__ import annotations

import json
import time
from typing import Any
from uuid import uuid4

import pandas as pd
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from config.llm_config import (
    CHAT_MAX_HISTORY,
    LLM_API_KEY,
    LLM_BASE_URL,
    LLM_MODEL,
    MAX_TOKENS,
    TEMPERATURE,
)
from utils.db_utils import new_ch_client, ch_query_df_params
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# LLM factory (lazy singleton + bind_tools cache)
# ---------------------------------------------------------------------------

_llm_instance = None
_llm_bound_cache: dict[str, Any] = {}


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


def _get_llm_with_tools(tools: list):
    """Get LLM with tools bound (cached per tool set)."""
    cache_key = ",".join(sorted(t.name for t in tools))
    if cache_key not in _llm_bound_cache:
        llm = _get_llm()
        _llm_bound_cache[cache_key] = llm.bind_tools(tools)
    return _llm_bound_cache[cache_key]


# ---------------------------------------------------------------------------
# Tools (imported from agent modules for graph binding)
# ---------------------------------------------------------------------------

from technical_agent import TECHNICAL_TOOLS
from news_agent import NEWS_TOOLS

ALL_TOOLS = TECHNICAL_TOOLS + NEWS_TOOLS


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are a professional crypto market analyst assistant.\n"
    "You have access to specialized agents for technical analysis and news.\n"
    "Answer in the same language the user uses.\n"
    "Be concise but thorough; cite specific numbers when available.\n"
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

    history: list[dict] = []
    try:
        q = (
            "SELECT role, content "
            "FROM chat_history "
            "WHERE session_id = {session_id:String} "
            "ORDER BY timestamp DESC "
            "LIMIT {limit:UInt32}"
        )
        df = ch_query_df_params(q, {"session_id": session_id, "limit": limit})
        if not df.empty:
            for _, row in df.iloc[::-1].iterrows():
                history.append({"role": row["role"], "content": row["content"]})
    except Exception as exc:
        logger.warning("Failed to load chat history: %s", exc)

    messages = [SystemMessage(content=SYSTEM_PROMPT.format(symbol=symbol))]
    for msg in history:
        if msg["role"] == "user":
            messages.append(HumanMessage(content=msg["content"]))
        else:
            messages.append(AIMessage(content=msg["content"]))
    messages.append(HumanMessage(content=user_message))

    return {"messages": messages}


# ---------------------------------------------------------------------------
# Node: save_history — persist conversation + agent_trace
# ---------------------------------------------------------------------------

async def save_history(state: dict[str, Any]) -> dict[str, Any]:
    """Extract the final reply, save to ClickHouse, save agent_trace."""
    session_id = state["session_id"]
    symbol = state["symbol"]
    user_message = state["user_message"]
    start_time = state.get("_start_time", time.time())

    # Extract reply
    reply = state.get("direct_reply", "")
    if not reply:
        for msg in reversed(state["messages"]):
            if isinstance(msg, AIMessage):
                if msg.content:
                    reply = msg.content.strip()
                    break
                rc = msg.additional_kwargs.get("reasoning_content", "")
                if rc:
                    reply = rc.strip()
                    break

    if not reply:
        reply = "Sorry, I could not generate a response. Please try again."

    # Collect tools used from messages
    tools_used = []
    agents_called = []
    for msg in state["messages"]:
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            for tc in msg.tool_calls:
                tools_used.append(tc["name"])
        if hasattr(msg, "name") and msg.name:
            agents_called.append(msg.name)

    context_summary = {
        "symbol": symbol,
        "supervisor_decision": state.get("supervisor_decision", []),
        "supervisor_reason": state.get("supervisor_reason", ""),
        "tools_used": list(set(tools_used)),
        "agents_called": list(set(agents_called)),
        "provider": LLM_BASE_URL,
    }

    # Save to ClickHouse
    rows = [
        {
            "session_id": session_id,
            "message_id": str(uuid4()),
            "symbol": symbol,
            "role": "user",
            "content": user_message,
            "timeframe": None,
            "context_summary": "",
        },
        {
            "session_id": session_id,
            "message_id": str(uuid4()),
            "symbol": symbol,
            "role": "assistant",
            "content": reply,
            "timeframe": None,
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
    except Exception as exc:
        logger.error("Failed to save chat history: %s", exc)

    # Save agent_trace
    duration_ms = int((time.time() - start_time) * 1000)
    _save_agent_trace(
        session_id=session_id,
        message_id=str(uuid4()),
        supervisor_decision=state.get("supervisor_decision", []),
        agents_called=list(set(agents_called)),
        tools_used=list(set(tools_used)),
        reasoning=state.get("supervisor_reason", ""),
        duration_ms=duration_ms,
    )

    return {"reply": reply, "context_summary": context_summary}


def _save_agent_trace(
    session_id: str,
    message_id: str,
    supervisor_decision: list,
    agents_called: list,
    tools_used: list,
    reasoning: str,
    duration_ms: int,
) -> None:
    """Save agent routing trace to ClickHouse."""
    from datetime import datetime, timezone

    row = {
        "session_id": session_id,
        "message_id": message_id,
        "timestamp": datetime.now(timezone.utc),
        "supervisor_decision": ",".join(supervisor_decision),
        "agents_called": agents_called,
        "tools_used": tools_used,
        "reasoning": reasoning,
        "duration_ms": duration_ms,
    }
    try:
        df = pd.DataFrame([row])
        client = new_ch_client()
        try:
            client.insert_df("agent_trace", df)
        finally:
            client.close()
    except Exception as exc:
        logger.error("Failed to save agent trace: %s", exc)
