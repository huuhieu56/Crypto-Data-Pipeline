"""LangGraph StateGraph — Multi-Agent Architecture.

Supervisor → Technical Agent / News Agent → Deep Agent → Response

Each agent has its own ToolNode with only its specialized tools.
"""

from __future__ import annotations

from typing import Annotated, Any, TypedDict

from langchain_core.messages import AIMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from nodes import load_history, save_history
from technical_agent import technical_agent_node, TECHNICAL_TOOLS
from news_agent import news_agent_node, NEWS_TOOLS
from deep_agent import deep_agent_node
from supervisor import (
    supervisor_node,
    route_after_supervisor,
    route_after_technical,
    route_after_news,
    route_after_tools,
)


# ---------------------------------------------------------------------------
# State schema
# ---------------------------------------------------------------------------

class ChatState(TypedDict):
    # Input
    session_id: str
    symbol: str
    user_message: str

    # Messages (managed by add_messages reducer)
    messages: Annotated[list, add_messages]

    # Supervisor routing
    supervisor_decision: list
    supervisor_reason: str
    direct_reply: str
    _current_agent: str
    _start_time: float

    # Output
    reply: str
    context_summary: dict


# ---------------------------------------------------------------------------
# Per-agent ToolNodes
# ---------------------------------------------------------------------------

technical_tools_node = ToolNode(TECHNICAL_TOOLS)
news_tools_node = ToolNode(NEWS_TOOLS)


# ---------------------------------------------------------------------------
# Agent wrappers (track current agent + error handling)
# ---------------------------------------------------------------------------

async def technical_wrapper(state: dict[str, Any]) -> dict[str, Any]:
    result = await technical_agent_node(state)
    return {**result, "_current_agent": "technical"}


async def news_wrapper(state: dict[str, Any]) -> dict[str, Any]:
    result = await news_agent_node(state)
    return {**result, "_current_agent": "news"}


async def deep_wrapper(state: dict[str, Any]) -> dict[str, Any]:
    result = await deep_agent_node(state)
    return {**result, "_current_agent": "deep"}


async def technical_tools_wrapper(state: dict[str, Any]) -> dict[str, Any]:
    result = await technical_tools_node.ainvoke(state)
    return {**result, "_current_agent": "technical"}


async def news_tools_wrapper(state: dict[str, Any]) -> dict[str, Any]:
    result = await news_tools_node.ainvoke(state)
    return {**result, "_current_agent": "news"}


async def add_start_time(state: dict[str, Any]) -> dict[str, Any]:
    import time
    return {"_start_time": time.time()}


# ---------------------------------------------------------------------------
# Graph definition
# ---------------------------------------------------------------------------

workflow = StateGraph(ChatState)

# Nodes
workflow.add_node("init", add_start_time)
workflow.add_node("load_memory", load_history)
workflow.add_node("supervisor", supervisor_node)
workflow.add_node("technical", technical_wrapper)
workflow.add_node("technical_tools", technical_tools_wrapper)
workflow.add_node("news", news_wrapper)
workflow.add_node("news_tools", news_tools_wrapper)
workflow.add_node("deep", deep_wrapper)
workflow.add_node("save_memory", save_history)

# Edges
workflow.add_edge(START, "init")
workflow.add_edge("init", "load_memory")
workflow.add_edge("load_memory", "supervisor")

# Supervisor → agents
workflow.add_conditional_edges(
    "supervisor",
    route_after_supervisor,
    {
        "technical": "technical",
        "news": "news",
        "deep": "deep",
        "direct_response": "save_memory",
    },
)

# Technical agent → tools / news / deep / save
workflow.add_conditional_edges(
    "technical",
    route_after_technical,
    {
        "tools": "technical_tools",
        "news": "news",
        "deep": "deep",
        "save_memory": "save_memory",
    },
)

# News agent → tools / deep / save
workflow.add_conditional_edges(
    "news",
    route_after_news,
    {
        "tools": "news_tools",
        "deep": "deep",
        "save_memory": "save_memory",
    },
)

# Tools → back to the agent that called them
workflow.add_edge("technical_tools", "technical")
workflow.add_edge("news_tools", "news")

# Deep agent → save
workflow.add_edge("deep", "save_memory")

# Save → end
workflow.add_edge("save_memory", END)

# Compile
compiled_graph = workflow.compile()
