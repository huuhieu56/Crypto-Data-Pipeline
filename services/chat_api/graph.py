"""LangGraph StateGraph for the crypto chat assistant.

Agent pattern with tool-calling loop:
  load_history → agent ↔ tools (loop) → save_history
"""

from __future__ import annotations

from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from nodes import (
    TOOLS,
    agent_node,
    load_history,
    save_history,
    should_continue,
)


# ---------------------------------------------------------------------------
# State schema
# ---------------------------------------------------------------------------

class ChatState(TypedDict):
    # Input (set by the caller)
    session_id: str
    symbol: str
    user_message: str

    # Messages (managed by add_messages reducer — auto-appends)
    messages: Annotated[list, add_messages]

    # Output (set by save_history node)
    reply: str
    context_summary: dict


# ---------------------------------------------------------------------------
# Graph definition
# ---------------------------------------------------------------------------

workflow = StateGraph(ChatState)

# Nodes
workflow.add_node("load_history", load_history)
workflow.add_node("agent", agent_node)
workflow.add_node("tools", ToolNode(TOOLS))
workflow.add_node("save_history", save_history)

# Edges
workflow.add_edge(START, "load_history")
workflow.add_edge("load_history", "agent")
workflow.add_conditional_edges(
    "agent",
    should_continue,
    {"tools": "tools", "save_history": "save_history"},
)
workflow.add_edge("tools", "agent")       # loop back after tool execution
workflow.add_edge("save_history", END)

# Compile
compiled_graph = workflow.compile()
