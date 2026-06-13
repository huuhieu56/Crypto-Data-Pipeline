"""Deep Reasoning Agent — cross-validation & synthesis.

No tools. Works on data collected by technical + news agents.
Cross-validates, synthesizes, and produces final analysis.
"""

from __future__ import annotations

from typing import Any

from langchain_core.messages import AIMessage, SystemMessage
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

DEEP_SYSTEM_PROMPT = (
    "You are a DEEP REASONING crypto analysis agent.\n"
    "You receive data from specialized agents and synthesize a final analysis.\n\n"
    "YOUR ROLE:\n"
    "- Cross-validate findings from technical and news agents.\n"
    "- Identify conflicts between signals (e.g., bullish RSI but negative news).\n"
    "- Provide a balanced, multi-dimensional assessment.\n"
    "- Give clear buy/sell/hold recommendations with reasoning.\n\n"
    "ANALYSIS FRAMEWORK:\n"
    "1. Technical Summary: key indicators and their signals\n"
    "2. News Summary: recent catalysts and sentiment\n"
    "3. Cross-validation: do technical and news agree?\n"
    "4. Risk Assessment: what could go wrong?\n"
    "5. Conclusion: clear recommendation with confidence level\n\n"
    "RULES:\n"
    "- Base your analysis ONLY on the data provided by other agents.\n"
    "- If data is insufficient, say so clearly.\n"
    "- Be concise but thorough.\n"
    "- Answer in the same language the user uses.\n"
)


# ---------------------------------------------------------------------------
# Agent node
# ---------------------------------------------------------------------------

async def deep_agent_node(state: dict[str, Any]) -> dict[str, Any]:
    """Deep reasoning agent — synthesizes data from other agents."""
    from nodes import _get_llm

    try:
        llm = _get_llm()

        messages = [SystemMessage(content=DEEP_SYSTEM_PROMPT)]
        for msg in state["messages"]:
            if not isinstance(msg, SystemMessage):
                messages.append(msg)

        response = await llm.ainvoke(messages)
        return {"messages": [response]}
    except Exception as exc:
        logger.error("Deep agent failed: %s", exc)
        return {"messages": [AIMessage(content=f"Deep analysis unavailable: {exc}")]}
