"""Chat API service — FastAPI backend for the Grafana LLM chatbox.

Powered by LangGraph for stateful, timeframe-aware market analysis.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Inside Docker the project is mounted at /opt/project.
# For local dev, fall back to resolving from this file's location.
_DOCKER_PROJECT = Path("/opt/project")
PROJECT_ROOT = str(_DOCKER_PROJECT if _DOCKER_PROJECT.is_dir() else Path(__file__).resolve().parent.parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# Also ensure the service directory itself is on the path so uvicorn can find main.
os.chdir(Path(__file__).resolve().parent)

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from config.llm_config import LLM_BASE_URL, LLM_PROVIDER
from utils.db_utils import ch_query_df_params, new_ch_client
from utils.logger import get_logger

from graph import compiled_graph

logger = get_logger(__name__)

app = FastAPI(title="Crypto Chat API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    session_id: str
    symbol: str
    message: str


class ChatResponse(BaseModel):
    reply: str
    context_summary: dict




# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

CHAT_UI_PATH = Path(__file__).parent / "chat_ui.html"


@app.get("/chat-ui", response_class=HTMLResponse)
async def chat_ui(request: Request, symbol: str = "BTCUSDT"):
    """Serve the chat interface HTML page."""
    html = CHAT_UI_PATH.read_text(encoding="utf-8")
    html = html.replace("__SYMBOL__", symbol)
    return HTMLResponse(content=html)


@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    """Process a chat message through the LangGraph pipeline."""
    symbol = req.symbol.strip().upper()
    user_message = req.message.strip()
    session_id = req.session_id.strip()

    if not symbol or not user_message or not session_id:
        return JSONResponse(
            status_code=400,
            content={"detail": "session_id, symbol, and message are required"},
        )

    # Run the LangGraph pipeline
    try:
        result = await compiled_graph.ainvoke({
            "session_id": session_id,
            "symbol": symbol,
            "user_message": user_message,
        })
    except Exception as exc:
        logger.error("Graph execution failed: %s", exc)
        return JSONResponse(
            status_code=502,
            content={"detail": f"Chat processing failed: {exc}"},
        )

    return ChatResponse(
        reply=result.get("reply", "No response generated."),
        context_summary=result.get("context_summary", {}),
    )


@app.get("/api/chat/history/{session_id}")
async def get_history(session_id: str):
    """Load chat history for a session."""
    try:
        q = (
            "SELECT role, content "
            "FROM chat_history "
            "WHERE session_id = {session_id:String} "
            "ORDER BY timestamp ASC"
        )
        df = ch_query_df_params(q, {"session_id": session_id})

        if df.empty:
            return {"messages": []}

        messages = []
        for _, row in df.iterrows():
            messages.append({"role": row["role"], "content": row["content"]})

        return {"messages": messages}

    except Exception as exc:
        logger.error("Failed to load history for %s: %s", session_id, exc)
        return JSONResponse(
            status_code=500,
            content={"detail": f"Failed to load history: {exc}"},
        )


@app.delete("/api/chat/history/{session_id}")
async def delete_history(session_id: str):
    """Delete chat history for a session."""
    try:
        client = new_ch_client()
        try:
            client.command(
                "ALTER TABLE chat_history DELETE WHERE session_id = {session_id:String}",
                parameters={"session_id": session_id},
            )
        finally:
            client.close()
        return {"status": "deleted", "session_id": session_id}

    except Exception as exc:
        logger.error("Failed to delete history for %s: %s", session_id, exc)
        return JSONResponse(
            status_code=500,
            content={"detail": f"Failed to delete history: {exc}"},
        )


@app.get("/health")
async def health():
    return {"status": "ok", "provider": LLM_BASE_URL or LLM_PROVIDER}
