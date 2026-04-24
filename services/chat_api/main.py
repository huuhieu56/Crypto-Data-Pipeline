"""Chat API service — FastAPI backend for the Grafana LLM chatbox."""

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

import aiohttp
import pandas as pd
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from config.llm_config import (
    CHAT_MAX_HISTORY,
    LLM_DAILY_CANDLES,
    LLM_PROVIDER,
)
from utils.db_utils import ch_query_df
from utils.exceptions import LLMQuotaExceededError
from utils.llm_utils import get_chat_response
from utils.logger import get_logger

logger = get_logger(__name__)

app = FastAPI(title="Crypto Chat API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ChatMessage(BaseModel):
    role: str = Field(..., pattern=r"^(user|assistant)$")
    content: str


class ChatRequest(BaseModel):
    symbol: str
    message: str
    history: list[ChatMessage] = Field(default_factory=list)


class ChatResponse(BaseModel):
    reply: str
    context_summary: dict


# ---------------------------------------------------------------------------
# Market data helpers (reuse ClickHouse queries from former llm_signal.py)
# ---------------------------------------------------------------------------

def _esc(value: str) -> str:
    return value.replace("'", "''")


def _fetch_daily_klines(symbol: str, limit_days: int) -> pd.DataFrame:
    q = (
        "SELECT "
        "  toDate(timestamp) AS day, "
        "  argMin(open, timestamp) AS open, "
        "  max(high) AS high, "
        "  min(low) AS low, "
        "  argMax(close, timestamp) AS close, "
        "  sum(volume) AS volume, "
        "  argMax(rsi_14, timestamp) AS rsi_14, "
        "  argMax(macd, timestamp) AS macd, "
        "  argMax(macd_signal, timestamp) AS macd_signal "
        "FROM klines "
        f"WHERE symbol = '{_esc(symbol)}' "
        "GROUP BY day "
        "ORDER BY day DESC "
        f"LIMIT {int(limit_days)}"
    )
    df = ch_query_df(q)
    if df.empty:
        return df
    df = df.iloc[::-1].reset_index(drop=True)
    df = df.rename(columns={"day": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def _fetch_ticker(symbol: str) -> tuple[float, float]:
    q = (
        "SELECT price_change_pct, volume_24h "
        "FROM ticker_24h "
        f"WHERE symbol = '{_esc(symbol)}' "
        "ORDER BY snapshot_time DESC LIMIT 2"
    )
    df = ch_query_df(q)
    if df.empty:
        return 0.0, 0.0

    latest_price_change = float(df["price_change_pct"].iloc[0]) if pd.notna(df["price_change_pct"].iloc[0]) else 0.0
    if len(df) < 2:
        return latest_price_change, 0.0

    curr_vol = float(df["volume_24h"].iloc[0]) if pd.notna(df["volume_24h"].iloc[0]) else 0.0
    prev_vol = float(df["volume_24h"].iloc[1]) if pd.notna(df["volume_24h"].iloc[1]) else 0.0
    vol_change = ((curr_vol - prev_vol) / prev_vol * 100.0) if prev_vol > 0 else 0.0
    return latest_price_change, vol_change


def _fetch_orderbook_imbalance(symbol: str) -> float:
    q = (
        "SELECT imbalance "
        "FROM order_book_snapshot "
        f"WHERE symbol = '{_esc(symbol)}' "
        "ORDER BY timestamp DESC LIMIT 1"
    )
    df = ch_query_df(q)
    if df.empty or pd.isna(df["imbalance"].iloc[0]):
        return 0.5
    return float(df["imbalance"].iloc[0])


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _format_candles(df: pd.DataFrame) -> str:
    lines: list[str] = []
    for _, row in df.iterrows():
        ts = pd.Timestamp(row["timestamp"]).strftime("%Y-%m-%d")
        rsi = float(row["rsi_14"]) if pd.notna(row["rsi_14"]) else 50.0
        line = (
            f"{ts} O:{float(row['open']):.4f} H:{float(row['high']):.4f} "
            f"L:{float(row['low']):.4f} C:{float(row['close']):.4f} V:{float(row['volume']):,.0f} "
            f"RSI:{rsi:.1f}"
        )
        lines.append(line)
    return "\n".join(lines)


def _build_system_prompt(
    symbol: str,
    daily: pd.DataFrame,
    pchg24h: float,
    vchg24h: float,
    ob_imbalance: float,
) -> tuple[str, dict]:
    """Build system prompt with market context and return context summary."""
    if daily.empty:
        latest_price = 0.0
        latest_rsi = 50.0
        macd_cross = "neutral"
    else:
        latest = daily.iloc[-1]
        latest_price = float(latest["close"])
        latest_rsi = float(latest["rsi_14"]) if pd.notna(latest["rsi_14"]) else 50.0
        latest_macd = float(latest["macd"]) if pd.notna(latest["macd"]) else 0.0
        latest_macd_signal = float(latest["macd_signal"]) if pd.notna(latest["macd_signal"]) else 0.0
        if latest_macd > latest_macd_signal:
            macd_cross = "bullish"
        elif latest_macd < latest_macd_signal:
            macd_cross = "bearish"
        else:
            macd_cross = "neutral"

    ob_tag = "strong buy" if ob_imbalance > 0.6 else "strong sell" if ob_imbalance < 0.4 else "balanced"

    prompt = (
        f"You are a professional crypto market analyst assistant. "
        f"You are helping a trader analyze {symbol}.\n\n"
        f"Below is the market context for {symbol} based on the last {LLM_DAILY_CANDLES} daily candles "
        f"and a real-time snapshot. Use this data to answer the user's questions.\n\n"
        "DAILY CANDLES (oldest → newest):\n"
        f"{_format_candles(daily)}\n\n"
        "CURRENT SNAPSHOT:\n"
        f"- Current price: {latest_price:.4f}\n"
        f"- RSI(14): {latest_rsi:.1f}\n"
        f"- MACD crossover: {macd_cross}\n"
        f"- Order book imbalance: {ob_imbalance:.3f} ({ob_tag})\n"
        f"- 24h price change: {pchg24h:+.2f}%\n"
        f"- 24h volume change: {vchg24h:+.2f}%\n\n"
        "RULES:\n"
        "1. Base your analysis ONLY on the provided data.\n"
        "2. Be concise but thorough. Use specific numbers from the data.\n"
        "3. When uncertain, say so clearly.\n"
        "4. Answer in the same language the user uses.\n"
    )

    context_summary = {
        "symbol": symbol,
        "candles_count": len(daily),
        "latest_price": latest_price,
        "rsi_14": latest_rsi,
        "macd_cross": macd_cross,
        "ob_imbalance": ob_imbalance,
        "price_change_24h": pchg24h,
        "volume_change_24h": vchg24h,
        "provider": LLM_PROVIDER,
    }
    return prompt, context_summary


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

CHAT_UI_PATH = Path(__file__).parent / "chat_ui.html"


@app.get("/chat-ui", response_class=HTMLResponse)
async def chat_ui(request: Request, symbol: str = "BTCUSDT"):
    """Serve the chat interface HTML page."""
    html = CHAT_UI_PATH.read_text(encoding="utf-8")
    # Inject the symbol into the page via a JS variable
    html = html.replace("__SYMBOL__", symbol)
    return HTMLResponse(content=html)


@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    """Process a chat message with market context."""
    symbol = req.symbol.strip().upper()
    user_message = req.message.strip()
    if not symbol or not user_message:
        return JSONResponse(
            status_code=400,
            content={"detail": "symbol and message are required"},
        )

    # Fetch market data
    try:
        daily = _fetch_daily_klines(symbol, LLM_DAILY_CANDLES)
        pchg24h, vchg24h = _fetch_ticker(symbol)
        ob_imbalance = _fetch_orderbook_imbalance(symbol)
    except Exception as exc:
        logger.error("Failed to fetch market data for %s: %s", symbol, exc)
        return JSONResponse(
            status_code=502,
            content={"detail": f"Failed to fetch market data: {exc}"},
        )

    # Build system prompt with market context
    system_prompt, context_summary = _build_system_prompt(
        symbol, daily, pchg24h, vchg24h, ob_imbalance,
    )

    # Build conversation messages (trim to max history)
    messages: list[dict[str, str]] = []
    for msg in req.history[-CHAT_MAX_HISTORY:]:
        messages.append({"role": msg.role, "content": msg.content})
    messages.append({"role": "user", "content": user_message})

    # Call LLM
    try:
        async with aiohttp.ClientSession() as session:
            reply = await get_chat_response(session, system_prompt, messages)
    except LLMQuotaExceededError:
        logger.error("LLM quota exceeded for %s", symbol)
        return JSONResponse(
            status_code=429,
            content={"detail": "LLM API quota exceeded. Please try again later."},
        )
    except Exception as exc:
        logger.error("LLM call failed: %s", exc)
        return JSONResponse(
            status_code=502,
            content={"detail": f"LLM call failed: {exc}"},
        )

    if reply is None:
        return JSONResponse(
            status_code=502,
            content={"detail": "LLM returned no response after retries."},
        )

    return ChatResponse(reply=reply, context_summary=context_summary)


@app.get("/health")
async def health():
    return {"status": "ok"}
